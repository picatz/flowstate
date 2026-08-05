package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/cel-go/cel"
	celpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The corpus in `examples/` is run by two harnesses now, one per driver, and this
// file is the part they must not each own a copy of.
//
// `examples_run_test.go` in package flowstatev1_test runs every example through the
// local driver; `engine/examples_durable_test.go` runs the same files through the
// durable one. Invariant 3 is about the two drivers agreeing, and a harness that
// classified examples differently from its counterpart — a different idea of which
// example reaches the network, a stand-in answering a different document — would
// manufacture disagreements it then reported as the drivers'. So the classification
// and the stand-in live here, where both drivers' packages already import them.
//
// The local harness carried its own copies for a while, deliberately the same
// shapes so that adopting them would be a deletion rather than a rewrite. It has
// adopted them, and that is what it was: both harnesses now read one answer to
// which example reaches the network, which one waits to be told something, and
// which one finishes on its own.

// ExampleInputsFile is the name of the file an example's arguments are written in,
// beside its `workflow.yaml`.
//
// Named rather than spelled at each use because it is the whole of the convention:
// every diagnostic about a missing argument has to say this word, or an author is
// told what is wrong without being told where to write the answer.
const ExampleInputsFile = "inputs.json"

// ExampleInputs reads the arguments an example is run with, from an inputs.json
// beside its workflow.yaml.
//
// Nil where there is no such file, which is every example but one: a workflow that
// declares no `inputs:`, or declares them all with defaults, runs with nothing — and
// an example must run as written, because "paste this and watch it work" is what an
// example is for.
//
// The one exception is the point of the convention. `parameterized-deploy` exists to
// show a *required* input, and a required input with a default is not one — so the
// example would either demonstrate nothing or not run here. The file beside it is
// what `flow run local --input-file` takes, so what CI runs is a command a reader can
// run, and the example's own comments name it.
//
// Read here rather than through the CLI's parser because that lives in package main.
// What is shared is the decoding rule the parser exists to get right — JSON numbers
// are read as written, so an int declaration is given an int — and the binding
// itself, which is [v1.BindRunInputs] on both sides.
//
// Shared by both harnesses for the reason the rest of this file is. The durable
// harness held the same answers in a Go map — `{"parameterized-deploy": {"service":
// "checkout"}}` — which is a second answer to "what does this example need", and the
// two would drift the first time an example's declarations changed: the file is what
// a reader runs, so a harness disagreeing with it tests something nobody can
// reproduce.
//
// The error is returned rather than fatal — this used to be tb.Fatalf in three
// places, on an unreadable file, a document that is not a JSON object, and a
// number with a fractional part — for the identical reason [BindExampleInputs]
// returns rather than requires its own: a caller in a per-example loop (#183's
// [TestEveryExampleRunsDurably]) means to report one bad fixture and move on to
// the next example, and Fatalf is FailNow, which a fatal call three frames below
// a checked `if err != nil; continue` still is. A malformed inputs.json used to
// take every alphabetically later example down with it exactly the way an
// unregistered wait_for_signal example did — the same defect, one layer deeper,
// because the guard at the call site cannot fire for an error that never reaches
// it.
func ExampleInputs(workflowPath string) (map[string]*v1.Value, error) {
	data, err := os.ReadFile(filepath.Join(filepath.Dir(workflowPath), ExampleInputsFile))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading %s beside %s: %w", ExampleInputsFile, workflowPath, err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var fields map[string]any
	if err := decoder.Decode(&fields); err != nil {
		return nil, fmt.Errorf("%s has an %s that is not a JSON object: %w",
			filepath.Dir(workflowPath), ExampleInputsFile, err)
	}

	inputs := make(map[string]*v1.Value, len(fields))
	for name, value := range fields {
		if number, ok := value.(json.Number); ok {
			// Read as written, the same rule ExampleInputs's own doc comment
			// states: a whole number is an int declaration's argument, and one
			// with a fraction is a float declaration's — `482.50` for an
			// `amount:` input has to survive this decode as a float rather than
			// being refused, the same way a caller's own `--input amount=482.50`
			// does through cmd/flow's parser. This harness does not see the
			// declaration to disambiguate a *whole* float from an int the way
			// that parser does, so a whole number stays int64 here; only a
			// fractional one is read as a float.
			if whole, err := number.Int64(); err == nil {
				inputs[name] = v1.NewLiteral(whole)

				continue
			}
			f, err := number.Float64()
			if err != nil {
				return nil, fmt.Errorf("input %q is not a number this harness can read: %w", name, err)
			}
			inputs[name] = v1.NewLiteral(f)

			continue
		}
		inputs[name] = v1.NewValue(value)
	}

	return inputs, nil
}

// BindExampleInputs answers an example's declared `inputs:` from its inputs.json and
// checks them the way a submitted run is checked.
//
// The error is returned rather than fatal, and it names the convention, because a
// harness that cannot start an example must say what a person would have to write to
// fix it. The failure it exists for is silent: an example declaring a required input
// with nowhere to read one from would otherwise be skipped, or "satisfied" by a value
// only a harness knows — which is an example nobody can actually run, passing CI.
//
// Bound here rather than inside each run, so both drivers are handed one already
// checked and defaulted map — which is also how a deployment does it, since the
// server binds at submit and carries the result across every Continue-As-New.
func BindExampleInputs(tb testing.TB, wf *v1.Workflow, workflowPath string) (map[string]*v1.Value, error) {
	tb.Helper()

	supplied, err := ExampleInputs(workflowPath)
	if err != nil {
		return nil, err
	}

	bound, err := v1.BindRunInputs(wf, supplied)
	if err == nil {
		return bound, nil
	}

	path := filepath.Join(filepath.Dir(workflowPath), ExampleInputsFile)
	if supplied == nil {
		return nil, fmt.Errorf("%w; write %s — the file `flow run local --input-file` takes — "+
			"rather than teaching a harness a value only it knows", err, path)
	}

	return nil, fmt.Errorf("%w; %s does not answer it", err, path)
}

// NewExamplesHTTPServer starts a stand-in for httpbin.org, and returns its base URL
// along with a function reporting every path an example asked for that it does not
// serve.
//
// The bodies are the shapes httpbin answers with, trimmed to the fields the examples
// reach and keeping the structure around them. The examples name `httpbin.org` on
// purpose — somebody reading one wants to paste it and watch it work — so the runs
// are pointed at this instead of the examples being rewritten. What that proves is
// everything the example controls: its inputs, its expressions, and what its steps do
// with a response of that shape. What it cannot prove is that httpbin is still up or
// still answers this way.
//
// The unserved report is the other half. An example added against a path this does not
// serve would otherwise pass against a 404 its expressions never look at.
func NewExamplesHTTPServer(tb testing.TB) (string, func() []string) {
	tb.Helper()

	var (
		mu      sync.Mutex
		missing []string
	)

	// httpbin reflects the request back under these names on /get, /post and
	// /anything, and the examples read `args`, `json` and `headers` out of it.
	reflect := func(r *http.Request) map[string]any {
		args := map[string]any{}
		for key, values := range r.URL.Query() {
			// One value is a string and several are a list — which is what the
			// `query:` example is about, so getting it wrong here would make that
			// example pass without testing anything.
			if len(values) == 1 {
				args[key] = values[0]

				continue
			}
			args[key] = values
		}

		headers := map[string]any{}
		for key := range r.Header {
			headers[key] = r.Header.Get(key)
		}

		return map[string]any{
			"args":    args,
			"headers": headers,
			"origin":  "127.0.0.1",
			"url":     "https://httpbin.org" + r.URL.RequestURI(),
		}
	}

	write := func(w http.ResponseWriter, body map[string]any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}

	mux := http.NewServeMux()

	// The slideshow document `http-json` and `http-output-shaping` reach into by
	// name — `slideshow.title`, `slideshow.author` — so its keys are the fixture
	// rather than decoration.
	mux.HandleFunc("/json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"slideshow":{"author":"Yours Truly","date":"date of publication",`+
			`"slides":[{"title":"Wake up to WonderWidgets!","type":"all"}],"title":"Sample Slide Show"}}`)
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		write(w, reflect(r))
	})

	mux.HandleFunc("/anything", func(w http.ResponseWriter, r *http.Request) {
		body := reflect(r)
		body["method"] = r.Method
		write(w, body)
	})

	mux.HandleFunc("/post", func(w http.ResponseWriter, r *http.Request) {
		body := reflect(r)
		raw, _ := io.ReadAll(r.Body)

		// httpbin splits a request body three ways and the examples read two of
		// them: `form` for the form-encoded one, `json` for the parsed JSON.
		form := map[string]any{}
		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
			if values, err := url.ParseQuery(string(raw)); err == nil {
				for key := range values {
					form[key] = values.Get(key)
				}
			}
		}

		var parsed any
		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			_ = json.Unmarshal(raw, &parsed)
		}

		body["data"] = ""
		if len(form) == 0 && parsed == nil {
			body["data"] = string(raw)
		}
		body["files"] = map[string]any{}
		body["form"] = form
		body["json"] = parsed
		write(w, body)
	})

	// `http-expect` asks for a 404 on purpose, to show that a status a workflow
	// declares acceptable is not a failure.
	mux.HandleFunc("/status/{code}", func(w http.ResponseWriter, r *http.Request) {
		code, err := strconv.Atoi(r.PathValue("code"))
		if err != nil || code < 100 || code > 599 {
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(code)
	})

	// A bare host with no path — `simple-http-multi-step` fetches one and reads
	// only the status code. `{$}` matches exactly "/", leaving "/" below free to be
	// the catch-all it has to be.
	mux.HandleFunc("/{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, "<html><body>ok</body></html>")
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		missing = append(missing, r.URL.Path)
		mu.Unlock()

		w.WriteHeader(http.StatusNotImplemented)
	})

	srv := httptest.NewServer(mux)
	tb.Cleanup(srv.Close)

	allowLoopback(tb)

	return srv.URL, func() []string {
		mu.Lock()
		defer mu.Unlock()

		return append([]string(nil), missing...)
	}
}

// ReachesTheNetwork reports whether any step, at any depth, makes a request.
//
// A step's compensation counts, because it is a request the run may make. It was
// missed at first and the miss was not theoretical: `saga-provisioning`'s undo
// steps went out to the real httpbin.org from inside a test suite that believed it
// had pointed every request at a stand-in, and answered 503. A walk that knows
// about one of a step's two task positions is a walk that is wrong about half of
// them.
func ReachesTheNetwork(nodes []*v1.Node) bool {
	return AnyStep(nodes, func(node *v1.Node) bool {
		return node.GetTask().GetName() == "http" || node.GetUndo().GetTask().GetName() == "http"
	})
}

// WaitsForASignal reports whether any step, at any depth, waits to be told
// something from outside the workload.
func WaitsForASignal(nodes []*v1.Node) bool {
	return AnyStep(nodes, func(node *v1.Node) bool {
		return node.GetWait().GetSignal() != nil
	})
}

// LapsesWithin reports whether every signal wait in the workflow has a timeout,
// and one no longer than the caller is prepared to wait — so a run nobody sends
// anything to reaches its end inside that budget.
//
// The question a harness has to answer about a gate is not "does this wait" but
// "will this finish if I answer nothing, before I give up on it", and both halves
// are load-bearing. A wait with no `timeout:` blocks for as long as the run lasts,
// which is right for an approval that must block until a person acts and wrong to
// start unattended. A wait with one produces `timed_out: true` when the deadline
// passes and the run carries on down whichever branch the file wrote — which is
// what `wait-timeout` demonstrates, and a run a harness can start and simply wait
// out.
//
// The budget is the caller's rather than a constant here, because it is a property
// of the harness: `approval-gate` lapses after a day, which is a lapse in every
// sense except the one a test can sit through. Asking "does it lapse" alone reported
// it as unattended and hung the run against its own bound — a bound reached is not
// the same as a question answered.
//
// Asked of every wait rather than of any, because a file with two gates finishes
// only if both do; an [AnyStep] here would call a workflow unattended on the
// strength of the one gate that happens to have a deadline.
//
// False when there is no signal wait at all: there is then no gate to lapse, and a
// caller asking this is asking about a workflow whose answer [WaitsForASignal]
// decides first.
func LapsesWithin(nodes []*v1.Node, budget time.Duration) bool {
	if !WaitsForASignal(nodes) {
		return false
	}

	return !AnyStep(nodes, func(node *v1.Node) bool {
		wait := node.GetWait()
		if wait.GetSignal() == nil {
			return false
		}

		timeout := wait.GetTimeout().AsDuration()

		return timeout <= 0 || timeout > budget
	})
}

// AnyStep reports whether pred holds for any step, at any nesting depth.
//
// Both questions above are about the whole workflow rather than its top level: an
// example's only `http` step is as likely to be inside a loop body as beside one.
func AnyStep(nodes []*v1.Node, pred func(*v1.Node) bool) bool {
	for _, node := range nodes {
		if pred(node) {
			return true
		}
		switch kind := node.GetKind().(type) {
		case *v1.Node_ForEach:
			if AnyStep(kind.ForEach.GetBody(), pred) {
				return true
			}
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				if AnyStep(branch.GetSteps(), pred) {
					return true
				}
			}
		}
	}

	return false
}

// PointAtStandIn rewrites every http step's host to the stand-in, keeping the path
// and the query the example wrote, and returns a diagnosis for each step it could
// not point anywhere.
//
// Those diagnoses are the whole point of the return value. A url built by an
// expression has no host to rewrite until it is evaluated, so it would be left as
// written and the run would reach the real host — which makes a suite depend on
// somebody else's service and fails outright on a machine with no egress. The
// caller refuses to run such an example rather than running it against the
// internet.
//
// Only the scheme and host move. The path selects the response shape and the query
// is what `http-query-and-json` is *about*, so a rewrite dropping either would turn
// the example into a different one.
//
// A loopback url is deliberately left alone and counts as pointed:
// `conditional-and-retry` aims one at a port nothing listens on to show a tolerated
// failure, and sending it somewhere that answers would delete the only thing that
// example demonstrates.
//
// # An expression is not automatically unpointable
//
// A literal `url:` was, until #183, the only shape this could rewrite — so a
// for_each body whose url varies per item (`url: ${service.url}`, the natural
// spelling for "one request per endpoint") reported unpointable even though the
// data behind the expression was written in the file as plain literals. The
// example that needed exactly that shape was forced into `parallel:` with the
// endpoints unrolled by hand instead, which is a constraint of this harness, not
// of the language.
//
// So the walk now traces the two ways a step can bind a name for itself, and
// rewrites the *data* an expression draws from where it can see it:
//
//   - A for_each iterator, selected with a field — `${service.url}` — traces back
//     to the loop's own `items:`. When that is a literal list of maps, every
//     entry's matching field is rewritten, because a for_each body runs once per
//     entry and every one of those requests has to be pointed, not just the first.
//   - A step's own var, read bare — `${target_url}`, never `${vars.target_url}`,
//     per the `vars:` field's own doc comment — traces back to that var's own
//     literal. In scope for the step that bound it and, when that step is a
//     for_each or a parallel, for the body nested inside it too: both executors
//     evaluate a block's own vars into the same scope its nested steps run in, so
//     a for_each whose own `vars:` holds a literal url a nested task reads bare
//     is exactly as traceable as the loop's iterator is.
//
// Anything else — items computed at run time, a var holding something other than
// a literal string, a select chain more than one field deep, an expression this
// walk does not recognize at all — keeps the honest refusal. What changes is that
// the refusal names *why*: which expression, on which step, and what this walk
// could and could not see about it, so an author reads a boundary of the harness
// rather than suspecting their file. A true line:column would need the file's own
// [flowfile.Positions], which this function is never handed — it only ever sees
// the compiled node tree — so the expression's own rendered text stands in as its
// position here.
func PointAtStandIn(nodes []*v1.Node, base string) []string {
	standIn, err := url.Parse(base)
	if err != nil {
		// Not a stand-in at all, so nothing can be pointed at it. Naming every http
		// step is the honest answer: the caller then refuses to run any of them.
		return httpStepIDs(nodes)
	}

	return pointAtStandIn(nodes, standIn, nil)
}

// binding is one name a walk beneath a step can trace a `url:` expression back
// to — the two shapes [PointAtStandIn]'s doc comment names, kept apart because
// they are traced differently. Exactly one of items or value is set.
type binding struct {
	name string

	// items is a for_each's own ForEach.Items literal — a list of maps, one
	// consumed per iteration — set when the binding came from a loop iterator.
	items *celpb.Value

	// value is a step's own var, set when the binding came from that step's
	// `vars:`. In scope for the step that bound it, and for whatever that step
	// recurses into below — a for_each's or a parallel's own vars reach their
	// body the same way a loop iterator does, per pointAtStandIn's comment on
	// `bindings` — but not for a sibling later in the same node list.
	value *v1.Value
}

// pointAtStandIn is [PointAtStandIn] with the enclosing for_each loops' iterator
// bindings threaded through, so a body step's `${iterator.field}` can be traced
// back to the loop that bound iterator.
func pointAtStandIn(nodes []*v1.Node, standIn *url.URL, loops []binding) []string {
	var unpointable []string

	for _, node := range nodes {
		// A step's own vars are bound bare — `${name}`, never `${vars.name}` — and
		// both executors evaluate a for_each's or a parallel's own body inside that
		// same node's scope, so a var the node declares for itself is in scope for
		// what is nested inside it too — not only for its own task and undo. It
		// still does not outlive the node: a sibling step later in the same list
		// never sees it, which is why this is added to bindings rather than to
		// loops itself, and why bindings — not loops — is threaded into whatever
		// this node recurses into below.
		bindings := loops
		if len(node.GetVars()) > 0 {
			bindings = make([]binding, 0, len(loops)+len(node.GetVars()))
			bindings = append(bindings, loops...)
			for name, value := range node.GetVars() {
				bindings = append(bindings, binding{name: name, value: value})
			}
		}

		if task := node.GetTask(); task.GetName() == "http" {
			if reason, ok := pointTaskURL(task, standIn, bindings); !ok {
				unpointable = append(unpointable, node.GetId()+reason)
			}
		}

		// A step's compensation is a request too. Left out at first, and the run
		// went to the real httpbin.org from a suite that thought it had pointed
		// everything at the stand-in — silently, because an unpointed *undo* is not
		// an unpointed step and nothing counted it.
		if undo := node.GetUndo().GetTask(); undo.GetName() == "http" {
			if reason, ok := pointTaskURL(undo, standIn, bindings); !ok {
				unpointable = append(unpointable, node.GetId()+" (undo)"+reason)
			}
		}

		switch kind := node.GetKind().(type) {
		case *v1.Node_ForEach:
			fe := kind.ForEach

			iterator := fe.GetIterator()
			if iterator == "" {
				// The schema's own default: see ForEach.iterator's doc comment.
				iterator = "item"
			}

			child := make([]binding, 0, len(bindings)+1)
			child = append(child, bindings...)
			child = append(child, binding{name: iterator, items: fe.GetItems().GetLiteral()})

			unpointable = append(unpointable, pointAtStandIn(fe.GetBody(), standIn, child)...)
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				unpointable = append(unpointable, pointAtStandIn(branch.GetSteps(), standIn, bindings)...)
			}
		}
	}

	return unpointable
}

// lookupBinding finds the innermost binding of name, matching the language's own
// shadowing rule: the schema refuses a step var or a loop iterator that collides
// with anything already bound (Node.vars's doc comment, "collisions are refused,
// not resolved"), so a compiled node tree never actually has two live bindings of
// the same name for this to choose between — but bindings is assembled inner
// scope last, so taking the last match is the correct read of it regardless.
func lookupBinding(bindings []binding, name string) (binding, bool) {
	for i := len(bindings) - 1; i >= 0; i-- {
		if bindings[i].name == name {
			return bindings[i], true
		}
	}
	return binding{}, false
}

// pointTaskURL points one task's `url:` input at the stand-in, either directly —
// the literal case — or by rewriting the binding an expression traces back to.
// The second return is false wherever the walk gives up, and the first is a
// reason to append after the step's id, naming why and, where there is one, the
// expression itself.
func pointTaskURL(task *v1.Task, standIn *url.URL, bindings []binding) (string, bool) {
	input := task.GetInputs()["url"]

	if literal := input.GetLiteral(); literal != nil {
		if rewriteURLLiteral(literal, standIn) {
			return "", true
		}
		// A literal that is not a rewritable url — not a string, or a string url.Parse
		// cannot make sense of. Naming the step is enough; there is no expression to
		// quote.
		return " has a `url` input this test cannot parse as a url", false
	}

	root := input.GetExpr().GetExpr()
	if root == nil {
		return " has no `url` input to point at the stand-in", false
	}

	text := describeExpr(input.GetExpr())

	if sel := root.GetSelectExpr(); sel != nil {
		if ident := sel.GetOperand().GetIdentExpr(); ident != nil {
			if b, ok := lookupBinding(bindings, ident.GetName()); ok && b.value == nil {
				// A select off a bound name that is not a step var is, by construction,
				// a for_each iterator (the only other binding [pointAtStandIn] adds) —
				// even when its own items are not a literal this walk can rewrite, which
				// is itself worth saying rather than falling through to the generic
				// refusal below.
				if b.items == nil {
					return fmt.Sprintf(
						" has `url: %s`, whose `%s` traces to for_each's own `items:` — but items is "+
							"computed rather than written as data, so there is nothing literal here to "+
							"rewrite; give the step a literal url, or teach PointAtStandIn the shape it "+
							"uses", text, ident.GetName()), false
				}
				if rewriteItemsField(b.items, sel.GetField(), standIn) {
					return "", true
				}
				return fmt.Sprintf(
					" has `url: %s`, which traces to the for_each's own items — but not every "+
						"entry has a literal string %q to rewrite, so this walk cannot point every "+
						"iteration's request at the stand-in; give every item's %[2]s field a literal, "+
						"or teach PointAtStandIn the shape it uses", text, sel.GetField()), false
			}
		}
	}

	if ident := root.GetIdentExpr(); ident != nil {
		if b, ok := lookupBinding(bindings, ident.GetName()); ok && b.value != nil {
			if literal := b.value.GetLiteral(); literal != nil && rewriteURLLiteral(literal, standIn) {
				return "", true
			}
			// Naming the var's own expression, not only its name, when it has one —
			// `base: ${steps.discover.json.primary_url}` is what actually made this
			// var untraceable, and a diagnosis that stops at "base is not a literal"
			// makes an author go find that binding themselves.
			varText := "not a literal url string"
			if varExpr := b.value.GetExpr(); varExpr != nil {
				varText = "`" + describeExpr(varExpr) + "`, not a literal"
			}
			return fmt.Sprintf(
				" has `url: %s`, which traces to the step's own var %q — but that var is %s, so "+
					"this walk cannot rewrite it; give it a literal, or teach PointAtStandIn the "+
					"shape it uses", text, ident.GetName(), varText), false
		}
	}

	// Traced nowhere this walk recognizes: an output of an earlier step, a
	// workflow-level input, a call expression, a select more than one field
	// deep. Named with the expression's own rendered text, because this
	// function never sees the file's line and column — only the compiled node
	// tree flowfile.ParseFile already reduced source positions out of — so the
	// expression itself is the most precise thing there is to point an author
	// at.
	return fmt.Sprintf(
		" has `url: %s`, an expression this test cannot trace to a literal it can rewrite; give "+
			"the step a literal url, or teach PointAtStandIn the shape it uses", text), false
}

// describeExpr renders an expression as it was written, for a diagnostic that
// has to name one precisely without a source position to point at.
//
// [cel.AstToString] is the same renderer flowfile's own compiler uses to
// normalize an expression's spelling (value.go's normalizeExpr) and to write one
// back out (marshal.go) — reusing it means this harness's diagnostics describe an
// expression the same way the compiler itself does, rather than growing a second,
// possibly-disagreeing unparser.
func describeExpr(parsed *celpb.ParsedExpr) string {
	text, err := cel.AstToString(cel.ParsedExprToAst(parsed))
	if err != nil {
		return "<expression>"
	}
	return text
}

// rewriteItemsField rewrites one field, in every entry of a for_each's items
// literal, to the stand-in — or refuses the whole rewrite if any entry cannot
// supply a literal string for it.
//
// All-or-nothing on purpose. A for_each body makes one request per entry, so an
// item this cannot rewrite is a request that would still reach the real host;
// pointing nine iterations at the stand-in and silently leaving the tenth aimed
// at the internet is a worse failure mode than refusing the file outright.
func rewriteItemsField(items *celpb.Value, field string, standIn *url.URL) bool {
	list, ok := items.GetKind().(*celpb.Value_ListValue)
	if !ok {
		return false
	}
	entries := list.ListValue.GetValues()
	if len(entries) == 0 {
		return false
	}

	for _, entry := range entries {
		m, ok := entry.GetKind().(*celpb.Value_MapValue)
		if !ok {
			return false
		}

		var found bool
		for _, kv := range m.MapValue.GetEntries() {
			key, ok := kv.GetKey().GetKind().(*celpb.Value_StringValue)
			if !ok || key.StringValue != field {
				continue
			}
			if !rewriteURLLiteral(kv.GetValue(), standIn) {
				return false
			}
			found = true

			break
		}
		if !found {
			return false
		}
	}

	return true
}

// httpStepIDs names every http step, at any depth.
func httpStepIDs(nodes []*v1.Node) []string {
	var ids []string
	for _, node := range nodes {
		if node.GetTask().GetName() == "http" {
			ids = append(ids, node.GetId())
		}
		if node.GetUndo().GetTask().GetName() == "http" {
			ids = append(ids, node.GetId()+" (undo)")
		}
		switch kind := node.GetKind().(type) {
		case *v1.Node_ForEach:
			ids = append(ids, httpStepIDs(kind.ForEach.GetBody())...)
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				ids = append(ids, httpStepIDs(branch.GetSteps())...)
			}
		}
	}

	return ids
}

// rewriteURLLiteral moves one literal url string onto the stand-in's host, in
// place, and reports whether it is now pointed somewhere the test controls.
//
// The one rewrite rule, shared by every literal a url expression can trace back
// to — a task's own `url:`, a for_each item's field, a step var — rather than a
// copy of it living in each. Only the scheme and host move, for the reason
// [PointAtStandIn]'s doc comment gives: the path selects the response shape and
// the query is what an example may be about. A loopback url is left alone and
// still counts as pointed, matching `conditional-and-retry`'s reason for having
// one at all.
//
// Mutating the literal in place, rather than replacing the map entry that holds
// it the way the pre-#183 version did, is what lets this same rule rewrite a
// for_each item's field: that literal sits inside a repeated structure this
// function does not otherwise know how to address by key.
func rewriteURLLiteral(literal *celpb.Value, standIn *url.URL) bool {
	str, ok := literal.GetKind().(*celpb.Value_StringValue)
	if !ok || str.StringValue == "" {
		return false
	}

	target, err := url.Parse(str.StringValue)
	if err != nil || target.Hostname() == "" {
		return false
	}
	if host := target.Hostname(); host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}

	target.Scheme = standIn.Scheme
	target.Host = standIn.Host
	str.StringValue = target.String()

	return true
}

// exampleFailures names the examples whose point is a run that does *not* succeed,
// against the text their failure must carry.
//
// One entry, and it should stay hard to add to. "Expected to fail" is the shape a
// harness classification most easily rots into an excuse — an example that broke,
// listed here rather than fixed — so the bar is that the failure is the thing being
// demonstrated and there is no version of the example without it.
//
// `saga-provisioning` meets it by construction. A saga has nothing to show when
// every step works: the compensations are written, nothing runs them, and the file
// proves only that it compiles. What is worth seeing is the run that fails partway
// and takes back what it already did, so the example ends in a failure the way
// `http-expect` ends in a 404.
//
// The expected text is [v1.UndoSummary]'s output and nothing else. Not the whole
// failure message: the two drivers legitimately differ before it — the durable one
// carries an activity envelope — and pinning that here would make an example test
// fail on a Temporal upgrade nothing in this repository caused.
//
// Shared between the two harnesses for the reason the rest of this file is. A
// classification each driver kept its own copy of is two answers to "is this
// example meant to fail", and the day they disagree the durable harness reports a
// driver disagreement it invented itself.
var exampleFailures = map[string]string{
	"saga-provisioning": `; compensation ran in reverse order: undid "volume", undid "network"`,
}

// ExampleFailure reports the text an example's failure must carry, for an example
// that is meant to fail.
//
// The second return is false for every other example, which is the ordinary case
// and the one both harnesses assert `NoError` for.
func ExampleFailure(name string) (string, bool) {
	want, expected := exampleFailures[name]

	return want, expected
}

// ExampleVariant is an additional invocation of an example beyond its default
// run — an override to one or more of its bound inputs, and, when the override
// is meant to make the run fail, the text that failure has to carry.
//
// [exampleFailures] answers whether an example's *default* arguments are meant
// to fail; this answers the same question for a run reached only by flipping
// an input the default arguments leave alone. order-fulfillment ships with
// `carrier_outage` defaulting to false — "paste this and watch it work" is the
// rule every example follows — so the compensation path, the property the
// example exists to demonstrate, is never reached by a harness that only ever
// binds an example's own defaults. A second invocation is the only way to
// reach it, and it needs its own entry because it needs its own inputs.
type ExampleVariant struct {
	// Name distinguishes this run from the example's default one, in a
	// subtest's name and in a durable run's workflow id.
	Name string

	// Overrides sit on top of the example's own bound inputs — its declared
	// defaults, or whatever its inputs.json answers — so a variant only has to
	// name what it changes rather than restate everything.
	Overrides map[string]*v1.Value

	// Fails is the text the run's failure must carry, in v1.UndoSummary's
	// rendering — the same contract exampleFailures holds its one entry to.
	// Empty for a variant that is meant to succeed like the ordinary run.
	Fails string
}

// WithOverrides returns bound inputs with this variant's overrides applied, so
// a caller does not have to know an example's other declarations to run one.
func (v ExampleVariant) WithOverrides(bound map[string]*v1.Value) map[string]*v1.Value {
	merged := make(map[string]*v1.Value, len(bound)+len(v.Overrides))
	for name, value := range bound {
		merged[name] = value
	}
	for name, value := range v.Overrides {
		merged[name] = value
	}

	return merged
}

// exampleVariants names an example against the additional invocations worth
// running beyond its default one.
//
// One entry, for the reason exampleFailures has one: it is meant to stay hard
// to add to, so an entry here is a real gap in what the default run reaches
// rather than a way to make an assertion pass.
var exampleVariants = map[string][]ExampleVariant{
	"order-fulfillment": {
		{
			Name:      "carrier-outage",
			Overrides: map[string]*v1.Value{"carrier_outage": v1.NewLiteral(true)},
			Fails:     `; compensation ran in reverse order: undid "charge_payment", undid "reserve_inventory"`,
		},
	},
}

// ExampleVariants reports the additional invocations an example needs beyond
// its default run, for the same reason [ExampleFailure] answers whether the
// default one is meant to fail: one table, shared by both harnesses, so a
// classification cannot disagree with itself.
func ExampleVariants(name string) []ExampleVariant {
	return exampleVariants[name]
}
