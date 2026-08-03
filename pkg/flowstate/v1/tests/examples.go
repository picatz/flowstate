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
// The local harness still carries its own copies at the time of writing; these are
// deliberately the same shapes, so that adopting them is a deletion rather than a
// rewrite.

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
func ExampleInputs(tb testing.TB, workflowPath string) map[string]*v1.Value {
	tb.Helper()

	data, err := os.ReadFile(filepath.Join(filepath.Dir(workflowPath), ExampleInputsFile))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		tb.Fatalf("reading %s beside %s: %v", ExampleInputsFile, workflowPath, err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var fields map[string]any
	if err := decoder.Decode(&fields); err != nil {
		tb.Fatalf("%s has an %s that is not a JSON object: %v",
			filepath.Dir(workflowPath), ExampleInputsFile, err)
	}

	inputs := make(map[string]*v1.Value, len(fields))
	for name, value := range fields {
		if number, ok := value.(json.Number); ok {
			whole, err := number.Int64()
			if err != nil {
				tb.Fatalf("input %q is not a whole number: %v", name, err)
			}
			inputs[name] = v1.NewLiteral(whole)

			continue
		}
		inputs[name] = v1.NewValue(value)
	}

	return inputs
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

	supplied := ExampleInputs(tb, workflowPath)

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
func ReachesTheNetwork(nodes []*v1.Node) bool {
	return AnyStep(nodes, func(node *v1.Node) bool {
		return node.GetTask().GetName() == "http"
	})
}

// WaitsForASignal reports whether any step, at any depth, waits to be told
// something from outside the workload.
func WaitsForASignal(nodes []*v1.Node) bool {
	return AnyStep(nodes, func(node *v1.Node) bool {
		return node.GetWait().GetSignal() != nil
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
// and the query the example wrote, and returns the ids of the steps it could not
// point anywhere.
//
// Those ids are the whole point of the return value. A url built by an expression
// has no host to rewrite until it is evaluated, so it would be left as written and
// the run would reach the real host — which makes a suite depend on somebody else's
// service and fails outright on a machine with no egress. The caller refuses to run
// such an example rather than running it against the internet.
//
// Only the scheme and host move. The path selects the response shape and the query
// is what `http-query-and-json` is *about*, so a rewrite dropping either would turn
// the example into a different one.
//
// A loopback url is deliberately left alone and counts as pointed:
// `conditional-and-retry` aims one at a port nothing listens on to show a tolerated
// failure, and sending it somewhere that answers would delete the only thing that
// example demonstrates.
func PointAtStandIn(nodes []*v1.Node, base string) []string {
	standIn, err := url.Parse(base)
	if err != nil {
		// Not a stand-in at all, so nothing can be pointed at it. Naming every http
		// step is the honest answer: the caller then refuses to run any of them.
		return httpStepIDs(nodes)
	}

	var unpointable []string
	for _, node := range nodes {
		if task := node.GetTask(); task.GetName() == "http" {
			if !rewriteURL(task, standIn) {
				unpointable = append(unpointable, node.GetId())
			}
		}
		switch kind := node.GetKind().(type) {
		case *v1.Node_ForEach:
			unpointable = append(unpointable, PointAtStandIn(kind.ForEach.GetBody(), base)...)
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				unpointable = append(unpointable, PointAtStandIn(branch.GetSteps(), base)...)
			}
		}
	}

	return unpointable
}

// httpStepIDs names every http step, at any depth.
func httpStepIDs(nodes []*v1.Node) []string {
	var ids []string
	for _, node := range nodes {
		if node.GetTask().GetName() == "http" {
			ids = append(ids, node.GetId())
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

// rewriteURL moves one http task's literal url onto the stand-in's host, and
// reports whether the step is now pointed somewhere the test controls.
func rewriteURL(task *v1.Task, standIn *url.URL) bool {
	literal := task.GetInputs()["url"].GetLiteral().GetStringValue()
	if literal == "" {
		return false
	}

	target, err := url.Parse(literal)
	if err != nil || target.Hostname() == "" {
		return false
	}
	if host := target.Hostname(); host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}

	target.Scheme = standIn.Scheme
	target.Host = standIn.Host
	task.Inputs["url"] = v1.NewLiteral(target.String())

	return true
}
