package flowstatev1_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

type exampleSecretProvider struct{}

func (exampleSecretProvider) Scheme() string { return "env" }
func (exampleSecretProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	return secrets.NewSecret(req.Ref, "example-token"), nil
}

type exampleExchanger struct{}

func (exampleExchanger) Name() string { return "example-sts" }
func (exampleExchanger) Requirement() auth.Requirement {
	return auth.Requirement{Audience: "https://api.example.com"}
}
func (exampleExchanger) Exchange(context.Context, auth.Assertion) (auth.Credential, error) {
	return auth.NewCredential(auth.CredentialBearer, time.Now().Add(time.Minute),
		map[string]string{auth.CredentialAccessToken: "example-jit-token"})
}

func exampleBroker(t *testing.T) *auth.Broker {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	key, err := auth.NewSigningKey("example", private)
	require.NoError(t, err)
	issuer, err := auth.NewIssuer("https://flowstate.example", key)
	require.NoError(t, err)
	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("partner-api", exampleExchanger{}), auth.WithAssumeAllowRules("true"))
	require.NoError(t, err)
	return broker
}

// CLAUDE.md says a capability is not done until an example exercises it, "those
// run in CI, which is what keeps them honest". They did not run. Every test over
// `examples/` compiled or validated them, and the difference is not academic: an
// example shipped here that `flow validate` called ok and `flow run local`
// refused on its first step, because `expect:` was written as a mapping where the
// http task wants an expression.
//
// It got through because `expect` is a *deferred* input — evaluated by the task
// against a scope the validator cannot see, so the validator correctly declines
// to judge it. Every deferred input has that shape. Validation cannot close this;
// only running can.
//
// So this runs them. It is the cheap half of what the rule already claimed.

// TestEveryOfflineExampleRuns executes each example that needs no network.
//
// Which ones those are is derived rather than listed: a workflow reaching the
// network is one with an `http` step somewhere in it, and asking the compiled
// workflow means a new example is covered the day it is written, without anyone
// remembering to add it here.
func TestEveryOfflineExampleRuns(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples found; the glob is wrong")

	var ran int
	for _, path := range paths {
		name := filepath.Base(filepath.Dir(path))

		data, err := os.ReadFile(path)
		require.NoError(t, err)

		wf, err := flowfile.Unmarshal(data)
		require.NoError(t, err, "%s does not compile", name)

		if reachesTheNetwork(wf.GetSteps()) {
			continue
		}
		// A gate is answered from outside the workload, which is the point of it.
		// The local driver takes one through `--signal`; a run without it is
		// refused, correctly, and that refusal is not something to assert here.
		if waitsForASignal(wf.GetSteps()) {
			continue
		}

		// Bound once, here, from the example's own inputs.json — the file
		// `flow run local --input-file` takes, and the only answer either harness
		// has to what an example requires. An example this cannot start fails
		// naming that file rather than being skipped.
		inputs, err := tests.BindExampleInputs(t, wf, path)
		require.NoError(t, err, "%s cannot be started", name)

		ran++

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Bounded, because an example that waits is an example that could hang
			// this suite, and a test whose failure mode is "CI times out in ten
			// minutes" is worse than one that says which example stopped.
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()

			outputs, err := v1.RunWithInputs(ctx, wf, inputs)
			require.NoError(t, err, "%s validates but does not run", name)
			require.NotNil(t, outputs)

			// The run reached the end, rather than "succeeding" having done almost
			// nothing — which the error above would not catch.
			//
			// Asserted over unconditional top-level task steps only, and each
			// exclusion is a real rule rather than a way to make this pass. A step
			// behind an `if:` that is false is *meant* to produce nothing. A
			// `parallel:` reports through its branches, whose outputs merge into the
			// enclosing scope under their own ids and not the block's. A loop reports
			// through `results`.
			for _, step := range wf.GetSteps() {
				if step.GetTask() == nil || step.GetCondition() != nil {
					continue
				}
				assert.Contains(t, outputs.GetStepValues(), step.GetId(),
					"step %q produced no outputs", step.GetId())
			}
		})
	}

	// The count is asserted so that a change making every example look like it
	// needs the network — a rename, a broken predicate — fails here rather than
	// silently running nothing and reporting success.
	assert.GreaterOrEqual(t, ran, 8,
		"expected most examples to be runnable offline; only %d were, which suggests the network check is wrong", ran)
}

// TestEveryNetworkedExampleRuns runs the other half — the ones skipped above.
//
// Skipping them was not a small gap. Eight of the nineteen examples reach the
// network, and they are precisely the ones demonstrating the http task: `query`,
// `json`, `form`, `expect`, `outputs` shaping, and reading a response back out of
// `steps.<id>.json`. Everything the offline test was written to catch lived in the
// half it could not see.
//
// And one of them was broken the whole time. `http-query-and-json` ended with
// `${string(steps.search.json.args)}`, which cannot evaluate — `string()` has no
// overload for a map, and `fields:` is string-valued deliberately — so its last
// step failed on every run it ever had. It was found by counting CEL evaluations
// for something else entirely, which is not a way to find bugs.
//
// The examples name `httpbin.org`, and they should: somebody reading one wants to
// paste it and watch it work. So the run is pointed at a stand-in instead of the
// example being rewritten. What that proves is the whole of what the example
// controls — its inputs, its expressions, and what its steps do with a response of
// that shape. What it does not prove is that httpbin is up, or that its shape is
// still what the bodies below were copied from. That second one is the real limit
// and it is why they are copies of a recorded response rather than something
// invented to make the assertions pass.
//
// Nothing runs until every one of its requests is pointed at the stand-in. An
// example this cannot point is refused rather than run, because running it would
// reach the real host — which makes the suite depend on somebody else's service
// and fails outright with no egress. See [rewriteURL] for why that is a refusal
// and not a fallback.
func TestEveryNetworkedExampleRuns(t *testing.T) {
	// Not parallel: the loopback exemption swaps a process-global registry entry
	// and restores it on cleanup, so two top-level tests holding one at once would
	// have the first one's restore land while the second still runs. Subtests may
	// still be parallel — cleanup waits for them.
	base, unserved := exampleHTTPServer(t)
	secretStore, err := secrets.NewStore(exampleSecretProvider{})
	require.NoError(t, err)
	secretPolicy, err := (auth.SecretAccessPolicy{Allow: []string{"true"}}).Compile()
	require.NoError(t, err)
	broker := exampleBroker(t)

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples found; the glob is wrong")

	var ran int
	for _, path := range paths {
		name := filepath.Base(filepath.Dir(path))

		data, err := os.ReadFile(path)
		require.NoError(t, err)

		wf, err := flowfile.Unmarshal(data)
		require.NoError(t, err, "%s does not compile", name)

		if !reachesTheNetwork(wf.GetSteps()) {
			continue
		}
		if waitsForASignal(wf.GetSteps()) {
			continue
		}

		inputs, err := tests.BindExampleInputs(t, wf, path)
		require.NoError(t, err, "%s cannot be started", name)

		ran++

		// Every request has to be pointed somewhere this test controls before the
		// example runs. A step it cannot point is not a step to run anyway: it would
		// reach the real host, which makes the suite depend on somebody else's
		// service being up and fails outright on a machine with no egress.
		require.Empty(t, pointAtStandIn(wf.GetSteps(), base),
			"%s has an http step this test cannot point at the stand-in, so running it would "+
				"reach the real host; give the step a literal url, or teach rewriteURL the shape it uses", name)

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			ctx = v1.ContextWithTaskRuntime(ctx, v1.TaskRuntime{
				Store: secretStore, Policy: secretPolicy, Broker: broker,
				Identity: auth.WorkloadIdentity{Subject: "examples", Issuer: "flowstate:test"},
				Step:     auth.StepRef{Workflow: wf.GetName(), Run: "example-run"},
			})

			outputs, err := v1.RunWithInputs(ctx, wf, inputs)
			require.NoError(t, err, "%s validates but does not run", name)
			require.NotNil(t, outputs)

			for _, step := range wf.GetSteps() {
				if step.GetTask() == nil || step.GetCondition() != nil {
					continue
				}
				assert.Contains(t, outputs.GetStepValues(), step.GetId(),
					"step %q produced no outputs", step.GetId())
			}
		})
	}

	// Same reason the offline test counts: a predicate that stopped matching would
	// otherwise run nothing and report success.
	assert.GreaterOrEqual(t, ran, 7,
		"expected the networked examples to be found; only %d were, which suggests the network check is wrong", ran)

	// The stand-in answers the paths the examples ask for and nothing else, so an
	// example added against a path it does not serve fails here saying so — rather
	// than passing against a 404 its expressions never look at.
	t.Cleanup(func() {
		assert.Empty(t, unserved(), "the examples stand-in was asked for paths it does not serve")
	})
}

// exampleHTTPServer returns a base URL standing in for httpbin.org, and a func
// reporting any path an example asked for that it does not serve.
//
// The bodies are copies of what httpbin answered on 2026-07-30, trimmed to the
// fields the examples reach and keeping the shape around them. Copied rather than
// composed, because a stand-in an author tuned until the tests passed proves only
// that it matches itself.
func exampleHTTPServer(tb testing.TB) (string, func() []string) {
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
			// One value is a string and several are a list — which is the whole
			// point of the `query:` example, so getting it wrong here would make
			// that example pass without testing anything.
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

	// The slideshow document, which `http-json` and `http-output-shaping` reach
	// into by name — `slideshow.title` and `slideshow.author` — so its keys are
	// the fixture rather than decoration.
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

		// httpbin splits a request body three ways, and the examples read two of
		// them: `form` for the form-encoded one, `json` for the parsed JSON.
		form := map[string]any{}
		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
			values, err := url.ParseQuery(string(raw))
			if err == nil {
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
	// only the status code. `{$}` matches exactly "/", leaving "/" below free to
	// be the catch-all it needs to be.
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

	allowLoopbackEgress(tb)

	return srv.URL, func() []string {
		mu.Lock()
		defer mu.Unlock()

		return append([]string(nil), missing...)
	}
}

// allowLoopbackEgress registers an http task permitting loopback for the duration
// of the test, restoring the original afterwards.
//
// The shipped policy denies loopback, which is right — a workflow must not reach a
// worker's own internal endpoints — and is also why a stand-in on 127.0.0.1 is
// unreachable without saying so. Stated by the test rather than weakened in the
// default, so the default stays under test everywhere else.
func allowLoopbackEgress(tb testing.TB) {
	tb.Helper()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	require.NoError(tb, err, "building loopback egress policy")

	registry := v1.DefaultRegistry()
	original, existed := registry.Lookup("http")
	require.NoError(tb, registry.Register(v1.HTTPTaskDef(policy)), "registering loopback http task")

	tb.Cleanup(func() {
		if existed {
			_ = registry.Register(original)
		}
	})
}

// pointAtStandIn rewrites every http step's host to the stand-in, keeping the path
// and query the example wrote.
//
// Only the scheme and host move. The path is what selects the response shape and
// the query is what `http-query-and-json` is *about*, so a rewrite that dropped
// either would turn the example into a different one.
//
// A loopback URL is left alone. `conditional-and-retry` points one at a port
// nothing listens on, deliberately, to show a tolerated failure; sending it
// somewhere that answers would delete the only thing that example demonstrates.
// It returns the ids of steps it could not point anywhere, which the caller
// refuses to run. Silence there was the first version's mistake — see [rewriteURL].
func pointAtStandIn(nodes []*v1.Node, base string) []string {
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
			unpointable = append(unpointable, pointAtStandIn(kind.ForEach.GetBody(), base)...)
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				unpointable = append(unpointable, pointAtStandIn(branch.GetSteps(), base)...)
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
// reports whether the step is now pointed somewhere this test controls.
//
// The false return is the whole point, and the first version of this did not have
// one. A url built by an expression has no host to rewrite until it is evaluated,
// so this left it as written — and the comment claimed the unserved-path check
// would catch that. It could not: `unserved` records requests that reach the
// stand-in's mux, and a url pointed at a real host never reaches it. So the case
// the comment said was covered was precisely the one that would have made the
// suite fetch httpbin.org for real, or fail on a machine with no egress. A guard
// that cannot see the path it names is worse than no guard, because it stops
// anyone looking. Reported instead, and the caller refuses to run the example.
//
// Only the scheme and host move. The path selects the response shape and the
// query is what `http-query-and-json` is *about*, so a rewrite dropping either
// would turn the example into a different one.
//
// A loopback url is deliberately left alone and counts as pointed:
// `conditional-and-retry` aims one at a port nothing listens on to show a
// tolerated failure, and sending it somewhere that answers would delete the only
// thing that example demonstrates.
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

// waitsForASignal reports whether any step, at any depth, waits to be told
// something from outside the workload.
func waitsForASignal(nodes []*v1.Node) bool {
	return anyStep(nodes, func(node *v1.Node) bool {
		return node.GetWait().GetSignal() != nil
	})
}

// reachesTheNetwork reports whether any step, at any depth, makes a request.
func reachesTheNetwork(nodes []*v1.Node) bool {
	return anyStep(nodes, func(node *v1.Node) bool {
		return node.GetTask().GetName() == "http"
	})
}

// anyStep reports whether pred holds for any step, at any nesting depth.
//
// Both questions above are about the whole workflow rather than its top level: an
// example's only `http` step is as likely to be inside a loop body as beside one.
func anyStep(nodes []*v1.Node, pred func(*v1.Node) bool) bool {
	for _, node := range nodes {
		if pred(node) {
			return true
		}
		switch kind := node.GetKind().(type) {
		case *v1.Node_ForEach:
			if anyStep(kind.ForEach.GetBody(), pred) {
				return true
			}
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				if anyStep(branch.GetSteps(), pred) {
					return true
				}
			}
		}
	}
	return false
}
