// Package conformance holds the behavior cases both execution drivers must
// pass. The local interpreter and the durable engine each run every [Case]
// set here, which is what keeps `flow run local` honest about what a
// Temporal run will do; add a case to this package rather than to one
// driver's own tests, and check that both drivers actually call the set it
// joins.
//
// It is internal because every file in it imports "testing" and its only
// importers are test files inside this module — nothing outside
// pkg/flowstate/v1 can use it, so it is not public API.
package conformance

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// Case is a workflow paired with the outputs it must produce.
type Case struct {
	// Name of the workflow, used for test identification.
	Name string
	// Workflow is the actual workflow definition to be tested.
	Workflow *v1.Workflow
	// Inputs are the arguments the run is submitted with, checked against the
	// workflow's declarations by whichever driver runs the case. Nil for a case
	// about something else, which is every case that predates `inputs:`.
	Inputs map[string]*v1.Value
	// ExpectedOutputs is the expected outputs of the workflow steps after execution.
	//
	// Ignored when ExpectFailure is set — a case whose point is that the *run*
	// fails outright (rather than a step's failure being tolerated into an
	// `error` output) has no outputs to compare.
	ExpectedOutputs *v1.Workflow_StepOutputs

	// Trigger is how the run started, read by its steps under the `trigger`
	// root. Nil means each driver's own default for a run nobody said anything
	// about, which is a manual start on both — see [v1.TriggerFromContext] and
	// the durable driver's empty [v1.RunState.trigger].
	//
	// A field on the case rather than something a driver's runner arranges,
	// because the two arrange it through entirely different machinery — a context
	// value locally, a field of the state message durably — and the whole claim
	// worth pinning is that those two routes produce the same answer inside the
	// run. See [TriggerContextCases].
	Trigger *v1.TriggerContext

	// ExpectFailure marks a case whose workflow must not complete at all — a
	// depth refused, a placement the engine cannot honour — as distinct from a
	// step failure tolerated via `continue_on_error`, which is an ordinary case
	// asserted through ExpectedOutputs like any other.
	ExpectFailure bool

	// ExpectedErrorContains overrides the substring an ExpectFailure case's
	// error is checked against, for a runner that otherwise asserts one fixed
	// sentence for every case in its set — [LoopCases]' runners check "ran its
	// full budget" by default, because until this field existed every failing
	// loop case failed the same way: exhausting its iteration ceiling. A case
	// about a different failure — a loop value the engine cannot evaluate at
	// all, say — sets this instead of forcing every other case in the set to
	// carry an assertion it does not need. Empty means "use the set's default
	// sentence".
	ExpectedErrorContains string

	// ExpectedErrorOmits is a substring an ExpectFailure case's error must
	// *not* contain, for the cases whose claim is about what a failure does not
	// say. ExpectedErrorContains cannot make that claim: a sentence can hold
	// the expected wording and the forbidden material at once, and a case about
	// a value withheld from a diagnostic is precisely the case where it does.
	//
	// Both drivers assert it, because a failure sentence is where a value the
	// workload computed enters durable history, and a driver that composed the
	// sentence differently would be the one that leaked (#1396). Empty means
	// the case makes no such claim, which is every case that predates one.
	ExpectedErrorOmits string

	// ExpectedErrorMaxBytes caps how large an ExpectFailure case's rendered
	// error may be, for the cases whose claim is that a diagnostic bounds
	// something another party sized.
	//
	// Both drivers assert it against their own rendering, wrapper text and all,
	// which is the honest comparison: the durable driver's failure carries
	// Temporal's own framing around the sentence and is what has to fit in
	// history, so a cap set from the local driver's shorter string alone would
	// pass while the run that matters could not be persisted. Zero means the
	// case makes no such claim.
	ExpectedErrorMaxBytes int

	// ExpectedOutputsPredicate checks a run's outputs when the exact value is
	// not what is under test — a run-time CEL error's precise wording, say,
	// which is a property of the expression evaluator rather than of the two
	// drivers agreeing with each other. Takes priority over ExpectedOutputs
	// when set, and is only ever compared against outputs from a run that
	// succeeded (ExpectFailure and this are mutually exclusive).
	ExpectedOutputsPredicate func(*v1.Workflow_StepOutputs) bool
}

// NewHTTPServer starts a server returning deterministic responses for the http
// task, and returns its base URL.
//
// Tests use this rather than a public endpoint so the suite does not depend on
// the internet: a third-party outage previously failed three packages at once,
// and a response whose headers vary run to run cannot be asserted exactly.
func NewHTTPServer(tb testing.TB) string {
	tb.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/status/200", func(w http.ResponseWriter, r *http.Request) {
		// A fixed content type keeps the asserted headers stable; Go adds
		// Content-Length and Date, which the assertions below account for.
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
	})
	// Two failure codes, one permanent and one retryable, so a case can pin what a
	// tolerated failure *says* rather than only that it happened — and can pin that
	// the sentence is the same on the second attempt as on the first.
	mux.HandleFunc("/status/404", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/status/500", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("/json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"slideshow":{"title":"Sample Slide Show"}}`))
	})
	// Returns a body of the requested size, for a case about how much a run carries
	// rather than about what it says. Generated rather than echoed so the request
	// stays small: the interesting asymmetry is a tiny specification producing a
	// large run state, and a case that had to *send* the bytes could not show it.
	mux.HandleFunc("/bytes/", func(w http.ResponseWriter, r *http.Request) {
		size, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/bytes/"))
		if err != nil || size < 0 {
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, strings.Repeat("x", size))
	})
	// Returns a JSON array of the requested length, for a case about how many
	// *elements* a task's result carries rather than how many bytes it — the
	// resource [checkTaskOutputElementBound] bounds, which /bytes/ above cannot
	// exercise: a body under the byte cap can still carry tens of thousands of
	// small elements.
	mux.HandleFunc("/json-array/", func(w http.ResponseWriter, r *http.Request) {
		n, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/json-array/"))
		if err != nil || n < 0 {
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "[")
		for i := 0; i < n; i++ {
			if i > 0 {
				_, _ = io.WriteString(w, ",")
			}
			_, _ = io.WriteString(w, strconv.Itoa(i))
		}
		_, _ = io.WriteString(w, "]")
	})
	// Returns the request body unchanged, which is the only way a case can watch a
	// value travel *into* a task's inputs and come back out as an output.
	//
	// Nothing local produces a value since `echo` retired, and the shapes observe.go
	// offers reach a value through a condition — which sees the scope a step is
	// written in and not the inputs the step was handed. The distinction matters
	// exactly once, for a step's own `vars:`: they are deliberately out of scope for
	// that step's own `if:` (see runNodes), so a condition cannot see one at all.
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, r.Body)
	})
	// Reflects whatever Authorization header arrived, in both a header and the
	// body — the shape a peer that echoes a bearer token or a minted JIT
	// credential takes. [Authority]'s containment cases point a step at this so
	// the assertion is about what the *worker* does with a revealed value (scrub
	// it before it becomes an output) rather than about what the peer sends back.
	mux.HandleFunc("/reflect-authorization", func(w http.ResponseWriter, r *http.Request) {
		authorization := r.Header.Get("Authorization")
		w.Header().Set("X-Reflected", authorization)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "echo: "+strings.TrimPrefix(authorization, "Bearer "))
	})

	srv := httptest.NewServer(mux)
	tb.Cleanup(srv.Close)

	allowLoopback(tb)
	return srv.URL
}

// loopbackExemption counts the tests currently holding the loopback exemption,
// so the shipped default is restored when the last one finishes rather than
// when the first one does.
//
// # The bug this replaces
//
// Each holder used to save whatever was registered when it arrived and restore
// that on cleanup, which is correct for one test and wrong for two overlapping
// ones. Two parallel tests, T1 then T2: T1 saves the deny-loopback default and
// installs the exemption; T2 saves *T1's exemption* and installs its own; T1
// finishes and restores the deny-loopback default — into a process where T2 is
// still running. T2's next http step then fails with `denied by egress policy:
// 127.0.0.1`, naming a policy nobody in that test configured, and the run it
// was asserting about fails for a reason that has nothing to do with it.
//
// It reproduced roughly one full `-race` run of `./pkg/flowstate/v1/engine/` in
// four, always as some *other* test's failure, which is the worst shape a flake
// takes: the report names a file the defect is not in.
//
// # Why counting rather than a lock held for the test's duration
//
// A mutex held from acquire to cleanup would be correct and would serialize
// every test in the package that talks to a loopback server, which is most of
// them. Counting is correct for the same reason and costs nothing, because
// every holder wants the *identical* definition — there is nothing to
// serialize. A test wanting a *different* http definition nests inside this
// one instead, taking it off and putting it back; see
// [InstallEgressIdentityPolicy] for why that is sound and what it costs.
var loopbackExemption struct {
	mu       sync.Mutex
	holders  int
	original v1.TaskDef
	existed  bool
}

// allowLoopback registers an http task permitting loopback for the duration of
// the test, restoring the original once no test still needs it.
//
// The default egress policy denies loopback, which is correct — a workflow must
// not be able to reach a worker's own internal endpoints — but it also means the
// task cannot reach a test server. Rather than weakening the default so tests
// pass, the tests state the exemption they need, which keeps the shipped default
// under test everywhere else.
func allowLoopback(tb testing.TB) {
	tb.Helper()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	if err != nil {
		tb.Fatalf("building loopback egress policy: %v", err)
	}

	registry := v1.DefaultRegistry()

	loopbackExemption.mu.Lock()
	if loopbackExemption.holders == 0 {
		loopbackExemption.original, loopbackExemption.existed = registry.Lookup("http")
		if err := registry.Replace(v1.HTTPTaskDef(policy)); err != nil {
			loopbackExemption.mu.Unlock()
			tb.Fatalf("registering loopback http task: %v", err)
		}
	}
	loopbackExemption.holders++
	loopbackExemption.mu.Unlock()

	tb.Cleanup(func() {
		loopbackExemption.mu.Lock()
		defer loopbackExemption.mu.Unlock()

		loopbackExemption.holders--
		if loopbackExemption.holders > 0 || !loopbackExemption.existed {
			return
		}
		_ = registry.Replace(loopbackExemption.original)
	})
}

// AllowLoopback is [allowLoopback] for a driver's own test package, which needs
// the exemption without needing a server this package started.
//
// Exported so that there is one of these rather than two. A second copy of a
// save-and-restore around a process global is a second copy of the bug
// [loopbackExemption] describes, and the engine package had one.
func AllowLoopback(tb testing.TB) {
	tb.Helper()

	allowLoopback(tb)
}

// loopbackExemptionHeld reports whether any test currently holds the loopback
// exemption, which is what the test pinning the counting asserts about.
func loopbackExemptionHeld() bool {
	loopbackExemption.mu.Lock()
	defer loopbackExemption.mu.Unlock()

	return loopbackExemption.holders > 0
}

// echoes returns an http step that posts body to the loopback server and records
// what came back under `said`.
//
// It is how a case observes a value that a condition cannot reach — see observe.go.
// Shaping the outputs down to one name keeps the assertion independent of the headers
// net/http adds.
func echoes(id, httpBaseURL, body string) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"method":  v1.NewLiteral(http.MethodPost),
				"url":     v1.NewLiteral(httpBaseURL + "/echo"),
				"body":    v1.NewExpr(body),
				"outputs": v1.NewExpr(`{"said": response.body}`),
			},
		}},
	}
}

// said is the outputs entry [echoes] produces.
func said(value string) *v1.Node_Outputs {
	return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"said": v1.NewLiteral(value)}}
}

// Workflows returns the workflows shared between the local and Temporal
// execution tests, so both drivers are held to identical expectations. The
// httpBaseURL should come from [NewHTTPServer].
//
// Keeping one definition for both drivers is what catches divergence between
// `flow run local` and durable execution; duplicating them would let the two
// drift apart silently.
func Workflows(httpBaseURL string) []Case {
	return []Case{
		{
			// The smallest thing this system can do: one step, which runs.
			Name: "a single step runs",
			Workflow: &v1.Workflow{
				Name:  "simple",
				Steps: []*v1.Node{says("a", "hello world")},
			},
			ExpectedOutputs: held("a"),
		},
		{
			// A step reading the step before it, which is the join that makes a
			// workflow more than a list. Bare `a.said` rather than `steps.a.said`
			// deliberately: both spellings resolve and only one of them is tested
			// elsewhere.
			Name: "a step reads the step before it",
			Workflow: &v1.Workflow{
				Name: "simple",
				Steps: append(
					[]*v1.Node{echoes("a", httpBaseURL, `"hello world"`)},
					pins("b", `a.said == "hello world"`)...,
				),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": said("hello world"),
				"b": {},
			}},
		},
		{
			// What `printf:` was for, in the spelling that replaced it. `format` is
			// specified at the CEL level rather than by Go's fmt, which is the
			// determinism story a task wrapping fmt could never tell.
			Name: "a formatted string is an expression",
			Workflow: &v1.Workflow{
				Name:    "simple",
				Profile: v1.CurrentProfile,
				Vars: map[string]*v1.Value{
					"greeting": v1.NewExpr(`"%s %s".format(["hello", "world"])`),
				},
				Steps: pins("a", `vars.greeting == "hello world"`),
			},
			ExpectedOutputs: held("a"),
		},
		{
			Name: "simple http workflow",
			Workflow: &v1.Workflow{
				Name: "simple",
				Steps: append([]*v1.Node{
					{
						Id: "web",
						Kind: &v1.Node_Task{
							Task: &v1.Task{
								Name: "http",
								Inputs: map[string]*v1.Value{
									"url":    v1.NewLiteral(httpBaseURL + "/status/200"),
									"method": v1.NewLiteral(http.MethodGet),
									// Shape the response to just the status code so the
									// assertion does not depend on headers net/http adds.
									"outputs": v1.NewExpr("{'status_code': response.status_code}"),
								},
							},
						},
					},
				}, pins("output", `string(web.status_code) == "200"`)...),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"web": {
						NamedValues: map[string]*v1.Value{
							"status_code": v1.NewLiteral(int64(200)),
						},
					},
					"output": {},
				},
			},
		},
		{
			// What `cel:` was for: composing a step's output into a new value. It is
			// written where the value is wanted now, rather than as a step whose only
			// job was to hold it.
			Name: "an expression composes a step's output",
			Workflow: &v1.Workflow{
				Name: "simple",
				Steps: append(
					[]*v1.Node{echoes("a", httpBaseURL, `"hello"`)},
					pins("b", `a.said + "!" == "hello!"`)...,
				),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": said("hello"),
				"b": {},
			}},
		},
		{
			// Exercises the default outputs — a workflow reads ${steps.web.body}
			// without declaring an outputs expression — while shaping the final
			// step so the assertion stays independent of headers net/http adds.
			Name: "http default body output workflow",
			Workflow: &v1.Workflow{
				Name: "http-defaults",
				Steps: append([]*v1.Node{
					{
						Id: "web",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "http",
							Inputs: map[string]*v1.Value{
								"url":     v1.NewLiteral(httpBaseURL + "/json"),
								"method":  v1.NewLiteral(http.MethodGet),
								"outputs": v1.NewExpr("{'body': response.body}"),
							},
						}},
					},
				}, pins("title", `json_parse(web.body)['slideshow']['title'] == "Sample Slide Show"`)...),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"web": {NamedValues: map[string]*v1.Value{
					"body": v1.NewLiteral(`{"slideshow":{"title":"Sample Slide Show"}}`),
				}},
				"title": {},
			}},
		},
	}
}

// ZeroValueCases exercise inputs and outputs that are legitimately empty.
//
// These are separated from [Workflows] because they assert a property rather
// than a fixed output: a zero value must survive a round trip. Empty strings,
// zero integers, and false booleans were previously rejected as invalid input
// and dropped from outputs, because the conversion layer tested whether an
// extracted value was non-zero instead of which kind it held.
//
// The round trip goes through the loopback server rather than through a local task,
// since no local task returns a value any more. That makes the trip longer and the
// property stronger: the empty string now survives a task input, a request, a
// response, an outputs expression and the run record.
func ZeroValueCases(httpBaseURL string) []Case {
	return []Case{
		{
			Name: "empty string message",
			Workflow: &v1.Workflow{
				Name:  "empty-string",
				Steps: []*v1.Node{echoes("a", httpBaseURL, `""`)},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": said(""),
			}},
		},
		{
			// The empty string having come from somewhere rather than been written
			// where it is used, which is the shape that used to drop it: a var is
			// evaluated, carried, and only then handed to a task.
			Name: "empty string through an expression",
			Workflow: &v1.Workflow{
				Name:    "empty-string-expr",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"blank": v1.NewExpr(`""`)},
				Steps: append(
					[]*v1.Node{echoes("a", httpBaseURL, "vars.blank")},
					pins("b", `a.said == "" && vars.blank == ""`)...,
				),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": said(""),
				"b": {},
			}},
		},
		{
			// An empty argument to a format, which is where an empty value is easiest
			// to lose: it is inside a list, inside an expression, and the result is a
			// string that looks the same whether the argument arrived or not.
			Name: "an empty format argument",
			Workflow: &v1.Workflow{
				Name:    "empty-format",
				Profile: v1.CurrentProfile,
				Steps:   pins("a", `"[%s]".format([""]) == "[]"`),
			},
			ExpectedOutputs: held("a"),
		},
		{
			// Zero and false, which the conversion layer used to drop for the same
			// reason it dropped the empty string.
			Name: "zero and false survive",
			Workflow: &v1.Workflow{
				Name:    "zero-and-false",
				Profile: v1.CurrentProfile,
				Vars: map[string]*v1.Value{
					"count":   v1.NewLiteral(int64(0)),
					"enabled": v1.NewLiteral(false),
				},
				Steps: pins("a", `vars.count == 0 && vars.enabled == false`),
			},
			ExpectedOutputs: held("a"),
		},
	}
}
