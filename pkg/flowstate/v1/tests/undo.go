package tests

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Saga compensation, held to one set of expectations across both drivers.
//
// This is the case set invariant 3 most needs, because compensation is where the
// two drivers have the most reason to be written separately: locally an undo is a
// function call in a deferred cleanup, durably it is an activity scheduled by a
// workflow that is on its way to failing. Everything an author can *see* about
// that has to be identical — which steps get undone, in what order, what a failing
// compensation does to the rest, and the sentence the failed run reports.
//
// Two things are asserted per case, and they answer different questions:
//
//   - Recorded is what the world saw. Each step and each compensation posts a
//     token to a recording server, so the assertion is over real side effects in
//     the real order rather than over the engine's own account of itself. A
//     compensation that was "registered" and never scheduled passes an assertion
//     about the summary and fails this one.
//   - Summary is what the person reading the failure sees. It is the exact output
//     of [v1.UndoSummary], which is the one renderer both drivers append to their
//     own failure — the same discipline `${steps.<id>.error}` is held to, and for
//     the same reason: an author on a laptop must be told what production would
//     tell them.
//
// Deliberately not asserted here: the whole failure message. The two drivers
// already differ before the summary — the durable one carries `engine: flowstate
// run failed:` and an activity envelope — and pinning that would pin a transport's
// rendering into a driver-agreement test, which `ErrorTextCases` explains at
// length is the wrong thing to hold onto.

// UndoCase is a workflow that fails, paired with what compensating it did.
type UndoCase struct {
	// Name of the case, used for test identification.
	Name string

	// Workflow is the specification to run. Every case here fails on purpose,
	// except the one whose point is that a run which succeeds compensates nothing.
	Workflow *v1.Workflow

	// Fails reports whether the run is expected to fail. False for the case that
	// pins the negative direction.
	Fails bool

	// Summary is the exact text [v1.UndoSummary] must produce, which both drivers
	// append to the failure they report. Empty when nothing was compensated.
	Summary string

	// Recorded is every token the recording server must have received, in order:
	// the steps that ran, then the compensations that ran.
	Recorded []string
}

// NewUndoServer starts a server that records what reached it, and returns its base
// URL along with a function reporting the tokens in the order they arrived.
//
// A recorder rather than the shared [NewHTTPServer], because these cases are about
// *effects* and their order. Every other case set in this package can assert on a
// step's outputs, which is enough when the question is what a step computed; it is
// not enough when the question is whether a compensation for a step that already
// finished actually ran, and after which other one.
//
// `/do/<token>` succeeds and records; `/fail/<token>` records and then answers 404,
// which the http task classifies as permanent — so a case that wants a failure gets
// one attempt rather than a retry schedule, and the recorded order stays the thing
// under test rather than a timing artefact.
func NewUndoServer(tb testing.TB) (string, func() []string) {
	tb.Helper()

	var (
		mu     sync.Mutex
		tokens []string
	)

	record := func(prefix string, r *http.Request) string {
		token := strings.TrimPrefix(r.URL.Path, prefix)
		mu.Lock()
		tokens = append(tokens, token)
		mu.Unlock()

		return token
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/do/", func(w http.ResponseWriter, r *http.Request) {
		token := record("/do/", r)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, token)
	})
	mux.HandleFunc("/fail/", func(w http.ResponseWriter, r *http.Request) {
		record("/fail/", r)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusNotFound)
	})

	srv := httptest.NewServer(mux)
	tb.Cleanup(srv.Close)

	allowLoopback(tb)

	return srv.URL, func() []string {
		mu.Lock()
		defer mu.Unlock()

		return append([]string(nil), tokens...)
	}
}

// records returns a step that posts a token and keeps it as `said`.
func records(id, base, token string) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"url":     v1.NewLiteral(base + "/do/" + token),
				"outputs": v1.NewExpr(`{"said": response.body}`),
			},
		}},
	}
}

// undoing attaches a compensation to a step, written the way an author writes one:
// a task that reads the step's *own* output to know what to take back.
//
// `${steps.<id>.said}` is the reference that only resolves here. Everywhere else in
// a file a step naming itself is a forward reference and is refused; inside its own
// `undo:` it is the ordinary case, because by then the step has finished. Using it
// in every case is deliberate — it means each of these would fail outright if
// registration ever stopped resolving against the step's own outputs.
func undoing(node *v1.Node, base, path string) *v1.Node {
	node.Undo = &v1.Compensation{Task: &v1.Task{
		Name: "http",
		Inputs: map[string]*v1.Value{
			"url": v1.NewExpr(`"` + base + path + `-" + steps.` + node.GetId() + `.said`),
		},
	}}

	return node
}

// fails returns a step whose request is answered 404, which the http task
// classifies as permanent — one attempt, no retry schedule.
func fails(id, base, token string) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name:   "http",
			Inputs: map[string]*v1.Value{"url": v1.NewLiteral(base + "/fail/" + token)},
		}},
	}
}

// UndoCases are the shared saga cases. Both drivers run every one of them.
func UndoCases(base string) []UndoCase {
	notFound := func(token string) string {
		return `task "http" failed (InvalidInput): GET ` + base + `/fail/` + token + ` returned status 404`
	}

	return []UndoCase{
		{
			// The shape the whole feature is for: two things provisioned, the third
			// fails, and the first two come back off in the opposite order.
			//
			// Reverse order is the assertion that matters and the one a sequential
			// implementation gets right by accident — so `Recorded` pins the actual
			// sequence of requests rather than the set.
			Name: "compensations run in reverse order when a later step fails",
			Workflow: &v1.Workflow{
				Name:    "undo-reverse",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					undoing(records("first", base, "a"), base, "/do/undo"),
					undoing(records("second", base, "b"), base, "/do/undo"),
					fails("third", base, "boom"),
				},
			},
			Fails:    true,
			Summary:  `; compensation ran in reverse order: undid "second", undid "first"`,
			Recorded: []string{"a", "b", "boom", "undo-b", "undo-a"},
		},
		{
			// A step that never ran has nothing to take back. The engine registers
			// on success rather than on declaration, and this is the direction that
			// distinguishes the two: a list built from the specification would undo
			// something that never happened.
			Name: "a skipped step registers no compensation",
			Workflow: &v1.Workflow{
				Name:    "undo-skipped",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					func() *v1.Node {
						node := undoing(records("skipped", base, "s"), base, "/do/undo")
						node.Condition = v1.NewLiteral(false)

						return node
					}(),
					undoing(records("ran", base, "r"), base, "/do/undo"),
					fails("boom", base, "boom"),
				},
			},
			Fails:    true,
			Summary:  `; compensation ran in reverse order: undid "ran"`,
			Recorded: []string{"r", "boom", "undo-r"},
		},
		{
			// A step that failed registers nothing either, even where
			// `continue_on_error:` let the run carry on past it.
			//
			// That is a real decision rather than an omission, and DSL.md argues it:
			// a failed effect may have partly applied and the engine cannot know, so
			// undoing it would be as likely to be wrong as right. What it must not do
			// is *pretend*, which is what registering a compensation for it would be.
			Name: "a step that failed registers no compensation",
			Workflow: &v1.Workflow{
				Name:    "undo-failed-step",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					undoing(records("good", base, "g"), base, "/do/undo"),
					func() *v1.Node {
						node := fails("tolerated", base, "t")
						node.Policy = &v1.StepPolicy{ContinueOnError: true}
						node.Undo = &v1.Compensation{Task: &v1.Task{
							Name:   "http",
							Inputs: map[string]*v1.Value{"url": v1.NewLiteral(base + "/do/undo-t")},
						}}

						return node
					}(),
					fails("boom", base, "boom"),
				},
			},
			Fails:    true,
			Summary:  `; compensation ran in reverse order: undid "good"`,
			Recorded: []string{"g", "t", "boom", "undo-g"},
		},
		{
			// Undoing three things where one cannot be undone must still undo the
			// others. Stopping at the first failure would leave *more* behind than
			// continuing, and would make how much is left behind depend on which
			// compensation happened to fail.
			//
			// The summary is the whole account: what came off, what did not, and why
			// — which is what somebody now cleaning up by hand needs, and the reason
			// successes are named rather than left implied by silence.
			Name: "a failing compensation does not stop the rest",
			Workflow: &v1.Workflow{
				Name:    "undo-partial",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					undoing(records("first", base, "a"), base, "/do/undo"),
					undoing(records("second", base, "b"), base, "/fail/undo"),
					fails("third", base, "boom"),
				},
			},
			Fails: true,
			Summary: `; compensation ran in reverse order: could not undo "second": ` +
				notFound("undo-b") + `, undid "first"`,
			Recorded: []string{"a", "b", "boom", "undo-b", "undo-a"},
		},
		{
			// The negative direction, which is the one a suite of failing runs would
			// never notice going wrong: a run that succeeds must take nothing back.
			// Registering compensations and running them at the end would pass every
			// case above and delete the work of every healthy run in production.
			Name: "a run that succeeds compensates nothing",
			Workflow: &v1.Workflow{
				Name:    "undo-unused",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					undoing(records("first", base, "a"), base, "/do/undo"),
					undoing(records("second", base, "b"), base, "/do/undo"),
				},
			},
			Fails:    false,
			Recorded: []string{"a", "b"},
		},
	}
}

// UndoPlacementCases are the shapes the engine refuses outright.
//
// They are run by both drivers because a refusal is behaviour: an engine that
// accepted one of these would run a saga whose compensations undo in one order
// locally and another in production, which is a local run lying about the one
// thing sagas exist to get right. `flow validate` refuses the same shapes earlier
// and with a position — see `flowfile`'s tests — and this is the backstop for a
// specification that never went through a Flowfile at all.
func UndoPlacementCases(base string) []UndoCase {
	return []UndoCase{
		{
			Name: "a compensation inside a loop body is refused",
			Workflow: &v1.Workflow{
				Name:    "undo-in-loop",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "loop",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewExpr("['x']"),
							Body:  []*v1.Node{undoing(records("inner", base, "i"), base, "/do/undo")},
						}},
					},
				},
			},
			Fails: true,
		},
		{
			Name: "a compensation on a step that is not a task is refused",
			Workflow: &v1.Workflow{
				Name:    "undo-on-control-flow",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "loop",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewExpr("['x']"),
							Body:  []*v1.Node{records("inner", base, "i")},
						}},
						Undo: &v1.Compensation{Task: &v1.Task{
							Name:   "http",
							Inputs: map[string]*v1.Value{"url": v1.NewLiteral(base + "/do/undo-loop")},
						}},
					},
				},
			},
			Fails: true,
		},
	}
}
