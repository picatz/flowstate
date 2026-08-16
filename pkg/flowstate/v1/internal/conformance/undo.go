package conformance

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/types/known/durationpb"
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

	// UnorderedPrefix is how many leading entries of Recorded are compared as a
	// set rather than a sequence. Zero for every sequential case. A concurrent
	// case sets it to the number of work requests its children make, because
	// which child's request reaches the server first is the scheduler's choice
	// on the durable driver and no claim of this corpus; the compensations that
	// follow the prefix are the claim, and stay order-exact. Use
	// [AssertRecorded] rather than comparing Recorded directly.
	UnorderedPrefix int
}

// AssertRecorded compares what the recording server received against a case's
// expectation, honouring [UndoCase.UnorderedPrefix]. One comparison, shared by
// both drivers, so the two cannot come to disagree about what "in order" means.
func AssertRecorded(t testing.TB, c UndoCase, got []string) {
	t.Helper()

	require.Len(t, got, len(c.Recorded),
		"the number of effects is not what compensating should have produced")

	n := c.UnorderedPrefix
	require.ElementsMatch(t, c.Recorded[:n], got[:n],
		"the concurrent work that ran is not the set that should have run")
	require.Equal(t, c.Recorded[n:], got[n:],
		"the effects that happened, and their order, are not what compensating should have produced")
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

func recordsItem(id, base, iterator string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
		Name: "http", Inputs: map[string]*v1.Value{
			"url":     v1.NewExpr(`"` + base + `/do/" + ` + iterator),
			"outputs": v1.NewExpr(`{"said": response.body}`),
		},
	}}}
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
		{
			// A run can still fail after its last step succeeded: the declared
			// outputs are evaluated at the end, and a run that cannot produce the
			// answer it promised has not succeeded. Every other case here fails
			// *during* a step, which is the path both drivers already reached
			// compensation through — so this is the one that says the moment a
			// failure arrives does not decide whether the world gets put back.
			//
			// Neither driver compensated here before: a FAILED run with every
			// resource it created still standing, which is the outcome the
			// feature exists to prevent. The completion-time payload bound
			// returns on this same path, which is why it is the same case.
			Name: "a run that fails after its last step still compensates",
			Workflow: &v1.Workflow{
				Name:    "undo-outputs",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					undoing(records("first", base, "a"), base, "/do/undo"),
					undoing(records("second", base, "b"), base, "/do/undo"),
				},
				DeclaredOutputs: []*v1.OutputDeclaration{
					{Name: "answer", Value: v1.NewExpr(`1 / 0`)},
				},
			},
			Fails:    true,
			Summary:  `; compensation ran in reverse order: undid "second", undid "first"`,
			Recorded: []string{"a", "b", "undo-b", "undo-a"},
		},
		{
			Name: "for_each compensations use reverse input order rather than completion order",
			Workflow: &v1.Workflow{Name: "undo-for-each-order", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				{Id: "fan", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items: v1.NewExpr(`["a", "b"]`), Iterator: "item", MaxParallel: 2,
					Body: []*v1.Node{undoing(recordsItem("inner", base, "item"), base, "/do/undo")},
				}}},
				fails("boom", base, "boom"),
			}},
			Fails: true, Summary: `; compensation ran in reverse order: undid "inner", undid "inner"`,
			Recorded:        []string{"a", "b", "boom", "undo-b", "undo-a"},
			UnorderedPrefix: 3,
		},
		{
			// The failure path of the merge: an iteration that dies after
			// registering still hands its private log to the parent, so what the
			// iteration did before failing comes back off. A merge that only
			// happened at a successful join would leak exactly the work a failed
			// fan-out leaves behind, which is the case sagas exist for.
			Name: "a failing iteration still gives back what it did",
			Workflow: &v1.Workflow{Name: "undo-for-each-partial", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				{Id: "fan", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items: v1.NewExpr(`["a"]`), Iterator: "item",
					Body: []*v1.Node{
						undoing(recordsItem("inner", base, "item"), base, "/do/undo"),
						fails("boom", base, "boom"),
					},
				}}},
			}},
			Fails: true, Summary: `; compensation ran in reverse order: undid "inner"`,
			Recorded: []string{"a", "boom", "undo-a"},
		},
		{
			// A concurrent fan-out has launched every iteration before it can
			// learn that one failed, so the iteration past the failing index
			// still runs, registers, and is taken back on both drivers. The
			// failing iteration's step registers nothing, exactly as a failed
			// top-level step registers nothing; the sibling's compensation is
			// the point. One activity per iteration, because the recorded
			// sequence is asserted exactly and one-per-child is the shape whose
			// scheduling order the corpus has already proven deterministic.
			Name: "a concurrent fan-out compensates iterations past the failure",
			Workflow: &v1.Workflow{Name: "undo-for-each-past-failure", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				{Id: "fan", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items: v1.NewExpr(`["boom", "b"]`), Iterator: "item", MaxParallel: 2,
					Body: []*v1.Node{undoing(&v1.Node{Id: "inner", Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "http", Inputs: map[string]*v1.Value{
							"url":     v1.NewExpr(`item == "boom" ? "` + base + `/fail/boom" : "` + base + `/do/" + item`),
							"outputs": v1.NewExpr(`{"said": response.body}`),
						},
					}}}, base, "/do/undo")},
				}}},
			}},
			Fails: true, Summary: `; compensation ran in reverse order: undid "inner"`,
			Recorded:        []string{"boom", "b", "undo-b"},
			UnorderedPrefix: 2,
		},
		{
			// Branches are concurrent by declaration, so a failing first branch
			// hides neither the work nor the compensation of its sibling: the
			// second branch runs and its registration comes back off.
			Name: "a failing branch does not hide its sibling",
			Workflow: &v1.Workflow{Name: "undo-parallel-sibling", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				{Id: "both", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
					{Steps: []*v1.Node{fails("kaboom", base, "kaboom")}},
					{Steps: []*v1.Node{undoing(records("right", base, "right"), base, "/do/undo")}},
				}}}},
			}},
			Fails: true, Summary: `; compensation ran in reverse order: undid "right"`,
			Recorded:        []string{"kaboom", "right", "undo-right"},
			UnorderedPrefix: 2,
		},
		{
			// #418's slice 0.5, pinned: `undo:` unwinds in reverse *written*
			// order, never reverse completion order. If completion order could
			// reach the unwind, "quick" would come off last; the written-order
			// rule says it comes off first, because its branch is written last.
			// The compensations after the prefix are order-exact, which is the
			// whole claim.
			//
			// The `sleep:` opening the first branch is what makes the forcing
			// *deterministic* rather than probable. A branch that is merely
			// longer — more chained activities — still leaves the scheduler
			// free to finish it first, and in that execution a completion-order
			// merge produces the very order this case expects, so the
			// regression it pins against would pass unseen. A timer cannot be
			// raced past: the durable test environment advances virtual time
			// only once no activity is runnable, so the second branch's step
			// has completed — and registered into its private log — before the
			// first branch starts any work at all. Locally the branches are
			// sequential and the hold is a short real sleep, so nothing blocks
			// on work that has not started yet.
			Name: "parallel siblings unwind in reverse written order not completion order",
			Workflow: &v1.Workflow{Name: "undo-parallel-written-order", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				{Id: "both", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
					{Steps: []*v1.Node{
						{Id: "hold", Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_Duration{Duration: durationpb.New(100 * time.Millisecond)},
						}}},
						undoing(records("slow_a", base, "a1"), base, "/do/undo"),
						undoing(records("slow_b", base, "a2"), base, "/do/undo"),
					}},
					{Steps: []*v1.Node{undoing(records("quick", base, "c"), base, "/do/undo")}},
				}}}},
				fails("boom", base, "boom"),
			}},
			Fails: true,
			Summary: `; compensation ran in reverse order: undid "quick", undid "slow_b", ` +
				`undid "slow_a"`,
			Recorded:        []string{"a1", "a2", "c", "boom", "undo-c", "undo-a2", "undo-a1"},
			UnorderedPrefix: 3,
		},
		{
			// The membership half of the same rule. Written order decides where
			// a registration sits in the unwind; it never invents one for a
			// sibling that failed — registration still happens only at a step's
			// completion, so the failing branch's `undo:` stays absent while
			// the completed sibling's runs. Without this, reverse-written order
			// could be misread as a walk over the file.
			Name: "a failed parallel sibling registers no compensation",
			Workflow: &v1.Workflow{Name: "undo-parallel-failed-sibling", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				{Id: "both", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
					{Steps: []*v1.Node{func() *v1.Node {
						node := fails("kaboom", base, "kaboom")
						node.Undo = &v1.Compensation{Task: &v1.Task{
							Name:   "http",
							Inputs: map[string]*v1.Value{"url": v1.NewLiteral(base + "/do/undo-kaboom")},
						}}

						return node
					}()}},
					{Steps: []*v1.Node{undoing(records("right", base, "right"), base, "/do/undo")}},
				}}}},
			}},
			Fails: true, Summary: `; compensation ran in reverse order: undid "right"`,
			Recorded:        []string{"kaboom", "right", "undo-right"},
			UnorderedPrefix: 2,
		},
		{
			// Sequential steps and a parallel block unwind as one written
			// order: the step after the block comes off first, then the
			// block's branches in reverse written order, then the step before
			// it. The prefix covers only the block's two concurrent requests
			// plus the sequential step before them for the leading-window
			// mechanics; everything after — including every compensation — is
			// order-exact.
			Name: "sequential and parallel steps unwind as one reverse written order",
			Workflow: &v1.Workflow{Name: "undo-mixed-written-order", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				undoing(records("before", base, "s"), base, "/do/undo"),
				{Id: "both", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
					{Steps: []*v1.Node{undoing(records("left", base, "l"), base, "/do/undo")}},
					{Steps: []*v1.Node{undoing(records("right", base, "r"), base, "/do/undo")}},
				}}}},
				undoing(records("after", base, "t"), base, "/do/undo"),
				fails("boom", base, "boom"),
			}},
			Fails: true,
			Summary: `; compensation ran in reverse order: undid "after", undid "right", ` +
				`undid "left", undid "before"`,
			Recorded:        []string{"s", "l", "r", "t", "boom", "undo-t", "undo-r", "undo-l", "undo-s"},
			UnorderedPrefix: 3,
		},
		{
			// #418 slice 1 against #418 slice 0.5: the same reverse-written-order
			// rule, now with steps whose *joins* happen in an order the text does
			// not have. `use_b` is written before `use_a`, so the second async
			// step is joined first — and an implementation that appended each
			// compensation where it was joined would build the log as
			// setup, b, a and unwind a before b. Written order says the opposite,
			// and it is the whole claim: the reader of a failed run must be told
			// what was taken back in the order the file can be read in, not in
			// the order a scheduler happened to resolve things.
			//
			// The forcing needs no timer, unlike the `parallel:` case above, and
			// that is what makes this deterministic on both drivers: the
			// disagreement is created by the *text* (which step reads which)
			// rather than by which coroutine finished first, so a local run whose
			// async work is sequential reproduces it exactly.
			Name: "async steps unwind in reverse written order not join order",
			Workflow: &v1.Workflow{Name: "undo-async-written-order", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				undoing(records("setup", base, "s"), base, "/do/undo"),
				async(undoing(records("a", base, "a"), base, "/do/undo")),
				async(undoing(records("b", base, "b"), base, "/do/undo")),
				joins("use_b", base, "ub", "b"),
				joins("use_a", base, "ua", "a"),
				fails("boom", base, "boom"),
			}},
			Fails: true,
			Summary: `; compensation ran in reverse order: undid "b", undid "a", ` +
				`undid "setup"`,
			Recorded: []string{
				"s", "a", "b", "ub-b", "ua-a", "boom",
				"undo-b", "undo-a", "undo-s",
			},
			// Everything up to the failure is a set: which of the two async
			// requests reaches the server first is the scheduler's choice and no
			// claim of this corpus, and the two readers are only ordered relative
			// to the step each joins. The compensations after it are order-exact,
			// which is the claim.
			UnorderedPrefix: 5,
		},
		{
			// An effect that happened is compensable even though nothing joined
			// it. `a` is started and the scope leaves on a failure before any
			// step reads it — so the scope's end joins it on the way out, and the
			// compensation the step registered when it succeeded comes off.
			// Without the drain on the failing path this is the case that would
			// leave a provisioned thing behind, or register it after the unwind
			// had already run, depending on scheduling.
			Name: "an async step nothing joined is still compensated",
			Workflow: &v1.Workflow{Name: "undo-async-unjoined", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				async(undoing(records("a", base, "a"), base, "/do/undo")),
				fails("boom", base, "boom"),
			}},
			Fails: true, Summary: `; compensation ran in reverse order: undid "a"`,
			Recorded:        []string{"a", "boom", "undo-a"},
			UnorderedPrefix: 2,
		},
		{
			// The membership half, for async: written order decides where a
			// registration sits in the unwind and never invents one. The async
			// step fails, so its `undo:` stays absent while the sequential step
			// written after it — which ran, because a failure not yet joined
			// cannot stop anything — is taken back.
			Name: "a failed async step registers no compensation",
			Workflow: &v1.Workflow{Name: "undo-async-failed", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				func() *v1.Node {
					node := async(fails("kaboom", base, "kaboom"))
					node.Undo = &v1.Compensation{Task: &v1.Task{
						Name:   "http",
						Inputs: map[string]*v1.Value{"url": v1.NewLiteral(base + "/do/undo-kaboom")},
					}}

					return node
				}(),
				undoing(records("right", base, "right"), base, "/do/undo"),
				joins("reads_it", base, "never", "kaboom"),
			}},
			Fails: true, Summary: `; compensation ran in reverse order: undid "right"`,
			Recorded:        []string{"kaboom", "right", "undo-right"},
			UnorderedPrefix: 2,
		},
		{
			// A compensation's inputs are a reference site, so they join like any
			// other. `later` names nothing outstanding in its own task — its URL is
			// a literal — and its `undo:` names `steps.a.said`, which is the async
			// step still in flight. The join analysis has to see that, because the
			// order of events is what makes missing it unrecoverable: the step runs,
			// its effect happens, and *then* the compensation is resolved against a
			// scope that does not hold `a` yet. The run fails there having performed
			// an effect it has no registered way to take back, which is the one
			// outcome the undo invariant exists to forbid.
			//
			// With the join in place `a` is published before `later` runs, so `a`'s
			// token arrives first and the compensation resolves — the order is exact
			// rather than a set, which is itself the assertion.
			Name: "a compensation input joins the async step it names",
			Workflow: &v1.Workflow{Name: "undo-async-undo-input", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				async(records("a", base, "a")),
				func() *v1.Node {
					node := records("later", base, "later")
					node.Undo = &v1.Compensation{Task: &v1.Task{
						Name: "http",
						Inputs: map[string]*v1.Value{
							"url": v1.NewExpr(`"` + base + `/do/undo-later-" + steps.a.said`),
						},
					}}

					return node
				}(),
				fails("boom", base, "boom"),
			}},
			Fails: true, Summary: `; compensation ran in reverse order: undid "later"`,
			Recorded: []string{"a", "later", "boom", "undo-later-a"},
		},
	}
}

// async marks a step to be started where it is written and joined where it is
// read.
func async(node *v1.Node) *v1.Node {
	node.Async = true

	return node
}

// joins returns a step that posts a token and, in posting it, names an async
// step's output — which is what joins that step here.
//
// The reference is in the URL rather than in a condition so that the join and
// the effect are the same request: the recorded token carries the joined step's
// own answer (`ub-b`), so a case cannot pass by joining the wrong step.
func joins(id, base, token, step string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
		Name: "http",
		Inputs: map[string]*v1.Value{
			"url":     v1.NewExpr(`"` + base + `/do/` + token + `-" + steps.` + step + `.said`),
			"outputs": v1.NewExpr(`{"said": response.body}`),
		},
	}}}
}

// UndoCancellationCase is a workflow cancelled while it is parked, paired with
// what compensating it did.
//
// A separate type from [UndoCase] because the thing under test is a different
// outcome, not a different failure: these runs end CANCELED rather than FAILED,
// and `Fails` would have to be read as "was stopped", which is precisely the
// conflation `flow cancel` exists to avoid.
//
// What the driver supplies, and why it is not in here: *when* the cancellation
// arrives. Locally that is cancelling a context; durably it is a cancellation
// request delivered to a workflow. Those have nothing in common to write down, in
// the same way that running one compensation has nothing in common between a
// function call and an activity — which is why [v1.RunUndoLog] takes it as a
// parameter. What every case does share is that the run is parked on a long wait
// when it happens, so the cancellation lands at a known point rather than racing
// the steps.
type UndoCancellationCase struct {
	// Name of the case, used for test identification.
	Name string

	// Workflow is the specification to run. Every case here ends in a wait long
	// enough that the run is certainly still on it when the cancellation arrives.
	Workflow *v1.Workflow

	// Summary is the exact text [v1.UndoSummary] must produce for what was taken
	// back. Empty when nothing was.
	Summary string

	// Recorded is every token the recording server must have received, in order.
	Recorded []string

	// UnorderedPrefix is how many leading entries of Recorded are compared as a
	// set rather than a sequence, exactly as [UndoCase.UnorderedPrefix] is. Zero
	// for every sequential case; a case with an `async:` step sets it to the
	// number of work requests in flight together, because which of them reaches
	// the server first is the durable driver's scheduler's choice and no claim of
	// this corpus. The compensations that follow the prefix stay order-exact,
	// which is the claim. Use [AssertCancellationRecorded] rather than comparing
	// Recorded directly.
	UnorderedPrefix int
}

// AssertCancellationRecorded compares what the recording server received against a
// cancellation case's expectation. One comparison, shared by both drivers, for the
// reason [AssertRecorded] is: the two must not come to disagree about what "in
// order" means on the path where they have the least code in common.
func AssertCancellationRecorded(t testing.TB, c UndoCancellationCase, got []string) {
	t.Helper()

	AssertRecorded(t, UndoCase{
		Recorded:        c.Recorded,
		UnorderedPrefix: c.UnorderedPrefix,
	}, got)
}

// parks returns a step that waits far longer than any test will, so that a
// cancellation arriving at any moment finds the run on it.
func parks(id string) *v1.Node {
	return &v1.Node{
		Id:   id,
		Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Duration{Duration: durationpb.New(time.Hour)}}},
	}
}

// reaches returns a recording step with no compensation, written between the last
// compensated step and the wait.
//
// It exists to make the local driver's cancellation deterministic, and the reason
// is worth writing down because it is not obvious. The recording server logs a
// token when the *request arrives*, not when the step finishes, so a test that
// cancels the moment the last compensated step's token appears can be cancelling
// while that step is still in flight — and a step cancelled mid-flight never
// succeeds, so it registers no compensation and the run takes back one thing fewer.
// The assertion would fail for a reason that is entirely the test's.
//
// A token from a step with nothing to undo is the signal that the step before it
// has certainly finished. Cancelling during *this* step and cancelling at the wait
// are then indistinguishable in everything these cases assert, which is what makes
// them deterministic rather than merely usually right.
func reaches(id, base, token string) *v1.Node {
	return records(id, base, token)
}

// UndoCancellationCases are the shared cases for the compensation `flow cancel`
// triggers. Both drivers run every one of them.
//
// This is the half of saga compensation that shipped second, and it shipped
// because the CLI had been promising it since before there was anything to
// promise: `flow cancel --help` says a workload that has to release a lock or undo
// a partial change still does, and for one release it did not. A capability three
// user-facing surfaces describe and no code performs is worse than an absent one —
// an operator who cancels a half-finished provisioning run and is told cleanup ran
// stops looking for what is still allocated.
//
// The cases are deliberately the same shapes as [UndoCases], with the failure
// replaced by a cancellation. Compensation must not acquire a second personality
// depending on what triggered it: reverse registration order, every entry
// attempted, one summary. What is genuinely different is the scope it runs in and
// the bound on it, and neither is visible from here — which is the point. An
// author reading DSL.md learns one rule.
func UndoCancellationCases(base string) []UndoCancellationCase {
	notFound := func(token string) string {
		return `task "http" failed (InvalidInput): GET ` + base + `/fail/` + token + ` returned status 404`
	}

	return []UndoCancellationCase{
		{
			// The shape the feature is for, and the exact scenario the CLI's help
			// text describes: two things provisioned, somebody stops the run, and
			// both come back off in the opposite order.
			Name: "cancelling a parked run takes its steps back in reverse order",
			Workflow: &v1.Workflow{
				Name:    "undo-cancel-reverse",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					undoing(records("first", base, "a"), base, "/do/undo"),
					undoing(records("second", base, "b"), base, "/do/undo"),
					reaches("reached", base, "z"),
					parks("hold"),
				},
			},
			Summary:  `; compensation ran in reverse order: undid "second", undid "first"`,
			Recorded: []string{"a", "b", "z", "undo-b", "undo-a"},
		},
		{
			// The negative direction, and the one that would be easy to lose while
			// making the positive one work: a cancelled run with nothing registered
			// must take nothing back and must not invent a summary. A run parked at
			// a gate with no `undo:` anywhere in it is every workflow that predates
			// the feature.
			Name: "cancelling a run with no compensations takes nothing back",
			Workflow: &v1.Workflow{
				Name:    "undo-cancel-none",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					records("first", base, "a"),
					reaches("reached", base, "z"),
					parks("hold"),
				},
			},
			Recorded: []string{"a", "z"},
		},
		{
			// A compensation that cannot run must not stop the others, on this path
			// as on the failure path. Worth its own case rather than assumed from
			// [UndoCases] because this one runs on a context the cancellation does
			// not reach: an implementation that got the disconnected scope subtly
			// wrong would fail *every* entry here, and a suite that only asserted
			// "some compensation ran" would not see it.
			Name: "cancelling a run still undoes the rest when one compensation fails",
			Workflow: &v1.Workflow{
				Name:    "undo-cancel-partial",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					undoing(records("first", base, "a"), base, "/do/undo"),
					undoing(records("second", base, "b"), base, "/fail/undo"),
					reaches("reached", base, "z"),
					parks("hold"),
				},
			},
			Summary: `; compensation ran in reverse order: could not undo "second": ` +
				notFound("undo-b") + `, undid "first"`,
			Recorded: []string{"a", "b", "z", "undo-b", "undo-a"},
		},
		{
			// The two features joined rather than each half: `async:` and
			// cancellation. `a` is started where it is written, nothing reads it, and
			// the cancellation arrives while it is still the scope's outstanding
			// work — so what takes it back is the scope's end draining what it
			// started on a path that is leaving, followed by a compensation running
			// in a scope the cancellation does not reach. Either half missing loses
			// the same thing: an effect that happened, with nothing taken back.
			//
			// [UndoCases] has the failure-path twin of this ("an async step nothing
			// joined is still compensated"). Cancellation gets its own because it is
			// the path where the drain and the disconnected scope have to compose,
			// and the corpus had no case where an async step was outstanding when a
			// run was stopped.
			Name: "cancelling a run compensates an async step nothing joined",
			Workflow: &v1.Workflow{
				Name:    "undo-cancel-async-unjoined",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					async(undoing(records("a", base, "a"), base, "/do/undo")),
					reaches("reached", base, "z"),
					parks("hold"),
				},
			},
			Summary: `; compensation ran in reverse order: undid "a"`,
			// The two work requests overlap on the durable driver, so which arrives
			// first is the scheduler's; the compensation after them is the claim.
			Recorded:        []string{"a", "z", "undo-a"},
			UnorderedPrefix: 2,
		},
	}
}

// UndoPlacementCases are the shapes the engine refuses outright.
//
// Both remaining refusals are about what a compensation may attach to, never
// about where it sits: a step with no effect of its own (control flow, or the
// `call:` step itself) has nothing a compensation could take back. Position
// stopped being a reason to refuse when concurrent scopes gained a structural
// ordering key; a compensation inside a `for_each` body or a `parallel` branch
// is ordinary now, and [UndoCases] asserts the order it undoes in. They are run
// by both drivers because a refusal is behaviour. `flow validate` refuses the
// same shapes earlier and with a position, see `flowfile`'s tests, and this is
// the backstop for a specification that never went through a Flowfile at all.
func UndoPlacementCases(base string) []UndoCase {
	return []UndoCase{
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
		{
			// Issue #219 problem 1: a compensation on the `call:` step itself is
			// still refused — a call has no effect of its own to take back — but the
			// refusal must now say so and point at the callee's own steps, not lump
			// a call in with wait/parallel "no effect of their own" wording that
			// names the wrong construct.
			Name: "a compensation on a call step itself is refused",
			Workflow: &v1.Workflow{
				Name:    "undo-on-call",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					func() *v1.Node {
						node := callNode("provision", &v1.Workflow{
							Name:    "undo-on-call-callee",
							Profile: v1.CurrentProfile,
							Steps:   []*v1.Node{records("inner", base, "i")},
						}, nil)
						node.Undo = &v1.Compensation{Task: &v1.Task{
							Name:   "http",
							Inputs: map[string]*v1.Value{"url": v1.NewLiteral(base + "/do/undo-call")},
						}}
						return node
					}(),
				},
			},
			Fails: true,
		},
	}
}

// UndoCallCases are the shared cases for compensation composing *through* a
// `call:` boundary — issue #219's decision. Both drivers run every one of them.
//
// A callee's steps register onto the same run-level [v1.UndoLog] a top-level
// step's would (see [v1.UndoScopeCall]), so these are deliberately shaped like
// [UndoCases]' first case — two things provisioned, a later step fails, both
// come back off in reverse — except one of the two provisioning steps sits
// inside a called workflow. What is under test is that the boundary is
// transparent to that reversal: a compensation registered inside the callee
// undoes *after* one registered before the call, and before whatever the
// caller registered after the call returns, in exactly the reverse of the
// order the two sides actually ran in.
func UndoCallCases(base string) []UndoCase {
	notFound := func(token string) string {
		return `task "http" failed (InvalidInput): GET ` + base + `/fail/` + token + ` returned status 404`
	}

	calleeWith := func(steps ...*v1.Node) *v1.Workflow {
		return &v1.Workflow{
			Name:    "undo-call-callee",
			Profile: v1.CurrentProfile,
			Steps:   steps,
		}
	}

	return []UndoCase{
		{
			// The compose-through shape itself: a step before the call registers a
			// compensation, the call's callee registers one of its own, and a step
			// after the call fails. Reverse of registration order crosses the
			// boundary cleanly: the callee's compensation (registered second) runs
			// first, the caller's own (registered first) runs second — the ordering
			// a `for_each` or `parallel` cannot offer, because a call is sequential,
			// compile-time-vendored control flow rather than concurrent work.
			Name: "a callee's compensation composes onto the caller's stack and undoes in reverse across the boundary",
			Workflow: &v1.Workflow{
				Name:    "undo-call-reverse",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					undoing(records("before", base, "a"), base, "/do/undo"),
					callNode("provision", calleeWith(
						undoing(records("inner", base, "b"), base, "/do/undo"),
					), nil),
					fails("boom", base, "boom"),
				},
			},
			Fails:    true,
			Summary:  `; compensation ran in reverse order: undid "inner", undid "before"`,
			Recorded: []string{"a", "b", "boom", "undo-b", "undo-a"},
		},
		{
			// The callee registering more than one compensation, so the assertion
			// covers ordering *within* the callee as well as across its boundary:
			// the callee's own two steps undo in their own reverse order first,
			// nested inside the overall reverse-of-registration sequence.
			Name: "a callee's own steps undo in reverse among themselves, inside the caller's reversal",
			Workflow: &v1.Workflow{
				Name:    "undo-call-reverse-nested",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					undoing(records("before", base, "a"), base, "/do/undo"),
					callNode("provision", calleeWith(
						undoing(records("first", base, "b"), base, "/do/undo"),
						undoing(records("second", base, "c"), base, "/do/undo"),
					), nil),
					fails("boom", base, "boom"),
				},
			},
			Fails: true,
			Summary: `; compensation ran in reverse order: undid "second", undid "first", ` +
				`undid "before"`,
			Recorded: []string{"a", "b", "c", "boom", "undo-c", "undo-b", "undo-a"},
		},
		{
			// A failing compensation inside the callee must not stop the caller's
			// own compensation from running — the same "a failing compensation does
			// not stop the rest" rule [UndoCases] pins, now asserted across a call
			// boundary rather than only within one level.
			Name: "a compensation that fails inside a callee does not stop the caller's own",
			Workflow: &v1.Workflow{
				Name:    "undo-call-partial",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					undoing(records("before", base, "a"), base, "/do/undo"),
					callNode("provision", calleeWith(
						undoing(records("inner", base, "b"), base, "/fail/undo"),
					), nil),
					fails("boom", base, "boom"),
				},
			},
			Fails: true,
			Summary: `; compensation ran in reverse order: could not undo "inner": ` +
				notFound("undo-b") + `, undid "before"`,
			Recorded: []string{"a", "b", "boom", "undo-b", "undo-a"},
		},
		{
			// The negative direction: a call whose callee succeeds and whose caller
			// also succeeds must take nothing back, exactly as an all-succeeding
			// top-level run does in [UndoCases].
			Name: "a run that succeeds through a call compensates nothing",
			Workflow: &v1.Workflow{
				Name:    "undo-call-unused",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					undoing(records("before", base, "a"), base, "/do/undo"),
					callNode("provision", calleeWith(
						undoing(records("inner", base, "b"), base, "/do/undo"),
					), nil),
				},
			},
			Fails:    false,
			Recorded: []string{"a", "b"},
		},
	}
}

// rollout is the progressive-rollout loop #253 is about: a `loop:` carrying a
// traffic percentage 5 → 25 → 50, running `body` once per value.
//
// The ladder is written as a `? :` rather than as arithmetic so the three values
// are the ones an operator would actually roll out to, and so the tokens each
// iteration records name the percentage rather than an index — which is what lets
// the assertions below read the *order iterations were undone in* out of a flat
// list of effects.
func rollout(body ...*v1.Node) *v1.Node {
	return &v1.Node{
		Id: "rollout",
		Kind: &v1.Node_Loop{Loop: &v1.Loop{
			State:         "percent",
			Initial:       v1.NewLiteral(int64(5)),
			Update:        v1.NewExpr("percent == 5 ? 25 : 50"),
			Until:         v1.NewExpr("percent >= 50"),
			MaxIterations: 10,
			Body:          body,
		}},
	}
}

// shifts is a loop-body step that records the percentage the current iteration is
// shifting to, keeping it as `said` so the step's own `undo:` can name it.
//
// The URL is an expression over the loop's carried state rather than a literal,
// which is the whole reason this helper exists rather than [records]: a body step
// runs once per iteration, and a case that cannot tell one iteration's effect from
// another's cannot see an ordering bug — the #226 lesson, applied to a loop.
func shifts(id, base string) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"url":     v1.NewExpr(`"` + base + `/do/p" + string(percent)`),
				"outputs": v1.NewExpr(`{"said": response.body}`),
			},
		}},
	}
}

// UndoLoopCases are the shared cases for compensation inside a `loop:` body —
// issue #253's decision. Both drivers run every one of them.
//
// # What is under test, and why it needed its own set
//
// [UndoCases] and [UndoCallCases] each register at most one compensation per
// step, so "reverse of registration order" and "reverse of declaration order"
// agree in both, and either would pass a driver that reversed the wrong list. A
// loop body is the first placement where they come apart: one step registers
// three times, and nothing about the specification says which of the three comes
// off first. Only the order they ran in does.
//
// # The order is asserted twice, in two different currencies
//
// `Recorded` is the sequence of requests the world actually saw, so it catches a
// driver that undid the right set in the wrong order. `Summary` is the sentence
// the failed run reports, and one case below deliberately makes every
// compensation fail so that each entry in that sentence carries the URL naming
// its own iteration — because a summary of three successes reads `undid "shift"`
// three times, which is membership, not order. Asserting order through the text
// an operator reads is the point: a run that unwound 5% before 50% and said so
// would pass a set-shaped assertion and be a live incident.
func UndoLoopCases(base string) []UndoCase {
	notFound := func(token string) string {
		return `task "http" failed (InvalidInput): GET ` + base + `/fail/` + token + ` returned status 404`
	}

	return []UndoCase{
		{
			// The shape the issue is named for. Three iterations raise traffic,
			// a step after the loop fails, and the rollout comes back down in the
			// order an operator would do it by hand: 50, then 25, then 5.
			Name: "a loop body's compensations undo newest iteration first",
			Workflow: &v1.Workflow{
				Name:    "undo-loop-reverse",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					rollout(undoing(shifts("shift", base), base, "/do/undo")),
					fails("boom", base, "boom"),
				},
			},
			Fails:    true,
			Summary:  `; compensation ran in reverse order: undid "shift", undid "shift", undid "shift"`,
			Recorded: []string{"p5", "p25", "p50", "boom", "undo-p50", "undo-p25", "undo-p5"},
		},
		{
			// The same reversal, asserted through the *failure text* rather than
			// through the effects — see this set's doc. Every compensation is
			// pointed at `/fail/`, so each entry in [v1.UndoSummary]'s sentence
			// carries the URL of the iteration it belongs to, and the sentence is
			// only correct if 50 precedes 25 precedes 5.
			//
			// It is also the "a failing compensation does not stop the rest" rule
			// applied across iterations: a rollout that cannot undo 50% must still
			// try to undo 25% and 5%, because stopping leaves more traffic on the
			// new version than continuing does.
			Name: "the failure text names each iteration's compensation, newest first",
			Workflow: &v1.Workflow{
				Name:    "undo-loop-reverse-text",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					rollout(undoing(shifts("shift", base), base, "/fail/undo")),
					fails("boom", base, "boom"),
				},
			},
			Fails: true,
			Summary: `; compensation ran in reverse order: ` +
				`could not undo "shift": ` + notFound("undo-p50") + `, ` +
				`could not undo "shift": ` + notFound("undo-p25") + `, ` +
				`could not undo "shift": ` + notFound("undo-p5"),
			Recorded: []string{"p5", "p25", "p50", "boom", "undo-p50", "undo-p25", "undo-p5"},
		},
		{
			// #253's motivating composition, and the boundary the issue is filed
			// against: the compensated step is not in the loop body at all, it is
			// inside a workflow the loop body `call:`s. Before #253 this was the
			// remedy the loop refusal named and the one `IntoCall` refused.
			//
			// A top-level compensated step sits before the loop, so the assertion
			// covers both boundaries at once: the callee's three registrations come
			// off newest-first among themselves, and all three before the caller's
			// own — reverse of registration, straight through two levels of nesting.
			Name: "a callee's compensation reached by call from a loop body composes onto the run's stack",
			Workflow: &v1.Workflow{
				Name:    "undo-loop-call",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					undoing(records("before", base, "a"), base, "/do/undo"),
					rollout(callNode("shift", &v1.Workflow{
						Name:    "undo-loop-call-callee",
						Profile: v1.CurrentProfile,
						DeclaredInputs: []*v1.InputDeclaration{
							{Name: "percent", Type: v1.InputDeclaration_TYPE_INT, Required: true},
						},
						Steps: []*v1.Node{
							undoing(&v1.Node{
								Id: "inner",
								Kind: &v1.Node_Task{Task: &v1.Task{
									Name: "http",
									Inputs: map[string]*v1.Value{
										"url":     v1.NewExpr(`"` + base + `/do/p" + string(inputs.percent)`),
										"outputs": v1.NewExpr(`{"said": response.body}`),
									},
								}},
							}, base, "/do/undo"),
						},
					}, map[string]*v1.Value{"percent": v1.NewExpr("percent")})),
					fails("boom", base, "boom"),
				},
			},
			Fails: true,
			Summary: `; compensation ran in reverse order: undid "inner", undid "inner", ` +
				`undid "inner", undid "before"`,
			Recorded: []string{
				"a", "p5", "p25", "p50", "boom",
				"undo-p50", "undo-p25", "undo-p5", "undo-a",
			},
		},
		{
			// An iteration that fails part way through still has the part that
			// succeeded taken back. The second body step answers 404 once the
			// rollout reaches 25%, so iteration two registered a compensation for
			// `shift` and then failed — and that registration is exactly the one an
			// engine registering per *step* rather than per *success* would miss,
			// or that a driver unwinding only completed iterations would drop.
			//
			// It is also the case that would catch a driver reversing the
			// declaration list: `check` declares no compensation at all, so a
			// specification-shaped unwind has nothing to reverse and this passes
			// only if registration is what is being reversed.
			Name: "an iteration that fails part way through still undoes the part that succeeded",
			Workflow: &v1.Workflow{
				Name:    "undo-loop-partial-iteration",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					rollout(
						undoing(shifts("shift", base), base, "/do/undo"),
						&v1.Node{
							Id: "check",
							Kind: &v1.Node_Task{Task: &v1.Task{
								Name: "http",
								Inputs: map[string]*v1.Value{
									"url": v1.NewExpr(
										`"` + base + `/" + (percent >= 25 ? "fail" : "do") + "/c" + string(percent)`),
								},
							}},
						},
					),
				},
			},
			Fails:    true,
			Summary:  `; compensation ran in reverse order: undid "shift", undid "shift"`,
			Recorded: []string{"p5", "c5", "p25", "c25", "undo-p25", "undo-p5"},
		},
		{
			// The negative direction, which a set of failing rollouts would never
			// notice going wrong: a loop that runs to completion inside a run that
			// succeeds takes nothing back. An engine that unwound at the end of the
			// loop rather than on failure would pass every case above and roll back
			// every healthy deployment in production.
			Name: "a loop whose run succeeds compensates nothing",
			Workflow: &v1.Workflow{
				Name:    "undo-loop-unused",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					rollout(undoing(shifts("shift", base), base, "/do/undo")),
				},
			},
			Fails:    false,
			Recorded: []string{"p5", "p25", "p50"},
		},
	}
}
