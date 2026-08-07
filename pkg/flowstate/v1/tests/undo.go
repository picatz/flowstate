package tests

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
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
			Name: "a compensation inside a for_each body is refused",
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
			// The refusal #253 made necessary rather than one it removed. A
			// `loop:` body accepts a compensation now, and a `loop:` may be
			// written inside a `for_each` body — only a loop directly inside
			// another loop is refused — so a loop is exactly the wrapper that
			// could route one around the concurrency refusal. That is #219's
			// escape hatch with `loop:` standing where `call:` stood, and
			// [v1.UndoScope.IntoLoop] is what closes it on both drivers.
			//
			// Written as the negative direction of [UndoLoopCases]' first case:
			// the identical body, one construct further out, must be refused.
			Name: "a loop inside a for_each body, whose body compensates, is refused",
			Workflow: &v1.Workflow{
				Name:    "undo-loop-inside-for-each",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "fan",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewExpr("['x']"),
							Body: []*v1.Node{
								{
									Id: "page",
									Kind: &v1.Node_Loop{Loop: &v1.Loop{
										State:         "n",
										Initial:       v1.NewLiteral(int64(0)),
										Update:        v1.NewExpr("n + 1"),
										Until:         v1.NewExpr("n >= 1"),
										MaxIterations: 5,
										Body:          []*v1.Node{undoing(records("inner", base, "i"), base, "/do/undo")},
									}},
								},
							},
						}},
					},
				},
			},
			Fails: true,
		},
		{
			// The same closure with a `call:` inside the loop inside the
			// fan-out, which is the composition an author reaching for #253's
			// motivating shape would actually write — and the one that has to
			// stay refused, because [v1.UndoScope.IntoCall] and
			// [v1.UndoScope.IntoLoop] compose in sequence: Concurrent survives
			// both.
			Name: "a call inside a loop inside a for_each body, whose callee compensates, is refused",
			Workflow: &v1.Workflow{
				Name:    "undo-call-in-loop-inside-for-each",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "fan",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewExpr("['x']"),
							Body: []*v1.Node{
								{
									Id: "page",
									Kind: &v1.Node_Loop{Loop: &v1.Loop{
										State:         "n",
										Initial:       v1.NewLiteral(int64(0)),
										Update:        v1.NewExpr("n + 1"),
										Until:         v1.NewExpr("n >= 1"),
										MaxIterations: 5,
										Body: []*v1.Node{
											callNode("provision", &v1.Workflow{
												Name:    "undo-call-in-loop-inside-for-each-callee",
												Profile: v1.CurrentProfile,
												Steps:   []*v1.Node{undoing(records("inner", base, "i"), base, "/do/undo")},
											}, nil),
										},
									}},
								},
							},
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
		{
			// Test that A cannot reach B, not that A can reach A: every other case
			// in this set proves a shape is refused when it is written directly;
			// this one proves the refusal survives a `call:` standing in between.
			// A callee's compensation composes onto the caller's stack when the
			// call is reached from the top level or from another call — but a call
			// reached from *inside* a `for_each` body must not become a way to
			// route a compensation around the concurrency refusal that already
			// applies there. See [v1.UndoScope.IntoCall], which composes the
			// callee's placement with the scope the call itself sits in rather
			// than always granting [v1.UndoScopeCall].
			Name: "a call inside a for_each body, whose callee compensates, is refused",
			Workflow: &v1.Workflow{
				Name:    "undo-call-inside-for-each",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "loop",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewExpr("['x']"),
							Body: []*v1.Node{
								callNode("provision", &v1.Workflow{
									Name:    "undo-call-inside-for-each-callee",
									Profile: v1.CurrentProfile,
									Steps:   []*v1.Node{undoing(records("inner", base, "i"), base, "/do/undo")},
								}, nil),
							},
						}},
					},
				},
			},
			Fails: true,
		},
		{
			// The same escape hatch, closed for a `parallel:` branch too.
			Name: "a call inside a parallel branch, whose callee compensates, is refused",
			Workflow: &v1.Workflow{
				Name:    "undo-call-inside-parallel",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "fan",
						Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
							Branches: []*v1.Parallel_Branch{
								{
									Steps: []*v1.Node{
										callNode("provision", &v1.Workflow{
											Name:    "undo-call-inside-parallel-callee",
											Profile: v1.CurrentProfile,
											Steps:   []*v1.Node{undoing(records("inner", base, "i"), base, "/do/undo")},
										}, nil),
									},
								},
							},
						}},
					},
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
