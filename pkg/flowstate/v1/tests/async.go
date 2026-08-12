package tests

import (
	"net/http"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Structured concurrency, held to one set of expectations across both drivers.
//
// `async: true` is the marker that lets execution depart from written order
// (issue #418), and the reason this corpus exists is that departing from written
// order is exactly the kind of thing two drivers implement separately and then
// disagree about. Locally an async step's work runs where it is written and its
// result is held until the join; durably it is a coroutine scheduling activities
// that genuinely overlap. Everything an author can *see* has to be identical
// anyway, and that is what these pin:
//
//   - Where an output becomes visible: at the join, never before it.
//   - Where a failure is heard: at the join, named for the step that failed.
//   - What a mention is: every syntactic one, including an `if:` that then skips
//     the step and a `has()` guard, which is the totality that keeps completion
//     order unobservable.
//   - What the scope's end does: joins whatever it started, so nothing is left
//     running and nothing is silently dropped.
//
// What is deliberately *not* here is anything about latency, which is the one
// thing `async:` is for and the one thing the two drivers legitimately differ
// about. The claim that an async step actually overlaps later work is a claim
// about the durable driver's concurrency and is asserted there, against its own
// scheduler, in engine's TestRunWorkflowAsyncOverlapsLaterWork.

// asyncEchoes is [echoes] marked async: the same step, started where it is
// written and joined where it is read.
func asyncEchoes(id, httpBaseURL, body string) *v1.Node {
	node := echoes(id, httpBaseURL, body)
	node.Async = true

	return node
}

// AsyncCases are the shared `async:` cases. Both drivers run every one of them.
func AsyncCases(httpBaseURL string) []Case {
	return []Case{
		{
			// The join, in its smallest form: a step that reads an async step's
			// output waits for it there, and the output is what it would have been
			// had nothing been marked at all.
			Name: "a reference to an async step joins it",
			Workflow: &v1.Workflow{
				Name:    "async-join",
				Profile: v1.CurrentProfile,
				Steps: append(
					[]*v1.Node{asyncEchoes("a", httpBaseURL, `"hello world"`)},
					pins("b", `a.said == "hello world"`)...,
				),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": said("hello world"),
				"b": {},
			}},
		},
		{
			// The N-graph, which is the shape the whole issue is about: `c` depends
			// on `a` only and `d` on `b` only, so neither waits for the other's
			// dependency. Written as two barriers this is `parallel: [a, b]` then
			// `parallel: [c, d]`, and `c` waits for `b` for no reason. Here the
			// edges are the references, and every output still comes out the same.
			//
			// It is also the case that would catch a join implemented as "wait for
			// everything outstanding": `d` reading `b` must not require `a`, and if
			// it did nothing here would fail — which is why the undo corpus carries
			// the ordering claim that *can* tell the difference.
			Name: "a crossing dependency joins only what it names",
			Workflow: &v1.Workflow{
				Name:    "async-n-graph",
				Profile: v1.CurrentProfile,
				Steps: append(
					[]*v1.Node{
						asyncEchoes("a", httpBaseURL, `"from a"`),
						asyncEchoes("b", httpBaseURL, `"from b"`),
					},
					append(
						pins("c", `a.said == "from a"`),
						pins("d", `b.said == "from b"`)...,
					)...,
				),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": said("from a"),
				"b": said("from b"),
				"c": {},
				"d": {},
			}},
		},
		{
			// The scope's end joins what it started. Nothing reads `a`, and its
			// output is in the run's record anyway — "fire and forget" does not
			// exist, only "fire and definitely collect before leaving".
			Name: "an async step nothing reads is joined at the end of the scope",
			Workflow: &v1.Workflow{
				Name:    "async-scope-end",
				Profile: v1.CurrentProfile,
				Steps: append(
					[]*v1.Node{asyncEchoes("a", httpBaseURL, `"unread"`)},
					pins("b", `1 == 1`)...,
				),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": said("unread"),
				"b": {},
			}},
		},
		{
			// A mention in an `if:` is a mention. The condition joins `a` and then
			// skips the step, which surprises authors expecting a skip to avoid the
			// wait and is correct: the data is what decided the skip. `b` is absent
			// from the record because it never ran; `a` is present because it did.
			Name: "a condition that names an async step joins it and may still skip",
			Workflow: &v1.Workflow{
				Name:    "async-condition-join",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					asyncEchoes("a", httpBaseURL, `"nope"`),
					func() *v1.Node {
						node := pins("b", `1 == 1`)[0]
						node.Condition = v1.NewExpr(`a.said == "yes"`)

						return node
					}(),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": said("nope"),
			}},
		},
		{
			// The presence check joins too, which is the total-join rule doing the
			// work it exists for: a `has()` that could answer "not finished yet"
			// would let a file observe completion order through a guard, which is
			// select/first-of-N wearing a guard's clothes. Here it answers about
			// the finished step, so it is true.
			Name: "a has() guard on an async step joins it",
			Workflow: &v1.Workflow{
				Name:    "async-has-join",
				Profile: v1.CurrentProfile,
				Steps: append(
					[]*v1.Node{asyncEchoes("a", httpBaseURL, `"guarded"`)},
					pins("b", `has(steps.a.said) && steps.a.said == "guarded"`)...,
				),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": said("guarded"),
				"b": {},
			}},
		},
		{
			// A failure is heard at the join and named for the step that failed,
			// not for the step that read it. Steps written between the two still
			// run, because the failure was not knowable where the step is written —
			// which is the same thing a `parallel:` block already does with a
			// failing branch, and the reason both drivers must run every launched
			// step rather than stopping at the written position.
			Name: "an async step's failure is heard where it is joined",
			Workflow: &v1.Workflow{
				Name:    "async-failure-at-join",
				Profile: v1.CurrentProfile,
				Steps: append([]*v1.Node{
					{
						Id:    "a",
						Async: true,
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "http",
							Inputs: map[string]*v1.Value{
								"url":    v1.NewLiteral(httpBaseURL + "/status/404"),
								"method": v1.NewLiteral(http.MethodGet),
							},
						}},
					},
				}, pins("b", `a.status_code == 404`)...),
			},
			ExpectFailure: true,
		},
		{
			// `continue_on_error:` means what it means on the same step written in
			// order: the failure is tolerated and recorded under the step's own id,
			// so a later step can branch on it. Being async moves *where* the
			// failure is heard and nothing else.
			Name: "a tolerated async failure is recorded under its own id",
			Workflow: &v1.Workflow{
				Name:    "async-tolerated",
				Profile: v1.CurrentProfile,
				Steps: append([]*v1.Node{
					{
						Id:     "a",
						Async:  true,
						Policy: &v1.StepPolicy{ContinueOnError: true},
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "http",
							Inputs: map[string]*v1.Value{
								"url":    v1.NewLiteral(httpBaseURL + "/status/404"),
								"method": v1.NewLiteral(http.MethodGet),
							},
						}},
					},
				}, pins("b", `has(steps.a.error)`)...),
			},
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				failed, recorded := out.GetStepValues()["a"]
				if !recorded {
					return false
				}
				_, hasError := failed.GetNamedValues()["error"]
				_, ran := out.GetStepValues()["b"]

				return hasError && ran
			},
		},
		{
			// The placement refusals, run rather than only validated: a
			// specification does not have to have come from a Flowfile, and the
			// alternative to refusing is a run that quietly does something other
			// than what it was asked. `flow validate` reports the same refusal with
			// a position, which is where an author meets it.
			Name: "async on a wait is refused by the engine",
			Workflow: &v1.Workflow{
				Name:    "async-on-wait",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{Id: "hold", Async: true, Kind: &v1.Node_Wait{Wait: &v1.Wait{
						Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "go"}},
					}}},
				},
			},
			ExpectFailure: true,
		},
		{
			Name: "async inside a parallel branch is refused by the engine",
			Workflow: &v1.Workflow{
				Name:    "async-in-parallel",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{Id: "both", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
						{Steps: []*v1.Node{asyncEchoes("a", httpBaseURL, `"nope"`)}},
					}}}},
				},
			},
			ExpectFailure: true,
		},
	}
}
