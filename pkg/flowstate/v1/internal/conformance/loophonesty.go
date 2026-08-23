package conformance

import (
	"fmt"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The two honesty rules #157's question 3 asks of loop reporting, held on both
// drivers by the cases in this file:
//
//   - Exhaustion distinguishes failed from not-attempted. A loop that spends
//     its whole budget without `until:` holding fails — that part is old — and
//     the transcript entry it leaves behind carries the iterations that ran
//     under `results`, tolerated failures included, so a reader can tell an
//     iteration that ran and failed (present, with a step `error`) from one
//     that was never attempted (absent, because the budget was already spent).
//     A claim about something that did not happen is a wrong answer, not a
//     formatting choice — the same distinction `undo:` draws between "was
//     attempted and failed" and "was never attempted" ([v1.ErrUndoBudget]).
//   - A tolerated failed iteration names its `as:` binding. When a body step's
//     failure is tolerated by `continue_on_error:`, the recorded entry carries
//     the value the iteration ran with — a `for_each`'s item, a `loop:`'s
//     carried state — under [v1.StepErrorItemOutput], so "which records
//     failed" is read straight off the failures rather than reconstructed
//     downstream by set subtraction from their complement.
//
// Both rules live in one place each ([v1.LoopExhaustedError],
// [v1.AttachIterationBinding]) and both drivers are held to them here — the
// local driver in eval_test.go, the durable driver in the engine package —
// because each is exactly the kind of observable a rehearsal exists to predict.

// exhaustedRecordedError is the sentence an exhausted loop records, spelled out
// rather than rendered by [v1.LoopIterationLimitError] — a test that builds its
// expectation with the function under test agrees with any change to it,
// including a wrong one. Keep this in step with loop.go's constructor.
func exhaustedRecordedError(max int) string {
	return fmt.Sprintf(
		"loop ran its full budget of %d iterations without the `until:` condition becoming true; "+
			"the loop did not finish — raise `max_iterations:` if this many is legitimate, or check "+
			"that `until:` and `update:` can actually reach the stop condition",
		max)
}

// outOfBounds is the recorded text of the deliberate failure these cases use: a
// step's own `vars:` indexing past the end of a one-element list. The same
// server-free failure [PartialTranscriptCases] uses, whose sentence
// [ToleratedStepFailureCases] already pins across drivers.
const outOfBounds = `var "bad": evaluate expression: index out of bounds: 1`

// tolerated marks a node's failure as one the run continues past.
func tolerated(node *v1.Node) *v1.Node {
	node.Policy = &v1.StepPolicy{ContinueOnError: true}

	return node
}

// failsWhen returns a `log:` step whose own `vars:` fail exactly when cond
// holds, tolerated — the body step every case here watches. The failure is
// real (the var indexes out of bounds) and the sentence it records is
// [outOfBounds].
func failsWhen(id, cond string) *v1.Node {
	return tolerated(withVars(says(id, "attempting"), map[string]*v1.Value{
		"bad": v1.NewExpr(`[""][` + cond + ` ? 1 : 0]`),
	}))
}

// LoopExhaustionTranscriptCases cover what an exhausted loop's transcript entry
// says, which both drivers must answer identically (invariant 3).
//
// [PartialTranscriptCase]-shaped because the run fails on purpose and the value
// under test is what it hands back beside the failure. Compared whole, so a
// driver recording an iteration the loop never ran fails here exactly as one
// dropping an iteration it did — and the entry count doubles as the
// bound-was-reached assertion: exactly `max_iterations:` entries, the budget
// spent to its last trip and nothing past it ever attempted.
func LoopExhaustionTranscriptCases() []PartialTranscriptCase {
	return []PartialTranscriptCase{
		{
			// The mixed case: iteration 0 succeeds, iterations 1 and 2 fail and
			// are tolerated, and the budget of three is then spent. Each of the
			// three is reported as what it is — an empty entry ran, an entry
			// with a step `error` ran and failed (naming the state it ran with,
			// the other half of #157's question 3) — and the fourth iteration
			// has no entry because it never happened. The steps around the loop
			// pin the same rule at the run level: `before` ran and is recorded,
			// `after` never ran and is absent.
			Name: "an exhausted loop's entry reports ran, ran-and-failed, and never-attempted as three different things",
			Workflow: &v1.Workflow{
				Name:    "loop-exhausted-mix",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("before", "ran"),
					{
						Id: "grind",
						Kind: &v1.Node_Loop{Loop: &v1.Loop{
							State:         "s",
							Initial:       v1.NewLiteral("a"),
							Update:        v1.NewExpr(`s + "x"`),
							Until:         v1.NewExpr("false"),
							MaxIterations: 3,
							// Fails for every state longer than the initial one:
							// "a" succeeds, "ax" and "axx" fail and are tolerated.
							Body: []*v1.Node{failsWhen("work", "size(s) > 1")},
						}},
					},
					says("after", "unreachable"),
				},
			},
			Expected: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"before": {},
				"grind": {NamedValues: map[string]*v1.Value{
					"error": v1.NewLiteral(exhaustedRecordedError(3)),
					"results": v1.NewLiteralList(
						map[string]any{"work": map[string]any{}},
						map[string]any{"work": map[string]any{"error": outOfBounds, "item": "ax"}},
						map[string]any{"work": map[string]any{"error": outOfBounds, "item": "axx"}},
					),
				}},
			}},
		},
		{
			// The pure case: nothing in the body fails, the loop simply never
			// stops. Both budgeted iterations are recorded — carrying no `item`,
			// because a loop that binds nothing has nothing to attach — and the
			// exact comparison proves there are exactly two, the whole budget
			// and not one trip more.
			Name: "a stateless exhausted loop records every budgeted iteration and nothing beyond them",
			Workflow: &v1.Workflow{
				Name:    "loop-exhausted-stateless",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "solo",
						Kind: &v1.Node_Loop{Loop: &v1.Loop{
							Until:         v1.NewExpr("false"),
							MaxIterations: 2,
							Body:          []*v1.Node{says("tick", "still going")},
						}},
					},
				},
			},
			Expected: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"solo": {NamedValues: map[string]*v1.Value{
					"error": v1.NewLiteral(exhaustedRecordedError(2)),
					"results": v1.NewLiteralList(
						map[string]any{"tick": map[string]any{}},
						map[string]any{"tick": map[string]any{}},
					),
				}},
			}},
		},
	}
}

// ToleratedIterationIdentityCases cover the `as:` binding riding on a tolerated
// iteration failure, read back the way an author would read it: from the
// transcript, and from a later step's own expression.
//
// The idiom these retire is set subtraction — `inputs.records` minus the ids
// that succeeded — which reconstructed downstream, from its complement, a value
// the engine held at the moment it recorded the failure.
// `examples/data-enrichment` spells the direct form from a Flowfile; these hold
// both drivers to the shape it reads.
//
// httpBaseURL serves the one case that needs a task capable of *declaring*
// outputs under reserved-looking names: the decoy case, whose successful step
// shapes its response into outputs literally named `error` and `item`. That
// case is what pins the attachment to the walk's own record of tolerance — an
// implementation keyed on the presence of an output named `error` would misread
// the decoy as a failure and inject (and overwrite) `item` inside a successful
// step's declared shape, which is exactly the misclassification it must fail
// on.
func ToleratedIterationIdentityCases(httpBaseURL string) []Case {
	// The direct spelling: the failed iterations, named by their own failure
	// entries, with no reference to the input list at all.
	const failedItems = `steps.fan.results.filter(r, has(r.work.error)).map(r, r.work.item) == ["b"]`

	fan := func(maxParallel int32) *v1.Node {
		return &v1.Node{
			Id: "fan",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:       v1.NewLiteralList("a", "b", "c"),
				Iterator:    "record",
				MaxParallel: maxParallel,
				Body:        []*v1.Node{failsWhen("work", `record == "b"`)},
			}},
		}
	}

	// One entry per item: the failed one carries its error and its item, the
	// ones that succeeded carry neither.
	fanOutputs := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"results": v1.NewLiteralList(
			map[string]any{"work": map[string]any{}},
			map[string]any{"work": map[string]any{"error": outOfBounds, "item": "b"}},
			map[string]any{"work": map[string]any{}},
		),
	}}

	return []Case{
		{
			Name: "a tolerated for_each failure names its item, and a later step reads it",
			Workflow: &v1.Workflow{
				Name:    "foreach-failure-names-item",
				Profile: v1.CurrentProfile,
				Steps:   append([]*v1.Node{fan(0)}, pins("saw", failedItems)...),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"fan": fanOutputs,
				"saw": {},
			}},
		},
		{
			// The identical claim under declared concurrency, so the durable
			// driver's concurrent path is held to the sequential path's answer:
			// which scheduling ran an iteration must not change what its
			// failure entry names.
			Name: "a tolerated for_each failure names its item under max_parallel",
			Workflow: &v1.Workflow{
				Name:    "foreach-failure-names-item-parallel",
				Profile: v1.CurrentProfile,
				Steps:   append([]*v1.Node{fan(2)}, pins("saw", failedItems)...),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"fan": fanOutputs,
				"saw": {},
			}},
		},
		{
			// A `loop:`'s binding is its carried state — the same `as:` keyword
			// in the Flowfile — and a tolerated failure inside a loop that goes
			// on to *succeed* still names the state its iteration ran with.
			Name: "a tolerated loop-body failure names the state its iteration carried",
			Workflow: &v1.Workflow{
				Name:    "loop-failure-names-state",
				Profile: v1.CurrentProfile,
				Steps: append([]*v1.Node{
					{
						Id: "poll",
						Kind: &v1.Node_Loop{Loop: &v1.Loop{
							State:         "s",
							Initial:       v1.NewLiteral("a"),
							Update:        v1.NewExpr(`s + "x"`),
							Until:         v1.NewExpr("size(s) >= 2"),
							MaxIterations: 10,
							// Fails only on the first iteration, whose state is "a".
							Body: []*v1.Node{failsWhen("work", `s == "a"`)},
						}},
					},
				}, pins("saw", `steps.poll.results[0].work.item == "a" && !has(steps.poll.results[1].work.error)`)...),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"poll": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"work": map[string]any{"error": outOfBounds, "item": "a"}},
						map[string]any{"work": map[string]any{}},
					),
					"state": v1.NewLiteral("ax"),
				}},
				"saw": {},
			}},
		},
		{
			// The decoy: a step that SUCCEEDS while declaring outputs literally
			// named `error` and `item` — the http task shaping its response
			// into those names — beside a sibling that genuinely fails and is
			// tolerated. The successful step's declared shape must come through
			// untouched (its `item` still the value it declared, no binding
			// injected over it), while the real failure still names its item.
			// This is the case that keys the attachment to the driver's own
			// record of tolerance: an output-name heuristic reads the decoy as
			// a failure and overwrites `decoy` with the iteration's item, which
			// the exact comparison and the reading step both refuse.
			Name: "a successful step whose declared outputs are named error and item is not mistaken for a failure",
			Workflow: &v1.Workflow{
				Name:    "foreach-decoy-outputs",
				Profile: v1.CurrentProfile,
				Steps: append([]*v1.Node{
					{
						Id: "fan",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items:    v1.NewLiteralList("a", "b"),
							Iterator: "record",
							Body: []*v1.Node{
								{
									Id: "shape",
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name: "http",
										Inputs: map[string]*v1.Value{
											"method":  v1.NewLiteral("GET"),
											"url":     v1.NewLiteral(httpBaseURL + "/status/200"),
											"outputs": v1.NewExpr(`{"error": "all good", "item": "decoy"}`),
										},
									}},
								},
								failsWhen("work", `record == "b"`),
							},
						}},
					},
				}, pins("saw",
					`steps.fan.results[0].shape.item == "decoy" && steps.fan.results[1].shape.item == "decoy" && steps.fan.results[1].work.item == "b"`)...),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"fan": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{
							"shape": map[string]any{"error": "all good", "item": "decoy"},
							"work":  map[string]any{},
						},
						map[string]any{
							"shape": map[string]any{"error": "all good", "item": "decoy"},
							"work":  map[string]any{"error": outOfBounds, "item": "b"},
						},
					),
				}},
				"saw": {},
			}},
		},
		{
			// Exhaustion tolerated on the loop step itself: the run continues,
			// and the account is not just in the transcript but in scope — a
			// later step branches on the recorded `error` and counts the
			// iterations that ran, which is the "later steps can see it" half
			// of the claim applied to the exhaustion record.
			Name: "a tolerated exhausted loop leaves its account for later steps to read",
			Workflow: &v1.Workflow{
				Name:    "loop-exhausted-tolerated",
				Profile: v1.CurrentProfile,
				Steps: append([]*v1.Node{
					tolerated(&v1.Node{
						Id: "spin",
						Kind: &v1.Node_Loop{Loop: &v1.Loop{
							Until:         v1.NewExpr("false"),
							MaxIterations: 2,
							Body:          []*v1.Node{says("tick", "going")},
						}},
					}),
				}, pins("saw", "has(steps.spin.error) && size(steps.spin.results) == 2")...),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"spin": {NamedValues: map[string]*v1.Value{
					"error": v1.NewLiteral(exhaustedRecordedError(2)),
					"results": v1.NewLiteralList(
						map[string]any{"tick": map[string]any{}},
						map[string]any{"tick": map[string]any{}},
					),
				}},
				"saw": {},
			}},
		},
	}
}
