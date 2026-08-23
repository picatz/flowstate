package conformance

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// LoopCases are the shared cases that hold both drivers to one behaviour for the
// `loop:` primitive — a bounded loop that carries state between iterations until a
// condition holds or its ceiling is reached.
//
// Run by both the local driver ([flowstatev1] eval_test.go) and the durable driver
// (engine workflow_test.go), which is what makes "the loop counts down to the same
// three iterations, carries the same final state, and fails at the same ceiling"
// something the two cannot disagree about. A loop whose iteration count or state
// transitions differed between a local rehearsal and a durable run would be exactly
// the divergence invariant 3 exists to prevent — and the reason the ceiling and the
// outcome are read through one constant and one error constructor in loop.go.
//
// The cases lean on `results` and `state` — outputs the *engine* records rather than
// a task returns — so they need no task that returns a value: a `pins` body asserts
// its claim through a `log:` step, and the loop's own outputs carry everything under
// test.
func LoopCases() []Case {
	return []Case{
		{
			// A loop carrying an integer counts it down and stops when the condition
			// holds. `results` has one entry per iteration and `state` is the value the
			// loop was carrying when it stopped — the two outputs a loop reports.
			Name: "loop carries state and counts down to its condition",
			Workflow: &v1.Workflow{
				Name:    "loop-countdown",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "count",
						Kind: &v1.Node_Loop{Loop: &v1.Loop{
							State:         "n",
							Initial:       v1.NewLiteral(int64(3)),
							Update:        v1.NewExpr("n - 1"),
							Until:         v1.NewExpr("n <= 1"),
							MaxIterations: 100,
							// Runs each iteration (n is 3, 2, 1, all >= 1), so the body
							// output is `tick` every time and never `tick_else`.
							Body: pins("tick", "n >= 1"),
						}},
					},
				},
			},
			// Do-while: the body runs for n = 3, 2, 1 and then `until:` (1 <= 1) holds,
			// so three iterations, and the state carried at the stop is 1.
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"count": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"tick": map[string]any{}},
						map[string]any{"tick": map[string]any{}},
						map[string]any{"tick": map[string]any{}},
					),
					"state": v1.NewLiteral(int64(1)),
				}},
			}},
		},
		{
			// A loop that carries no state at all: it names none, so it binds nothing
			// and reports no `state` output — just `results`. `until:` is true after the
			// first iteration, which a do-while runs before it checks, so the body runs
			// exactly once.
			Name: "loop without carried state runs its body and stops",
			Workflow: &v1.Workflow{
				Name:    "loop-no-state",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "solo",
						Kind: &v1.Node_Loop{Loop: &v1.Loop{
							Until:         v1.NewExpr("true"),
							MaxIterations: 100,
							Body:          pins("once", "true"),
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"solo": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"once": map[string]any{}},
					),
				}},
			}},
		},
		{
			// The final iteration's `update:` is never evaluated, and both drivers
			// must agree on that. `until:` holds after the body of the iteration whose
			// state is `{i: 2}`, so the loop stops there — and its `update:` would
			// index `[10, 20][2]`, out of range, if it were evaluated. The local driver
			// returns before evaluating it; the durable driver used to evaluate it
			// after `until:` succeeded, which failed a run the local rehearsal
			// completed — invariant 3. This case fails on that driver and passes on
			// this one, so it holds them together: the run must complete, with three
			// iterations, on both.
			Name: "a loop never evaluates the final iteration's update",
			Workflow: &v1.Workflow{
				Name:    "loop-update-after-until",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "check",
						Kind: &v1.Node_Loop{Loop: &v1.Loop{
							State:   "acc",
							Initial: v1.NewExpr("{'i': 0}"),
							// Advances the counter and computes a value valid for every
							// index the loop actually updates from (0 and 1) and out of
							// range for the final one (2), which the loop must never reach
							// because `until:` stops it first.
							Update:        v1.NewExpr("{'i': acc.i + 1, 'check': [10, 20][acc.i]}"),
							Until:         v1.NewExpr("acc.i >= 2"),
							MaxIterations: 100,
							Body:          pins("tick", "acc.i >= 0"),
						}},
					},
				},
			},
			// Asserted through a predicate rather than exact outputs: the carried state
			// is a CEL map whose encoded entry order is not worth pinning, and what this
			// case is really about is that the run *completes* — which the harness's own
			// no-error check enforces on both drivers before this runs — with the body
			// having run three times.
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				results := out.GetStepValues()["check"].GetNamedValues()["results"]
				return len(results.GetLiteral().GetListValue().GetValues()) == 3
			},
		},
		{
			// The boundary the exhaustion cases sit one step past: a loop whose
			// `until:` holds on its final budgeted iteration *completes*, at
			// exactly the bound. Running max_iterations times and finishing is
			// success with a full `results`; running max_iterations times
			// without finishing is the distinct exhaustion failure
			// ([LoopExhaustionTranscriptCases]) — the trip count alone does not
			// decide, `until:` does, and both drivers must draw the line
			// between the two on the same side of the same iteration.
			Name: "a loop whose condition holds on its final budgeted iteration completes at the bound",
			Workflow: &v1.Workflow{
				Name:    "loop-exactly-at-bound",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "exact",
						Kind: &v1.Node_Loop{Loop: &v1.Loop{
							State:   "s",
							Initial: v1.NewLiteral("a"),
							Update:  v1.NewExpr(`s + "x"`),
							// Holds after the third body run (s is "a", "ax",
							// "axx"), which is the last trip the budget allows.
							Until:         v1.NewExpr("size(s) >= 3"),
							MaxIterations: 3,
							Body:          []*v1.Node{says("tick", "going")},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"exact": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"tick": map[string]any{}},
						map[string]any{"tick": map[string]any{}},
						map[string]any{"tick": map[string]any{}},
					),
					"state": v1.NewLiteral("axx"),
				}},
			}},
		},
		{
			// A loop whose condition never holds runs its whole budget and then fails,
			// distinctly. The bound is *reached* here, not merely respected: with
			// `until:` false and no state to change, the only way this run ends is by
			// exhausting its three iterations — so the failure is proof the ceiling was
			// hit, which is what makes it a case worth sharing rather than a unit test
			// of one driver.
			Name: "loop that never stops fails at its iteration ceiling",
			Workflow: &v1.Workflow{
				Name:    "loop-runaway",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "spin",
						Kind: &v1.Node_Loop{Loop: &v1.Loop{
							Until:         v1.NewExpr("false"),
							MaxIterations: 3,
							Body:          []*v1.Node{says("body", "still going")},
						}},
					},
				},
			},
			ExpectFailure: true,
		},
		{
			// #534's sibling: `init:`/`update:` are evaluated by the workflow
			// the same way a wait's shaped `outputs:` is, and the compiler
			// now refuses a bare `${secret(...)}` there — but a spec submitted
			// straight to the Run RPC never goes through the compiler, so the
			// runtime backstop is what actually protects it. [v1.EvalLoopValue]
			// has no case for `*Value_SecretRef` in its switch, so it falls to
			// the default arm and fails the run rather than leaking the
			// resolved value into carried state or silently treating it as
			// nothing.
			Name:                  "loop state holding a secret reference fails the run rather than leaking it",
			ExpectFailure:         true,
			ExpectedErrorContains: "unsupported loop value kind",
			Workflow: &v1.Workflow{
				Name:    "loop-init-secret-ref",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "accumulate",
						Kind: &v1.Node_Loop{Loop: &v1.Loop{
							State: "n",
							Initial: &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
								Scheme: "env", Name: "API_TOKEN",
							}}},
							Until:         v1.NewExpr("true"),
							MaxIterations: 100,
							Body:          []*v1.Node{says("body", "runs once")},
						}},
					},
				},
			},
		},
	}
}
