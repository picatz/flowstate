package tests

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
	}
}
