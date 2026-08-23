package conformance

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Cases for `log:`, run by both execution drivers.
//
// What these assert is the part a reader of the workflow's *result* can see, which for
// this task is deliberately almost nothing: a step ran, and it produced no values. That
// sounds like a weak claim and is the exact one worth pinning. A task returning nil
// outputs and a task returning empty outputs are indistinguishable in Go and different
// in the run record — one says the step contributed nothing, the other says the step did
// not happen — and the two drivers record step outputs through separate code paths, so
// nothing but a shared case checks they agree about which.
//
// Where the message *goes* is not observable from a workflow's result by design, and is
// tested against a captured logger where that is decided.

// LogCases returns the shared cases for the `log` task.
func LogCases() []Case {
	return []Case{
		{
			// A log step is present in the outputs and empty, at every level.
			Name: "a log step produces no values",
			Workflow: &v1.Workflow{
				Name:    "log-empty",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "plain",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "log",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("starting")},
						}},
					},
					{
						Id: "loud",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "log",
							Inputs: map[string]*v1.Value{
								"message": v1.NewLiteral("something is wrong"),
								"level":   v1.NewLiteral("error"),
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"plain": {},
				"loud":  {},
			}},
		},
		{
			// A message built from an expression, which is the ordinary shape — a
			// constant log line is a comment. This is also the case that would fail if
			// a driver forgot to resolve the task's inputs before running it, since
			// the message would arrive as an unevaluated expression.
			Name: "a log message is an expression like any other input",
			Workflow: &v1.Workflow{
				Name:    "log-expression",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"service": v1.NewLiteral("checkout")},
				Steps: []*v1.Node{
					{
						Id: "announce",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "log",
							Inputs: map[string]*v1.Value{
								"message": v1.NewExpr(`"deploying " + vars.service`),
								"fields":  v1.NewExpr(`{"service": vars.service}`),
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"announce": {},
			}},
		},
		{
			// A log step inside a loop, which is where the empty-outputs decision has a
			// visible consequence: the loop's `results` carries one entry per iteration
			// and each holds an empty mapping for the body step. A driver returning nil
			// here produces a differently shaped list, which is the divergence a
			// per-driver test would not see.
			Name: "a log step inside a loop still fills a results entry",
			Workflow: &v1.Workflow{
				Name:    "log-in-loop",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "each",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items:    v1.NewLiteralList("a", "b"),
							Iterator: "name",
							Body: []*v1.Node{
								{
									Id: "note",
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name:   "log",
										Inputs: map[string]*v1.Value{"message": v1.NewExpr("name")},
									}},
								},
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"each": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"note": map[string]any{}},
						map[string]any{"note": map[string]any{}},
					),
				}},
			}},
		},
	}
}
