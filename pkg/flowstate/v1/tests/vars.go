package tests

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Cases for the workflow's `vars:` block, run by both execution drivers.
//
// Shared for the reason invariant 3 exists, and this feature is a sharper example of
// it than most: the two drivers reach the same state by genuinely different routes.
// The local driver evaluates vars in process, before the first step; the durable one
// evaluates them in an activity, because a profile pins which functions exist and not
// how cel-go implements them, and then carries the answer across Continue-As-New so a
// later segment cannot recompute a different one. Two routes to one observable, which
// is exactly the shape that drifts when each driver is tested on its own.

// VarsCases returns the shared cases for workflow-level `vars:`.
func VarsCases() []Case {
	return []Case{
		{
			// The plain case: a literal var read by a step.
			Name: "a literal var is readable as vars.<name>",
			Workflow: &v1.Workflow{
				Name:    "vars-literal",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1")},
				Steps: []*v1.Node{
					{
						Id: "show",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewExpr("vars.region")},
						}},
					},
				},
			},
			ExpectedOutputs: outputs(map[string]string{"show": "eu-west-1"}),
		},
		{
			// An expression var, which is where the two drivers' routes differ most:
			// this is the one that is evaluated in an activity durably and in process
			// locally, and both must produce the same string.
			Name: "an expression var is evaluated before any step runs",
			Workflow: &v1.Workflow{
				Name:    "vars-expression",
				Profile: v1.CurrentProfile,
				Vars: map[string]*v1.Value{
					"greeting": v1.NewExpr(`"hello " + "world"`),
				},
				Steps: []*v1.Node{
					{
						Id: "show",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewExpr("vars.greeting")},
						}},
					},
				},
			},
			ExpectedOutputs: outputs(map[string]string{"show": "hello world"}),
		},
		{
			// Every step sees the same set, which is what "ambient" means and what
			// distinguishes a var from a step output. A var read by the *last* step is
			// the case that would fail if vars were somehow scoped to where they are
			// first used.
			Name: "every step sees the same vars",
			Workflow: &v1.Workflow{
				Name:    "vars-ambient",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"tag": v1.NewLiteral("v2")},
				Steps: []*v1.Node{
					{
						Id: "first",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewExpr("vars.tag")},
						}},
					},
					{
						Id: "second",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewExpr("vars.tag")},
						}},
					},
				},
			},
			ExpectedOutputs: outputs(map[string]string{"first": "v2", "second": "v2"}),
		},
		{
			// A var reaches *inside* a loop, where the scope is rebuilt per iteration
			// and carried to an activity by hand. That hand-copying is what silently
			// dropped the iterator once; a var read from a loop body is the same code
			// path seen from the other namespace.
			Name: "a var is readable inside a loop body",
			Workflow: &v1.Workflow{
				Name:    "vars-in-loop",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"prefix": v1.NewLiteral("host-")},
				Steps: []*v1.Node{
					{
						Id: "each",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items:    v1.NewLiteralList("a", "b"),
							Iterator: "name",
							Body: []*v1.Node{
								{
									Id: "label",
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name: "echo",
										Inputs: map[string]*v1.Value{
											"message": v1.NewExpr("vars.prefix + name"),
										},
									}},
								},
							},
						}},
					},
				},
			},
			// The loop's own outputs, one entry per iteration.
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"each": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"label": map[string]any{"result": "host-a"}},
						map[string]any{"label": map[string]any{"result": "host-b"}},
					),
				}},
			}},
		},
	}
}

// outputs builds the step outputs an echo-only workflow produces.
func outputs(results map[string]string) *v1.Workflow_StepOutputs {
	values := make(map[string]*v1.Node_Outputs, len(results))
	for id, result := range results {
		values[id] = &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"result": v1.NewLiteral(result)},
		}
	}

	return &v1.Workflow_StepOutputs{StepValues: values}
}
