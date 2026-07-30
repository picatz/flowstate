package tests

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Cases for `vars:` at both of its positions, run by both execution drivers.
//
// Shared for the reason invariant 3 exists, and this feature is a sharper example of
// it than most: the two drivers reach the same state by genuinely different routes.
// The workflow's block is evaluated in process before the first step locally and in an
// activity durably, because a profile pins which functions exist and not how cel-go
// implements them — and then carried across Continue-As-New so a later segment cannot
// recompute a different answer. A step's block is evaluated just before the step in
// both, but the durable driver reaches it by swapping the executor's scope, so a
// nested executor built from the wrong one is a divergence only a shared case sees.

// VarsCases returns the shared cases for `vars:`, workflow-level and step-level.
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
		{
			// A step's own vars, which are bare rather than rooted — the other half of
			// the feature, and the half where the two drivers evaluate at different
			// moments: locally in process just before the step, durably in workflow
			// code just before the activity is scheduled.
			Name: "a step's own var is readable bare",
			Workflow: &v1.Workflow{
				Name:    "step-vars-bare",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id:   "greet",
						Vars: map[string]*v1.Value{"who": v1.NewExpr(`"world"`)},
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewExpr(`"hello " + who`)},
						}},
					},
				},
			},
			ExpectedOutputs: outputs(map[string]string{"greet": "hello world"}),
		},
		{
			// The same name declared by two steps with two values, which is the
			// property that makes a step's vars safe to name freely: they are private
			// to the step, so nothing about the first reaches the second.
			//
			// Positive on both sides deliberately — each step reads *its own* value, so
			// a leak shows up as a wrong string rather than as an unbound name. The
			// refusal of the case where a leak would be silent (a step naming something
			// already in scope) is a validation rule, tested where it is enforced.
			Name: "a step's vars are private to it",
			Workflow: &v1.Workflow{
				Name:    "step-vars-private",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id:   "first",
						Vars: map[string]*v1.Value{"tag": v1.NewLiteral("one")},
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewExpr("tag")},
						}},
					},
					{
						Id:   "second",
						Vars: map[string]*v1.Value{"tag": v1.NewLiteral("two")},
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewExpr("tag")},
						}},
					},
				},
			},
			ExpectedOutputs: outputs(map[string]string{"first": "one", "second": "two"}),
		},
		{
			// A step's var reads everything in scope where the step is written: the
			// workflow's rooted vars and the output of a step that already ran. This is
			// the case that separates the two blocks — a workflow var may reference
			// nothing, and a step var may reference whatever has happened by then.
			Name: "a step's var reads the workflow's vars and an earlier step",
			Workflow: &v1.Workflow{
				Name:    "step-vars-scope",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"sep": v1.NewLiteral("/")},
				Steps: []*v1.Node{
					{
						Id: "base",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("root")},
						}},
					},
					{
						Id: "path",
						Vars: map[string]*v1.Value{
							"joined": v1.NewExpr(`steps.base.result + vars.sep + "leaf"`),
						},
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewExpr("joined")},
						}},
					},
				},
			},
			ExpectedOutputs: outputs(map[string]string{"base": "root", "path": "root/leaf"}),
		},
		{
			// Vars on the loop *node*, visible to its items expression and throughout
			// its body — the lexical reading, and the one the durable driver reaches by
			// a different route, since it swaps the executor's scope and every nested
			// executor is built from it.
			Name: "a loop's own vars are visible in its body",
			Workflow: &v1.Workflow{
				Name:    "loop-vars",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "each",
						Vars: map[string]*v1.Value{
							"suffix": v1.NewLiteral("!"),
							"names":  v1.NewLiteralList("a", "b"),
						},
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items:    v1.NewExpr("names"),
							Iterator: "name",
							Body: []*v1.Node{
								{
									Id: "shout",
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name: "echo",
										Inputs: map[string]*v1.Value{
											"message": v1.NewExpr("name + suffix"),
										},
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
						map[string]any{"shout": map[string]any{"result": "a!"}},
						map[string]any{"shout": map[string]any{"result": "b!"}},
					),
				}},
			}},
		},
		{
			// A var declared on a step *inside* a loop body, reading the iterator. The
			// binding is rebuilt per iteration and the var is evaluated against it, so
			// this fails if a step's vars were computed once and reused — which is what
			// happens if they are bound anywhere but immediately before the step runs.
			Name: "a var inside a loop body reads the iterator",
			Workflow: &v1.Workflow{
				Name:    "loop-body-step-vars",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "each",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items:    v1.NewLiteralList("a", "b"),
							Iterator: "name",
							Body: []*v1.Node{
								{
									Id:   "upper",
									Vars: map[string]*v1.Value{"loud": v1.NewExpr("name.upperAscii()")},
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name:   "echo",
										Inputs: map[string]*v1.Value{"message": v1.NewExpr("loud")},
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
						map[string]any{"upper": map[string]any{"result": "A"}},
						map[string]any{"upper": map[string]any{"result": "B"}},
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
