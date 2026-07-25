package tests

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// PolicyCases exercise conditions and per-step policy.
//
// They live beside [Workflows] so both execution drivers are held to the same
// expectations. Control flow is exactly where local and durable execution would
// be most tempting to implement separately, and a condition that skipped a step
// in one and ran it in the other would make local runs actively misleading.
func PolicyCases() []Case {
	return []Case{
		{
			Name: "condition true runs the step",
			Workflow: &v1.Workflow{
				Name: "condition-true",
				Steps: []*v1.Node{
					{
						Id: "gate",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("go")},
						}},
					},
					{
						Id:        "guarded",
						Condition: v1.NewExpr("gate.result == 'go'"),
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("ran")},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate":    {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("go")}},
				"guarded": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("ran")}},
			}},
		},
		{
			// A skipped step produces no outputs at all, rather than an empty
			// entry. Recording it as present-but-empty would let a later
			// reference resolve to nothing instead of failing, which hides the
			// mistake of depending on a step that did not run.
			Name: "condition false skips the step",
			Workflow: &v1.Workflow{
				Name: "condition-false",
				Steps: []*v1.Node{
					{
						Id: "gate",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("stop")},
						}},
					},
					{
						Id:        "guarded",
						Condition: v1.NewExpr("gate.result == 'go'"),
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("ran")},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("stop")}},
			}},
		},
		{
			Name: "literal false condition skips the step",
			Workflow: &v1.Workflow{
				Name: "condition-literal",
				Steps: []*v1.Node{
					{
						Id: "always",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")},
						}},
					},
					{
						Id:        "never",
						Condition: v1.NewLiteral(false),
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("nope")},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"always": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hi")}},
			}},
		},
		{
			// An unknown task is a permanent failure, so this also pins that
			// continue_on_error tolerates a failure without retrying something
			// that cannot succeed.
			Name: "continue_on_error records the failure and proceeds",
			Workflow: &v1.Workflow{
				Name: "continue-on-error",
				Steps: []*v1.Node{
					{
						Id:     "flaky",
						Policy: &v1.StepPolicy{ContinueOnError: true},
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "nosuchtask",
							Inputs: map[string]*v1.Value{},
						}},
					},
					{
						Id: "after",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("still here")},
						}},
					},
				},
			},
			// The failure text is engine-specific, so only the surviving step is
			// asserted exactly; PolicyCaseFailedSteps names the step that must
			// report an error.
			ExpectedOutputs: nil,
		},
		{
			Name: "policy timeout and retry are accepted",
			Workflow: &v1.Workflow{
				Name: "policy-accepted",
				Steps: []*v1.Node{
					{
						Id: "quick",
						Policy: &v1.StepPolicy{
							Timeout: durationpb.New(30 * 1e9), // 30s
							Retry: &v1.RetryPolicy{
								MaxAttempts:        2,
								InitialInterval:    durationpb.New(1e6), // 1ms
								BackoffCoefficient: 2,
							},
						},
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("done")},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"quick": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("done")}},
			}},
		},
	}
}

// ControlFlowCases exercise loops and parallel branches.
//
// Both drivers run these. Nested control flow is where the two implementations
// differ most — one schedules concurrent activities, the other calls functions in
// order — so agreeing on the observable result is exactly the property worth
// pinning.
func ControlFlowCases() []Case {
	return []Case{
		{
			// A loop's results are a list, one element per iteration, each a map
			// of body step id to that step's outputs. Body outputs deliberately
			// do not leak into the enclosing scope, where multiple iterations
			// would overwrite each other.
			Name: "for_each over a literal list",
			Workflow: &v1.Workflow{
				Name: "loop-literal",
				Steps: []*v1.Node{
					{
						Id: "loop",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewExpr("['a', 'b']"),
							Body: []*v1.Node{
								{
									Id: "shout",
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name: "printf",
										Inputs: map[string]*v1.Value{
											"format": v1.NewLiteral("<%s>"),
											"args":   v1.NewExpr("[item]"),
										},
									}},
								},
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"loop": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"shout": map[string]any{"result": "<a>"}},
						map[string]any{"shout": map[string]any{"result": "<b>"}},
					),
				}},
			}},
		},
		{
			Name: "for_each over a previous step's output",
			Workflow: &v1.Workflow{
				Name: "loop-referenced",
				Steps: []*v1.Node{
					{
						Id: "src",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "cel",
							Inputs: map[string]*v1.Value{"expr": v1.NewLiteral("[1, 2, 3]")},
						}},
					},
					{
						Id: "double",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items:    v1.NewExpr("src.result"),
							Iterator: "n",
							Body: []*v1.Node{
								{
									Id: "calc",
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name:   "cel",
										Inputs: map[string]*v1.Value{"expr": v1.NewLiteral("n * 2")},
									}},
								},
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"src": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteralList(1, 2, 3)}},
				"double": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"calc": map[string]any{"result": 2}},
						map[string]any{"calc": map[string]any{"result": 4}},
						map[string]any{"calc": map[string]any{"result": 6}},
					),
				}},
			}},
		},
		{
			// An empty list is a normal outcome, not an error: a workload that
			// found nothing to do has succeeded.
			Name: "for_each over an empty list runs nothing",
			Workflow: &v1.Workflow{
				Name: "loop-empty",
				Steps: []*v1.Node{
					{
						Id: "loop",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewExpr("[]"),
							Body: []*v1.Node{
								{
									Id: "never",
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name:   "echo",
										Inputs: map[string]*v1.Value{"message": v1.NewLiteral("x")},
									}},
								},
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"loop": {NamedValues: map[string]*v1.Value{"results": v1.NewLiteralList()}},
			}},
		},
		{
			// Branch outputs are visible after the block, so a later step can
			// join them. Ordering between branches is not defined, which is why
			// the join reads both by name rather than relying on sequence.
			Name: "parallel branches join afterwards",
			Workflow: &v1.Workflow{
				Name: "parallel-join",
				Steps: []*v1.Node{
					{
						Id: "fan",
						Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
							Branches: []*v1.Parallel_Branch{
								{Steps: []*v1.Node{{
									Id: "left",
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name:   "echo",
										Inputs: map[string]*v1.Value{"message": v1.NewLiteral("L")},
									}},
								}}},
								{Steps: []*v1.Node{{
									Id: "right",
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name:   "echo",
										Inputs: map[string]*v1.Value{"message": v1.NewLiteral("R")},
									}},
								}}},
							},
						}},
					},
					{
						Id: "join",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "printf",
							Inputs: map[string]*v1.Value{
								"format": v1.NewLiteral("%s%s"),
								"args":   v1.NewExpr("[left.result, right.result]"),
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"left":  {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("L")}},
				"right": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("R")}},
				"join":  {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("LR")}},
			}},
		},
		{
			Name: "condition inside a loop body",
			Workflow: &v1.Workflow{
				Name: "loop-condition",
				Steps: []*v1.Node{
					{
						Id: "loop",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewExpr("['keep', 'skip']"),
							Body: []*v1.Node{
								{
									Id:        "act",
									Condition: v1.NewExpr("item == 'keep'"),
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name:   "echo",
										Inputs: map[string]*v1.Value{"message": v1.NewExpr("item")},
									}},
								},
							},
						}},
					},
				},
			},
			// The second iteration's body is skipped, so its result map is empty
			// rather than absent: the iteration still happened.
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"loop": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"act": map[string]any{"result": "keep"}},
						map[string]any{},
					),
				}},
			}},
		},
	}
}

// PolicyCaseFailedSteps maps a case name to the step that must report a failure
// through its `error` output, for cases whose exact failure text is not asserted.
func PolicyCaseFailedSteps() map[string]string {
	return map[string]string{
		"continue_on_error records the failure and proceeds": "flaky",
	}
}
