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
			// The gate is a loop because `results` is the only step output a case can
			// produce without a server since `echo` retired, and a condition reading a
			// *step* rather than a var is the path worth keeping under test.
			Name: "condition true runs the step",
			Workflow: &v1.Workflow{
				Name: "condition-true",
				Steps: []*v1.Node{
					counter("gate", "go"),
					guarded("guarded", "size(gate.results) == 1", "ran"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(map[string]any{"gate_body": map[string]any{}}),
				}},
				"guarded": {},
			}},
		},
		{
			// A skipped step produces no outputs at all, rather than an empty
			// entry. Recording it as present-but-empty would let a later
			// reference resolve to nothing instead of failing, which hides the
			// mistake of depending on a step that did not run.
			//
			// It is also what every `pins` pair in this package rests on, so this case
			// is load-bearing for the rest of them rather than only for itself.
			Name: "condition false skips the step",
			Workflow: &v1.Workflow{
				Name: "condition-false",
				Steps: []*v1.Node{
					counter("gate", "stop"),
					guarded("guarded", "size(gate.results) == 99", "ran"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(map[string]any{"gate_body": map[string]any{}}),
				}},
			}},
		},
		{
			Name: "literal false condition skips the step",
			Workflow: &v1.Workflow{
				Name: "condition-literal",
				Steps: []*v1.Node{
					says("always", "hi"),
					{
						Id:        "never",
						Condition: v1.NewLiteral(false),
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "log",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("nope")},
						}},
					},
				},
			},
			ExpectedOutputs: held("always"),
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
					says("after", "still here"),
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
							Name:   "log",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("done")},
						}},
					},
				},
			},
			ExpectedOutputs: held("quick"),
		},
	}
}

// ControlFlowCases exercise loops and parallel branches.
//
// Both drivers run these. Nested control flow is where the two implementations
// differ most — one schedules concurrent activities, the other calls functions in
// order — so agreeing on the observable result is exactly the property worth
// pinning.
//
// The base URL should come from [NewHTTPServer]; it is what lets a branch produce a
// value another step can join, now that no local task returns one.
func ControlFlowCases(httpBaseURL string) []Case {
	return []Case{
		{
			// A loop's results are a list, one element per iteration, each a map
			// of body step id to that step's outputs. Body outputs deliberately
			// do not leak into the enclosing scope, where multiple iterations
			// would overwrite each other.
			Name: "for_each over a literal list",
			Workflow: &v1.Workflow{
				Name:    "loop-literal",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "loop",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewExpr("['a', 'b']"),
							Body:  pins("shout", `"<%s>".format([item]) in ["<a>", "<b>"]`),
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"loop": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"shout": map[string]any{}},
						map[string]any{"shout": map[string]any{}},
					),
				}},
			}},
		},
		{
			// The list comes from a step rather than from the file, which is the
			// ordinary shape: something is fetched, then worked through.
			Name: "for_each over a previous step's output",
			Workflow: &v1.Workflow{
				Name:    "loop-referenced",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					echoes("src", httpBaseURL, `"[1, 2, 3]"`),
					{
						Id: "double",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items:    v1.NewExpr("json_parse(src.said)"),
							Iterator: "n",
							// Doubled as a double: a JSON number arrives as one, and CEL
							// has no int/double promotion — `n * 2` is `no such overload`
							// rather than 2, 4, 6.
							Body: pins("calc", `(n * 2.0) in [2.0, 4.0, 6.0]`),
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"src": said("[1, 2, 3]"),
				"double": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"calc": map[string]any{}},
						map[string]any{"calc": map[string]any{}},
						map[string]any{"calc": map[string]any{}},
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
							Body:  []*v1.Node{says("never", "x")},
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
				Steps: append([]*v1.Node{
					{
						Id: "fan",
						Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
							Branches: []*v1.Parallel_Branch{
								{Steps: []*v1.Node{echoes("left", httpBaseURL, `"L"`)}},
								{Steps: []*v1.Node{echoes("right", httpBaseURL, `"R"`)}},
							},
						}},
					},
				}, pins("join", `left.said + right.said == "LR"`)...),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"left":  said("L"),
				"right": said("R"),
				"join":  {},
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
							Body:  []*v1.Node{guarded("act", "item == 'keep'", "kept")},
						}},
					},
				},
			},
			// The second iteration's body is skipped, so its result map is empty
			// rather than absent: the iteration still happened.
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"loop": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"act": map[string]any{}},
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
