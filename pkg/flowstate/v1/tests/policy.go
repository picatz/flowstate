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

// ErrorTextCases pin what `${steps.<id>.error}` actually says, in both drivers.
//
// The other policy cases assert that a tolerated failure *happened* — which step
// carries an error output and which does not. That is the half a paging-shaped
// test would call the page rather than the walk: it stayed green while the two
// drivers recorded completely different sentences, because nothing compared
// them. The local driver recorded the task's own error and the durable driver
// recorded Temporal's envelope around it, with event ids that vary per run.
//
// So these assert the string literally, and deliberately not by calling
// [v1.StepErrorText] — a test that renders its expectation with the same
// function under test agrees with any change to it, including a wrong one.
// Writing the sentence out means changing it is a change somebody reads.
//
// Both a permanent and a retryable failure, because the durable driver reaches
// the recorded text through a different path when there has been more than one
// attempt, and a sentence that is stable on attempt one and not on attempt two
// is the same defect wearing a longer sleeve.
func ErrorTextCases(baseURL string) []Case {
	permanent := `task "http" failed (InvalidInput): GET ` + baseURL + `/status/404 returned status 404`
	retryable := `task "http" failed (Upstream): GET ` + baseURL + `/status/500 returned status 500`

	return []Case{
		{
			Name: "a tolerated permanent failure records what went wrong",
			Workflow: &v1.Workflow{
				Name: "error-text-permanent",
				Steps: []*v1.Node{
					{
						Id:     "flaky",
						Policy: &v1.StepPolicy{ContinueOnError: true},
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "http",
							Inputs: map[string]*v1.Value{
								"url": v1.NewValue(baseURL + "/status/404"),
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"flaky": v1.FailedStepOutputs(permanent),
				},
			},
		},
		{
			Name: "a tolerated retryable failure says the same thing on every attempt",
			Workflow: &v1.Workflow{
				Name: "error-text-retryable",
				Steps: []*v1.Node{
					{
						Id: "flaky",
						Policy: &v1.StepPolicy{
							ContinueOnError: true,
							Retry: &v1.RetryPolicy{
								MaxAttempts:     2,
								InitialInterval: durationpb.New(1e6), // 1ms, so the case is not a wait
							},
						},
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "http",
							Inputs: map[string]*v1.Value{
								"url": v1.NewValue(baseURL + "/status/500"),
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"flaky": v1.FailedStepOutputs(retryable),
				},
			},
		},
	}
}

// ToleratedStepFailureCases cover a non-task failure tolerated at the *outermost*
// step — the step that raised it is the step that forgives it.
//
// This is the direction [ErrorTextCases] and [NestedErrorTextCases] both miss.
// The first tolerates a classified task failure, which converges by construction:
// errors.As finds the TaskError through any wrapping, so both drivers render the
// canonical sentence. The second tolerates a failure raised one level *below* the
// tolerating step, so the position it carries is a nested one — `iteration 0:
// step "child": …` — which both drivers had reason to keep.
//
// Nothing covered the failure raised by the tolerating step itself, and two
// separate defects lived there.
//
// The first was that the local driver did not tolerate it at all when it came
// from a step's own `vars:`: that evaluation returned straight out of runNodes,
// one statement above the `continue_on_error` check, so the whole local run
// aborted where the durable run carried on. A local run stricter than production
// is the failure mode local runs exist to prevent.
//
// The second was what the text said once both drivers did tolerate it. The
// durable driver attached `step "<id>": ` where the failure was raised, so the
// step recorded its own id inside the value recorded *under* that id —
// `${steps.gate.error}` reading `step "gate": evaluating items: …` durably and
// `evaluating items: …` locally, for the same file. The position belongs to the
// level a failure passes out of, so a step that never passes it out records it
// without one; a run-level failure still names the step, because there nothing
// else does.
//
// Three raising sites, because each reaches its driver's tolerance check through
// different code and one being right says nothing about the others: a step's own
// `vars:`, a loop's `items:`, and a wait's `wait_until:`.
//
// A fourth belongs here and is deliberately absent: a *task's* `inputs:` failing
// at a tolerated top-level step. The two drivers disagree about more than the
// position there, and the disagreement is not this one's to fix. The local driver
// hands an unresolved task to the task itself when the scope binds no names
// (`Task.EvalInScope`), so the failure comes back classified —
// `task "log" failed (InvalidInput): field "message": …` — while the durable
// driver resolves inputs in workflow code before scheduling anything and reports
// `input "message": …`. Same file, same failure, two sentences and two error
// *kinds*. That is the recorded-text half of the roadmap's Z9(a) — where each
// driver resolves inputs — and it also decides whether the failure is retried, so
// it wants fixing at the resolution point rather than at the rendering one. A
// case pinning a text here would have to pin one of the two wrong answers.
//
// The nested corpus does cover the resolution failure, because a loop body's
// scope binds the iterator: local resolves inputs itself there and both drivers
// then say `input "message": …`. What is uncovered is precisely the top-level
// step, which is what makes this the same missing-direction shape as the rest.
func ToleratedStepFailureCases() []Case {
	// One expression for all four, indexing past the end of a list: it compiles,
	// it fails when evaluated, and it carries no TaskError — so what each case
	// pins is the wrapping its own site adds, and the four expectations differ by
	// exactly that.
	const oops = "['a'][5]"
	const evaluated = "evaluate expression: index out of bounds: 5"

	return []Case{
		{
			// Bug Z2.6: locally this aborted the run instead of being tolerated.
			Name: "a tolerated step vars failure records what went wrong and continues",
			Workflow: &v1.Workflow{
				Name: "tolerated-step-vars",
				Steps: []*v1.Node{
					func() *v1.Node {
						node := withVars(says("gate", "unreachable"), map[string]*v1.Value{
							"bad": v1.NewExpr(oops),
						})
						node.Policy = &v1.StepPolicy{ContinueOnError: true}

						return node
					}(),
					// The step after it proves the run continued rather than merely
					// recording something on its way out.
					says("after", "still here"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate":  v1.FailedStepOutputs(`var "bad": ` + evaluated),
				"after": {},
			}},
		},
		{
			Name: "a tolerated for_each items failure records what went wrong",
			Workflow: &v1.Workflow{
				Name: "tolerated-loop-items",
				Steps: []*v1.Node{
					{
						Id:     "gate",
						Policy: &v1.StepPolicy{ContinueOnError: true},
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewExpr(oops),
							Body:  []*v1.Node{says("never", "unreachable")},
						}},
					},
					says("after", "still here"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate":  v1.FailedStepOutputs("evaluating items: " + evaluated),
				"after": {},
			}},
		},
		{
			// A wait is the site with the most raising points — validation, the
			// deadline expression, the timer, the signal — and every one of them
			// was prefixed durably and bare locally.
			Name: "a tolerated wait_until failure records what went wrong",
			Workflow: &v1.Workflow{
				Name: "tolerated-wait-until",
				Steps: []*v1.Node{
					{
						Id:     "gate",
						Policy: &v1.StepPolicy{ContinueOnError: true},
						Kind: &v1.Node_Wait{Wait: &v1.Wait{
							Kind: &v1.Wait_Until{Until: v1.NewExpr(oops)},
						}},
					},
					says("after", "still here"),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"gate":  v1.FailedStepOutputs("evaluating wait_until: " + evaluated),
				"after": {},
			}},
		},
	}
}

// NestedErrorTextCases cover the failure that is *not* a classified task error,
// tolerated at an enclosing node.
//
// The task case above converges because errors.As reaches the TaskError through
// every structural wrapper, so both drivers render the canonical sentence and
// drop the position. A runtime expression failure has no TaskError to find, so
// the position is part of what the error says — and there the two drivers were
// still disagreeing after the task case was fixed: the local driver kept
// `iteration 0: step "child": …` from its %w chain, while the durable driver
// read the innermost recorded text straight out of the envelope and dropped
// every wrapper on the way.
//
// Worth its own set because it fails in the opposite direction from the task
// case: there the danger was keeping transport wrapping, here it is losing
// structural wrapping an author needs to know *which* iteration went wrong.
func NestedErrorTextCases() []Case {
	// Indexing past the end of a list: legal to compile, fails when evaluated,
	// and carries no TaskError because it never reaches a task.
	const recorded = `iteration 0: step "child": ` +
		`input "message": evaluate expression: index out of bounds: 5`

	return []Case{
		{
			Name: "a tolerated nested expression failure says which iteration",
			Workflow: &v1.Workflow{
				Name: "nested-error-text",
				Steps: []*v1.Node{
					{
						Id:     "outer",
						Policy: &v1.StepPolicy{ContinueOnError: true},
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewExpr("[1]"),
							Body: []*v1.Node{
								{
									Id: "child",
									Kind: &v1.Node_Task{Task: &v1.Task{
										Name: "log",
										Inputs: map[string]*v1.Value{
											"message": v1.NewExpr("['a'][5]"),
										},
									}},
								},
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"outer": v1.FailedStepOutputs(recorded),
				},
			},
		},
	}
}
