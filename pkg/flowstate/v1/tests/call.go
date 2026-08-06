package tests

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Cases for `call:`, run by both execution drivers.
//
// A call is the one node that runs a *workflow* rather than a piece of one, so
// what these pin is exactly the three rules [v1.CallScope], [v1.CallOutputs]
// and [v1.CheckCallDepth] state and both drivers reach: what the callee can
// see, what comes back under the step's id, and how deep this may go. Isolation
// is asserted in the negative direction — a callee referencing a caller's step
// or var *fails* — because a case that only shows a callee reading its own
// arguments would pass even if the isolation were never enforced at all.

// simpleCallee is a callee with one required input, one step, and one declared
// output — the shape most of the cases below call.
func simpleCallee(name string) *v1.Workflow {
	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "who", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
		},
		Steps: []*v1.Node{says("greet", "hi")},
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "greeting", Value: v1.NewExpr(`"hello " + inputs.who`)},
		},
	}
}

// callNode returns a `call:` step, id "provision", binding arguments against
// callee.
func callNode(id string, callee *v1.Workflow, arguments map[string]*v1.Value) *v1.Node {
	return &v1.Node{
		Id:   id,
		Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee, Arguments: arguments}},
	}
}

// CallCases returns the shared cases for `call:`.
// loopingCallee is a workflow whose single step is a `loop:` — a top-level loop,
// which is the shape `flow validate` accepts. Used to prove that a loop reached
// *through a call* runs on both drivers, which is the remedy `validateNamedLoop`
// points a nested-loop author at.
func loopingCallee(name string) *v1.Workflow {
	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id: "page",
				Kind: &v1.Node_Loop{Loop: &v1.Loop{
					State:         "n",
					Initial:       v1.NewLiteral(int64(0)),
					Update:        v1.NewExpr("n + 1"),
					Until:         v1.NewExpr("n >= 2"),
					MaxIterations: 10,
					Body:          []*v1.Node{says("tick", "inner")},
				}},
			},
		},
	}
}

func CallCases() []Case {
	return []Case{
		{
			// A loop may reach another loop *through a call*, which is the remedy
			// validateNamedLoop names for a nested loop: the callee is an isolated
			// unit with its own frame handling, so its loop is top-level within it and
			// runs atomically inside each of the outer loop's iterations. Asserted on
			// both drivers, because the diagnostic promising this works must not be a
			// promise only the local driver keeps. Run by TestRunWorkflowCall in both
			// the v1 (local) and engine (durable) packages.
			Name: "a loop may call a workflow that itself loops",
			Workflow: &v1.Workflow{
				Name:    "loop-call-loop",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "rounds",
						Kind: &v1.Node_Loop{Loop: &v1.Loop{
							State:         "round",
							Initial:       v1.NewLiteral(int64(0)),
							Update:        v1.NewExpr("round + 1"),
							Until:         v1.NewExpr("round >= 1"),
							MaxIterations: 5,
							Body:          []*v1.Node{callNode("sub", loopingCallee("inner-loop"), nil)},
						}},
					},
				},
			},
			// Two outer rounds (round 0 and 1), each running the inner loop to
			// completion. Asserted through a predicate: what matters is that the run
			// completes — the harness's no-error check enforces that on both drivers —
			// with the outer loop reporting both rounds.
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				results := out.GetStepValues()["rounds"].GetNamedValues()["results"]
				return len(results.GetLiteral().GetListValue().GetValues()) == 2
			},
		},
		{
			// The plain case: a call's declared outputs come back under the step's
			// own id, exactly as a task's would, and an argument is resolved in the
			// caller's own scope — a literal here, which is the simplest scope there
			// is.
			Name: "a call's outputs come back under the step id",
			Workflow: &v1.Workflow{
				Name:    "call-outputs",
				Profile: v1.CurrentProfile,
				Steps: append(
					[]*v1.Node{callNode("provision", simpleCallee("callee-outputs"), map[string]*v1.Value{
						"who": v1.NewLiteral("world"),
					})},
					pins("check", `steps.provision.greeting == "hello world"`)...,
				),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"provision": {NamedValues: map[string]*v1.Value{
					"greeting": v1.NewLiteral("hello world"),
				}},
				"check": {},
			}},
		},
		{
			// An argument resolved against a *step's* output in the caller's scope,
			// not a literal — the case that would pass even with no scope threading
			// at all if the only argument ever tested were a literal.
			Name: "a call's argument is resolved in the caller's scope",
			Workflow: &v1.Workflow{
				Name:    "call-argument-scope",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"name": v1.NewLiteral("caller")},
				Steps: append(
					[]*v1.Node{callNode("provision", simpleCallee("callee-argument-scope"), map[string]*v1.Value{
						"who": v1.NewExpr(`vars.name`),
					})},
					pins("check", `steps.provision.greeting == "hello caller"`)...,
				),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"provision": {NamedValues: map[string]*v1.Value{
					"greeting": v1.NewLiteral("hello caller"),
				}},
				"check": {},
			}},
		},
		{
			// A callee that declares no outputs produces none — legal, and the shape
			// a workflow called for its effects takes.
			Name: "a callee with no outputs produces none",
			Workflow: &v1.Workflow{
				Name:    "call-no-outputs",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					callNode("effect", &v1.Workflow{
						Name:    "callee-no-outputs",
						Profile: v1.CurrentProfile,
						Steps:   []*v1.Node{says("only", "did a thing")},
					}, nil),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"effect": {},
			}},
		},
		{
			// Isolation, asserted in the negative direction: a callee referencing a
			// caller's step by id must fail to resolve, because [v1.CallScope] hands
			// it a fresh scope holding only its bound arguments and the profile. A
			// case that only showed a callee reading its own arguments successfully
			// would pass whether or not this isolation was ever enforced.
			Name: "a callee cannot reference the caller's steps",
			Workflow: &v1.Workflow{
				Name:    "call-isolation-steps",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					says("caller_step", "caller ran"),
					callNode("leaky", &v1.Workflow{
						Name:    "callee-reads-caller-step",
						Profile: v1.CurrentProfile,
						Steps:   pins("peek", `steps.caller_step.said == "caller ran"`),
					}, nil),
				},
			},
			ExpectFailure: true,
		},
		{
			// The other half of isolation: a callee cannot read a var the *caller*
			// declared, even though both files share the same profile and the same
			// `vars.` root spelling.
			Name: "a callee cannot reference the caller's vars",
			Workflow: &v1.Workflow{
				Name:    "call-isolation-vars",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"secret": v1.NewLiteral("do-not-leak")},
				Steps: []*v1.Node{
					callNode("leaky", &v1.Workflow{
						Name:    "callee-reads-caller-vars",
						Profile: v1.CurrentProfile,
						Steps:   pins("peek", `vars.secret == "do-not-leak"`),
					}, nil),
				},
			},
			ExpectFailure: true,
		},
		{
			// A callee's own failure, tolerated at the call step: recorded under
			// `error` the same way any other tolerated step's is, and prefixed
			// `workflow "<name>": ...` — [v1.CheckCallDepth]'s sibling rules, named
			// after the callee for the same reason a loop iteration's failure names
			// its index, so a reader is not left looking through the caller for a
			// step that lives in a different file. Checked through a condition,
			// which is what makes this one case both drivers can run through the
			// ordinary [Case] harness: the property under test is the *prefix*,
			// which is deterministic, rather than a CEL engine's exact wording for
			// an unresolved name, which is not this package's contract to pin.
			Name: `a callee's failure is tolerated and named after it`,
			Workflow: &v1.Workflow{
				Name:    "call-failure-text",
				Profile: v1.CurrentProfile,
				Steps: append(
					[]*v1.Node{
						{
							Id: "provision",
							Kind: &v1.Node_Call{Call: &v1.Call{
								Workflow: callFailureCallee(),
							}},
							Policy: &v1.StepPolicy{ContinueOnError: true},
						},
					},
					pins("check", `has(steps.provision.error) && steps.provision.error.startsWith('workflow "callee-fails"')`)...,
				),
			},
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				provision := out.GetStepValues()["provision"]
				_, hasError := provision.GetNamedValues()["error"]
				_, checkRan := out.GetStepValues()["check"]
				_, checkElseRan := out.GetStepValues()["check_else"]
				return hasError && checkRan && !checkElseRan
			},
		},
		{
			// A callee's own `vars:` are evaluated for it — the same block a
			// direct run of the same file would evaluate — and are visible both
			// to its steps (bare, `${prefix}`) and to its own declared outputs.
			// This is the case that would fail if a driver forgot to evaluate a
			// callee's vars at all: CallScope hands the callee's steps an empty
			// AmbientVars in that world, and `${prefix}` fails to resolve rather
			// than merely reading something wrong.
			Name: "a callee's own vars are usable in its steps and its declared outputs",
			Workflow: &v1.Workflow{
				Name:    "call-vars",
				Profile: v1.CurrentProfile,
				Steps: append(
					[]*v1.Node{callNode("provision", varsCallee("callee-vars", "eu-"), nil)},
					pins("check", `steps.provision.region == "eu-west"`)...,
				),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"provision": {NamedValues: map[string]*v1.Value{
					"region": v1.NewLiteral("eu-west"),
				}},
				"check": {},
			}},
		},
		{
			// The collision case: caller and callee both declare a var of the
			// same name, with different values. A callee reading its own
			// `vars.prefix` must see *its* value, never the caller's — proving
			// isolation holds even where a name match could let a leak hide
			// behind a coincidence, rather than only where the names plainly
			// differ (the case above).
			Name: "a callee's own vars win a name collision with the caller's",
			Workflow: &v1.Workflow{
				Name:    "call-vars-collision",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"prefix": v1.NewLiteral("do-not-leak-")},
				Steps: append(
					[]*v1.Node{callNode("provision", varsCallee("callee-vars-collision", "eu-"), nil)},
					pins("check", `steps.provision.region == "eu-west"`)...,
				),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"provision": {NamedValues: map[string]*v1.Value{
					"region": v1.NewLiteral("eu-west"),
				}},
				"check": {},
			}},
		},
		{
			// Depth is bounded past what any real composition needs — see
			// [v1.MaxCallDepth] — and refused rather than left to recurse until the
			// stack ends the process, for a specification that never passed through
			// a parser (the parser's own cycle detection is a compile-time concern;
			// this is the runtime bound both drivers share).
			Name: "calls nested past the depth limit are refused",
			Workflow: &v1.Workflow{
				Name:    "call-depth",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{callNode("first", deepCallChain(v1.MaxCallDepth+1), nil)},
			},
			ExpectFailure: true,
		},
	}
}

// varsCallee returns a callee declaring one var (`prefix`, bound to prefix),
// read bare by a step and again by a declared output — the shape that only
// works if the driver evaluated the callee's *own* `vars:` block for it,
// through the same mechanism a direct run of the file would.
func varsCallee(name, prefix string) *v1.Workflow {
	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		Vars:    map[string]*v1.Value{"prefix": v1.NewLiteral(prefix)},
		Steps:   pins("labeled", `(vars.prefix + "west") == "eu-west"`),
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "region", Value: v1.NewExpr(`vars.prefix + "west"`)},
		},
	}
}

// callFailureCallee returns a callee whose one step fails deterministically: a
// `for_each` whose `items:` names a variable nothing declares.
func callFailureCallee() *v1.Workflow {
	return &v1.Workflow{
		Name:    "callee-fails",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id:   "boom",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{Items: v1.NewExpr("nosuchvar")}},
			},
		},
	}
}

// deepCallChain returns a workflow that calls another, n levels deep, each
// level declaring no inputs so no arguments need binding.
//
// n counts the calls remaining including this one, so deepCallChain(1) is a
// workflow with no call in it at all — the base case a chain bottoms out at.
func deepCallChain(n int) *v1.Workflow {
	if n <= 0 {
		return &v1.Workflow{
			Name:    "call-depth-bottom",
			Profile: v1.CurrentProfile,
			Steps:   []*v1.Node{says("bottom", "reached")},
		}
	}

	return &v1.Workflow{
		Name:    "call-depth-level",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{callNode("next", deepCallChain(n-1), nil)},
	}
}
