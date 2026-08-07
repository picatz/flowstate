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
//
// What each case *observes* is a condition rather than a step's result — see observe.go
// for why there is nothing else left to observe, and why the claim is pinned from both
// directions.

// VarsCases returns the shared cases for `vars:`, workflow-level and step-level.
//
// The base URL should come from [NewHTTPServer]. Exactly one case needs it: a step's
// own vars are out of scope for that step's own `if:`, so watching one reach a task's
// inputs is the one thing a condition cannot do.
func VarsCases(httpBaseURL string) []Case {
	return []Case{
		{
			// The plain case: a literal var read by a step.
			Name: "a literal var is readable as vars.<name>",
			Workflow: &v1.Workflow{
				Name:    "vars-literal",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1")},
				Steps:   pins("show", `vars.region == "eu-west-1"`),
			},
			ExpectedOutputs: held("show"),
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
				Steps: pins("show", `vars.greeting == "hello world"`),
			},
			ExpectedOutputs: held("show"),
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
				Steps: append(
					pins("first", `vars.tag == "v2"`),
					pins("second", `vars.tag == "v2"`)...,
				),
			},
			ExpectedOutputs: held("first", "second"),
		},
		{
			// A var reaches *inside* a loop, where the scope is rebuilt per iteration
			// and carried to an activity by hand. That hand-copying is what silently
			// dropped the iterator once; a var read from a loop body is the same code
			// path seen from the other namespace.
			//
			// The claim names both the var and the iterator, so an iteration that saw
			// the wrong one of either fails — a claim mentioning only the var would
			// hold even if the body had been handed no iterator at all.
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
							Body:     pins("label", `(vars.prefix + name) in ["host-a", "host-b"]`),
						}},
					},
				},
			},
			// The loop's own outputs, one entry per iteration. Each holds the arm that
			// ran and not the arm that did not, which is what makes this an assertion
			// about the value rather than about the loop.
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"each": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"label": map[string]any{}},
						map[string]any{"label": map[string]any{}},
					),
				}},
			}},
		},
		{
			// A step's own vars, which are bare rather than rooted — the other half of
			// the feature, and the half where the two drivers evaluate at different
			// moments: locally in process just before the step, durably in workflow
			// code just before the activity is scheduled.
			//
			// Observed through a request body rather than a condition, because a step's
			// vars are deliberately *not* in scope for that step's own `if:` — the
			// condition decides whether the step runs, so a name the step declares does
			// not exist yet when the question is asked. What is under test is the value
			// reaching the step's inputs, and the loopback server hands it back.
			Name: "a step's own var is readable bare",
			Workflow: &v1.Workflow{
				Name:    "step-vars-bare",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					withVars(
						echoes("greet", httpBaseURL, `"hello " + who`),
						map[string]*v1.Value{"who": v1.NewExpr(`"world"`)},
					),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"greet": said("hello world"),
			}},
		},
		{
			// The same name declared by two steps with two values, which is the
			// property that makes a step's vars safe to name freely: they are private
			// to the step, so nothing about the first reaches the second.
			//
			// Both sides positive deliberately — each step reads *its own* value, so a
			// leak shows up as a wrong string rather than as an unbound name. The
			// refusal of the case where a leak would be silent (a step naming something
			// already in scope) is a validation rule, tested where it is enforced.
			Name: "a step's vars are private to it",
			Workflow: &v1.Workflow{
				Name:    "step-vars-private",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					withVars(
						echoes("first", httpBaseURL, "tag"),
						map[string]*v1.Value{"tag": v1.NewLiteral("one")},
					),
					withVars(
						echoes("second", httpBaseURL, "tag"),
						map[string]*v1.Value{"tag": v1.NewLiteral("two")},
					),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"first":  said("one"),
				"second": said("two"),
			}},
		},
		{
			// A step's var reads everything in scope where the step is written: the
			// workflow's rooted vars and the output of a step that already ran. This is
			// the case that separates the two blocks — a workflow var may reference
			// nothing, and a step var may reference whatever has happened by then.
			//
			// `base` is a loop rather than a task because `results` is the only step
			// output a case can produce locally now, and a step var reading a *step*
			// is the half of the claim that a workflow var cannot make.
			Name: "a step's var reads the workflow's vars and an earlier step",
			Workflow: &v1.Workflow{
				Name:    "step-vars-scope",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"want": v1.NewLiteral(int64(2))},
				Steps: []*v1.Node{
					counter("base", "a", "b"),
					withVars(
						echoes("check", httpBaseURL, `string(seen) + "/" + string(vars.want)`),
						map[string]*v1.Value{"seen": v1.NewExpr("size(steps.base.results)")},
					),
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"base": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"base_body": map[string]any{}},
						map[string]any{"base_body": map[string]any{}},
					),
				}},
				"check": said("2/2"),
			}},
		},
		{
			// Vars on the loop *node*, visible to its items expression and throughout
			// its body — the lexical reading, and the one the durable driver reaches by
			// a different route, since it swaps the executor's scope and every nested
			// executor is built from it.
			//
			// A loop's own vars *are* in scope for its body, unlike a task step's for
			// its condition, so the body can pin them directly.
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
							Body:     pins("shout", `(name + suffix) in ["a!", "b!"]`),
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"each": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"shout": map[string]any{}},
						map[string]any{"shout": map[string]any{}},
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
								withVars(
									echoes("upper", httpBaseURL, "loud"),
									map[string]*v1.Value{"loud": v1.NewExpr("name.upperAscii()")},
								),
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"each": {NamedValues: map[string]*v1.Value{
					"results": v1.NewLiteralList(
						map[string]any{"upper": map[string]any{"said": "A"}},
						map[string]any{"upper": map[string]any{"said": "B"}},
					),
				}},
			}},
		},
	}
}

// VarsSecretRefusalCases returns specifications that must be refused before
// anything runs, on either driver, because a `vars:` block holds a secret
// reference.
//
// The negative direction of [VarsCases], and the direction that set could not have
// covered: every case above asserts a var reaches the scope a step reads, which is
// exactly the property that makes a secret in one a leak. A corpus that only ever
// binds ordinary values proves the binding works and says nothing about whether
// anything refuses the one value it must never bind.
//
// Refused rather than deferred, and refused at both levels: see
// [flowstatev1.CheckVarsHoldNoSecretRef] for why a var has no contained place to
// resolve one, and `flowfile.notInVarHelp` for the same rule said to an author
// against a line and a column. Shared, because "refused" is an observable and the
// local driver exists to predict what production does — a local run that evaluated
// a var holding a reference would be a rehearsal that resolves a secret production
// refuses to.
//
// Every case is built here in Go rather than parsed, which is the point: the
// compiler refuses these files, so the only way one of these specifications reaches
// a driver is by being built by something that never was a Flowfile. That is the
// submission this pins.
func VarsSecretRefusalCases() []Refusal {
	ref := func() *v1.Value {
		return &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
			Scheme: "env", Name: "TOKEN",
		}}}
	}

	return []Refusal{
		{
			Name: "a workflow var holding a secret reference is refused",
			Workflow: &v1.Workflow{
				Name:    "wfvar-secret",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"token": ref()},
				Steps:   []*v1.Node{says("noop", "hi")},
			},
			Contains: `workflow var "token" is a secret reference`,
		},
		{
			Name: "a step var holding a secret reference is refused",
			Workflow: &v1.Workflow{
				Name:    "stepvar-secret",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					withVars(says("noop", "hi"), map[string]*v1.Value{"token": ref()}),
				},
			},
			Contains: `step "noop" var "token" is a secret reference`,
		},
		{
			// Nested in a structure, which is the spelling the schema deliberately
			// makes representable: a structure's entries are values in their own
			// right, so a reference can sit arbitrarily deep in one. A check that
			// only looked at the top of a var's value would pass exactly the value
			// [v1.Value_Structure] exists for.
			Name: "a workflow var holding a structure with a nested reference is refused",
			Workflow: &v1.Workflow{
				Name:    "wfvar-secret-nested",
				Profile: v1.CurrentProfile,
				Vars: map[string]*v1.Value{
					"headers": v1.NewStructureMap(map[string]*v1.Value{"Authorization": ref()}),
				},
				Steps: []*v1.Node{says("noop", "hi")},
			},
			Contains: `workflow var "headers" is a secret reference`,
		},
		{
			// Inside a loop body, where a var is evaluated once per iteration. A
			// walk that stopped at the top level of `steps:` would miss it, and a
			// var in a body is no more contained for being deeper.
			Name: "a step var inside a loop body holding a secret reference is refused",
			Workflow: &v1.Workflow{
				Name:    "loopvar-secret",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "each",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items:    v1.NewLiteralList("a"),
							Iterator: "name",
							Body: []*v1.Node{
								withVars(says("inner", "hi"), map[string]*v1.Value{"token": ref()}),
							},
						}},
					},
				},
			},
			Contains: `step "inner" var "token" is a secret reference`,
		},
		{
			// A callee is inlined in the caller's own specification and its
			// `vars:` are evaluated by the same [v1.EvalVars] a top-level block
			// goes through, so a walk that stopped at the `call:` step would
			// check the half of a submission that happens to be spelled at the
			// top and pass the half that is not.
			Name: "a callee's var holding a secret reference is refused",
			Workflow: &v1.Workflow{
				Name:    "calleevar-secret",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "delegate",
						Kind: &v1.Node_Call{Call: &v1.Call{
							Workflow: &v1.Workflow{
								Name:    "callee",
								Profile: v1.CurrentProfile,
								Vars:    map[string]*v1.Value{"token": ref()},
								Steps:   []*v1.Node{says("inner", "hi")},
							},
						}},
					},
				},
			},
			Contains: `workflow "callee" var "token" is a secret reference`,
		},
	}
}
