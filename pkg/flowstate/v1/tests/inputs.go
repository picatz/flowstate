package tests

import (
	"net/http"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Cases for `inputs:` and `outputs:`, run by both execution drivers.
//
// Shared for the reason invariant 3 exists, and this feature reaches further into
// each driver than most: the arguments are checked and defaulted once at a submit
// boundary that is a server handler on one side and a function call on the other,
// bound into a scope that one driver builds in process and the other rebuilds from
// `RunState` at the top of every segment, read from expression positions that
// resolve in workflow code except for the ones an activity resolves — and answered
// by a block of expressions evaluated after the last step, at the one moment there
// is nothing left to retry.
//
// What each case *observes* is a condition, for the reason observe.go gives, plus
// one thing conditions cannot see: the run's declared outputs, which are the values
// the feature exists to produce and which ride in the same message a case already
// compares.

// declares builds a workflow that takes inputs and answers with outputs.
func declares(
	name string,
	inputs []*v1.InputDeclaration,
	outputs []*v1.OutputDeclaration,
	steps ...*v1.Node,
) *v1.Workflow {
	return &v1.Workflow{
		Name:            name,
		Profile:         v1.CurrentProfile,
		DeclaredInputs:  inputs,
		DeclaredOutputs: outputs,
		Steps:           steps,
	}
}

// input declares one input.
func input(name string, t v1.InputDeclaration_Type, required bool, defaultValue *v1.Value) *v1.InputDeclaration {
	return &v1.InputDeclaration{
		Name:     name,
		Type:     t,
		Required: required,
		Default:  defaultValue,
	}
}

// output declares one output computed by an expression.
func output(name, expression string) *v1.OutputDeclaration {
	return &v1.OutputDeclaration{Name: name, Value: v1.NewExpr(expression)}
}

// answers is the run outputs a case expects, beside the steps it expects to have
// run.
//
// The two travel in one message, which is what makes a driver that computes the
// right values at the wrong moment — or not at all — a failing assertion rather
// than a second thing to remember to check.
func answers(steps *v1.Workflow_StepOutputs, values map[string]*v1.Value) *v1.Workflow_StepOutputs {
	steps.RunOutputs = &v1.RunOutputs{Values: values}

	return steps
}

// InputOutputCases returns the shared cases for `inputs:` and `outputs:`.
//
// The base URL should come from [NewHTTPServer]. It is needed by the case that
// watches an input reach a task's *inputs* rather than a condition, and by the one
// that reads an input from inside an expression the task evaluates for itself —
// which is the position that resolves on whichever worker took the activity, and so
// the position a scope field is most easily forgotten in.
func InputOutputCases(httpBaseURL string) []Case {
	return []Case{
		{
			// The plain case: a required input, supplied, read in a condition.
			Name: "a supplied input is readable as inputs.<name>",
			Workflow: declares("inputs-supplied",
				[]*v1.InputDeclaration{input("region", v1.InputDeclaration_TYPE_STRING, true, nil)},
				nil,
				pins("show", `inputs.region == "eu-west-1"`)...,
			),
			Inputs:          map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1")},
			ExpectedOutputs: held("show"),
		},
		{
			// The default, applied where the caller sent nothing — and applied at the
			// submit boundary rather than where the value is read, so both drivers see
			// a value that was already decided.
			Name: "an absent optional input takes its default",
			Workflow: declares("inputs-default",
				[]*v1.InputDeclaration{
					input("retries", v1.InputDeclaration_TYPE_INT, false, v1.NewLiteral(int64(3))),
					input("verbose", v1.InputDeclaration_TYPE_BOOL, false, v1.NewLiteral(false)),
				},
				nil,
				pins("show", `inputs.retries == 3 && inputs.verbose == false`)...,
			),
			ExpectedOutputs: held("show"),
		},
		{
			// A supplied value beats the default, which is the whole point of having
			// one — and the zero-shaped value is the one that goes missing when
			// "supplied" is decided by whether a value looks empty.
			Name: "a supplied value wins over the default, including a zero one",
			Workflow: declares("inputs-default-overridden",
				[]*v1.InputDeclaration{input("retries", v1.InputDeclaration_TYPE_INT, false, v1.NewLiteral(int64(3)))},
				nil,
				pins("show", `inputs.retries == 0`)...,
			),
			Inputs:          map[string]*v1.Value{"retries": v1.NewLiteral(int64(0))},
			ExpectedOutputs: held("show"),
		},
		{
			// An input an author left out entirely: no default, not required. The root
			// still resolves, so this is a missing *key* rather than an unresolved
			// reference — which is what lets a file ask whether it was given one.
			Name: "an undeclared value is a missing key rather than an unbound root",
			Workflow: declares("inputs-absent",
				[]*v1.InputDeclaration{input("tag", v1.InputDeclaration_TYPE_STRING, false, nil)},
				nil,
				pins("show", `!has(inputs.tag)`)...,
			),
			ExpectedOutputs: held("show"),
		},
		{
			// The other expression positions the language has, in one file: a step's
			// own `vars:`, a loop's `items:`, and a body reading the item it produced.
			Name: "an input reaches vars, a loop's items, and a body",
			Workflow: declares("inputs-positions",
				[]*v1.InputDeclaration{
					input("regions", v1.InputDeclaration_TYPE_LIST, true, nil),
					input("prefix", v1.InputDeclaration_TYPE_STRING, false, v1.NewLiteral("eu-")),
				},
				nil,
				&v1.Node{
					Id: "each",
					Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
						Items:    v1.NewExpr("inputs.regions"),
						Iterator: "region",
						Body: []*v1.Node{
							withVars(
								guarded("body", `region.startsWith(inputs.prefix)`, "matched"),
								nil,
							),
						},
					}},
				},
				// A step's own `vars:` reading an input, observed the one way a step
				// var can be: through a task input. They are deliberately out of scope
				// for the step's own `if:`, so a condition cannot see one.
				withVars(
					echoes("after", httpBaseURL, "string(count)"),
					map[string]*v1.Value{"count": v1.NewExpr("size(inputs.regions)")},
				),
			),
			Inputs: map[string]*v1.Value{
				"regions": v1.NewLiteralList("eu-west-1", "eu-west-2"),
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"each": v1.LoopOutputs([]*v1.Workflow_StepOutputs{
					{StepValues: map[string]*v1.Node_Outputs{"body": {}}},
					{StepValues: map[string]*v1.Node_Outputs{"body": {}}},
				}),
				"after": said("2"),
			}},
		},
		{
			// An input reaching a *task input*, and then reaching the expression the
			// task evaluates for itself — the http task's `outputs:`, which is resolved
			// inside an activity on whichever worker took it. That is the one position
			// where the scope has to have carried the arguments across a wire.
			Name: "an input reaches a task's inputs and the scope an activity evaluates in",
			Workflow: declares("inputs-task",
				[]*v1.InputDeclaration{input("greeting", v1.InputDeclaration_TYPE_STRING, true, nil)},
				nil,
				&v1.Node{
					Id: "a",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "http",
						Inputs: map[string]*v1.Value{
							"method":  v1.NewLiteral(http.MethodPost),
							"url":     v1.NewLiteral(httpBaseURL + "/echo"),
							"body":    v1.NewExpr("inputs.greeting"),
							"outputs": v1.NewExpr(`{"said": response.body, "asked": inputs.greeting}`),
						},
					}},
				},
			),
			Inputs: map[string]*v1.Value{"greeting": v1.NewLiteral("hello world")},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": {NamedValues: map[string]*v1.Value{
					"said":  v1.NewLiteral("hello world"),
					"asked": v1.NewLiteral("hello world"),
				}},
			}},
		},
		{
			// The answer, computed from all three of the things in scope at the end: a
			// step's output, a workflow var, and an input.
			Name: "outputs compute from steps, vars and inputs",
			Workflow: func() *v1.Workflow {
				wf := declares("outputs-compose",
					[]*v1.InputDeclaration{input("name", v1.InputDeclaration_TYPE_STRING, true, nil)},
					[]*v1.OutputDeclaration{
						output("said", "steps.a.said"),
						output("greeting", `vars.hello + " " + inputs.name`),
						output("count", "size(steps.a.said)"),
					},
					echoes("a", httpBaseURL, "inputs.name"),
				)
				wf.Vars = map[string]*v1.Value{"hello": v1.NewLiteral("hello")}

				return wf
			}(),
			Inputs: map[string]*v1.Value{"name": v1.NewLiteral("world")},
			ExpectedOutputs: answers(
				&v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{"a": said("world")}},
				map[string]*v1.Value{
					"said":     v1.NewLiteral("world"),
					"greeting": v1.NewLiteral("hello world"),
					"count":    v1.NewLiteral(int64(5)),
				},
			),
		},
		{
			// A literal output, which is a strange thing to write and a legal one: it
			// must not be mistaken for an expression, and must not be dropped.
			Name: "a literal output is reported as itself",
			Workflow: declares("outputs-literal",
				nil,
				[]*v1.OutputDeclaration{{Name: "version", Value: v1.NewLiteral(int64(2))}},
				says("a", "hello"),
			),
			ExpectedOutputs: answers(held("a"), map[string]*v1.Value{"version": v1.NewLiteral(int64(2))}),
		},
		{
			// Outputs on a workflow whose last step is a wait. The wait's own outputs
			// are readable, and the outputs are evaluated *after* it — which under the
			// durable driver means after a timer fired and the run resumed, and under
			// the local driver means after a sleep in a process.
			Name: "outputs are evaluated after a final wait",
			Workflow: declares("outputs-after-wait",
				[]*v1.InputDeclaration{input("label", v1.InputDeclaration_TYPE_STRING, false, v1.NewLiteral("done"))},
				[]*v1.OutputDeclaration{
					output("label", "inputs.label"),
					output("waited", "!steps.pause.timed_out"),
				},
				says("a", "before"),
				&v1.Node{
					Id: "pause",
					Kind: &v1.Node_Wait{Wait: &v1.Wait{
						Kind: &v1.Wait_Duration{Duration: durationpb.New(10 * time.Millisecond)},
					}},
				},
			),
			ExpectedOutputs: answers(
				&v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"a": {},
					"pause": {NamedValues: map[string]*v1.Value{
						v1.TimedOutOutput: v1.NewLiteral(false),
					}},
				}},
				map[string]*v1.Value{
					"label":  v1.NewLiteral("done"),
					"waited": v1.NewLiteral(true),
				},
			),
		},
	}
}

// A Refusal is a submission both drivers must reject, and the words they must
// reject it in.
//
// Its own type rather than a [Case] with no expected outputs, because what it pins
// is a refusal at the *submit boundary*: nothing runs, so there are no outputs to
// describe, and a case shaped like the others would read as though there were.
type Refusal struct {
	// Name identifies the case.
	Name string

	// Workflow is what is being submitted.
	Workflow *v1.Workflow

	// Inputs are the arguments submitted with it.
	Inputs map[string]*v1.Value

	// Contains is a fragment the refusal's message must carry, so the case pins
	// which rule refused rather than only that something did.
	Contains string
}

// InputRefusalCases returns submissions that must be refused before anything runs,
// on either driver.
//
// The negative direction, which is the one a test of this shape usually omits: a
// corpus that only ever supplies correct arguments proves that the happy path
// works and says nothing about whether the checks exist. Every one of these is a
// value a caller chose, which is what makes each of them a fail-closed rule rather
// than a convenience.
func InputRefusalCases() []Refusal {
	takesRegion := func() *v1.Workflow {
		return declares("inputs-checked",
			[]*v1.InputDeclaration{
				input("region", v1.InputDeclaration_TYPE_STRING, true, nil),
				input("retries", v1.InputDeclaration_TYPE_INT, false, v1.NewLiteral(int64(3))),
			},
			nil,
			says("a", "hello"),
		)
	}

	return []Refusal{
		{
			Name:     "an undeclared input is refused",
			Workflow: takesRegion(),
			Inputs: map[string]*v1.Value{
				"region": v1.NewLiteral("eu-west-1"),
				"reigon": v1.NewLiteral("eu-west-1"),
			},
			Contains: `input "reigon" is not declared`,
		},
		{
			Name:     "a missing required input is refused",
			Workflow: takesRegion(),
			Inputs:   map[string]*v1.Value{"retries": v1.NewLiteral(int64(1))},
			Contains: `input "region" is required`,
		},
		{
			Name:     "a value of the wrong type is refused",
			Workflow: takesRegion(),
			Inputs: map[string]*v1.Value{
				"region":  v1.NewLiteral("eu-west-1"),
				"retries": v1.NewLiteral("three"),
			},
			Contains: `input "retries" is declared int but was given string`,
		},
		{
			Name:     "an expression is refused where a value belongs",
			Workflow: takesRegion(),
			Inputs:   map[string]*v1.Value{"region": v1.NewExpr(`"eu-" + "west-1"`)},
			Contains: `input "region" is an expression`,
		},
		{
			Name:     "a secret reference is refused",
			Workflow: takesRegion(),
			Inputs: map[string]*v1.Value{
				"region": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "REGION"}}},
			},
			Contains: `input "region" is a secret reference`,
		},
		{
			// A workflow declaring nothing at all, which is every workflow written
			// before this feature existed: it takes no arguments, and a caller sending
			// one is refused rather than having it silently ignored.
			Name: "an input to a workflow that declares none is refused",
			Workflow: &v1.Workflow{
				Name:    "declares-nothing",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{says("a", "hello")},
			},
			Inputs:   map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1")},
			Contains: "declares no `inputs:` at all",
		},
		{
			// #204: a list-typed input with no `must:`/`unique:` declared at all —
			// the exact gap PR #205's constraint-only bound left open, because a
			// `for_each` or an `if:` reaches the identical CEL evaluator over the
			// identical caller-chosen list without ever passing through a
			// constraint check. This declaration has neither, so before the fix
			// this list reached the workflow's steps completely unbounded; after
			// it, BindRunInputs refuses it before anything runs.
			Name: "an oversized list input with no declared constraint is refused",
			Workflow: declares("inputs-unconstrained-list",
				[]*v1.InputDeclaration{input("items", v1.InputDeclaration_TYPE_LIST, true, nil)},
				nil,
				&v1.Node{
					Id: "each",
					Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
						Items:    v1.NewExpr("inputs.items"),
						Iterator: "item",
						Body:     []*v1.Node{says("noop", "hi")},
					}},
				},
			),
			Inputs:   map[string]*v1.Value{"items": v1.NewLiteralList(manyInts(10_001)...)},
			Contains: "list elements",
		},
	}
}

// manyInts returns n small integers as []any, for building an oversized list
// literal without repeating the loop at every call site that needs one.
func manyInts(n int) []any {
	items := make([]any, n)
	for i := range items {
		items[i] = i
	}
	return items
}
