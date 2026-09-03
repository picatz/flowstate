package conformance

import (
	"net/http"
	"strings"
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

// typedOutput declares one output that also says what its value is.
//
// values is the closed set for a `type: enum` declaration and is left empty for
// every other type, which is the same pairing an input declares — see
// [v1.OutputDeclaration.type].
func typedOutput(name, expression string, t v1.InputDeclaration_Type, values ...string) *v1.OutputDeclaration {
	return &v1.OutputDeclaration{
		Name:   name,
		Value:  v1.NewExpr(expression),
		Type:   t,
		Values: values,
	}
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
		{
			// A declared output type changes nothing about the answer, which is
			// the claim: the same three values, in the same message, under both
			// drivers. What it changes is that each one is now checked against
			// its declaration before it is reported — by [v1.CheckOutputValue]
			// inside [v1.EvalRunOutputs], the one function both drivers reach at
			// the one moment they evaluate outputs — so a driver that skipped
			// the check, or ran it against a differently-shaped value, would
			// diverge here rather than in production.
			//
			// All four kinds of declaration in one case, on purpose: a string, an
			// int computed rather than echoed, an enum whose value is inside its
			// declared set, and an untyped output beside them, which is the shape
			// every workflow written before there was a type to declare still has.
			Name: "typed outputs round-trip as themselves",
			Workflow: declares("outputs-typed",
				[]*v1.InputDeclaration{
					input("name", v1.InputDeclaration_TYPE_STRING, true, nil),
					{
						Name:   "channel",
						Type:   v1.InputDeclaration_TYPE_ENUM,
						Values: []string{"stable", "beta"},
					},
				},
				[]*v1.OutputDeclaration{
					typedOutput("said", "steps.a.said", v1.InputDeclaration_TYPE_STRING),
					typedOutput("length", "size(steps.a.said)", v1.InputDeclaration_TYPE_INT),
					typedOutput("channel", "inputs.channel", v1.InputDeclaration_TYPE_ENUM, "stable", "beta"),
					output("untyped", `"whatever"`),
				},
				echoes("a", httpBaseURL, "inputs.name"),
			),
			Inputs: map[string]*v1.Value{
				"name":    v1.NewLiteral("world"),
				"channel": v1.NewLiteral("beta"),
			},
			ExpectedOutputs: answers(
				&v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{"a": said("world")}},
				map[string]*v1.Value{
					"said":    v1.NewLiteral("world"),
					"length":  v1.NewLiteral(int64(5)),
					"channel": v1.NewLiteral("beta"),
					"untyped": v1.NewLiteral("whatever"),
				},
			),
		},
		{
			// The negative direction of the same claim, which is what makes the
			// case above worth having: a declared type that nothing enforced
			// would be decoration. The expression is a closed one so the value it
			// produces is a fact about this file rather than about the http
			// server, and it is outside the set the declaration closed — so the
			// run fails at the one moment there is nothing left to retry, under
			// both drivers, with the declaration's own choices in the sentence.
			Name:          "an enum output outside its declared values fails the run",
			ExpectFailure: true,
			Workflow: declares("outputs-enum-violated",
				nil,
				[]*v1.OutputDeclaration{
					typedOutput("channel", `"canary"`, v1.InputDeclaration_TYPE_ENUM, "stable", "beta"),
				},
				says("a", "hello"),
			),
			ExpectedErrorContains: `output "channel" is "canary", which is not one of the values channel declares`,
		},
		{
			// #1396, and the reason it is a conformance case rather than a unit
			// test: the sentence a refused output produces *is* the run's
			// failure text, and a run's failure text is what each driver
			// persists — locally as the error a caller reads, durably as the
			// workflow's own failure in Temporal's history. A value withheld by
			// one driver and echoed by the other would be invariant 7 broken on
			// exactly one of them, which is the divergence a shared case exists
			// to make impossible.
			//
			// [Case.ExpectedErrorOmits] carries the whole claim; the substring
			// beside it only says the author still learns which promise broke.
			Name:          "a sensitive enum output outside its declared values withholds the value",
			ExpectFailure: true,
			Workflow: declares("outputs-sensitive-enum-violated",
				nil,
				[]*v1.OutputDeclaration{
					sensitive(typedOutput("token", `"`+sensitiveAnswer+`"`,
						v1.InputDeclaration_TYPE_ENUM, "stable", "beta")),
				},
				says("a", "hello"),
			),
			ExpectedErrorContains: `output "token" is ` + v1.SensitiveMarker +
				`, which is not one of the values token declares: "stable", "beta"`,
			ExpectedErrorOmits: sensitiveAnswer,
		},
		{
			// The other refusal a declared output can earn, on the same claim.
			// `must:` predates the type, so this half of #1396 predates #1392 —
			// and it reaches durable history by the identical route, which is
			// why both are pinned here rather than only the newer one.
			Name:          "a sensitive output failing its own must withholds the value",
			ExpectFailure: true,
			Workflow: declares("outputs-sensitive-must-violated",
				nil,
				[]*v1.OutputDeclaration{
					mustSatisfy(sensitive(output("token", `"`+sensitiveAnswer+`"`)), `this == "expected"`),
				},
				says("a", "hello"),
			),
			ExpectedErrorContains: "output \"token\" must satisfy `this == \"expected\"`; got " + v1.SensitiveMarker,
			ExpectedErrorOmits:    sensitiveAnswer,
		},
		{
			// #1404, the positive direction first: a `type: struct` output is
			// the map a caller reads as a plain object, and both an ordinary one
			// and an empty one are that. Here because the refusal below is only
			// worth having if the shape it guards still passes — a key check
			// written as "every map is suspect" would take the empty map with
			// it, and an empty answer is a legal answer. The empty string is also
			// a legal string key, rather than the absence of a key.
			Name: "a struct output round-trips as the map it computed",
			Workflow: declares("outputs-struct-string-keys",
				nil,
				[]*v1.OutputDeclaration{
					typedOutput("detail", `{"host": "a"}`, v1.InputDeclaration_TYPE_STRUCT),
					typedOutput("blank", `{"": "value"}`, v1.InputDeclaration_TYPE_STRUCT),
					typedOutput("nothing", `{}`, v1.InputDeclaration_TYPE_STRUCT),
				},
				says("a", "hello"),
			),
			// One entry rather than several, for a reason unrelated to the
			// claim: an [expr.MapValue]'s entries are a repeated field and CEL
			// gives a map literal's entry order no meaning, so a multi-key
			// computed map is not a value a proto diff can assert on.
			ExpectedOutputs: answers(
				held("a"),
				map[string]*v1.Value{
					"detail":  v1.NewLiteralMap(map[string]any{"host": "a"}),
					"blank":   v1.NewLiteralMap(map[string]any{"": "value"}),
					"nothing": v1.NewLiteralMap(map[string]any{}),
				},
			),
		},
		{
			// The refusal, at completion because that is the first moment the
			// key exists: the expression is closed, so what it produces is a
			// fact about this file, and the kind check alone accepts it because
			// a map keyed by anything is a map. Left accepted, the run reported
			// success and the answer reached a caller in the schema's own tagged
			// encoding instead of the object `type: struct` promised — a
			// declaration broken by the thing that read it rather than by the
			// run, which is why the check belongs at the seam both drivers share
			// rather than in the renderer that noticed.
			Name:          "a struct output keyed by an int fails the run",
			ExpectFailure: true,
			Workflow: declares("outputs-struct-int-keys",
				nil,
				[]*v1.OutputDeclaration{
					typedOutput("detail", `{1: "value"}`, v1.InputDeclaration_TYPE_STRUCT),
				},
				says("a", "hello"),
			),
			ExpectedErrorContains: `output "detail" is declared struct but computed a map with int keys; ` +
				`a struct is a map with string keys`,
		},
		{
			// The same promise one level down. The projection converts the whole
			// value, so a nested non-string key defeats it exactly as an outer
			// one does — and this is the arm a check on the outer map's keys
			// alone would pass, which is what makes it worth its own case.
			Name:          "a struct output holding a map keyed by a bool fails the run",
			ExpectFailure: true,
			Workflow: declares("outputs-struct-nested-bool-keys",
				nil,
				[]*v1.OutputDeclaration{
					typedOutput("detail", `{"inner": {true: "value"}}`, v1.InputDeclaration_TYPE_STRUCT),
				},
				says("a", "hello"),
			),
			ExpectedErrorContains: `output "detail" is declared struct but computed a map with bool keys; ` +
				`a struct is a map with string keys`,
		},
		{
			// `list` makes the same promise `struct` does — the projection
			// converts a whole output and gives up on all of it — so a map keyed
			// by an int inside an element defeats the plain array exactly as an
			// outer key defeats an object. One rule, two declared containers,
			// which is why both are decided by the one check rather than by a
			// second one written for lists later.
			Name:          "a list output holding a map keyed by an int fails the run",
			ExpectFailure: true,
			Workflow: declares("outputs-list-int-keys",
				nil,
				[]*v1.OutputDeclaration{
					typedOutput("items", `[{1: "value"}]`, v1.InputDeclaration_TYPE_LIST),
				},
				says("a", "hello"),
			),
			ExpectedErrorContains: `output "items" is declared list but holds a map with int keys; ` +
				`a list reads back as a plain array, whose maps have string keys`,
		},
		{
			// The positive direction for the other container, for the reason the
			// struct one above has it: a rule that refused every list of maps
			// would take the ordinary shape with it, and an empty list is a legal
			// answer.
			Name: "a list output round-trips as the array it computed",
			Workflow: declares("outputs-list-string-keys",
				nil,
				[]*v1.OutputDeclaration{
					typedOutput("items", `[{"host": "a"}]`, v1.InputDeclaration_TYPE_LIST),
					typedOutput("nothing", `[]`, v1.InputDeclaration_TYPE_LIST),
				},
				says("a", "hello"),
			),
			ExpectedOutputs: answers(
				held("a"),
				map[string]*v1.Value{
					"items":   v1.NewLiteralList(map[string]any{"host": "a"}),
					"nothing": v1.NewLiteralList(),
				},
			),
		},
		{
			// The bound half of the same sentence, and a both-drivers case for
			// the reason the withholding ones are: an output's value is sized by
			// whoever produced it, up to [v1.MaxTaskOutputBytes], and the
			// refusal used to quote all of it. Temporal has a blob limit, so an
			// unbounded sentence is a failure the durable driver cannot persist
			// while the local driver returns it — invariant 3 broken by a
			// diagnostic, which is the shape a unit test on one side would miss.
			//
			// The value is a closed expression for the reason the case above it
			// gives: what it produces is a fact about this file rather than
			// about the http server. It is far larger than any bound a sentence
			// could reasonably carry and still ordinary ASCII, so an unbounded
			// rendering fails the cap by orders of magnitude rather than
			// marginally.
			Name:          "a rejected enum output's value is bounded in the failure",
			ExpectFailure: true,
			Workflow: declares("outputs-enum-violated-large",
				nil,
				[]*v1.OutputDeclaration{
					typedOutput("channel", `"`+strings.Repeat("x", oversizedOutputBytes)+`"`,
						v1.InputDeclaration_TYPE_ENUM, "stable", "beta"),
				},
				says("a", "hello"),
			),
			// Still an author-readable sentence: the output and the closed set
			// are what the file declared, and neither is sized by the run.
			ExpectedErrorContains: `not one of the values channel declares: "stable", "beta"`,
			ExpectedErrorMaxBytes: maxConformanceRefusalBytes,
		},
	}
}

// oversizedOutputBytes is how long the rejected value in the bound case above
// is.
//
// Large enough that an unbounded refusal misses [maxConformanceRefusalBytes] by
// more than an order of magnitude, and small enough to sit far under
// [v1.MaxRunStateBytes] — it travels in the workflow specification, which the
// durable driver carries in `RunState` through every segment, so a case that
// proved the point by nearly blowing a different bound would be its own defect.
const oversizedOutputBytes = 32 << 10

// maxConformanceRefusalBytes caps the rendered failure in that case.
//
// Sized for the durable driver rather than the local one: Temporal's framing
// wraps the sentence and repeats it, so the string this is asserted against is
// several times the message itself. Still two orders of magnitude below
// [oversizedOutputBytes], which is what makes it a real assertion rather than a
// formality.
const maxConformanceRefusalBytes = 2048

// sensitiveAnswer is the value the two #1396 cases compute.
//
// Closed rather than fetched, so what those cases observe is a property of the
// diagnostic under both drivers rather than of the http server standing beside
// them, and distinctive enough that a substring assertion about its absence
// means something.
const sensitiveAnswer = "sk-live-0PENSESAME"

// sensitive marks a declared output the way a file's `sensitive: true` does.
//
// A modifier over [output] and [typedOutput] rather than a third constructor,
// because sensitivity is orthogonal to whether a declaration also states a
// type — the same reason the schema carries it as its own field.
func sensitive(decl *v1.OutputDeclaration) *v1.OutputDeclaration {
	decl.Sensitive = true

	return decl
}

// mustSatisfy attaches a `must:` predicate over `this` to a declared output.
func mustSatisfy(decl *v1.OutputDeclaration, must string) *v1.OutputDeclaration {
	decl.Must = &must

	return decl
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

	// Omits is a fragment the refusal's message must not carry, for the cases
	// whose claim is about what a refusal does not say. Contains cannot make
	// that claim: a sentence can hold the expected wording and the forbidden
	// material at once, and a refusal about a `sensitive:` declaration is
	// exactly where it does (#1396). Empty means the case makes no such claim.
	Omits string
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
	mustAlwaysPass := "true"
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
			// #1465: `must:` is part of the workflow's CEL dialect. Before it
			// resolved the build's CurrentProfile, this hand-built spec passed
			// submission even though the worker does not know the profile the
			// declaration records. Both drivers must fail closed before the
			// literal step can run.
			Name: "a must constraint under an unknown recorded profile is refused",
			Workflow: &v1.Workflow{
				Name:    "inputs-unknown-must-profile",
				Profile: "2099.9",
				DeclaredInputs: []*v1.InputDeclaration{{
					Name: "region", Type: v1.InputDeclaration_TYPE_STRING, Must: &mustAlwaysPass,
				}},
				Steps: []*v1.Node{says("a", "hello")},
			},
			Inputs:   map[string]*v1.Value{"region": v1.NewLiteral("eu-west-1")},
			Contains: `unknown language profile "2099.9"`,
		},
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
			// The structural schema lands before its runtime projection. Even an
			// optional input nobody supplied must refuse the workflow at submit;
			// otherwise the same contract starts or fails based only on whether a
			// caller happened to provide a value.
			Name: "a structural-only input is refused until the runtime projects it",
			Workflow: declares("inputs-structural-only",
				[]*v1.InputDeclaration{{
					Name: "region",
					ValueType: &v1.Type{Kind: &v1.Type_Scalar_{
						Scalar: v1.Type_SCALAR_STRING,
					}},
				}},
				nil,
				says("a", "hello"),
			),
			Contains: "the legacy type is required until structural-only declarations are safe across rolling upgrades",
		},
		{
			// The same declaration nested in a callee must be found while the
			// parent is admitted, not when execution reaches the call after an
			// earlier parent step has already made a request.
			Name: "a callee structural-only input is refused before its caller starts",
			Workflow: declares("calls-structural-only-input",
				nil,
				nil,
				says("before-call", "side effect"),
				callNode("callee", declares("inputs-structural-only-callee",
					[]*v1.InputDeclaration{{
						Name: "region",
						ValueType: &v1.Type{Kind: &v1.Type_Scalar_{
							Scalar: v1.Type_SCALAR_STRING,
						}},
					}},
					nil,
					says("inside", "unreachable"),
				), nil),
			),
			Contains: "the legacy type is required until structural-only declarations are safe across rolling upgrades",
		},
		{
			// Programmatic local callers do not necessarily run schema validation
			// before admission. The runtime must therefore enforce the same
			// cross-field agreement rule as the server rather than silently using
			// only the legacy half.
			Name: "disagreeing legacy and structural input types are refused",
			Workflow: declares("inputs-type-disagreement",
				[]*v1.InputDeclaration{{
					Name: "region",
					Type: v1.InputDeclaration_TYPE_STRING,
					ValueType: &v1.Type{Kind: &v1.Type_Scalar_{
						Scalar: v1.Type_SCALAR_INT,
					}},
				}},
				nil,
				says("a", "hello"),
			),
			Contains: "value_type and the legacy type must describe the same input type when both are set",
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
