package tests

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// ValueCases are the shared cases that hold both drivers to one behaviour for
// `value:`, a step that names what an expression computes.
//
// Run by both the local driver ([flowstatev1] eval_test.go) and the durable
// driver (engine workflow_test.go), which is what makes "the value is the same,
// under the same name, and a skipped one leaves nothing behind" something the two
// cannot disagree about. It is the shape invariant 3 protects most directly: a
// value's whole observable behaviour is the answer it computed, so a driver
// computing it in a different scope, at a different moment, or under a different
// output name would produce a local rehearsal that told an author the wrong thing
// about production with nothing to give the difference away.
//
// Three cases, which are the three the issue's gate names.
//
//   - A value is read. The result lands under [v1.ValueOutput] and a later `if:`
//     reads it there, which is the ordinary path and also the one that would
//     break if a driver stored the scalar as the step itself.
//   - A value is skipped and then referenced. `if:` composes on this kind as it
//     does on every other, so a skipped value produces no outputs and a later
//     reference to it does not resolve, which is the honest outcome, and a run failure
//     rather than a value quietly read as empty on one driver and not the other.
//   - A value is read into the run's declared `outputs:`. That is the position
//     evaluated after the last step, once there is nothing left to retry, and it
//     is what the corpus files this feature exists for actually do with a value.
func ValueCases() []Case {
	return []Case{
		{
			// The plain read. `over` computes a boolean from the run's inputs
			// alone, with no step involved, which is one of the two shapes no
			// wait's `outputs:` shaping could name, and two later steps observe
			// it through the pair `pins` builds.
			//
			// The claim spells `steps.over.value` out rather than building it
			// from [v1.ValueOutput], and that is the point of writing it twice:
			// the constant is what keeps the two drivers agreeing, and only a
			// literal can pin what the constant has to *be*. Renaming it would
			// leave every reference built from it agreeing with itself and this
			// case failing, which is the right way round.
			Name: "a value is recorded under its own name and read back",
			Workflow: declares("value-read",
				[]*v1.InputDeclaration{
					input("amount", v1.InputDeclaration_TYPE_INT, true, nil),
					input("threshold", v1.InputDeclaration_TYPE_INT, true, nil),
				},
				nil,
				append([]*v1.Node{
					{
						Id:   "over",
						Kind: &v1.Node_Value{Value: v1.NewExpr("inputs.amount >= inputs.threshold")},
					},
				}, pins("show", "steps.over.value")...)...,
			),
			Inputs: map[string]*v1.Value{
				"amount":    v1.NewLiteral(int64(500)),
				"threshold": v1.NewLiteral(int64(100)),
			},
			// The value's own entry is asserted beside the observing steps', so
			// the case pins the *name* the result is stored under and not merely
			// that something downstream agreed with it. A driver that recorded
			// the scalar bare, or under any other name, fails here.
			ExpectedOutputs: withStep(held("show"), "over", map[string]*v1.Value{
				v1.ValueOutput: v1.NewLiteral(true),
			}),
		},
		{
			// Any output-legal value, not only a boolean: the decision recorded
			// on #411 was that a value holds whatever a step output can hold, so
			// this is the case that fails if a driver narrows the result on its
			// way into `step_values`.
			//
			// A list rather than a mapping, because the expectation is compared
			// exactly: a mapping's entries are a repeated field in the literal
			// and their order is whatever CEL's map iteration produced, so a case
			// asserting one would be asserting something neither driver promises.
			// The claim below reads *into* the value, which is the part that
			// would break if a driver stored a rendering of it instead.
			Name: "a value may hold a whole list, not only a scalar",
			Workflow: &v1.Workflow{
				Name:    "value-composite",
				Profile: v1.CurrentProfile,
				Steps: append([]*v1.Node{
					{
						Id:   "regions",
						Kind: &v1.Node_Value{Value: v1.NewExpr(`["eu", "us", "ap"]`)},
					},
				}, pins("show", `steps.regions.`+v1.ValueOutput+`[1] == "us" && size(steps.regions.`+v1.ValueOutput+`) == 3`)...),
			},
			ExpectedOutputs: withStep(held("show"), "regions", map[string]*v1.Value{
				v1.ValueOutput: v1.NewLiteralList("eu", "us", "ap"),
			}),
		},
		{
			// Skipped, then referenced. The skip is the ordinary `if:` rule, and
			// the reference downstream is what makes the skip observable: a
			// skipped step is *absent* from the outputs rather than present and
			// empty, so `steps.absent.value` resolves against nothing and the run
			// fails. Both drivers must fail it, because a driver that read it as
			// an empty value would let a workload proceed on a fact it never
			// computed.
			Name: "a skipped value leaves nothing behind, and reading it fails the run",
			Workflow: &v1.Workflow{
				Name:    "value-skipped",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id:        "absent",
						Condition: v1.NewExpr("false"),
						Kind:      &v1.Node_Value{Value: v1.NewExpr("1 + 1")},
					},
					guarded("reads", "steps.absent."+v1.ValueOutput+" == 2", "read a value that never ran"),
				},
			},
			ExpectFailure: true,
		},
		{
			// Into the run's declared outputs, which is where the corpus files
			// this feature exists for read their values: the answer document is
			// built after the last step, in its own evaluation position, and a
			// value has to be visible from there exactly as any step output is.
			Name: "a value is readable from the run's declared outputs",
			Workflow: declares("value-in-outputs",
				[]*v1.InputDeclaration{input("amount", v1.InputDeclaration_TYPE_INT, true, nil)},
				[]*v1.OutputDeclaration{
					output("decision", `steps.large.`+v1.ValueOutput+` ? "review" : "auto"`),
					// The negation the whole feature is for: one spelling of the
					// fact and one `!`, rather than a hand-expanded complement
					// that can drift from what it negates.
					output("automatic", `!steps.large.`+v1.ValueOutput),
				},
				&v1.Node{
					Id:   "large",
					Kind: &v1.Node_Value{Value: v1.NewExpr("inputs.amount > 1000")},
				},
			),
			Inputs: map[string]*v1.Value{"amount": v1.NewLiteral(int64(50))},
			ExpectedOutputs: answers(
				withStep(&v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}},
					"large", map[string]*v1.Value{v1.ValueOutput: v1.NewLiteral(false)}),
				map[string]*v1.Value{
					"decision":  v1.NewLiteral("auto"),
					"automatic": v1.NewLiteral(true),
				},
			),
		},
	}
}

// withStep adds one step's named outputs to an expectation built by [held].
//
// [held] says "this step ran and produced nothing", which is every `log:` step a
// shared case observes through. A value step produces something, so it needs the
// entry written out, and writing it out is the assertion that matters here,
// since the name the result is stored under is the half of this feature every
// tool downstream depends on.
func withStep(outputs *v1.Workflow_StepOutputs, id string, values map[string]*v1.Value) *v1.Workflow_StepOutputs {
	outputs.StepValues[id] = &v1.Node_Outputs{NamedValues: values}

	return outputs
}
