package conformance

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A declared output whose value is already known is checked at the submit
// boundary, not at completion.
//
// [v1.CheckOutputConstraintShape] has run in [v1.BindRunInputs] since outputs
// gained `must:`, for the reason that function's own doc gives: it is the one
// place every submit path passes through, so it is where a specification that
// never was a Flowfile gets the check the compiler would otherwise have been
// the only one to make. The *value* half arrived later and had the same hole —
// a literal (or an all-literal structure) contradicting its own `type:` or
// `values:` is wrong on submission, and nothing a run does can make it right,
// yet it was discovered inside [v1.EvalRunOutputs] after every step and
// whatever side effects those steps had.
//
// These are the shared cases for the closed half. The open half stays open on
// purpose and [InputOutputCases] pins it: a *computed* output is still judged
// at completion, because that is the first moment its value exists.

// literalOutput declares one output whose value is a literal rather than an
// expression — the shape a hand-built [v1.Workflow] carries, and the one a
// Flowfile reaches through a mapping, a list, or a non-string scalar.
func literalOutput(name string, value *v1.Value, t v1.InputDeclaration_Type, values ...string) *v1.OutputDeclaration {
	return &v1.OutputDeclaration{
		Name:   name,
		Value:  value,
		Type:   t,
		Values: values,
	}
}

// OutputValueRefusalCases returns specifications both drivers must refuse
// before anything runs, because an output they declare already contradicts
// itself.
//
// Every one is a [v1.Workflow] built as a message rather than parsed from a
// file, which is the point: `flow validate` reports each of these against a
// line and a column, and until the submit boundary ran the same check a
// programmatic caller got none at all. What these pin is that the two paths now
// refuse the same specification in the same words, because both reach
// [v1.CheckOutputValue].
func OutputValueRefusalCases() []Refusal {
	return []Refusal{
		{
			// The enum half. A literal string outside the declared set is a
			// promise the specification breaks against itself, knowable without
			// running anything.
			Name: "a literal enum output outside its declared values is refused at submit",
			Workflow: declares("outputs-literal-enum-violated",
				nil,
				[]*v1.OutputDeclaration{
					literalOutput("channel", v1.NewLiteral("canary"),
						v1.InputDeclaration_TYPE_ENUM, "stable", "beta"),
				},
				says("a", "hello"),
			),
			Contains: `output "channel" is "canary", which is not one of the values channel declares`,
		},
		{
			// The type half, which reaches a different check inside
			// [v1.CheckOutputValue] and so is its own case rather than a
			// variation on the one above.
			Name: "a literal output of the wrong type is refused at submit",
			Workflow: declares("outputs-literal-type-violated",
				nil,
				[]*v1.OutputDeclaration{
					literalOutput("count", v1.NewLiteral(int64(3)), v1.InputDeclaration_TYPE_STRING),
				},
				says("a", "hello"),
			),
			Contains: `output "count" is declared string but computed int`,
		},
		{
			// Admission is a surface too. The refusal above names the value, and
			// must not when the declaration says the value is sensitive — the
			// #1396 rule, which reaches here because both boundaries render the
			// sentence through the same [v1.CheckOutputValue].
			//
			// A specification refused at submit still produces text somebody
			// reads and something stores: the server logs it, the CLI prints it,
			// and a schedule records why the run it would have started did not.
			Name: "a sensitive literal enum output is refused without echoing its value",
			Workflow: declares("outputs-literal-enum-sensitive",
				nil,
				[]*v1.OutputDeclaration{
					sensitive(literalOutput("token", v1.NewLiteral(sensitiveAnswer),
						v1.InputDeclaration_TYPE_ENUM, "stable", "beta")),
				},
				says("a", "hello"),
			),
			Contains: `output "token" is ` + v1.SensitiveMarker +
				`, which is not one of the values token declares: "stable", "beta"`,
			Omits: sensitiveAnswer,
		},
	}
}
