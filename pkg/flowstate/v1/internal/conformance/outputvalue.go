package conformance

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
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
			// A structural declaration is schema-valid before the runtime knows
			// how to enforce it. Refuse the whole workflow at submit rather than
			// reading the legacy zero value as an untyped output and reporting an
			// unchecked answer.
			Name: "a structural-only output is refused until the runtime projects it",
			Workflow: declares("outputs-structural-only",
				nil,
				[]*v1.OutputDeclaration{{
					Name:  "answer",
					Value: v1.NewLiteral("ok"),
					ValueType: &v1.Type{Kind: &v1.Type_Scalar_{
						Scalar: v1.Type_SCALAR_STRING,
					}},
				}},
				says("a", "hello"),
			),
			Contains: `output "answer" uses value_type without a legacy type`,
		},
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
		{
			// #1404. The kind half of the type check passes — a map keyed by
			// anything is a map — and the declaration is still broken: `struct`
			// is the promise that this output reads back as a plain object, and
			// a non-string key is one nothing outside this schema can spell.
			// Answerable at submit for the same reason the two above are: the
			// literal is written down, so no run can change what it holds.
			Name: "a literal struct output keyed by an int is refused at submit",
			Workflow: declares("outputs-literal-struct-int-keys",
				nil,
				[]*v1.OutputDeclaration{
					literalOutput("detail", intKeyedMap(), v1.InputDeclaration_TYPE_STRUCT),
				},
				says("a", "hello"),
			),
			Contains: `output "detail" is declared struct but computed a map with int keys; ` +
				`a struct is a map with string keys`,
		},
		{
			// The other declared container, refused at the same boundary by the
			// same check. Its own case rather than a variation, because the
			// sentence a `list` earns names a different promise — the plain
			// array, not the plain object — and a fix that closed only the outer
			// map would pass this one.
			Name: "a literal list output holding an int-keyed map is refused at submit",
			Workflow: declares("outputs-literal-list-int-keys",
				nil,
				[]*v1.OutputDeclaration{
					literalOutput("items", v1.NewLiteralList(intKeyedMap().GetLiteral()),
						v1.InputDeclaration_TYPE_LIST),
				},
				says("a", "hello"),
			),
			Contains: `output "items" is declared list but holds a map with int keys; ` +
				`a list reads back as a plain array, whose maps have string keys`,
		},
		{
			// The bound on the walk itself, at the boundary that reaches it
			// first. [v1.CheckOutputValue] converts a declared container with
			// [v1.LiteralToGo], which recurses, and [v1.BindRunInputs] runs
			// ahead of [v1.CheckSubmissionSize] — so on the local driver an
			// in-process caller reaches this recursion with a literal nobody
			// weighed. Deep enough, and an unbounded walk exhausts the goroutine
			// stack: a crash of the embedding process, which is not an outcome a
			// caller can recover from or a server can report (invariant 5).
			//
			// The refusal names the bound rather than the depth, because the
			// walk stops at the bound instead of measuring how much further the
			// value goes.
			//
			// Only the refused side is a shared case. Its partner one level
			// shallower — admitted, and converted again at completion — is
			// `TestAContainerOutputIsBoundedByWalkDepth` in the v1 package,
			// because an accepted deep value has to be *compared*, and
			// `protocmp.Transform` under `cmp.Diff` costs roughly 15x per four
			// levels of message nesting: 4s at depth 16, 80s at depth 20, and
			// unrunnable at this bound. That is a property of the comparison in
			// both drivers' runners, not of the value, so the boundary is
			// asserted where it can be asserted directly and the both-drivers
			// claim stays on the refusal, which is the observable behavior.
			Name: "a literal struct output nested past the walk's bound is refused at submit",
			Workflow: declares("outputs-literal-struct-too-deep",
				nil,
				[]*v1.OutputDeclaration{
					literalOutput("detail", NestedMapLiteral(v1.MaxStructureDepth+1),
						v1.InputDeclaration_TYPE_STRUCT),
				},
				says("a", "hello"),
			),
			Contains: `output "detail" is declared struct but nests deeper than the 32 levels ` +
				`this server can walk`,
		},
	}
}

// NestedMapLiteral is a literal whose maps nest depth levels above a string.
//
// Exported because the two halves of the depth boundary are asserted in
// different places — the refusal here, across both drivers, and the accepted
// value in the v1 package's own test — and a boundary asserted against two
// differently built values would not be a boundary. One entry per level, so
// depth is the only thing it varies.
func NestedMapLiteral(depth int) *v1.Value {
	literal := &expr.Value{Kind: &expr.Value_StringValue{StringValue: "leaf"}}
	for range depth {
		literal = &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{
			Entries: []*expr.MapValue_Entry{{
				Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: "k"}},
				Value: literal,
			}},
		}}}
	}

	return &v1.Value{Kind: &v1.Value_Literal{Literal: literal}}
}

// intKeyedMap is `{1: "value"}` as a literal, which no Flowfile can write.
//
// A mapping under `value:` compiles to a structure whose keys are the document's
// own — YAML keys are strings — so this shape reaches a declared output only
// from an expression the run evaluates or from a specification built as a
// message. Both are refused by [v1.CheckOutputValue]; this is the second, which
// is the one a submit boundary answers.
func intKeyedMap() *v1.Value {
	return &v1.Value{Kind: &v1.Value_Literal{Literal: &expr.Value{
		Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{
			Entries: []*expr.MapValue_Entry{{
				Key:   &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 1}},
				Value: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "value"}},
			}},
		}},
	}}}
}
