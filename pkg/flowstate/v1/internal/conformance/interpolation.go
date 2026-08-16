package conformance

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// InterpolationSource is the expression the Flowfile compiler produces for one
// particular interpolated scalar, written out here so that the two drivers can
// be held to the same answer for it.
//
// It is a constant rather than a string built in each case for the reason
// CLAUDE.md gives about values with one meaning written down twice: the whole
// point of these cases is that two drivers agree about one expression, and two
// copies of that expression is the shape every disagreement found so far has
// had. `TestInterpolationDesugarsToTheSharedDriverCase` in the flowfile package
// compiles the Flowfile spelling and asserts the compiler emits exactly this, so
// the constant cannot drift away from the thing it stands for either.
//
// The scalar it comes from is:
//
//	kinds: s=${inputs.s} i=${inputs.i} u=${uint(7)} d=${2.5} b=${true} dur=${duration('90s')} t=${timestamp('2026-08-12T00:00:00Z')}
const InterpolationSource = `'kinds: s=' + string(inputs.s) + ' i=' + string(inputs.i) + ` +
	`' u=' + string(uint(7)) + ' d=' + string(2.5) + ' b=' + string(true) + ` +
	`' dur=' + string(duration('90s')) + ' t=' + string(timestamp('2026-08-12T00:00:00Z'))`

// InterpolationCases are the shared cases that hold both drivers to one
// behaviour for a scalar mixing literal text with ${...} expressions (#413).
//
// # Why these cases exist at all, given the desugaring
//
// Interpolation is compiled away: a mixed scalar becomes one CEL expression in
// the Flowfile compiler, which both drivers read their workflows from, so
// neither driver has any interpolation code to get wrong. That is the design,
// and it is deliberately the design — it is what makes driver agreement
// structural instead of tested.
//
// What is *not* compiled away is the stringification. Every fence is wrapped in
// CEL's `string()`, and `string()` is evaluated by each driver's own evaluator
// against its own environment, at its own moment. A double that renders as `2.5`
// under one driver and `2.5000000` under the other, or a timestamp that picks up
// a local zone in one place and UTC in the other, is precisely the class of
// disagreement invariant 3 exists to stop: a local rehearsal that shows an author
// a message their production run will not print.
//
// So the cases pin the rendering of every type the conversion is defined on —
// string, int, uint, double, bool, duration, timestamp — in one value, compared
// exactly. The exact comparison is the point; a predicate asking whether the
// output "contains" each piece would pass on a driver that rendered a double
// with a different precision.
func InterpolationCases() []Case {
	return []Case{
		{
			// One value carrying every convertible type, rather than a case per
			// type. A single expression is how a real message is written, and it
			// also means a driver that got one type wrong fails with the whole
			// rendering side by side with the expected one, which is the readable
			// way to be told.
			Name: "interpolation renders every convertible type the same way on both drivers",
			Workflow: declares("interpolation-kinds",
				[]*v1.InputDeclaration{
					input("s", v1.InputDeclaration_TYPE_STRING, true, nil),
					input("i", v1.InputDeclaration_TYPE_INT, true, nil),
				},
				nil,
				&v1.Node{
					Id:   "message",
					Kind: &v1.Node_Value{Value: v1.NewExpr(InterpolationSource)},
				},
			),
			Inputs: map[string]*v1.Value{
				"s": v1.NewLiteral("hello"),
				"i": v1.NewLiteral(int64(-3)),
			},
			ExpectedOutputs: withStep(held(), "message", map[string]*v1.Value{
				v1.ValueOutput: v1.NewLiteral(
					"kinds: s=hello i=-3 u=7 d=2.5 b=true dur=90s t=2026-08-12T00:00:00Z"),
			}),
		},
		{
			// The whole-value boundary, which is the one thing about
			// interpolation that a driver could observe as a *type* rather than
			// as text. `${0}` is the integer zero and `${0} ` is the string "0 ",
			// and the difference is decided once, in the compiler, by whether the
			// fence is the whole value. Both drivers are held to it here so that
			// the boundary is a property of the system rather than of the parser
			// alone: a driver that coerced a value on its way into `step_values`
			// would erase exactly this distinction.
			Name: "a whole-value fence keeps its type where an interpolated one becomes text",
			Workflow: &v1.Workflow{
				Name:    "interpolation-boundary",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id:   "typed",
						Kind: &v1.Node_Value{Value: v1.NewExpr("0")},
					},
					{
						Id: "text",
						// What the compiler emits for `${0} `, spelled out for
						// the same reason InterpolationSource is.
						Kind: &v1.Node_Value{Value: v1.NewExpr(`string(0) + ' '`)},
					},
				},
			},
			ExpectedOutputs: withStep(withStep(held(), "typed", map[string]*v1.Value{
				v1.ValueOutput: v1.NewLiteral(int64(0)),
			}), "text", map[string]*v1.Value{
				v1.ValueOutput: v1.NewLiteral("0 "),
			}),
		},
	}
}
