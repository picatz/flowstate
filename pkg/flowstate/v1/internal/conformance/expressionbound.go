package conformance

import (
	"strconv"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// ExpressionElementBoundCases returns the shared cases for #1769: a list
// *manufactured inside* an expression, rather than submitted as an input or
// returned by a task, carrying more elements than an expression may walk
// cheaply. See cellistbound.go in flowstatev1 for the bound and where it sits.
//
// The bound is [taskOutputElementBound] — the same 10,000 #204 chose for the
// other two origins, pinned here a second time for the reason that constant's
// own comment gives.
//
// A shared set rather than one driver's test because the defect was a
// disagreement: at eb8172f the issue's file completed locally in fourteen
// seconds and never completed durably, because consecutive `value:` steps
// evaluate on the workflow side inside one workflow task and two evaluations
// of the expression exceeded the deadlock budget, so the durable driver's
// task panicked and was rescheduled forever. Both drivers must now refuse the
// same file at the same step with the same words, and quickly — the runners
// bound the elapsed time, because a refusal that came only after the
// quadratic work had run would leave the durable half of the defect in place.
func ExpressionElementBoundCases() []Case {
	// The issue's expression, verbatim.
	const spin = "lists.range(60000).map(i, lists.range(100)).flatten().size()"

	return []Case{
		{
			// The file from the issue: four consecutive value steps of the
			// expression, so the case is the shape that wedged the durable
			// driver and not a smaller stand-in. It fails at the first step —
			// asserted by name, so a driver that evaluated a later step first,
			// or ran them all before reporting, would disagree here.
			Name: "the issue's file is refused at its first step on both drivers",
			Workflow: &v1.Workflow{
				Name:    "spin",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{Id: "s1", Kind: &v1.Node_Value{Value: v1.NewExpr(spin)}},
					{Id: "s2", Kind: &v1.Node_Value{Value: v1.NewExpr(spin)}},
					{Id: "s3", Kind: &v1.Node_Value{Value: v1.NewExpr(spin)}},
					{Id: "s4", Kind: &v1.Node_Value{Value: v1.NewExpr(spin)}},
				},
				DeclaredOutputs: []*v1.OutputDeclaration{
					output("n", "steps.s4."+v1.ValueOutput),
				},
			},
			ExpectFailure:         true,
			ExpectedErrorContains: "`lists.range(60000)` would build a list of 60000 elements, over the 10000",
		},
		{
			// A range exactly at the bound runs, on both drivers, and the
			// comprehension over it too: the bound is reached rather than
			// merely not exceeded, per #204's own closing line.
			Name: "a list built at the element bound is allowed",
			Workflow: &v1.Workflow{
				Name:    "at-bound",
				Profile: v1.CurrentProfile,
				Steps: append([]*v1.Node{
					{
						Id: "n",
						Kind: &v1.Node_Value{Value: v1.NewExpr(
							"lists.range(" + strconv.Itoa(taskOutputElementBound) + ").map(i, i).size()")},
					},
				}, pins("show", "steps.n."+v1.ValueOutput+" == "+strconv.Itoa(taskOutputElementBound))...),
			},
			ExpectedOutputs: withStep(held("show"), "n", map[string]*v1.Value{
				v1.ValueOutput: v1.NewLiteral(int64(taskOutputElementBound)),
			}),
		},
		{
			// One past it, built by a comprehension's own accumulation rather
			// than by `lists.range`, so the case reaches the check at the
			// append that crossed the bound and not the one at the range.
			Name: "a list built one past the element bound is refused",
			Workflow: &v1.Workflow{
				Name:    "past-bound",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id: "n",
						Kind: &v1.Node_Value{Value: v1.NewExpr(
							"lists.range(" + strconv.Itoa(taskOutputElementBound/2+1) +
								").map(i, [i, i]).flatten().size()")},
					},
				},
			},
			ExpectFailure:         true,
			ExpectedErrorContains: "`flatten` built a list of 10002 elements, over the 10000",
		},
	}
}
