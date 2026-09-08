package conformance

import v1 "github.com/picatz/flowstate/pkg/flowstate/v1"

// PureHelperPrototype returns one imported helper reused by a caller and an
// embedded callee. The zero divisor exercises short-circuit error preservation:
// expansion must not evaluate the division after the first arm is true.
func PureHelperPrototype() (*v1.Workflow, []*v1.PureHelper, *v1.Workflow_StepOutputs) {
	helper := &v1.PureHelper{
		Name: "environment.safeRatio",
		Parameters: []*v1.PureHelperParameter{
			{Name: "numerator", Type: v1.InputDeclaration_TYPE_INT},
			{Name: "denominator", Type: v1.InputDeclaration_TYPE_INT},
		},
		ResultType: v1.InputDeclaration_TYPE_BOOL,
		Body:       v1.NewExpr("denominator == 0 || numerator / denominator > 1"),
		Source:     "modules/environment/helpers.cel",
		SourceLine: 12,
	}
	callee := &v1.Workflow{
		Name:    "helper-callee",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "safe", Kind: &v1.Node_Value{Value: v1.NewExpr("environment.safeRatio(1, 0)")},
		}},
		DeclaredOutputs: []*v1.OutputDeclaration{{Name: "safe", Value: v1.NewExpr("steps.safe.value")}},
	}
	root := &v1.Workflow{
		Name:    "helper-caller",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{Id: "ratio", Kind: &v1.Node_Value{Value: v1.NewExpr("environment.safeRatio(10, 2)")}},
			{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}},
		},
	}
	expected := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"ratio":  {NamedValues: map[string]*v1.Value{"value": v1.NewLiteral(true)}},
		"nested": {NamedValues: map[string]*v1.Value{"safe": v1.NewLiteral(true)}},
	}}
	return root, []*v1.PureHelper{helper}, expected
}
