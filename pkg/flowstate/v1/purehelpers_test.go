package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

func TestPureHelperIsTypedExpandedAndReusableAcrossWorkflows(t *testing.T) {
	wf, helpers, expected := conformance.PureHelperPrototype()
	require.NoError(t, v1.ExpandPureHelpers(wf, helpers))

	var expressions []string
	v1.WalkWorkflow(wf, v1.Walk{Value: func(site v1.ValueSite) {
		if site.Value.GetExpr() == nil {
			return
		}
		text, err := cel.AstToString(cel.ParsedExprToAst(site.Value.GetExpr()))
		require.NoError(t, err)
		expressions = append(expressions, text)
	}})
	require.NotEmpty(t, expressions)
	for _, expression := range expressions {
		require.NotContains(t, expression, "environment.safeRatio", "a runtime helper call survived normalization")
	}
	require.Contains(t, strings.Join(expressions, "\n"), "cel.bind", "arguments were not bound for single evaluation")

	require.NoError(t, v1.ResolveTaskCapabilities(wf, v1.DefaultRegistry()))
	out, err := v1.Run(t.Context(), wf)
	require.NoError(t, err, "the zero-divisor call lost CEL short-circuit error behavior")
	require.True(t, proto.Equal(expected, out))
}

func TestPureHelperDiagnosticsNameDefinitionSourceAndUse(t *testing.T) {
	t.Run("definition body", func(t *testing.T) {
		wf := &v1.Workflow{Name: "bad-definition"}
		helper := &v1.PureHelper{
			Name: "environment.bad", Source: "modules/environment/helpers.cel", SourceLine: 27,
			Parameters: []*v1.PureHelperParameter{{Name: "n", Type: v1.InputDeclaration_TYPE_INT}},
			ResultType: v1.InputDeclaration_TYPE_BOOL,
			Body:       v1.NewExpr(`n + "wrong"`),
		}
		err := v1.ExpandPureHelpers(wf, []*v1.PureHelper{helper})
		require.ErrorContains(t, err, `pure helper "environment.bad" at modules/environment/helpers.cel:27`)
		require.ErrorContains(t, err, "body does not type-check")
	})

	t.Run("call site", func(t *testing.T) {
		wf, helpers, _ := conformance.PureHelperPrototype()
		wf.GetSteps()[0].GetValue().Kind = v1.NewExpr(`environment.safeRatio("ten", 2)`).GetKind()
		err := v1.ExpandPureHelpers(wf, helpers)
		require.ErrorContains(t, err, `workflow "helper-caller" step "ratio" value`)
		require.ErrorContains(t, err, "no matching overload")
	})

	t.Run("no workflow closure", func(t *testing.T) {
		wf := &v1.Workflow{Name: "closed-helper"}
		helper := &v1.PureHelper{
			Name: "environment.leaky", Source: "helpers.cel", SourceLine: 3,
			ResultType: v1.InputDeclaration_TYPE_STRING,
			Body:       v1.NewExpr("inputs.secret"),
		}
		err := v1.ExpandPureHelpers(wf, []*v1.PureHelper{helper})
		require.ErrorContains(t, err, "undeclared reference to 'inputs'")
	})
}

func TestPureHelperExpansionIsBoundedAndAtomic(t *testing.T) {
	wf, helpers, _ := conformance.PureHelperPrototype()
	calls := make([]string, 1025)
	for i := range calls {
		calls[i] = "environment.safeRatio(1, 1)"
	}
	wf.GetSteps()[1].GetCall().GetWorkflow().GetSteps()[0].GetValue().Kind = v1.NewExpr("[" + strings.Join(calls, ",") + "]").GetKind()
	before := proto.Clone(wf).(*v1.Workflow)
	err := v1.ExpandPureHelpers(wf, helpers)
	require.ErrorContains(t, err, "more than 1024 times")
	require.True(t, proto.Equal(before, wf), "a refused expansion partially rewrote the workflow")
}

func TestPureHelperExpansionUsesEachWorkflowProfile(t *testing.T) {
	wf, helpers, _ := conformance.PureHelperPrototype()
	wf.GetSteps()[1].GetCall().GetWorkflow().Profile = "2099.9"
	before := proto.Clone(wf).(*v1.Workflow)
	err := v1.ExpandPureHelpers(wf, helpers)
	require.ErrorContains(t, err, `workflow "helper-callee": unknown language profile "2099.9"`)
	require.True(t, proto.Equal(before, wf), "a profile refusal partially rewrote the workflow")
}

func TestPureHelperArgumentsEvaluateInCallerScope(t *testing.T) {
	wf := &v1.Workflow{
		Name: "caller-scope",
		Vars: map[string]*v1.Value{"n": v1.NewLiteral(42)},
		Steps: []*v1.Node{{
			Id:   "pick",
			Kind: &v1.Node_Value{Value: v1.NewExpr(`helpers.pick({"n": 7}, vars.n)`)},
		}},
	}
	helper := &v1.PureHelper{
		Name: "helpers.pick",
		Parameters: []*v1.PureHelperParameter{
			{Name: "vars", Type: v1.InputDeclaration_TYPE_STRUCT},
			{Name: "n", Type: v1.InputDeclaration_TYPE_INT},
		},
		ResultType: v1.InputDeclaration_TYPE_INT,
		Body:       v1.NewExpr("n"),
	}

	require.NoError(t, v1.ExpandPureHelpers(wf, []*v1.PureHelper{helper}))
	out, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)
	require.EqualValues(t, 42, out.GetStepValues()["pick"].GetNamedValues()[v1.ValueOutput].GetLiteral().GetInt64Value())
}

func TestPureHelperExpandsOnlyItsResolvedOverload(t *testing.T) {
	wf := &v1.Workflow{
		Name: "overload",
		Steps: []*v1.Node{{
			Id: "absolute", Kind: &v1.Node_Value{Value: v1.NewExpr("math.abs(-1)")},
		}},
	}
	helper := &v1.PureHelper{
		Name:       "math.abs",
		Parameters: []*v1.PureHelperParameter{{Name: "value", Type: v1.InputDeclaration_TYPE_STRING}},
		ResultType: v1.InputDeclaration_TYPE_STRING,
		Body:       v1.NewExpr("value"),
	}

	require.NoError(t, v1.ExpandPureHelpers(wf, []*v1.PureHelper{helper}))
	out, err := v1.Run(t.Context(), wf)
	require.NoError(t, err)
	require.EqualValues(t, 1, out.GetStepValues()["absolute"].GetNamedValues()[v1.ValueOutput].GetLiteral().GetInt64Value())
}

func TestPureHelperBodyBoundAppliesBeforeTypeChecking(t *testing.T) {
	wf := &v1.Workflow{Name: "bounded"}
	helper := &v1.PureHelper{
		Name:       "helpers.tooLarge",
		ResultType: v1.InputDeclaration_TYPE_LIST,
		Body:       v1.NewExpr("[" + strings.Repeat("1,", 4097) + "1]"),
	}

	err := v1.ExpandPureHelpers(wf, []*v1.PureHelper{helper})
	require.ErrorContains(t, err, "before type checking")
	require.ErrorContains(t, err, "at most 4096")
}

func TestPureHelperNilParameterFailsClosed(t *testing.T) {
	err := v1.ExpandPureHelpers(&v1.Workflow{Name: "invalid"}, []*v1.PureHelper{{
		Name: "helpers.invalid", Parameters: []*v1.PureHelperParameter{nil},
		ResultType: v1.InputDeclaration_TYPE_INT, Body: v1.NewExpr("1"),
	}})
	require.Error(t, err)
}
