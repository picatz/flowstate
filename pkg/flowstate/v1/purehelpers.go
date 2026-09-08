package flowstatev1

import (
	"fmt"
	"slices"
	"strings"

	"github.com/google/cel-go/cel"
	commonast "github.com/google/cel-go/common/ast"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

const (
	maxPureHelpers       = 64
	maxPureHelperBodyAST = 4096
	maxPureHelperCalls   = 1024
	maxExpandedHelperAST = 100_000
)

type checkedPureHelper struct {
	definition *PureHelper
	body       *cel.Ast
}

// ExpandPureHelpers normalizes imported, typed pure helpers into ordinary CEL
// expressions throughout wf, including embedded callees. Definitions do not
// enter the Workflow and workers need no helper registry or second evaluator.
// Arguments are wrapped in cel.bind during expansion, preserving one evaluation
// and CEL's existing short-circuit and error behavior.
func ExpandPureHelpers(wf *Workflow, helpers []*PureHelper) error {
	if wf == nil {
		return fmt.Errorf("cannot expand pure helpers in an empty workflow")
	}
	if len(helpers) == 0 {
		return nil
	}
	expanded := proto.Clone(wf).(*Workflow)
	if err := expandPureHelpers(expanded, helpers); err != nil {
		return err
	}
	proto.Reset(wf)
	proto.Merge(wf, expanded)
	return nil
}

func expandPureHelpers(wf *Workflow, helpers []*PureHelper) error {
	if len(helpers) > maxPureHelpers {
		return fmt.Errorf("helper library exports %d pure helpers; at most %d may be imported", len(helpers), maxPureHelpers)
	}
	if err := expandPureHelpersInTree(wf, "", helpers, 0); err != nil {
		return fmt.Errorf("expanding pure helpers: %w", err)
	}
	return nil
}

func expandPureHelpersInTree(wf *Workflow, callerProfile string, helpers []*PureHelper, depth int) error {
	if depth > maxWorkflowScanDepth {
		return fmt.Errorf("calls nest more than %d deep, past what a specification is checked to", maxWorkflowScanDepth)
	}
	profile := CalleeProfile(callerProfile, wf)
	checked, declarations, err := checkPureHelpers(profile, helpers)
	if err != nil {
		return fmt.Errorf("workflow %q: %w", wf.GetName(), err)
	}
	var sites []ValueSite
	var callees []*Workflow
	truncated := false
	WalkWorkflow(wf, Walk{Value: func(site ValueSite) {
		if site.Value.GetExpr() != nil {
			sites = append(sites, site)
		}
	}, Node: func(node *Node) {
		if callee := node.GetCall().GetWorkflow(); callee != nil {
			callees = append(callees, callee)
		}
	}, Truncated: func(ValueSite) {
		truncated = true
	}})
	if truncated {
		return fmt.Errorf("workflow %q has values nested past the inspection bound", wf.GetName())
	}
	for _, site := range sites {
		if err := expandHelpersInValue(profile, site.Value, checked, declarations); err != nil {
			where := site.Field()
			if site.Step != "" {
				where = fmt.Sprintf("step %q %s", site.Step, where)
			}
			return fmt.Errorf("workflow %q %s: %w", wf.GetName(), where, err)
		}
	}
	for _, callee := range callees {
		if err := expandPureHelpersInTree(callee, profile, helpers, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func checkPureHelpers(profile string, helpers []*PureHelper) (map[string]checkedPureHelper, []cel.EnvOption, error) {
	libs, err := ProfileLibraries(profile)
	if err != nil {
		return nil, nil, err
	}
	base, err := DefaultEvaluator().Env(libs...)
	if err != nil {
		return nil, nil, err
	}

	checked := make(map[string]checkedPureHelper, len(helpers))
	declarations := make([]cel.EnvOption, 0, len(helpers))
	for _, helper := range helpers {
		if helper == nil {
			return nil, nil, fmt.Errorf("helper library contains an empty declaration")
		}
		if err := Validate(helper); err != nil {
			return nil, nil, fmt.Errorf("pure helper %q%s is invalid: %w", helper.GetName(), helperSource(helper), err)
		}
		if _, exists := checked[helper.GetName()]; exists {
			return nil, nil, fmt.Errorf("pure helper %q is declared more than once", helper.GetName())
		}
		body := helper.GetBody().GetExpr()
		if body == nil {
			return nil, nil, fmt.Errorf("pure helper %q%s must have an expression body", helper.GetName(), helperSource(helper))
		}

		parameterNames := make(map[string]bool, len(helper.GetParameters()))
		envOpts := make([]cel.EnvOption, 0, len(helper.GetParameters()))
		argTypes := make([]*cel.Type, 0, len(helper.GetParameters()))
		for _, parameter := range helper.GetParameters() {
			if parameterNames[parameter.GetName()] {
				return nil, nil, fmt.Errorf("pure helper %q%s declares parameter %q more than once", helper.GetName(), helperSource(helper), parameter.GetName())
			}
			parameterNames[parameter.GetName()] = true
			t := pureHelperCELType(parameter.GetType())
			envOpts = append(envOpts, cel.Variable(parameter.GetName(), t))
			argTypes = append(argTypes, t)
		}
		env, err := base.Extend(envOpts...)
		if err != nil {
			return nil, nil, fmt.Errorf("pure helper %q%s: build its type environment: %w", helper.GetName(), helperSource(helper), err)
		}
		bodyAST := cel.ParsedExprToAst(body)
		bodyChecked, issues := env.Check(bodyAST)
		if issues != nil && issues.Err() != nil {
			return nil, nil, fmt.Errorf("pure helper %q%s body does not type-check: %w", helper.GetName(), helperSource(helper), issues.Err())
		}
		if got, want := bodyChecked.OutputType(), pureHelperCELType(helper.GetResultType()); !want.IsAssignableType(got) {
			return nil, nil, fmt.Errorf("pure helper %q%s declares result %s but its body produces %s", helper.GetName(), helperSource(helper), DeclaredTypeName(helper.GetResultType()), got)
		}
		if n := expressionASTSize(bodyChecked); n > maxPureHelperBodyAST {
			return nil, nil, fmt.Errorf("pure helper %q%s body expands to %d CEL nodes; at most %d are allowed", helper.GetName(), helperSource(helper), n, maxPureHelperBodyAST)
		}

		checked[helper.GetName()] = checkedPureHelper{definition: helper, body: bodyChecked}
		overload := strings.NewReplacer(".", "_", "-", "_").Replace(helper.GetName()) + "_pure_helper"
		declarations = append(declarations, cel.Function(helper.GetName(), cel.Overload(overload, argTypes, pureHelperCELType(helper.GetResultType()))))
	}
	return checked, declarations, nil
}

func helperSource(helper *PureHelper) string {
	if helper.GetSource() == "" {
		return ""
	}
	if helper.GetSourceLine() == 0 {
		return " at " + helper.GetSource()
	}
	return fmt.Sprintf(" at %s:%d", helper.GetSource(), helper.GetSourceLine())
}

func pureHelperCELType(t InputDeclaration_Type) *cel.Type {
	switch t {
	case InputDeclaration_TYPE_STRING, InputDeclaration_TYPE_ENUM:
		return cel.StringType
	case InputDeclaration_TYPE_INT:
		return cel.IntType
	case InputDeclaration_TYPE_FLOAT:
		return cel.DoubleType
	case InputDeclaration_TYPE_BOOL:
		return cel.BoolType
	case InputDeclaration_TYPE_STRUCT:
		return cel.MapType(cel.StringType, cel.DynType)
	case InputDeclaration_TYPE_LIST:
		return cel.ListType(cel.DynType)
	default:
		return cel.DynType
	}
}

func expandHelpersInValue(profile string, value *Value, helpers map[string]checkedPureHelper, declarations []cel.EnvOption) error {
	parsed := value.GetExpr()
	libs, err := ProfileLibraries(profile)
	if err != nil {
		return err
	}
	env, err := DefaultEvaluator().Env(libs...)
	if err != nil {
		return err
	}
	rootNames := expressionIdentifiers(parsed.GetExpr())
	opts := make([]cel.EnvOption, 0, len(declarations)+len(rootNames))
	opts = append(opts, declarations...)
	for _, name := range rootNames {
		opts = append(opts, cel.Variable(name, cel.DynType))
	}
	env, err = env.Extend(opts...)
	if err != nil {
		return err
	}
	checked, issues := env.Check(cel.ParsedExprToAst(parsed))
	if issues != nil && issues.Err() != nil {
		return issues.Err()
	}

	optimizer, err := cel.NewStaticOptimizer(&pureHelperOptimizer{helpers: helpers})
	if err != nil {
		return err
	}
	expanded, issues := optimizer.Optimize(env, checked)
	if issues != nil && issues.Err() != nil {
		return issues.Err()
	}
	if n := expressionASTSize(expanded); n > maxExpandedHelperAST {
		return fmt.Errorf("pure-helper expansion produces %d CEL nodes; at most %d are allowed", n, maxExpandedHelperAST)
	}
	out, err := cel.AstToParsedExpr(expanded)
	if err != nil {
		return fmt.Errorf("encode expanded expression: %w", err)
	}
	value.Kind = &Value_Expr{Expr: out}
	return nil
}

type pureHelperOptimizer struct {
	helpers map[string]checkedPureHelper
	calls   int
}

func (o *pureHelperOptimizer) Optimize(ctx *cel.OptimizerContext, tree *commonast.AST) *commonast.AST {
	root := commonast.NavigateAST(tree)
	matches := commonast.MatchDescendants(root, func(expr commonast.NavigableExpr) bool {
		if expr.Kind() != commonast.CallKind || expr.AsCall().IsMemberFunction() {
			return false
		}
		_, ok := o.helpers[expr.AsCall().FunctionName()]
		return ok
	})
	for _, match := range matches {
		o.calls++
		if o.calls > maxPureHelperCalls {
			ctx.ReportErrorAtID(match.ID(), "expression calls imported pure helpers more than %d times", maxPureHelperCalls)
			return tree
		}
		call := match.AsCall()
		helper := o.helpers[call.FunctionName()]
		if len(call.Args()) != len(helper.definition.GetParameters()) {
			// The checker normally reports this first. Keep the optimizer total for
			// a hand-built checked AST that omitted overload metadata.
			ctx.ReportErrorAtID(match.ID(), "pure helper %s expects %d arguments, got %d", call.FunctionName(), len(helper.definition.GetParameters()), len(call.Args()))
			return tree
		}
		replacement := ctx.CopyASTAndMetadata(helper.body.NativeRep())
		for i := len(call.Args()) - 1; i >= 0; i-- {
			bindID := match.ID()
			if i != 0 {
				bindID = ctx.NewIdent("unused").ID()
			}
			var macro commonast.Expr
			replacement, macro = ctx.NewBindMacro(bindID, helper.definition.GetParameters()[i].GetName(), call.Args()[i], replacement)
			ctx.SetMacroCall(bindID, macro)
		}
		ctx.UpdateExpr(match, replacement)
	}
	return tree
}

func expressionASTSize(a *cel.Ast) int {
	if a == nil {
		return 0
	}
	return len(commonast.MatchDescendants(commonast.NavigateAST(a.NativeRep()), commonast.AllMatcher()))
}

func expressionIdentifiers(root *exprpb.Expr) []string {
	// Parsed expression trees are walked through their protobuf representation
	// here because it is the representation Workflow already carries.
	if root == nil {
		return nil
	}
	seen := map[string]bool{}
	stack := []*exprpb.Expr{root}
	for len(stack) > 0 {
		e := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		switch kind := e.GetExprKind().(type) {
		case *exprpb.Expr_IdentExpr:
			seen[kind.IdentExpr.GetName()] = true
		case *exprpb.Expr_SelectExpr:
			stack = append(stack, kind.SelectExpr.GetOperand())
		case *exprpb.Expr_CallExpr:
			stack = append(stack, kind.CallExpr.GetTarget())
			stack = append(stack, kind.CallExpr.GetArgs()...)
		case *exprpb.Expr_ListExpr:
			stack = append(stack, kind.ListExpr.GetElements()...)
		case *exprpb.Expr_StructExpr:
			for _, entry := range kind.StructExpr.GetEntries() {
				stack = append(stack, entry.GetMapKey(), entry.GetValue())
			}
		case *exprpb.Expr_ComprehensionExpr:
			c := kind.ComprehensionExpr
			stack = append(stack, c.GetIterRange(), c.GetAccuInit(), c.GetLoopCondition(), c.GetLoopStep(), c.GetResult())
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}
