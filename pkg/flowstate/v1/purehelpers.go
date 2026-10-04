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

// helperDefinition is the one form every declaration of a reusable pure
// computation is checked into, so the expander has a single way to read one
// whether it arrived as an imported [PureHelper] or as a file's own
// [FunctionDeclaration].
type helperDefinition struct {
	name string
	// label is how a diagnostic names the definition, source position included.
	label  string
	params []helperParameter
	result *cel.Type
	// resultName is the declared result as its author wrote it, for the message
	// that says the body produced something else.
	resultName string
	body       *exprpb.ParsedExpr
}

type helperParameter struct {
	name string
	typ  *cel.Type
}

type checkedPureHelper struct {
	definition helperDefinition
	body       *cel.Ast
	// size is the node count of body once any helper it calls has been inlined,
	// which is what a call to this helper costs the expression it lands in.
	size int
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
		if err := expandHelpersInValue(profile, site.Value, checked, declarations, false); err != nil {
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
	checker, err := newHelperChecker(profile, false)
	if err != nil {
		return nil, nil, err
	}
	for _, helper := range helpers {
		if helper == nil {
			return nil, nil, fmt.Errorf("helper library contains an empty declaration")
		}
		label := fmt.Sprintf("pure helper %q%s", helper.GetName(), helperSource(helper))
		if err := Validate(helper); err != nil {
			return nil, nil, fmt.Errorf("%s is invalid: %w", label, err)
		}
		if helper.GetBody().GetExpr() == nil {
			return nil, nil, fmt.Errorf("%s must have an expression body", label)
		}
		definition := helperDefinition{
			name:       helper.GetName(),
			label:      label,
			result:     pureHelperCELType(helper.GetResultType()),
			resultName: DeclaredTypeName(helper.GetResultType()),
			body:       helper.GetBody().GetExpr(),
		}
		for _, parameter := range helper.GetParameters() {
			definition.params = append(definition.params, helperParameter{name: parameter.GetName(), typ: pureHelperCELType(parameter.GetType())})
		}
		if err := checker.add(definition); err != nil {
			return nil, nil, err
		}
	}
	return checker.checked, checker.declarations, nil
}

// helperChecker type-checks definitions one at a time, in the order given, and
// keeps what the expander needs: each checked body and the typed signature an
// expression is checked against.
type helperChecker struct {
	base *cel.Env
	// compose lets a body call a definition added before it. An imported
	// [PureHelper] may not: its body is its parameters and the profile's
	// vocabulary, which is what its contract says.
	compose      bool
	checked      map[string]checkedPureHelper
	declarations []cel.EnvOption

	// budget is the most CEL nodes the composed bodies may add up to, across every
	// definition added; 0 is no aggregate bound. Each body is bounded alone, but
	// sixty-four near-limit wrappers around one near-limit body are each within it
	// and together are what a file's author chose to have allocated before a single
	// call was written.
	budget, spent int
}

func newHelperChecker(profile string, compose bool) (*helperChecker, error) {
	libs, err := ProfileLibraries(profile)
	if err != nil {
		return nil, err
	}
	base, err := DefaultEvaluator().Env(libs...)
	if err != nil {
		return nil, err
	}
	return &helperChecker{base: base, compose: compose, checked: map[string]checkedPureHelper{}}, nil
}

// add checks one definition and, when it may call earlier ones, inlines them into
// its body so the body a use copies is already plain CEL.
func (hc *helperChecker) add(def helperDefinition) error {
	if _, exists := hc.checked[def.name]; exists {
		return fmt.Errorf("%s is declared more than once", strings.TrimSuffix(def.label, labelSourceSuffix(def.label)))
	}

	seen := make(map[string]bool, len(def.params))
	envOpts := make([]cel.EnvOption, 0, len(hc.declarations)+len(def.params))
	if hc.compose {
		envOpts = append(envOpts, hc.declarations...)
	}
	argTypes := make([]*cel.Type, 0, len(def.params))
	for _, parameter := range def.params {
		if seen[parameter.name] {
			return fmt.Errorf("%s declares parameter %q more than once", def.label, parameter.name)
		}
		seen[parameter.name] = true
		envOpts = append(envOpts, cel.Variable(parameter.name, parameter.typ))
		argTypes = append(argTypes, parameter.typ)
	}
	env, err := hc.base.Extend(envOpts...)
	if err != nil {
		return fmt.Errorf("%s: build its type environment: %w", def.label, err)
	}
	body, issues := env.Check(cel.ParsedExprToAst(def.body))
	if issues != nil && issues.Err() != nil {
		return fmt.Errorf("%s body does not type-check: %w", def.label, issues.Err())
	}
	if got := body.OutputType(); !def.result.IsAssignableType(got) {
		return fmt.Errorf("%s declares result %s but its body produces %s", def.label, def.resultName, got)
	}
	if n := expressionASTSize(body); n > maxPureHelperBodyAST {
		return fmt.Errorf("%s body expands to %d CEL nodes; at most %d are allowed", def.label, n, maxPureHelperBodyAST)
	}

	if hc.compose {
		if hc.budget > 0 && len(helperCallMatches(commonast.NavigateAST(body.NativeRep()), hc.checked)) > 0 {
			// Charged from the arithmetic before the body is built, for the reason
			// [projectedHelperSize] gives.
			n := projectedHelperSize(body, hc.checked)
			if hc.spent+n > hc.budget {
				return fmt.Errorf("%s calls functions that expand past %d CEL nodes across this file's definitions altogether; "+
					"call fewer functions from the definitions, or make the large ones smaller", def.label, hc.budget)
			}
			hc.spent += n
		}
		if body, err = expandWithin(env, body, hc.checked, def.label, true); err != nil {
			return err
		}
	}

	hc.checked[def.name] = checkedPureHelper{definition: def, body: body, size: expressionASTSize(body)}
	overload := strings.NewReplacer(".", "_", "-", "_").Replace(def.name) + "_pure_helper"
	hc.declarations = append(hc.declarations, cel.Function(def.name, cel.Overload(overload, argTypes, def.result)))
	return nil
}

// labelSourceSuffix returns the " at file:line" tail of a label, so a message that
// names only the definition can drop it.
func labelSourceSuffix(label string) string {
	if i := strings.LastIndex(label, " at "); i >= 0 {
		return label[i:]
	}
	return ""
}

// projectedHelperSize is what an expression holding the calls in a would grow to if
// they were inlined, computed before any inlining happens: an expansion is built in
// memory in full before it can be measured, so the bound has to be applied to the
// arithmetic rather than to the result. Each parameter costs a few nodes for the
// `cel.bind` that carries it.
func projectedHelperSize(a *cel.Ast, helpers map[string]checkedPureHelper) int {
	total := expressionASTSize(a)
	for _, match := range helperCallMatches(commonast.NavigateAST(a.NativeRep()), helpers) {
		helper := helpers[match.AsCall().FunctionName()]
		total += helper.size + 6*len(helper.definition.params)
	}
	return total
}

// expandWithin inlines every call in checked to a definition in helpers and
// returns the re-checked result, refusing before it builds one that would pass the
// bound.
func expandWithin(env *cel.Env, checked *cel.Ast, helpers map[string]checkedPureHelper, label string, keepCalls bool) (*cel.Ast, error) {
	if len(helperCallMatches(commonast.NavigateAST(checked.NativeRep()), helpers)) == 0 {
		return checked, nil
	}
	if n := projectedHelperSize(checked, helpers); n > maxExpandedHelperAST {
		return nil, fmt.Errorf("%s expands to at least %d CEL nodes once the functions it calls are inlined; at most %d are allowed", label, n, maxExpandedHelperAST)
	}
	optimizer, err := cel.NewStaticOptimizer(&pureHelperOptimizer{helpers: helpers, keepCalls: keepCalls})
	if err != nil {
		return nil, err
	}
	expanded, issues := optimizer.Optimize(env, checked)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("%s: %w", label, issues.Err())
	}
	if n := expressionASTSize(expanded); n > maxExpandedHelperAST {
		return nil, fmt.Errorf("%s expands to %d CEL nodes once the functions it calls are inlined; at most %d are allowed", label, n, maxExpandedHelperAST)
	}
	return expanded, nil
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
	case InputDeclaration_TYPE_TIMESTAMP:
		return cel.TimestampType
	case InputDeclaration_TYPE_DURATION:
		return cel.DurationType
	case InputDeclaration_TYPE_BYTES:
		return cel.BytesType
	default:
		return cel.DynType
	}
}

func expandHelpersInValue(profile string, value *Value, helpers map[string]checkedPureHelper, declarations []cel.EnvOption, keepCalls bool) error {
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

	expanded, err := expandWithin(env, checked, helpers, "pure-helper expansion", keepCalls)
	if err != nil {
		return err
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
	// keepCalls records each call as the macro call its expansion came from, so the
	// expression writes back as the author wrote it (`slug(inputs.title)`) while the
	// tree that runs holds the body. cel-go's unparser prints a node that has a
	// macro call as that call, which is how `cel.bind(...)` and `xs.map(x, ...)`
	// round trip already. Off for an imported [PureHelper], whose expansion is
	// written as the `cel.bind` it is.
	keepCalls bool
}

// helperCallMatches returns every global call in root to a helper in helpers.
// A member call is never one: a helper is declared as a global function, so
// `x.name()` is not a use of it.
func helperCallMatches(root commonast.NavigableExpr, helpers map[string]checkedPureHelper) []commonast.NavigableExpr {
	return commonast.MatchDescendants(root, func(expr commonast.NavigableExpr) bool {
		if expr.Kind() != commonast.CallKind || expr.AsCall().IsMemberFunction() {
			return false
		}
		_, ok := helpers[expr.AsCall().FunctionName()]
		return ok
	})
}

func (o *pureHelperOptimizer) Optimize(ctx *cel.OptimizerContext, tree *commonast.AST) *commonast.AST {
	root := commonast.NavigateAST(tree)
	for _, match := range helperCallMatches(root, o.helpers) {
		o.calls++
		if o.calls > maxPureHelperCalls {
			ctx.ReportErrorAtID(match.ID(), "expression calls imported pure helpers more than %d times", maxPureHelperCalls)
			return tree
		}
		call := match.AsCall()
		helper := o.helpers[call.FunctionName()]
		params := helper.definition.params
		if len(call.Args()) != len(params) {
			// The checker normally reports this first. Keep the optimizer total for
			// a hand-built checked AST that omitted overload metadata.
			ctx.ReportErrorAtID(match.ID(), "pure helper %s expects %d arguments, got %d", call.FunctionName(), len(params), len(call.Args()))
			return tree
		}
		replacement := ctx.CopyASTAndMetadata(helper.body.NativeRep())
		names := make([]string, len(params))
		for i, parameter := range params {
			names[i] = parameter.name
		}
		if argumentsMentionEarlierParameters(call.Args(), names) {
			// Binding the arguments straight to the parameter names would evaluate a
			// later argument inside the binding of an earlier parameter, so an
			// argument that happens to be spelled like one (a loop variable named
			// `denominator`, say) would read that parameter instead of the caller's
			// own. Bind each argument to a name an author has no reason to write first, and
			// the parameters to those.
			aliases := freshAliases(call, o.calls, names)
			refs := make([]commonast.Expr, len(params))
			for i := range params {
				refs[i] = ctx.NewIdent(aliases[i])
			}
			replacement = bindInOrder(ctx, -1, names, refs, replacement, !o.keepCalls)
			replacement = bindInOrder(ctx, match.ID(), aliases, call.Args(), replacement, !o.keepCalls)
		} else {
			replacement = bindInOrder(ctx, match.ID(), names, call.Args(), replacement, !o.keepCalls)
		}
		if o.keepCalls {
			ctx.SetMacroCall(match.ID(), ctx.NewCall(call.FunctionName(), call.Args()...))
		}
		ctx.UpdateExpr(match, replacement)
	}
	return tree
}

// freshAliases names one binding per parameter of call's callee, each spelled
// like no identifier in the call's arguments and like no parameter.
//
// A name an author is unlikely to write is not a name an author cannot write: a
// caller whose own parameters are `numerator` and `__sub_1_numerator` can spell
// the alias the expander would have chosen, and the alias would then shadow it
// while a later argument is evaluated. So the candidate is checked against every
// identifier the arguments mention, a comprehension's variables included, and is
// extended until it is absent from them.
func freshAliases(call commonast.CallExpr, n int, params []string) []string {
	taken := make(map[string]bool, len(params))
	for _, name := range params {
		taken[name] = true
	}
	for _, arg := range call.Args() {
		for _, e := range commonast.MatchDescendants(commonast.NavigateExpr(nil, arg), func(e commonast.NavigableExpr) bool {
			return e.Kind() == commonast.IdentKind || e.Kind() == commonast.ComprehensionKind
		}) {
			if e.Kind() == commonast.IdentKind {
				taken[e.AsIdent()] = true

				continue
			}
			taken[e.AsComprehension().IterVar()] = true
			taken[e.AsComprehension().IterVar2()] = true
			taken[e.AsComprehension().AccuVar()] = true
		}
	}

	prefix := strings.NewReplacer(".", "_").Replace(call.FunctionName())
	aliases := make([]string, len(params))
	for i, name := range params {
		alias := fmt.Sprintf("__%s_%d_%s", prefix, n, name)
		for taken[alias] {
			alias += "_"
		}
		taken[alias] = true
		aliases[i] = alias
	}

	return aliases
}

// bindInOrder wraps body in one cel.bind per name, outermost first, so inits[0] is
// evaluated with nothing bound and inits[i] with names[:i] bound. The outermost
// bind takes the id outerID, which is the id of the call it replaces; a negative
// outerID gives it a fresh one, for a chain that is itself wrapped by another.
// record says whether each bind is registered as a macro call, which is what
// makes it write back as `cel.bind(...)`.
func bindInOrder(ctx *cel.OptimizerContext, outerID int64, names []string, inits []commonast.Expr, body commonast.Expr, record bool) commonast.Expr {
	for i := len(names) - 1; i >= 0; i-- {
		bindID := outerID
		if i != 0 || outerID < 0 {
			bindID = ctx.NewIdent("unused").ID()
		}
		var macro commonast.Expr
		body, macro = ctx.NewBindMacro(bindID, names[i], inits[i], body)
		if record {
			ctx.SetMacroCall(bindID, macro)
		}
	}
	return body
}

// argumentsMentionEarlierParameters reports whether any argument after the first
// mentions an identifier spelled like a parameter bound before it. Conservative by
// design: an identifier a comprehension inside the argument binds itself is
// reported too, and the cost of that is one extra bind, not a wrong answer.
func argumentsMentionEarlierParameters(args []commonast.Expr, names []string) bool {
	for i := 1; i < len(args); i++ {
		mentioned := commonast.MatchDescendants(commonast.NavigateExpr(nil, args[i]), func(e commonast.NavigableExpr) bool {
			switch e.Kind() {
			case commonast.IdentKind:
				return slices.Contains(names[:i], e.AsIdent())
			case commonast.ComprehensionKind:
				comp := e.AsComprehension()
				return slices.Contains(names[:i], comp.IterVar()) || slices.Contains(names[:i], comp.IterVar2())
			}
			return false
		})
		if len(mentioned) > 0 {
			return true
		}
	}
	return false
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
