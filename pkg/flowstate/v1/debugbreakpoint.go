package flowstatev1

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// The breakpoint rules both drivers share: how a condition is compiled and how
// a hit condition counts. One spelling, so a local and a durable session cannot
// disagree about when a breakpoint fires.

// CompileDebugCondition compiles a breakpoint's CEL condition against profile,
// returning it in the shape a step's `if:` travels in, so that evaluating it is
// [EvalConditionInScope] — the engine's own function. A condition that does not
// parse, does not type-check, or is not a boolean is refused here rather than
// at its first arrival.
//
// It is compiled against an environment declaring the names *the expression
// itself mentions*, because a breakpoint is usually set before the run reaches
// the step it names, and the binding its condition reads — a loop's `as:` —
// does not exist yet. Whether those names can be bound where the breakpoint
// fires is [CheckDebugConditionScope]'s question, asked of the program.
func CompileDebugCondition(expression, profile string) (*Value, error) {
	if strings.TrimSpace(expression) == "" {
		return nil, errors.New("a condition needs an expression")
	}

	env, err := DefaultEvaluator().ProfileEnv(profile)
	if err != nil {
		return nil, err
	}
	ast, issues := env.Parse(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("parse condition: %w", issues.Err())
	}

	// Parsing is syntax only, so `1 + true` and `missing_function(n)` both
	// parse — and an accepted condition that cannot be compiled fails at every
	// arrival, which with the stop-on-error rule above means stopping at every
	// iteration. That is the exact behaviour a condition is typed to escape,
	// reached by a typo the prompt reported as accepted (Codex, #1116).
	//
	// Checked against an environment declaring the names *the expression
	// itself mentions*, which is `flowfile`'s spelling for this same problem
	// (`celcheck.go:177`, `envDeclaring(referencedNames(...))`) and the only
	// one that works here. A breakpoint is usually set before the run reaches
	// the step it names, so the binding a condition reads — a loop's `as:` —
	// does not exist in the scope this is typed in. Declaring what is in scope
	// now would reject `n == 7` typed at the first step, which is a false
	// diagnostic about a condition that will be perfectly valid when it fires.
	checked, err := debugCheckedInScope(env, ast)
	if err != nil {
		return nil, err
	}

	// And it has to be a boolean, refused here rather than at the first
	// arrival — the same shape `compileMustIn` uses for the other place this
	// repository compiles an author's boolean rule (`constraints.go:238-245`).
	if checked.OutputType() != cel.BoolType && checked.OutputType() != cel.DynType {
		return nil, fmt.Errorf("a condition must be a boolean, and this one is %s", checked.OutputType())
	}

	parsed, err := cel.AstToParsedExpr(ast)
	if err != nil {
		return nil, fmt.Errorf("parse condition: %w", err)
	}

	return &Value{Kind: &Value_Expr{Expr: parsed}}, nil
}

// debugCheckedInScope type-checks an expression against an environment extended
// with every identifier it references, declared dynamically.
//
// Dynamic because nothing here knows the type: a step output's shape is the
// task's, and a loop binding's is the collection's. What the check is for is
// the errors that do not depend on those — an unknown function, an operator
// applied to types that can never combine.
func debugCheckedInScope(env *cel.Env, ast *cel.Ast) (*cel.Ast, error) {
	parsed, err := cel.AstToParsedExpr(ast)
	if err != nil {
		return nil, fmt.Errorf("parse condition: %w", err)
	}

	names := map[string]struct{}{}
	collectDebugIdentifiers(parsed.GetExpr(), names)

	declarations := make([]cel.EnvOption, 0, len(names))
	for name := range names {
		declarations = append(declarations, cel.Variable(name, cel.DynType))
	}

	declaring, err := env.Extend(declarations...)
	if err != nil {
		// Extending failed, which is this build's problem rather than the
		// author's, so the condition is accepted unchecked rather than
		// refused: leaving the failure to evaluation is where it was before
		// this check existed, and blaming an author for it is worse.
		return ast, nil
	}

	checked, issues := declaring.Check(ast)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("condition: %w", issues.Err())
	}

	return checked, nil
}

// collectDebugIdentifiers gathers every bare name an expression reads, for the
// environment the type check declares.
//
// Only the *root* of a selection: `steps.build.ok` reads the identifier
// `steps`, and declaring `steps` dynamically is what makes the whole chain
// legal without claiming to know its shape. Macro bindings are included on
// purpose here — declaring one is harmless, and not declaring it would make
// `items.exists(i, i > 2)` fail a check over a name CEL itself provides.
//
// This is deliberately not the same question as [conditionNames]: declaring a
// name costs nothing, while *requiring* one to be bound at a step decides
// whether the run stops there.
func collectDebugIdentifiers(e *expr.Expr, into map[string]struct{}) {
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		into[kind.IdentExpr.GetName()] = struct{}{}

	case *expr.Expr_SelectExpr:
		collectDebugIdentifiers(kind.SelectExpr.GetOperand(), into)

	case *expr.Expr_CallExpr:
		collectDebugIdentifiers(kind.CallExpr.GetTarget(), into)
		for _, arg := range kind.CallExpr.GetArgs() {
			collectDebugIdentifiers(arg, into)
		}

	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			collectDebugIdentifiers(element, into)
		}

	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			collectDebugIdentifiers(entry.GetMapKey(), into)
			collectDebugIdentifiers(entry.GetValue(), into)
		}

	case *expr.Expr_ComprehensionExpr:
		comprehension := kind.ComprehensionExpr
		into[comprehension.GetIterVar()] = struct{}{}
		into[comprehension.GetIterVar2()] = struct{}{}
		into[comprehension.GetAccuVar()] = struct{}{}
		collectDebugIdentifiers(comprehension.GetIterRange(), into)
		collectDebugIdentifiers(comprehension.GetAccuInit(), into)
		collectDebugIdentifiers(comprehension.GetLoopCondition(), into)
		collectDebugIdentifiers(comprehension.GetLoopStep(), into)
		collectDebugIdentifiers(comprehension.GetResult(), into)
	}
}

// debugConditionRoots are the rooted namespaces a condition may read at any
// site, because the activation answers each whole wherever a step's `if:` is
// evaluated ([StepsOutputActivation.ResolveName]).
var debugConditionRoots = []string{StepsRoot, VarsRoot, InputsRoot, RunRoot, TriggerRoot}

// maxDebugConditionNamesListed bounds how many bare names a refusal lists.
const maxDebugConditionNamesListed = 8

// CheckDebugConditionScope refuses a compiled condition that reads a bare name
// none of the sites in at can bind, answering in the words `flow validate`
// uses for the same mistake in a step's `if:`.
//
// [CompileDebugCondition] cannot ask this: it declares whatever the expression
// mentions, because a condition is usually set before the run reaches the
// binding it reads. The sites answer it instead: a condition is evaluated
// where the step's `if:` is, so what it can read there is the five roots and
// the site's [DebugStaticSite.Locals]. A name bound at any site in at is
// admitted, since the breakpoint may fire at any of them.
//
// at empty means where the breakpoint fires is not known — the enumeration
// was cut short at [MaxDebugStaticSites] — and the condition is admitted as
// before this check existed; the caller says so. program is every site, for
// naming a step written bare and a binding that exists elsewhere.
func CheckDebugConditionScope(condition *Value, profile string, at, program []DebugStaticSite) error {
	parsed := condition.GetExpr()
	if parsed == nil || len(at) == 0 {
		return nil
	}
	env, err := DefaultEvaluator().ProfileEnv(profile)
	if err != nil {
		// This build's problem, not the author's: admitted, as
		// [debugCheckedInScope] admits a condition it cannot check.
		return nil
	}

	bindable := map[string]bool{}
	for _, root := range debugConditionRoots {
		bindable[root] = true
	}
	var locals []string
	for scope := range debugScopesOf(at) {
		for _, name := range scope.names {
			if !bindable[name] {
				bindable[name] = true
				locals = append(locals, name)
			}
		}
	}
	slices.Sort(locals)

	// A name the program binds anywhere is a scope read wherever it is
	// written, even one spelled like a type: the activation answers a bound
	// name before the type provider does, so `string` in a loop that binds
	// `string` is the binding, and outside that loop it is a read of nothing
	// rather than the type (Codex, #2202).
	boundInProgram := map[string]bool{}
	for scope := range debugScopesOf(program) {
		for _, name := range scope.names {
			boundInProgram[name] = true
		}
	}

	free := map[string]struct{}{}
	walk := &debugRootWalk{
		resolves: func(name string) bool {
			if boundInProgram[debugRootOf(name)] {
				return false
			}
			_, found := env.CELTypeProvider().FindIdent(name)

			return found
		},
		bound: map[string]int{},
		free:  free,
	}
	walk.walk(parsed.GetExpr())
	for _, name := range slices.Sorted(maps.Keys(free)) {
		if !bindable[name] {
			return debugUnboundName(name, locals, program, boundInProgram[name])
		}
	}

	return nil
}

// debugUnboundName says why name is not bound where a breakpoint fires, and
// what is. elsewhere reports that the program binds it at some other site.
func debugUnboundName(name string, locals []string, program []DebugStaticSite, elsewhere bool) error {
	if name == NowIdentifier {
		return errors.New("`now` is bound only inside a wait's own expressions, and a condition is " +
			"evaluated where the step's `if:` is, before the step is entered")
	}
	for _, site := range program {
		if path := site.Site.GetPath(); len(path) > 0 && path[len(path)-1] == name {
			return fmt.Errorf("`%s` is a step, and a step's outputs are read as `%s.%s.<output>`", name, StepsRoot, name)
		}
	}

	message := fmt.Sprintf("`%s` is not bound where this breakpoint fires", name)
	if elsewhere {
		message = fmt.Sprintf("`%s` is bound only inside the loops and steps that declare it, "+
			"and this breakpoint fires outside them", name)
	}
	message += "; a condition reads what the step's `if:` reads: `" + strings.Join(debugConditionRoots, "`, `") + "`"
	if len(locals) > 0 {
		listed := locals[:min(len(locals), maxDebugConditionNamesListed)]
		message += ", and here `" + strings.Join(listed, "`, `") + "`"
		if more := len(locals) - len(listed); more > 0 {
			message += fmt.Sprintf(" and %d more", more)
		}
	}
	// A name bound elsewhere is spelled right, so a near name would only
	// mislead.
	if suggestion, ok := nearest.Name(name, slices.Concat(debugConditionRoots, locals)); ok && !elsewhere {
		message += fmt.Sprintf("; did you mean `%s`?", suggestion)
	}

	return errors.New(message)
}

// debugRootWalk gathers the bare names an expression reads from its scope:
// every identifier not bound by one of its own comprehensions and not a name
// the environment itself resolves: a type or enum value, which the parser
// presents as an identifier, bare or qualified (`int` in `type(n) == int`,
// `google.protobuf.Timestamp`).
//
// bound counts the comprehension bindings in force, so a nested macro that
// rebinds a name unbinds only its own.
type debugRootWalk struct {
	resolves func(name string) bool
	bound    map[string]int
	free     map[string]struct{}
}

func (w *debugRootWalk) walk(e *expr.Expr) {
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		if name := kind.IdentExpr.GetName(); w.bound[name] == 0 && !w.resolves(name) {
			w.free[name] = struct{}{}
		}

	case *expr.Expr_SelectExpr:
		if qualified, ok := debugQualifiedName(e); ok && w.bound[debugRootOf(qualified)] == 0 && w.resolves(qualified) {
			return
		}
		w.walk(kind.SelectExpr.GetOperand())

	case *expr.Expr_CallExpr:
		// A namespaced function the profile declares arrives with no
		// target: the parser, which knows the profile's functions, resolves
		// `math.ceil(x)` to one call named `math.ceil`. A target left here
		// is a receiver the expression reads.
		call := kind.CallExpr
		w.walk(call.GetTarget())
		for _, arg := range call.GetArgs() {
			w.walk(arg)
		}

	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			w.walk(element)
		}

	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			w.walk(entry.GetMapKey())
			w.walk(entry.GetValue())
		}

	case *expr.Expr_ComprehensionExpr:
		// The range and the accumulator's start are read outside the
		// comprehension; the rest inside it, with its variables bound.
		comprehension := kind.ComprehensionExpr
		w.walk(comprehension.GetIterRange())
		w.walk(comprehension.GetAccuInit())

		names := []string{comprehension.GetIterVar(), comprehension.GetIterVar2(), comprehension.GetAccuVar()}
		for _, name := range names {
			w.bound[name]++
		}
		w.walk(comprehension.GetLoopCondition())
		w.walk(comprehension.GetLoopStep())
		w.walk(comprehension.GetResult())
		for _, name := range names {
			w.bound[name]--
		}
	}
}

// debugRootOf is the first segment of a dotted name.
func debugRootOf(qualified string) string {
	root, _, _ := strings.Cut(qualified, ".")

	return root
}

// debugQualifiedName renders a select chain rooted at an identifier as the
// dotted name it spells, the shape a qualified type name takes.
func debugQualifiedName(e *expr.Expr) (string, bool) {
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		return kind.IdentExpr.GetName(), true
	case *expr.Expr_SelectExpr:
		if kind.SelectExpr.GetTestOnly() {
			return "", false
		}
		operand, ok := debugQualifiedName(kind.SelectExpr.GetOperand())

		return operand + "." + kind.SelectExpr.GetField(), ok
	}

	return "", false
}

// DebugHitCondition filters a breakpoint's arrivals by their count.
type DebugHitCondition struct {
	op string
	n  uint64
}

// ParseDebugHitCondition reads `N` (from the Nth arrival on), `>= N`, `== N`,
// `> N`, `< N`, `<= N`, or `% N` (every Nth). Empty admits every arrival.
func ParseDebugHitCondition(text string) (DebugHitCondition, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return DebugHitCondition{}, nil
	}

	op := ">="
	for _, candidate := range []string{">=", "<=", "==", ">", "<", "%"} {
		if rest, ok := strings.CutPrefix(text, candidate); ok {
			op, text = candidate, strings.TrimSpace(rest)

			break
		}
	}

	n, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return DebugHitCondition{}, fmt.Errorf("%q is not a count; write N, >= N, == N, > N, < N, <= N, or %% N", text)
	}
	if op == "%" && n == 0 {
		return DebugHitCondition{}, errors.New("% 0 divides by zero")
	}

	return DebugHitCondition{op: op, n: n}, nil
}

// Admits reports whether the arrival counted hits passes the condition.
func (h DebugHitCondition) Admits(hits uint64) bool {
	switch h.op {
	case "":
		return true
	case ">=":
		return hits >= h.n
	case "<=":
		return hits <= h.n
	case "==":
		return hits == h.n
	case ">":
		return hits > h.n
	case "<":
		return hits < h.n
	default:
		return hits%h.n == 0
	}
}
