package flowstatev1

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
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
// does not exist yet.
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
