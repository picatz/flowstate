package flowstatev1

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"unicode/utf8"

	"github.com/google/cel-go/cel"
	"google.golang.org/protobuf/proto"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// What protovalidate calls a standard-rule vocabulary — pattern, min_len, min,
// min_items, unique, and the rest — landing on [InputDeclaration] and
// [OutputDeclaration] the way the same rules land on a task's own inputs
// through buf.validate: declarative keys for the common case, and `must:`
// behind them as the escape hatch for what the keys cannot say.
//
// # Where this differs from buf.validate, on purpose
//
// A `must:` expression can arrive inside a submitted specification — the RPC
// accepts a hand-built [Workflow], not only one [flowfile.Parse] compiled —
// so it is untrusted input the same way any other stored CEL expression here
// is, and every evaluation below goes through [Evaluator.Eval], which is what
// applies the cost bound and the cancellation contract. protovalidate assumes
// a trusted schema and evaluates `now` and other nondeterministic builtins
// without complaint; this schema refuses them in `must:` outright, because a
// constraint has to answer the same way on every replay and at every one of
// the surfaces it is checked from (author time, submit time, a call
// boundary), and an expression reading the clock cannot promise that.
//
// # One check, several callers
//
// [CheckInputConstraintShape] validates a declaration against itself — do the
// keys it uses apply to its own type, does `must:` compile and type-check —
// and is what "rules compile when configuration loads, not when a request
// arrives" means here: [BindRunInputs] runs it before it does anything else,
// and `flow validate` runs it early enough to report it as a diagnostic with
// a position rather than a run's own refusal.
//
// [CheckInputConstraints] checks one literal value against an already-shaped
// declaration, and is called wherever [CheckInputValue] is: at submit through
// [BindRunInputs], against a literal default or example at author time, and
// against a `with:` argument at a call boundary.

// constraintCELType returns the CEL type a declared input's own type checks
// `this` against inside a `must:` expression.
func constraintCELType(t InputDeclaration_Type) *cel.Type {
	switch t {
	case InputDeclaration_TYPE_STRING:
		return cel.StringType
	case InputDeclaration_TYPE_INT:
		return cel.IntType
	case InputDeclaration_TYPE_FLOAT:
		return cel.DoubleType
	case InputDeclaration_TYPE_BOOL:
		return cel.BoolType
	case InputDeclaration_TYPE_LIST:
		return cel.ListType(cel.DynType)
	case InputDeclaration_TYPE_STRUCT:
		return cel.MapType(cel.StringType, cel.DynType)
	default:
		return cel.DynType
	}
}

// mustEnvs caches the CEL environment built for each declared type's `must:`
// expressions, keyed by [InputDeclaration_Type]. Building an environment
// parses and type-checks every declaration in it, per [celenv.go]'s own
// reasoning for caching — small here because there are only the six declared
// types plus the output case, but the same reason applies.
var mustEnvs sync.Map // map[InputDeclaration_Type]*mustEnvResult

// outputMustEnv is the one environment every OutputDeclaration.must compiles
// against: `this` typed dyn, because an output carries no declared type the
// way an input does — see OutputDeclaration's schema doc.
var outputMustEnv = sync.OnceValues(func() (*cel.Env, error) {
	return cel.NewEnv(cel.Variable("this", cel.DynType))
})

type mustEnvResult struct {
	env *cel.Env
	err error
}

// mustEnvFor returns the cached environment for t's `must:` expressions,
// building it on first use.
func mustEnvFor(t InputDeclaration_Type) (*cel.Env, error) {
	if cached, ok := mustEnvs.Load(t); ok {
		res := cached.(*mustEnvResult)
		return res.env, res.err
	}

	env, err := cel.NewEnv(cel.Variable("this", constraintCELType(t)))
	res := &mustEnvResult{env: env, err: err}
	actual, _ := mustEnvs.LoadOrStore(t, res)
	stored := actual.(*mustEnvResult)
	return stored.env, stored.err
}

// CompileMustExpression parses and type-checks a `must:` expression against
// the CEL type a value of declared type t has, and refuses one that
// references anything nondeterministic.
//
// Exported so `flow validate` and the language server can compile a `must:`
// the moment it is written, before any value exists to check it against — the
// fail-closed rule applied to the language itself: a bad constraint is a
// defect in the specification, caught when it loads rather than when a run
// happens to exercise it.
func CompileMustExpression(mustExpr string, t InputDeclaration_Type) (*cel.Ast, error) {
	env, err := mustEnvFor(t)
	if err != nil {
		return nil, fmt.Errorf("build constraint environment: %w", err)
	}

	return compileMustIn(env, mustExpr)
}

// CompileOutputMustExpression is [CompileMustExpression] for an output's
// `must:`, whose `this` is dyn because [OutputDeclaration] carries no
// declared type.
func CompileOutputMustExpression(mustExpr string) (*cel.Ast, error) {
	env, err := outputMustEnv()
	if err != nil {
		return nil, fmt.Errorf("build constraint environment: %w", err)
	}

	return compileMustIn(env, mustExpr)
}

func compileMustIn(env *cel.Env, mustExpr string) (*cel.Ast, error) {
	parsed, iss := env.Parse(mustExpr)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("must: %w", iss.Err())
	}

	parsedExpr, err := cel.AstToParsedExpr(parsed)
	if err != nil {
		return nil, fmt.Errorf("must: %w", err)
	}
	if err := refuseNondeterministicMust(parsedExpr); err != nil {
		return nil, err
	}

	checked, iss := env.Check(parsed)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("must: %w", iss.Err())
	}
	if checked.OutputType() != cel.BoolType {
		return nil, fmt.Errorf(
			"must: %q evaluates to %s rather than a bool; a constraint is a predicate over `this` "+
				"that is either satisfied or not", mustExpr, checked.OutputType())
	}

	return checked, nil
}

// refuseNondeterministicMust reports the first name a `must:` expression
// reads other than `this`, the one name its environment declares.
//
// `env.Check` alone already refuses these — nothing else is declared, so a
// reference to anything else is an undeclared-reference error — but that
// message does not say *why* a constraint is different from every other
// expression in the language, which is worth saying explicitly for the one
// name an author is likeliest to reach for out of habit: `now` is bound
// everywhere a `wait_until:` is, and a `must:` is not a `wait_until:`.
func refuseNondeterministicMust(parsed *expr.ParsedExpr) error {
	free := map[string]struct{}{}
	collectFreeIdentifiers(parsed.GetExpr(), map[string]struct{}{}, free)

	names := make([]string, 0, len(free))
	for name := range free {
		if name == "this" {
			continue
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	name := names[0]

	if name == NowIdentifier {
		return fmt.Errorf(
			"must: may not reference `now`: a constraint is checked at author time, at submit, and at " +
				"every call boundary a value crosses, and has to answer the same way every time, but `now` " +
				"is the moment a `wait_until:` is evaluated and reads differently on each of those — write " +
				"the deadline as an ordinary input instead")
	}

	return fmt.Errorf(
		"must: references unknown name %q; a constraint sees only `this`, the value being checked, and "+
			"the language's own operators and functions", name)
}

// collectFreeIdentifiers walks e collecting every identifier not bound by an
// enclosing comprehension, the same walk flowfile's own reference checker
// does for a workflow's expressions — reused here at the scale a `must:`
// needs, since a constraint has exactly one root name rather than three.
func collectFreeIdentifiers(e *expr.Expr, bound map[string]struct{}, free map[string]struct{}) {
	if e == nil {
		return
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		if _, isBound := bound[kind.IdentExpr.GetName()]; !isBound {
			free[kind.IdentExpr.GetName()] = struct{}{}
		}
	case *expr.Expr_SelectExpr:
		collectFreeIdentifiers(kind.SelectExpr.GetOperand(), bound, free)
	case *expr.Expr_CallExpr:
		collectFreeIdentifiers(kind.CallExpr.GetTarget(), bound, free)
		for _, arg := range kind.CallExpr.GetArgs() {
			collectFreeIdentifiers(arg, bound, free)
		}
	case *expr.Expr_ListExpr:
		for _, el := range kind.ListExpr.GetElements() {
			collectFreeIdentifiers(el, bound, free)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			collectFreeIdentifiers(entry.GetMapKey(), bound, free)
			collectFreeIdentifiers(entry.GetValue(), bound, free)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		collectFreeIdentifiers(c.GetIterRange(), bound, free)
		collectFreeIdentifiers(c.GetAccuInit(), bound, free)

		inner := make(map[string]struct{}, len(bound)+3)
		for name := range bound {
			inner[name] = struct{}{}
		}
		for _, name := range []string{c.GetIterVar(), c.GetIterVar2(), c.GetAccuVar()} {
			if name != "" {
				inner[name] = struct{}{}
			}
		}
		collectFreeIdentifiers(c.GetLoopCondition(), inner, free)
		collectFreeIdentifiers(c.GetLoopStep(), inner, free)
		collectFreeIdentifiers(c.GetResult(), inner, free)
	}
}

// CheckInputConstraintShape reports what is wrong with a declaration's
// constraints as written, independent of any value: a key that does not apply
// to the declared type, an unusable pattern, a min above its max, or a
// `must:` that will not compile.
//
// This is the "rules compile when configuration loads" half of the fail-closed
// rule. [BindRunInputs] runs it before checking any submitted value, so a
// broken declaration is refused at submit even for a specification that never
// passed through `flow validate` — and `flow validate` runs the identical
// check early enough to report it as a diagnostic with a position.
func CheckInputConstraintShape(decl *InputDeclaration) error {
	name := decl.GetName()
	t := decl.GetType()

	if decl.Pattern != nil || decl.MinLen != nil || decl.MaxLen != nil {
		if t != InputDeclaration_TYPE_STRING {
			return fmt.Errorf(
				"input %q declares a string constraint (pattern, min_len or max_len) but is declared %s; "+
					"those apply only to a string input", name, DeclaredTypeName(t))
		}
	}
	if decl.Pattern != nil {
		if _, err := regexp.Compile(decl.GetPattern()); err != nil {
			return fmt.Errorf("input %q pattern %q is not a valid regular expression: %w", name, decl.GetPattern(), err)
		}
	}
	if decl.MinLen != nil && decl.MaxLen != nil && decl.GetMinLen() > decl.GetMaxLen() {
		return fmt.Errorf("input %q min_len (%d) is greater than max_len (%d), so no string can satisfy both",
			name, decl.GetMinLen(), decl.GetMaxLen())
	}

	if decl.Min != nil || decl.Max != nil {
		if t != InputDeclaration_TYPE_INT && t != InputDeclaration_TYPE_FLOAT {
			return fmt.Errorf(
				"input %q declares min or max but is declared %s; those apply only to an int or float input",
				name, DeclaredTypeName(t))
		}
	}
	if decl.Min != nil && decl.Max != nil && decl.GetMin() > decl.GetMax() {
		return fmt.Errorf("input %q min (%v) is greater than max (%v), so no value can satisfy both",
			name, decl.GetMin(), decl.GetMax())
	}

	if decl.MinItems != nil || decl.MaxItems != nil || decl.GetUnique() {
		if t != InputDeclaration_TYPE_LIST {
			return fmt.Errorf(
				"input %q declares min_items, max_items or unique but is declared %s; those apply only "+
					"to a list input", name, DeclaredTypeName(t))
		}
	}
	if decl.MinItems != nil && decl.MaxItems != nil && decl.GetMinItems() > decl.GetMaxItems() {
		return fmt.Errorf("input %q min_items (%d) is greater than max_items (%d), so no list can satisfy both",
			name, decl.GetMinItems(), decl.GetMaxItems())
	}

	if decl.Must != nil {
		if _, err := CompileMustExpression(decl.GetMust(), t); err != nil {
			return fmt.Errorf("input %q %w", name, err)
		}
	}

	return nil
}

// CheckOutputConstraintShape is [CheckInputConstraintShape] for an output's
// `must:`, the only constraint an output declares.
func CheckOutputConstraintShape(decl *OutputDeclaration) error {
	if decl.Must == nil {
		return nil
	}
	if _, err := CompileOutputMustExpression(decl.GetMust()); err != nil {
		return fmt.Errorf("output %q %w", decl.GetName(), err)
	}
	return nil
}

// CheckInputConstraints applies a declaration's standard-rule constraints and
// its `must:` escape hatch to a literal value already checked against the
// declared type by [CheckInputValue] — called immediately after it, from
// every one of the places that function is: [BindRunInputs] at submit, `flow
// validate` against a literal default or example, and a call boundary's
// `with:` argument.
//
// Nil for a value with no literal — an expression, refused earlier by
// [CheckInputValue] itself — so this never runs against something it cannot
// evaluate a rule over.
func CheckInputConstraints(name string, decl *InputDeclaration, value *Value) error {
	lit := value.GetLiteral()
	if lit == nil {
		return nil
	}

	if err := checkStringConstraints(name, decl, lit); err != nil {
		return err
	}
	if err := checkNumericConstraints(name, decl, lit); err != nil {
		return err
	}
	if err := checkListConstraints(name, decl, lit); err != nil {
		return err
	}

	if decl.Must == nil {
		return nil
	}

	// Run unconditionally rather than gated on decl.GetType() == TYPE_LIST: a
	// struct-typed input's `must:` reaches lists nested inside it just as
	// cheaply as a list-typed input's does, and gating on the declared type is
	// exactly what let a struct's nested list through uncounted.
	if err := checkConstraintValueBound("input", name, lit); err != nil {
		return err
	}

	ast, err := CompileMustExpression(decl.GetMust(), decl.GetType())
	if err != nil {
		// CheckInputConstraintShape already refuses a declaration whose must:
		// does not compile, so a caller reaching this without having run it —
		// a hand-built specification calling this function directly — gets
		// the identical refusal rather than a panic on a nil ast.
		return fmt.Errorf("input %q %w", name, err)
	}

	satisfied, err := evalMust(context.Background(), decl.GetType(), ast, lit)
	if err != nil {
		return fmt.Errorf("input %q: evaluating `must: %s`: %w", name, decl.GetMust(), err)
	}
	if !satisfied {
		got, _ := literalToNative(lit)
		return fmt.Errorf("input %q must satisfy `%s`; got %v", name, decl.GetMust(), got)
	}

	return nil
}

// CheckOutputConstraint is [CheckInputConstraints] for an output: it applies
// only `must:`, checked once the output's own expression has produced value,
// so a workflow cannot report an answer that violates its own declaration.
func CheckOutputConstraint(decl *OutputDeclaration, value *Value) error {
	if decl.Must == nil {
		return nil
	}

	lit := value.GetLiteral()
	if lit == nil {
		// An output that did not resolve to a literal — the engine failed to
		// evaluate it, or produced something this constraint layer has no
		// value to check — is a different failure, reported by the caller
		// that computed it.
		return nil
	}

	if err := checkConstraintValueBound("output", decl.GetName(), lit); err != nil {
		return err
	}

	ast, err := CompileOutputMustExpression(decl.GetMust())
	if err != nil {
		return fmt.Errorf("output %q %w", decl.GetName(), err)
	}

	env, err := outputMustEnv()
	if err != nil {
		return fmt.Errorf("output %q: %w", decl.GetName(), err)
	}
	thisVal, err := cel.ValueToRefValue(TypeAdapter, lit)
	if err != nil {
		return fmt.Errorf("output %q: %w", decl.GetName(), err)
	}

	out, err := DefaultEvaluator().Eval(context.Background(), env, ast, map[string]any{"this": thisVal})
	if err != nil {
		return fmt.Errorf("output %q: evaluating `must: %s`: %w", decl.GetName(), decl.GetMust(), err)
	}
	satisfied, ok := out.Value().(bool)
	if !ok || !satisfied {
		got, _ := literalToNative(lit)
		return fmt.Errorf("output %q must satisfy `%s`; got %v", decl.GetName(), decl.GetMust(), got)
	}

	return nil
}

// evalMust evaluates a compiled must: ast against one value, through
// [Evaluator.Eval] so the cost bound and cancellation this file's own doc
// comment promises actually apply.
func evalMust(ctx context.Context, t InputDeclaration_Type, ast *cel.Ast, lit *expr.Value) (bool, error) {
	env, err := mustEnvFor(t)
	if err != nil {
		return false, err
	}

	thisVal, err := cel.ValueToRefValue(TypeAdapter, lit)
	if err != nil {
		return false, err
	}

	out, err := DefaultEvaluator().Eval(ctx, env, ast, map[string]any{"this": thisVal})
	if err != nil {
		return false, err
	}

	b, ok := out.Value().(bool)
	return ok && b, nil
}

// checkStringConstraints applies pattern, min_len and max_len to a string
// literal.
//
// Silently returns for a value [inputTypeOf] does not read as a string —
// [CheckInputValue] already refused the mismatch, and this is not the place
// to report it a second time, per this repository's rule about one mistake
// getting one diagnostic.
func checkStringConstraints(name string, decl *InputDeclaration, lit *expr.Value) error {
	if decl.Pattern == nil && decl.MinLen == nil && decl.MaxLen == nil {
		return nil
	}
	s, ok := lit.GetKind().(*expr.Value_StringValue)
	if !ok {
		return nil
	}
	value := s.StringValue

	if decl.Pattern != nil {
		re, err := regexp.Compile(decl.GetPattern())
		if err != nil {
			return fmt.Errorf("input %q pattern %q is not a valid regular expression: %w", name, decl.GetPattern(), err)
		}
		if !re.MatchString(value) {
			return fmt.Errorf("input %q must match pattern %q; got %q", name, decl.GetPattern(), value)
		}
	}

	length := uint64(utf8.RuneCountInString(value))
	if decl.MinLen != nil && length < decl.GetMinLen() {
		return fmt.Errorf("input %q must be at least %d character(s) long; got %d", name, decl.GetMinLen(), length)
	}
	if decl.MaxLen != nil && length > decl.GetMaxLen() {
		return fmt.Errorf("input %q must be at most %d character(s) long; got %d", name, decl.GetMaxLen(), length)
	}

	return nil
}

// checkNumericConstraints applies min and max to an int or float literal.
func checkNumericConstraints(name string, decl *InputDeclaration, lit *expr.Value) error {
	if decl.Min == nil && decl.Max == nil {
		return nil
	}
	n, ok := numericLiteralValue(lit)
	if !ok {
		return nil
	}

	if decl.Min != nil && n < decl.GetMin() {
		return fmt.Errorf("input %q must be >= %v; got %v", name, decl.GetMin(), n)
	}
	if decl.Max != nil && n > decl.GetMax() {
		return fmt.Errorf("input %q must be <= %v; got %v", name, decl.GetMax(), n)
	}

	return nil
}

// numericLiteralValue reads a literal's numeric value regardless of which of
// the three numeric kinds a parsed value carries it as — the same latitude
// [inputTypeOf] gives an int declaration, and for the same reason: whether a
// scalar arrived signed is an artifact of how its digits were written.
func numericLiteralValue(lit *expr.Value) (float64, bool) {
	switch kind := lit.GetKind().(type) {
	case *expr.Value_Int64Value:
		return float64(kind.Int64Value), true
	case *expr.Value_Uint64Value:
		return float64(kind.Uint64Value), true
	case *expr.Value_DoubleValue:
		return kind.DoubleValue, true
	default:
		return 0, false
	}
}

// maxConstraintListElements bounds how many list elements a `unique:` check
// or a `must:` expression may examine, *summed across the whole value* —
// every element in every list reachable by walking the value, not the
// length of any one list in isolation.
//
// That total-rather-than-per-list shape is the fix for the bug this bound
// used to have: it was checked only against a literal declared
// `type: list`, so a `type: struct` input's `must:` reached an arbitrarily
// large list nested a level or two inside a map and it was never counted.
// Gating on the *declared* type was the mistake — the resource this bounds
// is how many elements a `must:` expression can be made to examine, and a
// struct's nested lists cost exactly as much to walk as a top-level one
// does. So [checkConstraintValueBound] walks the whole value regardless of
// declared type and adds every list element it finds to one running total,
// the same way this repository's billion-laughs bound on YAML alias
// expansion counts total nodes rather than chain depth: a struct holding a
// hundred lists of a few thousand elements each is exactly the shape a
// per-list bound lets through and a total bound catches.
//
// Both `unique:` and `must:` are quadratic, or worse, in element count by
// construction: `unique:` compares every pair, and a `must:` predicate can
// fold over a list inside a nested comprehension, the shape
// [TestEvaluatorCostLimit] proves the base evaluator's cost limit trips fast
// for — but that budget is abstract cost units tracked *during* evaluation,
// and a large enough input value spends real wall-clock time accumulating
// cost before the budget is reached (observed directly: a single
// non-nested `this.all(x, x >= 0)` over a 100,000-element list ran for over
// thirty seconds before this bound existed). The value is a caller's own
// choice — [BindRunInputs] runs this synchronously inside the RPC handler,
// before [CheckSubmissionSize] gets a chance to weigh the submitted inputs
// at all — so the resource actually being bounded here is element count,
// not bytes and not cost units, per this repository's rule: bound the
// resource the party you do not trust controls, and count what they
// actually spend.
const maxConstraintListElements = 10_000

// maxConstraintValueDepth bounds how deeply nested a value's lists and
// structs may be while [checkConstraintValueBound] walks it.
//
// This is a different resource than maxConstraintListElements, and CLAUDE.md
// is explicit that bounding one does not bound the other: a value can nest a
// single element a hundred thousand levels deep, never tripping an element
// count, while still exhausting the walker's own call stack — depth and
// breadth are independent attacker-controlled dimensions, so each gets its
// own bound and its own message, rather than one being asked to stand in for
// the other. Set to match [maxActivationDepth]'s reasoning and value: deep
// enough for anything a person writes by hand, shallow enough that recursion
// bounded by it cannot exhaust a goroutine's stack.
const maxConstraintValueDepth = 32

// checkConstraintListBound refuses a list value too long for unique: to
// examine cheaply, naming the bound so a caller knows what to shrink.
//
// unique: only ever runs against the top-level list a list-typed
// declaration carries directly — there is no nested case to walk into, so
// this stays a flat check rather than calling
// [checkConstraintValueBound]'s walk.
func checkConstraintListBound(name string, lit *expr.Value) error {
	list, ok := lit.GetKind().(*expr.Value_ListValue)
	if !ok {
		return nil
	}
	if n := len(list.ListValue.GetValues()); n > maxConstraintListElements {
		return fmt.Errorf(
			"input %q has %d items, over the %d a `unique:` or `must:` constraint may examine; "+
				"a list this large cannot be checked cheaply, and the caller's own choice of length "+
				"is not a cost this server bounds any other way", name, n, maxConstraintListElements)
	}
	return nil
}

// checkConstraintValueBound refuses a value whose `must:` a caller could
// make expensive to check: either because the total number of list elements
// reachable by walking it — through any nesting of lists and structs —
// exceeds [maxConstraintListElements], or because the value nests deeper
// than [maxConstraintValueDepth]. kind is "input" or "output", so the
// message names the right side of the constraint.
//
// Called unconditionally whenever a declaration has a `must:`, regardless of
// declared type: a list bound gated on `decl.GetType() == TYPE_LIST` is
// exactly the bug this replaces, since a struct-typed value's `must:` can
// still reach an arbitrarily large nested list.
func checkConstraintValueBound(kind, name string, lit *expr.Value) error {
	total := 0
	return walkConstraintValue(kind, name, lit, 0, &total)
}

// walkConstraintValue recursively counts list elements into *total and
// refuses once the running total exceeds maxConstraintListElements or the
// recursion exceeds maxConstraintValueDepth — the two bounds are checked
// independently, at every level, so neither resource can hide behind the
// other.
func walkConstraintValue(kind, name string, v *expr.Value, depth int, total *int) error {
	if v == nil {
		return nil
	}

	if depth > maxConstraintValueDepth {
		return fmt.Errorf(
			"%s %q nests %d levels deep, over the %d a `unique:` or `must:` constraint may descend "+
				"into; a value nested this deeply cannot be walked cheaply, and the caller's own choice "+
				"of nesting is not a cost this server bounds any other way", kind, name, depth, maxConstraintValueDepth)
	}

	switch k := v.GetKind().(type) {
	case *expr.Value_ListValue:
		for _, el := range k.ListValue.GetValues() {
			*total++
			if *total > maxConstraintListElements {
				return fmt.Errorf(
					"%s %q has at least %d list elements across its whole value, over the %d a `unique:` "+
						"or `must:` constraint may examine in total; a value this large cannot be checked "+
						"cheaply, and the caller's own choice of size is not a cost this server bounds any "+
						"other way", kind, name, *total, maxConstraintListElements)
			}
			if err := walkConstraintValue(kind, name, el, depth+1, total); err != nil {
				return err
			}
		}
	case *expr.Value_MapValue:
		for _, entry := range k.MapValue.GetEntries() {
			if err := walkConstraintValue(kind, name, entry.GetKey(), depth+1, total); err != nil {
				return err
			}
			if err := walkConstraintValue(kind, name, entry.GetValue(), depth+1, total); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkListConstraints applies min_items, max_items and unique to a list
// literal.
func checkListConstraints(name string, decl *InputDeclaration, lit *expr.Value) error {
	if decl.MinItems == nil && decl.MaxItems == nil && !decl.GetUnique() {
		return nil
	}
	list, ok := lit.GetKind().(*expr.Value_ListValue)
	if !ok {
		return nil
	}
	items := list.ListValue.GetValues()
	length := uint64(len(items))

	if decl.MinItems != nil && length < decl.GetMinItems() {
		return fmt.Errorf("input %q must have at least %d item(s); got %d", name, decl.GetMinItems(), length)
	}
	if decl.MaxItems != nil && length > decl.GetMaxItems() {
		return fmt.Errorf("input %q must have at most %d item(s); got %d", name, decl.GetMaxItems(), length)
	}

	if decl.GetUnique() {
		if err := checkConstraintListBound(name, lit); err != nil {
			return err
		}
		for i, item := range items {
			for _, earlier := range items[:i] {
				if proto.Equal(item, earlier) {
					got, _ := literalToNative(item)
					return fmt.Errorf("input %q must have unique items; %v appears more than once", name, got)
				}
			}
		}
	}

	return nil
}
