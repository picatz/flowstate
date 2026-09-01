package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/google/cel-go/cel"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// What protovalidate calls a standard-rule vocabulary — min_len, min,
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
	case InputDeclaration_TYPE_ENUM:
		// An enum value's wire shape is a string, the same shape TYPE_STRING
		// sends (see [InputDeclaration_values]'s own doc and [CheckInputValue]),
		// so `must:` binds `this` as a string exactly as it would for a
		// declared string input. Membership itself is checked separately, by
		// [checkEnumConstraint]; `must:` stays legal alongside `values:`,
		// redundant-but-legal, ANDed after the typed check.
		return cel.StringType
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
// against: `this` typed dyn.
//
// Dyn even now that an output may declare a `type:`, and deliberately: the
// type is optional and always will be (see [OutputDeclaration.type]), so
// binding `this` to it would make the same `must:` compile against a
// different `this` depending on whether a neighbouring key is present, and
// an author who adds a type to a working declaration would be shown an
// error in an expression they did not touch. What the declared type buys is
// checked separately and unconditionally by [CheckOutputValue], which runs
// before the `must:` does; tightening this binding is its own question,
// beside the `must:` scope one [OutputDeclaration.must] records.
//
// Built from the current profile's own library set — see [mustEnvFor]'s doc
// for why that set, and not a hand-copied one, is what belongs here.
var outputMustEnv = sync.OnceValues(func() (*cel.Env, error) {
	return mustBaseEnv(cel.Variable("this", cel.DynType))
})

type mustEnvResult struct {
	env *cel.Env
	err error
}

// mustBaseEnv returns the profile's own environment — the identical one
// [Evaluator.Env] builds and caches for `if:`, a task input, or any other
// expression position in a workflow — extended with extra (the declaration of
// `this`).
//
// This is the fix for the defect #234 verified: `must:` used to build
// `cel.NewEnv(cel.Variable("this", ...))` and nothing else, so `this.trim()`,
// `this.lowerAscii()`, `this.distinct()`, and `sets.contains(...)` all failed
// with "undeclared reference" — a diagnostic describing a typo the author did
// not make, and a second CEL dialect in one file, which CLAUDE.md's "one
// dialect per file, pinned per run" rule forbids. Sourcing the environment
// through [ProfileLibraries] and [Evaluator.Env] rather than copying
// `extensionLibraries`' contents into a second list is what keeps `must:`
// from drifting out of step with every other position the day a library is
// added or removed from the profile — a copied list is exactly the class of
// bug this repository's rule about one constant exists to prevent.
//
// # Why this is safe: every profile library is deterministic
//
// `must:` is evaluated at author time, at submit, and at every `call:`
// boundary a value crosses (see [refuseNondeterministicMust]'s own doc), so
// it has to answer identically every time. That requirement is what
// [extensionLibraries]' own doc comment already promises of every library it
// lists — "every entry must be deterministic and free of I/O" — so widening
// `must:` to the profile's set does not admit anything `must:` cannot afford:
//
//   - bindings, comprehensions (macros: `cel.bind`, two-variable
//     comprehensions) — pure control-flow sugar, no new values.
//   - encoders (base64 encode/decode), lists (distinct, flatten, sum, …),
//     math (greatest/least and friends), sets, strings (trim, lowerAscii, …)
//     — pure functions of their arguments.
//   - regex (`this.matches`, `extractAll`, …) — pure pattern matching, no
//     compilation cost beyond what [DefaultEvaluator]'s cost limit already
//     bounds.
//   - optional, protos — pure construction/inspection helpers.
//   - json (`json_parse`) — deterministic for a given input string.
//   - the always-on duration constructors (`days(3)`, `hours(12)`, …) —
//     pure arithmetic on the argument given, not the clock.
//
// None of the above reads a clock, a random source, or does I/O. The one
// nondeterministic name in the language — [NowIdentifier] — is not a library
// function but a *variable* bound only inside a wait's own
// evaluation (see wait.go), so it is not part of any profile library and
// widening the library set does not expose it; [refuseNondeterministicMust]
// still catches a `must:` that references it, by the identical free-identifier
// walk this had before. If a future library ever needs I/O or a clock, it
// does not belong in [extensionLibraries] at all — that map's own doc already
// says so — so there is no library this function could pull in that would
// weaken that guarantee.
func mustBaseEnv(extra ...cel.EnvOption) (*cel.Env, error) {
	libs, err := ProfileLibraries(CurrentProfile)
	if err != nil {
		return nil, fmt.Errorf("resolve profile libraries: %w", err)
	}

	// DefaultEvaluator().Env is the identical cached construction every other
	// expression position in a workflow resolves through
	// ([Evaluator.EvalParsedBase], [Evaluator.ProfileEnv]) — reusing it rather
	// than building a second environment from the same library names is what
	// makes it impossible for `must:`'s vocabulary to silently diverge from
	// theirs.
	base, err := DefaultEvaluator().Env(libs...)
	if err != nil {
		return nil, fmt.Errorf("build profile environment: %w", err)
	}

	env, err := base.Extend(extra...)
	if err != nil {
		return nil, fmt.Errorf("extend profile environment: %w", err)
	}
	return env, nil
}

// mustEnvFor returns the cached environment for t's `must:` expressions,
// building it on first use.
func mustEnvFor(t InputDeclaration_Type) (*cel.Env, error) {
	if cached, ok := mustEnvs.Load(t); ok {
		res := cached.(*mustEnvResult)
		return res.env, res.err
	}

	env, err := mustBaseEnv(cel.Variable("this", constraintCELType(t)))
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

// refuseNondeterministicMust reports a tailored refusal when a `must:`
// expression reads `now`, the one nondeterministic name an author is likeliest
// to reach for out of habit — `now` is bound inside a wait's own expressions,
// and a `must:` is not one of them.
//
// This is deliberately narrower than "reject every free name other than
// `this`." Since V1 widened `must:`'s environment to the profile's own
// libraries, a name that is free in the *raw parse tree* is not necessarily
// free in the *checked* expression: `sets.contains(a, b)` and
// `base64.encode(b)` parse as a select on the identifier `sets` or `base64`
// applied to a call, because the parser resolves only macros (expanded
// in-place before this ever runs — `cel.bind`, `math.greatest`, `math.least`,
// `proto.getExt`, `proto.hasExt`, every two-variable comprehension), not
// namespaced function declarations; whether `sets` or `base64` is a package
// prefix for a declared function or an actually-undeclared variable is a
// question the *checker* answers, not the parser. Flagging every such prefix
// here produced a false "unknown name" refusal for exactly the library
// functions V1 exists to enable.
//
// So this function only ever refuses `now`, pre-check, for the friendlier
// message; every other undeclared name — a step reference, a bare typo, a
// genuinely-unknown package prefix — is left to `env.Check`, which refuses it
// correctly, because nothing but `this` and the profile's own declared
// functions are ever declared in a `must:` environment, and an
// undeclared-reference error is exactly what that produces. `now` cannot
// itself be part of a namespaced function name — it does not appear as a
// package prefix in any profile library — so narrowing to it introduces no
// gap: every name this used to refuse generically, `env.Check` still refuses,
// just with a different message wrapping the identical `fmt.Errorf("must:
// %w", ...)` in [compileMustIn].
func refuseNondeterministicMust(parsed *expr.ParsedExpr) error {
	free := map[string]struct{}{}
	collectFreeIdentifiers(parsed.GetExpr(), map[string]struct{}{}, free)

	if _, ok := free[NowIdentifier]; ok {
		return fmt.Errorf(
			"must: may not reference `now`: a constraint is checked at author time, at submit, and at " +
				"every call boundary a value crosses, and has to answer the same way every time, but `now` " +
				"is the moment a `wait_until:` is evaluated and reads differently on each of those — write " +
				"the deadline as an ordinary input instead")
	}

	return nil
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
// to the declared type, a min above its max, or a `must:` that will not
// compile.
//
// This is the "rules compile when configuration loads" half of the fail-closed
// rule. [BindRunInputs] runs it before checking any submitted value, so a
// broken declaration is refused at submit even for a specification that never
// passed through `flow validate` — and `flow validate` runs the identical
// check early enough to report it as a diagnostic with a position.
func CheckInputConstraintShape(decl *InputDeclaration) error {
	name := decl.GetName()
	t := decl.GetType()

	if decl.MinLen != nil || decl.MaxLen != nil {
		if t != InputDeclaration_TYPE_STRING {
			return fmt.Errorf(
				"input %q declares a string constraint (min_len or max_len) but is declared %s; "+
					"those apply only to a string input", name, DeclaredTypeName(t))
		}
	}
	if decl.MinLen != nil && decl.MaxLen != nil && decl.GetMinLen() > decl.GetMaxLen() {
		return fmt.Errorf("input %q min_len (%d) is greater than max_len (%d), so no string can satisfy both",
			name, decl.GetMinLen(), decl.GetMaxLen())
	}

	if decl.MinItems != nil || decl.MaxItems != nil {
		if t != InputDeclaration_TYPE_LIST {
			return fmt.Errorf(
				"input %q declares min_items or max_items but is declared %s; those apply only "+
					"to a list input", name, DeclaredTypeName(t))
		}
	}
	if decl.MinItems != nil && decl.MaxItems != nil && decl.GetMinItems() > decl.GetMaxItems() {
		return fmt.Errorf("input %q min_items (%d) is greater than max_items (%d), so no list can satisfy both",
			name, decl.GetMinItems(), decl.GetMaxItems())
	}
	if decl.MinItems != nil && decl.GetMinItems() > maxListElements {
		return fmt.Errorf("input %q min_items (%d) is greater than %d, the most list elements this server "+
			"binds a run input to; no list can ever satisfy both, since every list over %d elements is "+
			"refused before this constraint runs",
			name, decl.GetMinItems(), maxListElements, maxListElements)
	}

	if len(decl.Values) > 0 && t != InputDeclaration_TYPE_ENUM {
		return fmt.Errorf(
			"input %q declares values but is declared %s; values apply only to an enum input",
			name, DeclaredTypeName(t))
	}
	if t == InputDeclaration_TYPE_ENUM && len(decl.Values) == 0 {
		return fmt.Errorf(
			"input %q is declared enum but declares no values; an enum needs at least one member "+
				"to be a closed set of anything", name)
	}
	if t == InputDeclaration_TYPE_ENUM && len(decl.Values) > 0 {
		if err := checkEnumValuesShape("input", name, decl.Values); err != nil {
			return err
		}
	}

	if decl.Must != nil {
		if _, err := CompileMustExpression(decl.GetMust(), t); err != nil {
			return fmt.Errorf("input %q %w", name, err)
		}
	}

	return nil
}

// EnumValuesShapeError reports that a declared enum's `values:` list
// violates one of the per-member or list-size rules the schema itself
// declares on [InputDeclaration.values] in
// proto/flowstate/v1/workflow.proto: at most 64 entries, each 1-128
// characters, all distinct.
//
// [CheckInputConstraintShape] returns one of these, rather than a bare
// error, so that a caller which knows where in a source document each part
// of the declaration was written — the flowfile compiler — can point a
// diagnostic at the exact member responsible instead of at the declaration
// as a whole. Field is empty for every other error
// [CheckInputConstraintShape] can return, so a caller distinguishes this
// case with [errors.As] rather than by matching message text.
type EnumValuesShapeError struct {
	// Kind is which half of the contract the declaration is — "input" or
	// "output" — since both declare `values:` under the identical rules and a
	// message naming the wrong one would send an author to the wrong block.
	// Empty reads as "input", so a caller predating an output's own `values:`
	// keeps the sentence it had.
	Kind string

	// Name is the declaration's own name, folded into
	// [EnumValuesShapeError.Error]'s message.
	Name string

	// Field is the path of the value at fault, relative to the declaration
	// itself the way protovalidate reports it: "values" for a rule about the
	// list as a whole (too many entries, a duplicate among them), or
	// "values[i]" when one member at index i is the one that failed (empty,
	// or over the length bound).
	Field string

	message string
}

// Error renders the violation as a sentence naming the input and, for a
// member-specific violation, which one — not the raw protovalidate message,
// which names a rule ID and a generic bound rather than the actual member or
// count at fault.
func (e *EnumValuesShapeError) Error() string {
	kind := e.Kind
	if kind == "" {
		kind = "input"
	}

	return fmt.Sprintf("%s %q %s", kind, e.Name, e.message)
}

// checkEnumValuesShape applies the schema's own bound on a declared enum's
// `values:` — at most 64 entries, each 1-128 characters, all distinct — to
// values.
//
// Rather than hand-writing those three numbers a second time, which is
// exactly the kind of copy CLAUDE.md's "one rule, not two" warns drifts from
// the schema the moment somebody edits the (buf.validate.field) annotation
// on [InputDeclaration.values], this builds a throwaway declaration carrying
// only a placeholder name and the values in question and runs it through
// [Validate] — the identical schema-driven validator the server applies to a
// submitted specification — so the two can never disagree about where the
// bound sits. The placeholder name is a valid identifier so that
// [InputDeclaration.name]'s own rules, irrelevant to this function, never
// fire and need filtering out of the result.
//
// A violation's rule ID and field path pick out which of the three
// author-facing messages to build; the values slice already in hand supplies
// the specifics — the actual duplicate, the actual length — that
// protovalidate's own message does not carry, per CLAUDE.md's rule that a
// diagnostic surfaced to an author is written for an editor rather than
// wrapped from the validator that found it.
func checkEnumValuesShape(kind, name string, values []string) error {
	probe := &InputDeclaration{
		Name:   "x",
		Type:   InputDeclaration_TYPE_ENUM,
		Values: values,
	}

	verr := Validate(probe)
	if verr == nil {
		return nil
	}

	var invalid *ValidationError
	if !errors.As(verr, &invalid) {
		// The validator itself could not be run at all (see
		// [ErrValidatorUnavailable]) — fail closed per CLAUDE.md rather than
		// let a declaration this function could not actually check through.
		return fmt.Errorf("%s %q values: %w", kind, name, verr)
	}

	for _, v := range invalid.Violations {
		switch {
		case v.Field == "values" && v.Rule == "repeated.max_items":
			return &EnumValuesShapeError{
				Kind: kind, Name: name, Field: v.Field,
				message: fmt.Sprintf(
					"declares %d values, but an enum may declare at most 64; trim the list, or split it "+
						"into more than one input", len(values)),
			}
		case v.Field == "values" && v.Rule == "repeated.unique":
			if dup, idx := firstDuplicateEnumValue(values); idx >= 0 {
				return &EnumValuesShapeError{
					Kind: kind, Name: name, Field: v.Field,
					message: fmt.Sprintf(
						"value %d (%q) repeats one already declared; an enum's values must be distinct",
						idx, dup),
				}
			}
			return &EnumValuesShapeError{
				Kind: kind, Name: name, Field: v.Field,
				message: "declares two identical values; an enum's values must be distinct",
			}
		case strings.HasPrefix(v.Field, "values[") && v.Rule == "string.min_len":
			return &EnumValuesShapeError{
				Kind: kind, Name: name, Field: v.Field,
				message: fmt.Sprintf(
					"value %d is empty; every enum value must be at least 1 character",
					enumValueIndex(v.Field)),
			}
		case strings.HasPrefix(v.Field, "values[") && v.Rule == "string.max_len":
			idx := enumValueIndex(v.Field)
			length := 0
			if idx >= 0 && idx < len(values) {
				length = utf8.RuneCountInString(values[idx])
			}
			return &EnumValuesShapeError{
				Kind: kind, Name: name, Field: v.Field,
				message: fmt.Sprintf(
					"value %d is %d characters, over the 128 an enum value may hold", idx, length),
			}
		}
	}

	// Every rule this function knows to translate lives on the schema today
	// and is handled above; this is reached only if the schema's own
	// annotation on `values` grows a rule this function has not been taught
	// yet. Fail closed rather than silently accept a declaration the server
	// would still refuse: the raw validator message is worse than a
	// hand-written one, but it is far better than reporting nothing wrong.
	return fmt.Errorf("%s %q %s", kind, name, invalid.Error())
}

// firstDuplicateEnumValue returns the first value in values that repeats one
// seen earlier, and the index it repeats at, so [checkEnumValuesShape] can
// name the actual duplicate rather than parrot protovalidate's
// rule-shaped-but-contentless "must contain unique items".
//
// Returns ("", -1) if values holds no duplicate — which callers only reach
// this for after protovalidate's own repeated.unique rule already fired, so
// in practice one always exists; the -1 sentinel exists so this function
// cannot panic if that ever stops being true.
func firstDuplicateEnumValue(values []string) (string, int) {
	seen := make(map[string]bool, len(values))
	for i, v := range values {
		if seen[v] {
			return v, i
		}
		seen[v] = true
	}
	return "", -1
}

// enumValueIndex parses the index out of a protovalidate field path shaped
// like "values[3]", returning -1 if field is not that shape.
func enumValueIndex(field string) int {
	open := strings.IndexByte(field, '[')
	if open < 0 || !strings.HasSuffix(field, "]") {
		return -1
	}
	n, err := strconv.Atoi(field[open+1 : len(field)-1])
	if err != nil {
		return -1
	}
	return n
}

// CheckOutputConstraintShape is [CheckInputConstraintShape] for an output: the
// two set-facts a declared `type:` brings with it, and the `must:` that was
// this function's whole subject before there was a type to have facts about.
//
// The `values:` rules are the input ones reached rather than restated —
// [checkEnumValuesShape] derives its own bound from the schema, so the two
// declarations cannot come to disagree about how many members an enum may have
// or how long one may be.
func CheckOutputConstraintShape(decl *OutputDeclaration) error {
	name := decl.GetName()
	t := decl.GetType()

	if len(decl.Values) > 0 && t != InputDeclaration_TYPE_ENUM {
		return fmt.Errorf(
			"output %q declares values but is declared %s; values apply only to an enum output",
			name, DeclaredTypeName(t))
	}
	if t == InputDeclaration_TYPE_ENUM && len(decl.Values) == 0 {
		return fmt.Errorf(
			"output %q is declared enum but declares no values; an enum needs at least one member "+
				"to be a closed set of anything", name)
	}
	if t == InputDeclaration_TYPE_ENUM {
		if err := checkEnumValuesShape("output", name, decl.Values); err != nil {
			return err
		}
	}

	if decl.Must == nil {
		return nil
	}
	if _, err := CompileOutputMustExpression(decl.GetMust()); err != nil {
		return fmt.Errorf("output %q %w", name, err)
	}
	return nil
}

// CheckOutputValue refuses a computed output whose value does not have the type
// its declaration promised, or whose value is outside a declared enum's set.
//
// The counterpart of [CheckInputValue] and [checkEnumConstraint], pointed the
// other way: an input is a value a caller chose and is refused while the caller
// is still there to be told, and an output is a value the run computed and is
// refused before it is reported as the run's answer. Both drivers reach this
// through [EvalRunOutputs], which is what makes the two agree by construction
// rather than by two matching implementations (invariant 3).
//
// Nil for an output that declares no type, which is every declaration written
// before there was one to declare and every declaration that still chooses not
// to — see [OutputDeclaration.type] on why that stays legal. Nil, too, for a
// value with no literal to judge: an expression the engine could not evaluate is
// a different failure, reported by whoever computed it.
//
// Also called by `flow validate` against a literal (or an all-constant
// structure) written directly under `value:`, where the answer is knowable
// without running anything — the same static-half/run-half split
// [CheckInputValue] has between a `with:` argument and a submitted one.
func CheckOutputValue(decl *OutputDeclaration, value *Value) error {
	t := decl.GetType()
	if t == InputDeclaration_TYPE_UNSPECIFIED {
		return nil
	}

	lit := value.GetLiteral()
	if lit == nil {
		if _, isStructure := value.GetKind().(*Value_Structure_); !isStructure {
			return nil
		}
		// A mapping or list written directly under `value:` compiles to a
		// structure rather than to a literal (see structure.go), and its type
		// is knowable regardless: flattened by the same function
		// [EvalRunOutputs] flattens it with, so a static answer and a run-time
		// one cannot differ. A structure that will not flatten holds something
		// a declared output may not hold at all, which is refused elsewhere
		// and is not this function's judgement to make.
		flattened, err := structureLiteral(value)
		if err != nil {
			return nil
		}
		lit = flattened
	}

	if err := checkDeclaredLiteralType("output", "computed", decl.GetName(), t, lit); err != nil {
		return err
	}

	return checkEnumMembership("output", decl.GetName(), t, decl.GetValues(),
		outputValueRendering(decl), lit)
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
	if err := checkListConstraints(name, decl, lit); err != nil {
		return err
	}
	if err := checkEnumConstraint(name, decl, lit); err != nil {
		return err
	}

	// Bounded here, unconditionally, regardless of whether `must:`/`unique:`
	// is declared — the same #204 gap [BindRunInputs] closes for a submitted
	// input applies identically to a literal checked at author time, because
	// every caller of this function reaches here with a literal value: a
	// submitted (or defaulted) run input through [BindRunInputs], a literal
	// `default:`/`example:` through [CheckInputDefault]/[CheckInputExample],
	// and a literal `with:` argument through `flowfile/validate_call.go`. A
	// second bound duplicated at each of those call sites could disagree with
	// this one; reusing [checkInputListElementBound] — the identical walker
	// and constant [BindRunInputs] uses — is what keeps them from ever being
	// able to. Only a literal is checked: an expression's value is not known
	// until [BindRunInputs] resolves it, so there is nothing here yet to walk.
	if err := checkInputListElementBound(name, lit); err != nil {
		return err
	}

	if decl.Must == nil {
		return nil
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
		// The predicate stays in the sentence and the value may not: `must:` is
		// written in the file, and the value is what the run computed. See
		// [redactedIfSensitive] for why an output's own refusal is the last
		// place that value can be withheld.
		//
		// Bounded on the way in for the reason [checkEnumMembership] states:
		// `must:` may be declared on an output of any type, so the rendering
		// here is a whole task result — a string, or a map of them — and an
		// unbounded one would be a failure the durable driver cannot persist
		// while the local driver returns it.
		return fmt.Errorf("output %q must satisfy `%s`; got %s",
			decl.GetName(), decl.GetMust(), redactedIfSensitive(decl.GetSensitive(), func() string {
				got, _ := literalToNative(lit)

				return truncateForError(fmt.Sprintf("%v", got))
			}))
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

// checkStringConstraints applies min_len and max_len to a string literal.
//
// A pattern rule used to live here too. It is gone: `must: this.matches('re')`
// says the identical thing through `must:`'s one CEL evaluation path — see
// [InputDeclaration]'s doc on the field number this retired — so a hostile
// regex is bounded exactly where every other `must:` expression already is,
// by [CompileMustExpression] and [DefaultEvaluator].Eval, rather than by a
// second, unbounded `regexp.Compile` call sitting beside it.
//
// Silently returns for a value [inputTypeOf] does not read as a string —
// [CheckInputValue] already refused the mismatch, and this is not the place
// to report it a second time, per this repository's rule about one mistake
// getting one diagnostic.
func checkStringConstraints(name string, decl *InputDeclaration, lit *expr.Value) error {
	if decl.MinLen == nil && decl.MaxLen == nil {
		return nil
	}
	s, ok := lit.GetKind().(*expr.Value_StringValue)
	if !ok {
		return nil
	}
	value := s.StringValue

	length := uint64(utf8.RuneCountInString(value))
	if decl.MinLen != nil && length < decl.GetMinLen() {
		return fmt.Errorf("input %q must be at least %d character(s) long; got %d", name, decl.GetMinLen(), length)
	}
	if decl.MaxLen != nil && length > decl.GetMaxLen() {
		return fmt.Errorf("input %q must be at most %d character(s) long; got %d", name, decl.GetMaxLen(), length)
	}

	return nil
}

// maxListElements bounds how many list elements a value may carry in total,
// *summed across the whole value* — every element in every list reachable by
// walking the value, not the length of any one list in isolation.
//
// Originally this bounded only what a `unique:` check or a `must:` expression
// could examine (#177 slice 1 / PR #205). #204 found that narrower scope was
// the gap: any list-typed value reaches CEL the identical way whether or not
// a declaration happens to carry `must:`/`unique:` — a step's `if:`, a
// `for_each`'s items, an ordinary `${inputs.records.all(...)}` all hand the
// same Go-native list to the same interpreter, and #204's own measurement
// (`this.all(x, x >= 0)`: 10k elements/228ms, 20k/886ms, 40k/5,271ms — while
// `this.size()` stays O(1) at every n, ruling out list conversion as the
// cost) showed the comprehension itself is quadratic in element count while
// CEL's cost accounting of it stays linear, so no [DefaultCostLimit] bounds
// the wall-clock time. Whether that list arrived via a declared `must:` or
// via a plain `for_each` changes nothing about how expensive it is to walk,
// so this one constant now bounds both: [checkConstraintValueBound] applies
// it for an output's `must:`, and [CheckInputConstraints] applies it to
// *every* literal it is handed regardless of whether `must:`/`unique:` is
// declared — which reaches it for every submitted (or defaulted) run input
// through [BindRunInputs], and for every literal `default:`, `example:`, and
// call-boundary `with:` argument checked at author time, since all of those
// call [CheckInputConstraints] too. See that function's own call for the
// reasoning that closes the gap.
//
// That total-rather-than-per-list shape is also the fix for a bug this bound
// used to have: it was checked only against a literal declared `type: list`,
// so a `type: struct` input reached an arbitrarily large list nested a level
// or two inside a map and it was never counted. Gating on the *declared*
// type was the mistake — the resource this bounds is how many elements an
// expression can be made to examine, and a struct's nested lists cost
// exactly as much to walk as a top-level one does. So the walk covers the
// whole value regardless of declared type and adds every list element it
// finds to one running total, the same way this repository's
// billion-laughs bound on YAML alias expansion counts total nodes rather
// than chain depth: a struct holding a hundred lists of a few thousand
// elements each is exactly the shape a per-list bound lets through and a
// total bound catches.
//
// # Why 10,000, and why the same number for both call sites
//
// `unique:` is quadratic in element count by construction — it compares
// every pair — and a `must:`, `if:`, or `for_each` expression can iterate a
// list inside a comprehension at the same quadratic-in-practice cost #204
// measured. Nothing about that cost profile depends on whether a constraint
// happens to be declared, so using a *different* number for the general
// input path than for the constraint path would be two bounds on one
// resource disagreeing with each other for no reason — exactly what this
// repository's rule about one constant says to avoid.
//
// 10,000 is deliberately generous rather than tight: #204 measured 20,000
// elements at 886ms and 40,000 at 5.27s, so the number is chosen well below
// where a single request starts costing whole seconds of a server core,
// while staying far above what an ordinary `for_each` fanout needs — the
// examples in this repository fan out over tens to low hundreds of items,
// not tens of thousands. A workflow that legitimately needs to process more
// than 10,000 items does not fit in one submitted literal either way: the
// fix is to page the work across multiple runs, or to have a step read the
// worklist itself (a database query, a paginated API) from a reference the
// caller passes instead of embedding the whole list as an input — which is
// also what the refusal below tells the caller to do.
const maxListElements = 10_000

// maxConstraintValueDepth bounds how deeply nested a value's lists and
// structs may be while [checkConstraintValueBound] (and, for every literal
// [CheckInputConstraints] is handed, [checkInputListElementBound]) walks it.
//
// This is a different resource than maxListElements, and CLAUDE.md is
// explicit that bounding one does not bound the other: a value can nest a
// single element a hundred thousand levels deep, never tripping an element
// count, while still exhausting the walker's own call stack — depth and
// breadth are independent attacker-controlled dimensions, so each gets its
// own bound and its own message, rather than one being asked to stand in for
// the other.
//
// Reads [MaxStructureDepth] rather than keeping its own number. This walk
// descends a compiled CEL literal (a
// google.golang.org/genproto/googleapis/api/expr/v1alpha1.Value) rather than
// a [Value_Structure] — a different Go type — but it is the identical
// resource: how many levels of list-or-map nesting one recursive walk must
// descend before it can answer a question about the whole value. Per the
// one-constant rule that makes it the same bound rather than a coincidentally
// equal one; before this was unified the two numbers agreed only because a
// comment said to keep them in sync by hand, which is exactly the shape
// CLAUDE.md's "both execution drivers must agree" section warns never
// survives being written down twice. Deep enough for anything a person writes
// by hand, shallow enough that recursion bounded by it cannot exhaust a
// goroutine's stack; see [maxActivationDepth] for the same reasoning applied
// to a third resource (CEL activation recursion) that happens to share the
// number without sharing the type being walked.
const maxConstraintValueDepth = MaxStructureDepth

// checkConstraintValueBound refuses a value whose `must:` a caller could
// make expensive to check: either because the total number of list elements
// reachable by walking it — through any nesting of lists and structs —
// exceeds [maxListElements], or because the value nests deeper than
// [maxConstraintValueDepth]. kind is "input" or "output", so the message
// names the right side of the constraint.
//
// Called from [CheckOutputConstraint] whenever an output declares `must:`.
// The input side used to call this the same way, gated on `decl.Must != nil`
// — exactly the bug this replaces, since a struct-typed value's `must:` can
// reach an arbitrarily large nested list regardless of declared type — but
// [CheckInputConstraints] now calls [checkInputListElementBound]
// unconditionally instead, so this function stays only as the output-side
// entry point; both still walk the identical value with [walkConstraintValue]
// and the identical bounds, because they are the identical resource.
func checkConstraintValueBound(kind, name string, lit *expr.Value) error {
	total := 0
	if v := walkConstraintValue(lit, 0, &total); v != nil {
		return inputSideConstraintBoundError(kind, name, v)
	}
	return nil
}

// checkInputListElementBound is [checkConstraintValueBound] reached without
// requiring a `must:`/`unique:` to have been declared — the general case
// #204 found missing. [CheckInputConstraints] calls this for every literal
// input value it is handed, unconditionally — whether it arrived through
// [BindRunInputs] (a caller's submitted argument, or a declaration's own
// default filling in for one left out), through [CheckInputDefault] or
// [CheckInputExample] checking a literal at author time, or through a call
// boundary's literal `with:` argument — because a list reaches `if:`,
// `for_each`, and every other CEL expression over it exactly as cheaply or
// expensively regardless of whether a constraint happens to be declared, or
// whether the value is being checked at submit or at author time. Gating the
// walk on `must:`/`unique:` being present is precisely the gap that let an
// unconstrained list-typed input through unbounded. Reuses
// [walkConstraintValue] rather than a second walker, per this repository's
// rule that one resource gets one bound: every path that reaches this must
// never be able to disagree about how many elements a value carries.
func checkInputListElementBound(name string, lit *expr.Value) error {
	total := 0
	if v := walkConstraintValue(lit, 0, &total); v != nil {
		return inputSideConstraintBoundError("input", name, v)
	}
	return nil
}

// checkTaskOutputElementBound is [checkInputListElementBound]'s counterpart
// for the other side of a task: what a task's result carries rather than
// what a caller submitted.
//
// #204 closed the caller-input half of this gap — every literal
// [BindRunInputs] binds is walked by [checkInputListElementBound] regardless
// of declared type or declared constraint. This is the half that gap left
// open: a task's own output is a value the *remote* side controls, not the
// caller — an `http` task's body is bounded in bytes (1 MiB), which a list of
// small integers turns into on the order of 150,000 elements, fifteen times
// this server's own input ceiling, and a plugin task's output is bounded in
// bytes by its transport and not bounded in element count anywhere. Whichever
// step in the workflow consumes that output — an `if:`, a `for_each`, a
// `${...}` referencing it — pays the identical quadratic-in-practice CEL cost
// [maxListElements]'s own doc comment measured, so the resource is the same
// one and gets the same bound.
//
// Called from [Task.EvalInScope], the one place both the local and the
// durable driver funnel every task's call through — a built-in task's `Fn`
// and a plugin task's host-function `Fn` alike — so bounding the value there
// makes both drivers agree by construction rather than by two call sites
// staying in sync. This does *not* cover an `http` task's own `expect:`/
// `outputs:` evaluation, which runs *inside* `def.Fn`, before this ever sees
// the result — [checkHTTPResponseElementBound] bounds that half, at the
// point the response is parsed, for exactly that reason.
//
// The total is summed *across every named value* in the output, not reset
// per name, matching [maxListElements]'s own "total across the whole value"
// accounting: a task returning ten output fields of a thousand list elements
// each costs a later expression exactly as much to walk as one field of ten
// thousand would, and a per-field bound lets the former through. Map keys are
// walked in sorted order so a run that trips the bound reports the identical
// count and message on every replay — a map iterates in Go in an order this
// package does not get to depend on.
//
// wait/wait_for_signal payloads do not reach this function — they never pass
// through EvalInScope, and #204's own scoping (recorded in this repository's
// issue tracker, not restated here) leaves that path to the byte bound and
// the attested-sender check #194 added, since a signal payload is a different
// trust boundary than a task's own result.
func checkTaskOutputElementBound(taskName string, out *Node_Outputs) error {
	if out == nil {
		return nil
	}

	values := out.GetNamedValues()
	if len(values) == 0 {
		return nil
	}

	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)

	total := 0
	for _, name := range names {
		lit := values[name].GetLiteral()
		if lit == nil {
			continue
		}
		if v := walkConstraintValue(lit, 0, &total); v != nil {
			return taskOutputConstraintBoundError(taskName, v)
		}
	}

	return nil
}

// constraintBoundViolation is the *data* [walkConstraintValue] finds when a
// value trips either bound it walks for — never a formatted message.
//
// Splitting the finding from its wording is what lets every caller of
// [walkConstraintValue] describe the *same* violation in the sentence that
// fits its own resource. [checkInputListElementBound]/[checkConstraintValueBound]
// are checking a value the *caller* chose the size of, and their message
// (formatted by [inputSideConstraintBoundError]) says so; [checkTaskOutputElementBound]
// and [checkHTTPResponseElementBound] are checking a value a *remote
// endpoint or plugin* chose the size of, and their own formatters say that
// instead — accurately, rather than by wrapping a sentence written for the
// other side. Before this split, every caller shared one formatted-error
// return, and a #224 review found that let a task-output refusal wrap the
// input-shaped sentence ("the caller's own choice of size") inside a second
// sentence saying the opposite: two contradictory causes and remedies in one
// error.
//
// Exactly one of Depth or TooManyElements is ever true.
type constraintBoundViolation struct {
	// Depth is true when the value nested deeper than [maxConstraintValueDepth];
	// DepthReached is the depth [walkConstraintValue] was at when it gave up.
	Depth        bool
	DepthReached int

	// TooManyElements is true when the running element count exceeded
	// [maxListElements]; ElementCount is the total at the point it did.
	TooManyElements bool
	ElementCount    int
}

// walkConstraintValue recursively counts list elements into *total and
// reports the first bound tripped — either the running total exceeding
// [maxListElements] or the recursion exceeding [maxConstraintValueDepth],
// checked independently at every level so neither resource can hide behind
// the other — as a [constraintBoundViolation], or nil once the whole value
// has been walked cheaply.
//
// Returns data rather than a formatted error so every caller can word the
// refusal for its own resource; see [constraintBoundViolation]'s own doc for
// why that split exists.
//
// Shared by every entry point that has to bound how many elements a CEL
// expression can be made to examine over a value, regardless of whether that
// value arrived as a caller's submitted input, a declared `must:`/`unique:`
// target, a task's own result, or an http task's parsed JSON response body —
// the cost is identical at every one of those origins, so this is the one
// walker all of them share.
func walkConstraintValue(v *expr.Value, depth int, total *int) *constraintBoundViolation {
	if v == nil {
		return nil
	}

	if depth > maxConstraintValueDepth {
		return &constraintBoundViolation{Depth: true, DepthReached: depth}
	}

	switch k := v.GetKind().(type) {
	case *expr.Value_ListValue:
		for _, el := range k.ListValue.GetValues() {
			*total++
			if *total > maxListElements {
				return &constraintBoundViolation{TooManyElements: true, ElementCount: *total}
			}
			if violation := walkConstraintValue(el, depth+1, total); violation != nil {
				return violation
			}
		}
	case *expr.Value_MapValue:
		for _, entry := range k.MapValue.GetEntries() {
			if violation := walkConstraintValue(entry.GetKey(), depth+1, total); violation != nil {
				return violation
			}
			if violation := walkConstraintValue(entry.GetValue(), depth+1, total); violation != nil {
				return violation
			}
		}
	}

	return nil
}

// inputSideConstraintBoundError renders a [constraintBoundViolation] for a
// value the *caller* chose the size of: a submitted (or defaulted) run
// input, or a literal a declared `must:`/`unique:` examines. kind is "input"
// or "output" — which side of a declaration's own constraint this is, not
// which side of the trust boundary the value came from; both are still
// values the workflow's own author supplied, hence "the caller's own choice
// of size" below.
//
// This is the exact wording [walkConstraintValue] itself used to produce
// inline before the two were split, kept unchanged so the input-side
// refusal, and the tests pinning it, do not regress.
func inputSideConstraintBoundError(kind, name string, v *constraintBoundViolation) error {
	if v.Depth {
		return fmt.Errorf(
			"%s %q nests %d levels deep, over the %d levels this server can walk cheaply while "+
				"evaluating an expression over it (`if:`, `for_each`, `must:`, `unique:`); a value nested "+
				"this deeply is not a cost this server bounds any other way — flatten it, or have a step "+
				"read it from a reference instead of submitting it nested this deep",
			kind, name, v.DepthReached, maxConstraintValueDepth)
	}
	return fmt.Errorf(
		"%s %q has at least %d list elements across its whole value, over the %d this server "+
			"can evaluate a CEL expression over cheaply (`if:`, `for_each`, `must:`, `unique:` "+
			"all pay the same cost); the caller's own choice of size is not a cost this server "+
			"bounds any other way — page the work across multiple runs, or have a step read the "+
			"list from a reference instead of submitting the whole thing as one input",
		kind, name, v.ElementCount, maxListElements)
}

// taskOutputConstraintBoundError renders a [constraintBoundViolation] for a
// task's own result — [checkTaskOutputElementBound]'s only caller. Unlike
// [inputSideConstraintBoundError], this never wraps that function's
// sentence: the two describe opposite causes (the caller's choice vs. the
// task's own result), and #224 review found wrapping one inside the other
// produced a refusal that contradicted itself mid-sentence.
func taskOutputConstraintBoundError(taskName string, v *constraintBoundViolation) error {
	if v.Depth {
		return fmt.Errorf(
			"task %q's result nests %d levels deep, over the %d levels a later step can walk cheaply "+
				"while evaluating an expression over it (an `if:`, a `for_each`, or a `${...}` reading "+
				"this result all pay the same cost); this is the shape of the task's own result, not "+
				"anything the workflow submitted — have the task return a flatter shape, or a reference "+
				"a later step reads instead of the nested value itself",
			taskName, v.DepthReached, maxConstraintValueDepth)
	}
	return fmt.Errorf(
		"task %q returned at least %d list elements across its result, over the %d a later step can "+
			"evaluate an expression over cheaply (an `if:`, a `for_each`, or a `${...}` reading this "+
			"result all pay the same cost); this is the size of the task's own result, not anything the "+
			"workflow submitted — narrow the query, page the work across multiple calls, or have the "+
			"task filter server-side so only the fields a later step needs come back",
		taskName, v.ElementCount, maxListElements)
}

// checkHTTPResponseElementBound is [checkTaskOutputElementBound]'s
// counterpart for the half a task-output check alone cannot reach: the
// `http` task's own `expect:`/`outputs:` evaluation runs *inside*
// `taskFuncHTTP`, against the parsed response body, before that function
// ever returns to [Task.EvalInScope] — so a comprehension in either one
// (`response.json.filter(...)`, `response.json.all(...)`) pays the
// quadratic-in-practice CEL cost [maxListElements]'s own doc comment
// measures *before* the task-output bound is in a position to refuse
// anything. Called from `taskFuncHTTP` immediately after the body is parsed
// and before either evaluation runs.
//
// url is the request URL, not the task's step id — the same reasoning
// [taskOutputConstraintBoundError] states: this is the size of what the
// *remote endpoint* answered with, not a choice the workflow's author made,
// so the message names the thing that chose the size.
func checkHTTPResponseElementBound(url string, parsedJSON *expr.Value) error {
	total := 0
	v := walkConstraintValue(parsedJSON, 0, &total)
	if v == nil {
		return nil
	}
	if v.Depth {
		return NewTaskError("http", ErrorKindLimitExceeded, fmt.Errorf(
			"the JSON response from %s nests %d levels deep, over the %d levels this server can walk "+
				"cheaply while evaluating `expect:` or `outputs:` over it; this is the shape the remote "+
				"endpoint returned, not anything the workflow wrote — narrow the request, or have a "+
				"later step read the body from a reference instead of walking the whole thing here",
			url, v.DepthReached, maxConstraintValueDepth))
	}
	return NewTaskError("http", ErrorKindLimitExceeded, fmt.Errorf(
		"the JSON response from %s carries at least %d list elements, over the %d an `expect:` or "+
			"`outputs:` expression can evaluate cheaply; this is the size of the remote endpoint's "+
			"response, not anything the workflow wrote — narrow the query, page the request, or ask "+
			"the endpoint to filter server-side before `expect:`/`outputs:` examines it",
		url, v.ElementCount, maxListElements))
}

// checkEnumConstraint refuses a value that is not one of a `type: enum`
// declaration's own `values`.
//
// Silently returns for a value [inputTypeOf] does not read as a string, or
// for a declaration that is not TYPE_ENUM — [CheckInputValue] already refused
// a type mismatch, and [CheckInputConstraintShape] already refuses `values:`
// declared on anything but an enum, per this file's own doc on where set-facts
// about *this* declaration belong.
//
// The refusal names the declaration's own choices verbatim, in the style
// established for a `case:` value against a `switch:` domain
// (`flowfile/validate_switch.go`), and offers the nearest spelling through
// [nearest.Name] — the one did-you-mean rule this repository keeps in one
// place rather than four.
func checkEnumConstraint(name string, decl *InputDeclaration, lit *expr.Value) error {
	return checkEnumMembership("input", name, decl.GetType(), decl.GetValues(), inputValueRendering, lit)
}

// checkEnumMembership is the membership rule itself, over a declared type and
// its choices rather than over a message that holds them.
//
// Written this way because an output declares the identical pair (see
// [OutputDeclaration.type]) and the rule about them is one rule: a run
// answering with a value outside its declared set has broken the same kind of
// promise a caller submitting one has. kind is the noun the sentence names the
// declaration by, "input" or "output".
//
// rendering says how the refusal may print the value, which is the one thing
// the two sides do not share — see [valueRendering].
//
// The did-you-mean clause goes with a withheld value rather than staying beside
// the marker. It is computed *from* the withheld string, so offering one
// narrows a reader's guess to the strings within [nearest.MaxDistance] of a
// declared choice — a smaller leak than the value, and a leak.
func checkEnumMembership(
	kind, name string,
	t InputDeclaration_Type,
	values []string,
	rendering valueRendering,
	lit *expr.Value,
) error {
	if t != InputDeclaration_TYPE_ENUM {
		return nil
	}
	s, ok := lit.GetKind().(*expr.Value_StringValue)
	if !ok {
		return nil
	}
	got := s.StringValue

	for _, choice := range values {
		if choice == got {
			return nil
		}
	}

	// Trimmed before it is quoted, not after, and everything below reads the
	// trimmed string rather than the original: [strconv.Quote] expands a
	// control byte to six characters, so quoting first would build the
	// oversized sentence the trim exists to prevent before shortening it.
	shown := rendering.show(got)

	message := fmt.Sprintf("%s %q is %s, which is not one of the values %s declares: %s",
		kind, name, redactedIfSensitive(rendering.sensitive, func() string { return strconv.Quote(shown) }),
		name, quotedStrings(values))
	if rendering.sensitive {
		return fmt.Errorf("%s", message)
	}

	// Distance is at least the difference between the two rune counts. Avoid
	// its O(len(got)*len(choice)) work when got is too long to be within the
	// repository-wide suggestion limit of even the longest declared choice.
	maxChoiceRunes := 0
	for _, choice := range values {
		maxChoiceRunes = max(maxChoiceRunes, utf8.RuneCountInString(choice))
	}
	// Over `shown`, so a trimmed side computes its suggestion from the bounded
	// string rather than the original. A value long enough to be trimmed is
	// already further from every declared choice than [nearest.MaxDistance]
	// allows, so nothing that would have earned a suggestion loses one; on the
	// untrimmed side `shown` is `got` and this is the guard it always was.
	if utf8.RuneCountInString(shown) <= maxChoiceRunes+nearest.MaxDistance {
		if suggestion, ok := nearest.Name(shown, values); ok {
			message += fmt.Sprintf("; did you mean %q?", suggestion)
		}
	}

	return fmt.Errorf("%s", message)
}

// valueRendering is how a refusal may print the value it is about.
//
// Both halves say the same thing from opposite ends: an *input* is a value the
// caller handed this process while the caller is still there to be told, and an
// *output* is a value the run computed and is refused into durable history. So
// the two differ in exactly two ways and are otherwise one rule
// ([checkEnumMembership]), which is why this travels as a value rather than as
// a second copy of the membership check.
type valueRendering struct {
	// sensitive withholds the value, and only the value: the declaration still
	// names itself, still says the value was not in the set, and still lists
	// the set, because all three are written in the file rather than computed
	// by the run. See [redactedIfSensitive].
	sensitive bool

	// bounded trims the value to what a sentence can carry. See
	// [outputValueRendering] for why only one side sets it.
	bounded bool
}

// show renders one value under this rendering's length rule.
func (r valueRendering) show(s string) string {
	if r.bounded {
		return truncateForError(s)
	}

	return s
}

// inputValueRendering prints a submitted value whole and in the clear.
//
// Neither flag, and both deliberately. `sensitive:` is not set here because an
// input's value is already in [SensitiveInputValues]' set and leaves the
// failure sentence through `cmd/flow`'s `redactFailureError` — the mechanism
// written for exactly this text — so withholding it again would be a second
// spelling of one redaction, and would cost the author of a *file* the word
// that tells them what they typed wrong, since `flow validate` reaches this
// against a literal `default:` with no run and no run failure in sight.
//
// `bounded` is not set because a submitted value is weighed by
// [CheckSubmissionSize] before it gets here and is the caller's own text to
// read back, which `TestBindRunInputsBoundsEnumSuggestionWork` pins
// deliberately: the quadratic suggestion scan is what that path bounds, not the
// sentence.
var inputValueRendering = valueRendering{}

// outputValueRendering prints a computed value withheld if the declaration says
// so, and trimmed always.
//
// Trimmed always because the size is not the workflow author's choice: an
// output's value is whatever a task answered with, up to [MaxTaskOutputBytes],
// and this sentence *is* the run's failure. Temporal has a blob limit, so an
// unbounded one is a failure the durable driver cannot persist while the local
// driver simply returns it — invariant 3 broken by a diagnostic, and invariant
// 5 unbounded at a seam another party controls.
func outputValueRendering(decl *OutputDeclaration) valueRendering {
	return valueRendering{sensitive: decl.GetSensitive(), bounded: true}
}

// redactedIfSensitive renders a value for a diagnostic that names it, or
// [SensitiveMarker] in its place when the declaration that produced the value
// is marked `sensitive:`.
//
// # Why an output needs this and an input does not
//
// A refusal from [EvalRunOutputs] *is* the run's failure text: it is returned
// before there is a [RunOutputs] for a renderer to redact, and it is what gets
// persisted as the run's answer. The redaction that clears a sensitive value
// out of that text — `cmd/flow`'s `redactFailureError`, over
// [SensitiveInputValues] — is built from the run's *arguments*, because those
// are the values the process holding the file also holds. A value the workload
// computed is in neither: nothing outside this package ever saw it, and by the
// time anything could, the sentence quoting it is already durable history
// (AGENTS.md invariant 7). So the withholding has to happen where the sentence
// is composed, which is here.
//
// render is a closure rather than a rendered string so that a withheld value
// costs nothing to format and, more to the point, is never converted to text
// that a later edit could pick up by accident.
func redactedIfSensitive(sensitive bool, render func() string) string {
	if sensitive {
		return SensitiveMarker
	}

	return render()
}

// quotedStrings renders a list of strings the way a diagnostic quotes a
// declaration's own choices, matching [flowfile]'s `quotedList`.
func quotedStrings(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = strconv.Quote(v)
	}
	return strings.Join(quoted, ", ")
}

// checkListConstraints applies min_items and max_items to a list literal.
func checkListConstraints(name string, decl *InputDeclaration, lit *expr.Value) error {
	if decl.MinItems == nil && decl.MaxItems == nil {
		return nil
	}
	list, ok := lit.GetKind().(*expr.Value_ListValue)
	if !ok {
		return nil
	}
	length := uint64(len(list.ListValue.GetValues()))

	if decl.MinItems != nil && length < decl.GetMinItems() {
		return fmt.Errorf("input %q must have at least %d item(s); got %d", name, decl.GetMinItems(), length)
	}
	if decl.MaxItems != nil && length > decl.GetMaxItems() {
		return fmt.Errorf("input %q must have at most %d item(s); got %d", name, decl.GetMaxItems(), length)
	}

	return nil
}
