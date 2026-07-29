package flowfile

import (
	"fmt"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// A secret reference is recognized when the workflow is compiled, not called when
// it runs.
//
// It looks like a function — ${secret('vault:prod/api#token')} — because that is
// what an author expects to write, but there is no such function and there must not
// be one. A function returns a value to whatever called it, and what called it here
// is workflow code, whose every result is written to durable, widely readable
// history. Compiling the call into a [flowstatev1.SecretRef] instead keeps the
// reference a reference for the whole path: the specification carries it, the
// control plane transports it, workflow-side evaluation refuses to read it, and only
// the worker executing the step resolves it.
//
// Recognizing it at compile time is also the only way to report a mistake. CEL's
// parser accepts a call to a function it has never heard of, so before this existed
// `flow validate` said "ok" to a misspelled reference and the run failed later with
// "no such overload" — a validator that passes and then breaks is worse than none.

// SecretMarker is what a Flowfile writes to reference a secret. It is compiled into
// the reference itself, so nothing evaluates a call by this name.
//
// It is exported so that editor completion and hover offer the same spelling the
// compiler recognizes.
const SecretMarker = "secret"

// secretPlacement says whether a value is somewhere a secret reference may appear,
// and when it is not, which of the two reasons applies.
type secretPlacement int

const (
	// secretAllowed is the whole value of a task input: the one place a reference
	// can go, because it reaches the task untouched.
	secretAllowed secretPlacement = iota

	// secretNotWholeValue is inside a larger expression, where the reference would
	// have to be combined with something.
	secretNotWholeValue

	// secretInStructure is nested in a list or a mapping.
	secretInStructure

	// secretNotEvaluable is a field the workflow evaluates itself: a step's
	// condition, or a loop's items.
	secretNotEvaluable
)

// Why a reference can be out of place. Each says what is actually true of the
// position it is in, because the three reasons are genuinely different and an
// author sent hunting for the wrong mistake is worse off than one told nothing.
const (
	notWholeValueHelp = "a secret reference has to be the whole value of a task input; " +
		"it names a value that does not exist until the worker running the step resolves it, " +
		"so nothing workflow-side can combine it with anything else"

	// This one is a limitation rather than a rule, and says so: the schema has no
	// way to carry a reference inside a structure, since a value is either a
	// reference or a structure and never a structure holding one.
	//
	// It looks like the check is merely in the wrong place, and it is not. Values
	// exist per task input, because Task.inputs is map<string, Value> — a mapping
	// containing any expression compiles to one Value_Expr holding a CEL
	// map-construction expression, and its entries are nodes inside that one AST.
	// An Authorization entry therefore never occupies a Value of its own, so there
	// is nowhere further down to anchor the check, and Value's oneof has no nested
	// kind that could hold a reference there even if something were willing to
	// resolve it.
	//
	// Lifting the limitation means one of two things, not a smaller check: a task
	// input that accepts a SecretRef directly, which is cheap and unblocks the
	// header case; or a Value kind that nests Values, which is general and forces
	// Task.HTTP.Inputs.headers to stop being map<string, string>.
	inStructureHelp = "a secret reference cannot be nested inside a list or a mapping: " +
		"a structure holding an expression compiles into a single expression, which the workflow " +
		"evaluates, and evaluating a secret is what would put it in history. " +
		"This is a current limitation rather than a mistake in the file — a value can be a reference " +
		"or a structure, never a structure containing one — so an authorization header needs a task " +
		"input that accepts the reference itself"

	notEvaluableHelp = "a secret reference cannot go where the workflow evaluates the value itself; " +
		"resolving it here would put the secret in workflow history, which is durable and " +
		"broadly readable, so pass it to a task input instead"

	malformedCallHelp = "secret() takes one reference, written out, like ${secret('env:API_KEY')}; " +
		"a computed reference cannot be checked when the workflow is compiled"
)

// secret compiles a ${secret(...)} marker into a reference, or reports why it
// cannot be one here.
//
// The third return reports whether the expression held a marker at all, so that an
// ordinary expression carries on being compiled as one.
func (c *compiler) secret(parsed *expr.ParsedExpr, src string, span Span, r ref, placement secretPlacement) (*v1.Value, bool) {
	call, found := findSecretCall(parsed.GetExpr())
	if !found {
		return nil, false
	}

	at := markerSpan(parsed, call.GetId(), src, span)

	if help := misplacedHelp(placement, call == parsed.GetExpr()); help != "" {
		c.report(at, r, "%s", help)
		return nil, true
	}

	text, ok := secretArgument(call)
	if !ok {
		c.report(at, r, "%s", malformedCallHelp)
		return nil, true
	}

	// The reference is validated by the package that resolves them, so that a
	// Flowfile cannot compile a reference a worker would later refuse. That
	// includes what the schema's pattern cannot say: a name holding a control
	// character is rejected, because a reference reaching a log must not be able to
	// forge lines in it.
	reference, err := secrets.ParseRef(text)
	if err != nil {
		c.report(at, r, "%s", err)
		return nil, true
	}

	return &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
		Scheme: reference.GetScheme(),
		Name:   reference.GetName(),
	}}}, true
}

// misplacedHelp explains why a reference cannot be where it is, or returns empty
// when it can.
//
// isWholeExpression distinguishes a reference written on its own from one buried in
// a larger expression, which is the difference between a task input that is a
// reference and one that computes with a value it must never see.
func misplacedHelp(placement secretPlacement, isWholeExpression bool) string {
	switch {
	case placement == secretNotEvaluable:
		return notEvaluableHelp
	case placement == secretInStructure:
		return inStructureHelp
	case placement == secretNotWholeValue, !isWholeExpression:
		return notWholeValueHelp
	default:
		return ""
	}
}

// findSecretCall returns the outermost call to the marker anywhere in an
// expression.
//
// The search is not limited to the root because a reference nested in a larger
// expression has to be reported rather than missed: ${'Bearer ' + secret('env:T')}
// is a mistake with a specific fix, and the alternative is compiling it into an
// expression that fails at run time.
//
// Only a global call counts. `secret` as a step id (${steps.secret.result}), a bare name
// (${secret}), or a method on something (${x.secret('a')}) is not the marker, since
// none of those is the thing a Flowfile writes to name a secret.
func findSecretCall(e *expr.Expr) (*expr.Expr, bool) {
	if e == nil {
		return nil, false
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_CallExpr:
		if kind.CallExpr.GetTarget() == nil && kind.CallExpr.GetFunction() == SecretMarker {
			return e, true
		}
		if found, ok := findSecretCall(kind.CallExpr.GetTarget()); ok {
			return found, true
		}
		for _, arg := range kind.CallExpr.GetArgs() {
			if found, ok := findSecretCall(arg); ok {
				return found, true
			}
		}
	case *expr.Expr_SelectExpr:
		return findSecretCall(kind.SelectExpr.GetOperand())
	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			if found, ok := findSecretCall(element); ok {
				return found, true
			}
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			if found, ok := findSecretCall(entry.GetMapKey()); ok {
				return found, true
			}
			if found, ok := findSecretCall(entry.GetValue()); ok {
				return found, true
			}
		}
	case *expr.Expr_ComprehensionExpr:
		comprehension := kind.ComprehensionExpr
		for _, part := range []*expr.Expr{
			comprehension.GetIterRange(),
			comprehension.GetAccuInit(),
			comprehension.GetLoopCondition(),
			comprehension.GetLoopStep(),
			comprehension.GetResult(),
		} {
			if found, ok := findSecretCall(part); ok {
				return found, true
			}
		}
	}
	return nil, false
}

// secretArgument returns the reference a marker names, when it names exactly one
// and names it literally.
func secretArgument(call *expr.Expr) (string, bool) {
	args := call.GetCallExpr().GetArgs()
	if len(args) != 1 {
		return "", false
	}

	constant, ok := args[0].GetExprKind().(*expr.Expr_ConstExpr)
	if !ok {
		return "", false
	}
	text, ok := constant.ConstExpr.GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return "", false
	}
	return text.StringValue, true
}

// markerSpan returns the span of the marker call within the expression source, so
// that a diagnostic about a reference buried in a longer expression lands on the
// reference.
//
// Two adjustments make it land where a reader looks. The parser records a call at
// its opening parenthesis, so the span moves back onto the name when the source
// confirms the name is there — and simply stays on the parenthesis when it does not,
// rather than trusting a convention it cannot see. And positions are offsets into
// the expression source, which is one line in any Flowfile not using a block scalar;
// a multi-line expression falls back to the whole span rather than guessing.
func markerSpan(parsed *expr.ParsedExpr, id int64, src string, span Span) Span {
	offset, ok := parsed.GetSourceInfo().GetPositions()[id]
	if !ok || !span.IsValid() || offset < 0 || strings.ContainsRune(src, '\n') {
		return span
	}

	runes := []rune(src)
	at := int(offset)
	if at > len(runes) {
		return span
	}
	if name := len([]rune(SecretMarker)); at >= name && string(runes[at-name:at]) == SecretMarker {
		at -= name
	}

	start := span.Start
	start.Column += at
	return Span{Start: start, End: span.End}
}

// secretRefToDSL renders a reference back into the form a Flowfile writes.
func secretRefToDSL(reference *v1.SecretRef) (string, error) {
	if err := secrets.ValidateRef(reference); err != nil {
		return "", fmt.Errorf("cannot be written: %w", err)
	}
	return fmt.Sprintf("%s%s(%s)%s",
		fenceOpen, SecretMarker, quoteCELString(secrets.RefString(reference)), fenceClose), nil
}
