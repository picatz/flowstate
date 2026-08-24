package flowstatev1

import (
	"context"
	"fmt"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// A switch's observable behaviour is decided in one place, called by both
// drivers: which body runs, and what the step's outputs record. See [Switch]
// for the semantics; what lives here is the one spelling of them.

// SwitchValueOutput is the output recording the discriminant a switch observed:
// `${steps.<id>.value}`.
//
// The same word the `value:` step and the workflow's `outputs:` block already
// use for "the value this name holds", so it means one thing in every position.
// A constant beside [TimedOutOutput] for the identical containment reason: both
// drivers write it and the validator and language server read it, and a case
// body does not get to forge the record — these names are the engine's, written
// after the fact from what actually happened.
const SwitchValueOutput = "value"

// SwitchCaseOutput is the output recording which case took the observed value:
// `${steps.<id>.case}`.
//
// It holds the case literal that matched, and null when none did — whether the
// `default:` body ran or nothing did. Null rather than a sentinel string,
// because any string is a value a case could legitimately match, which is the
// exact ambiguity [ValidateNamespace]-style sentinels exist to avoid. A
// downstream step dispatches on it as `${steps.<id>.case != null}`.
const SwitchCaseOutput = "case"

// SwitchBodies returns every body a switch holds — each case's steps in written
// order, then the default's when one exists.
//
// It is the walker's spelling of "descend every branch": every walk over the
// node tree that has to visit a switch's nested steps goes through this, so a
// walk cannot descend the cases and quietly miss the default. An empty body
// contributes an empty list rather than being skipped, because to a walk an
// empty body is a body.
func SwitchBodies(sw *Switch) [][]*Node {
	if sw == nil {
		return nil
	}
	out := make([][]*Node, 0, len(sw.GetCases())+1)
	for _, c := range sw.GetCases() {
		out = append(out, c.GetSteps())
	}
	if def := sw.GetDefault(); def != nil {
		out = append(out, def.GetSteps())
	}
	return out
}

// SelectSwitchCase evaluates a switch's discriminant once and decides what runs.
//
// It returns the body to run — the matching case's steps, the default's steps,
// or nil when nothing matched and no default exists — and the outputs the step
// records either way: the observed value under [SwitchValueOutput] and the
// matching case literal (null when none matched) under [SwitchCaseOutput].
//
// Both drivers call this and neither has its own copy, for the reason
// [EvalValueNode] is shared: which branch a dispatch takes is the most
// observable thing about it, and two spellings would be two answers waiting to
// differ.
//
// An unresolvable discriminant is an error, never "unmatched". Fail closed:
// `default:` means "a value arrived that I didn't enumerate", never "I couldn't
// compute the value" — an evaluation failure flowing into the default path
// would make `default:` swallow bugs, the opposite of what the slot is for.
func SelectSwitchCase(ctx context.Context, sw *Switch, scope *Scope) ([]*Node, *Node_Outputs, error) {
	if sw == nil {
		return nil, nil, fmt.Errorf("a `switch:` step must hold a value and cases, and this one holds nothing")
	}

	observed, err := evalSwitchValue(ctx, sw.GetValue(), scope)
	if err != nil {
		return nil, nil, err
	}

	record := func(matched *expr.Value) *Node_Outputs {
		took := &Value{Kind: &Value_Literal{Literal: &expr.Value{Kind: &expr.Value_NullValue{}}}}
		if matched != nil {
			took = &Value{Kind: &Value_Literal{Literal: matched}}
		}
		return &Node_Outputs{NamedValues: map[string]*Value{
			SwitchValueOutput: {Kind: &Value_Literal{Literal: observed}},
			SwitchCaseOutput:  took,
		}}
	}

	for i, c := range sw.GetCases() {
		for j, candidate := range c.GetValues() {
			literal, ok := candidate.GetKind().(*Value_Literal)
			if !ok {
				// `flow validate` refuses a computed case with a position; this
				// is the fail-closed backstop for a specification that never
				// passed through a parser. Erroring rather than skipping,
				// because a case that silently never matches is the exact
				// silent-nothing failure the construct exists to prevent.
				return nil, nil, fmt.Errorf(
					"switch case %d value %d is not a literal; cases are literals, and a computed comparison is what `if:` is for",
					i+1, j+1)
			}
			if SwitchLiteralsEqual(observed, literal.Literal) {
				return c.GetSteps(), record(literal.Literal), nil
			}
		}
	}

	if def := sw.GetDefault(); def != nil {
		return def.GetSteps(), record(nil), nil
	}

	// No case matched and no default exists: nothing runs, and the record says
	// so — an observable, greppable account rather than a failure or silence.
	return nil, record(nil), nil
}

// SwitchBodyError is the failure a switch raises when the body it selected
// fails, carrying the selection that had already been made so the switch's own
// transcript entry still says which arm ran.
//
// It is [LoopExhaustedError]'s shape applied to the other container whose failure
// used to erase its own account. A switch records the observed value and the
// matching literal ([SwitchValueOutput], [SwitchCaseOutput]) *after* its body
// returns, so a body step that failed left the step's entry holding the failure
// text alone — the branch that ran was no longer anywhere in the record. That
// cost `flow test` a true statement: a case whose whole point is
// `expect.failed: true` on an error arm did exercise that arm, handed back a
// [PartialTranscript] with no `case` in it, and had the arm reported unreached,
// so `--coverage-required` rejected a suite that covered it (issue #801, and
// #453 for the transcript half of the same argument).
//
// Raised by both drivers at the point each detects the body failure, and consumed
// by each driver's own step-failure recording site — the local driver's
// failureRecord and the durable driver's failedAt — which read it *directly*,
// never through an unwrap chain, for the containment reason [LoopExhaustedError]
// spells out: the selection belongs to the switch's own entry and to no other, so
// a failure propagating out through an enclosing call or for_each records as the
// plain failure it is at that level.
//
// Unwrap is present, and it is what keeps the wrapping invisible everywhere else:
// cancellation is still recognised through it, [StepErrorText] still finds the
// task failure inside, and the durable driver's message extraction still reads
// the nested run failure one level down. The text a reader gets is the body
// failure's own, unchanged.
type SwitchBodyError struct {
	// Err is the body's failure, exactly as it was raised.
	Err error

	// Selection is what [SelectSwitchCase] decided before the body ran: the
	// observed value and the case literal that matched.
	Selection *Node_Outputs
}

// Error is the body failure's own sentence, verbatim: the selection travels
// beside the failure, never inside the text an author's tooling matches on.
func (e *SwitchBodyError) Error() string { return e.Err.Error() }

// Unwrap exposes the body failure, so every errors.Is and errors.As question
// asked about it — cancellation, a [TaskError], a nested run failure — gets the
// answer it would have got had the switch not wrapped it.
func (e *SwitchBodyError) Unwrap() error { return e.Err }

// Record shapes the failure into the outputs recorded under the switch's own
// step id: the failure text under [StepErrorOutput], plus the selection the
// switch had already made, so a failed switch's entry names its arm exactly as a
// completed one's does.
//
// text is the failure sentence the calling driver has already rendered, and
// rendering it here instead is precisely how the two drivers would disagree: the
// durable driver's body failure arrives wrapped in this engine's own words
// (`engine: flowstate run failed: …`), which only its own extraction sheds. See
// [StepFailureRecord].
func (e *SwitchBodyError) Record(text string) *Node_Outputs {
	out := FailedStepOutputs(text)
	for name, value := range e.Selection.GetNamedValues() {
		out.NamedValues[name] = value
	}

	return out
}

// evalSwitchValue evaluates the discriminant to the literal that goes on the
// record. A literal passes through, exactly as a `value:` step's does.
func evalSwitchValue(ctx context.Context, value *Value, scope *Scope) (*expr.Value, error) {
	if value == nil {
		return nil, fmt.Errorf("a `switch:` needs `value:`, the expression it dispatches on")
	}

	if literal, ok := value.GetKind().(*Value_Literal); ok {
		return literal.Literal, nil
	}
	if _, isExpr := value.GetKind().(*Value_Expr); !isExpr {
		return nil, fmt.Errorf("a `switch:` dispatches on an expression or a literal, not a %T", value.GetKind())
	}

	out, err := DefaultEvaluator().EvalParsedBase(ctx, scope.GetProfile(), value.GetExpr(), scope.Activation(ctx))
	if err != nil {
		return nil, fmt.Errorf("evaluating switch value: %w", err)
	}
	literal, err := cel.RefValueToValue(out)
	if err != nil {
		return nil, fmt.Errorf("evaluating switch value: converting result: %w", err)
	}
	return literal, nil
}

// SwitchLiteralsEqual is the one spelling of case matching: CEL's equality for
// scalars. Numbers compare numerically across int, uint and double — `case: 1`
// matches a discriminant of `1.0`, because that is what `x == 1` in an `if:`
// would say and a switch must not disagree with the construct it replaces.
// Strings, booleans, bytes and null compare as themselves; values of
// incomparable kinds do not match, they simply take no case.
//
// The duplicate-case validator reads this too, so "these two literals are one
// case" and "this value takes that case" cannot drift apart.
func SwitchLiteralsEqual(a, b *expr.Value) bool {
	if an, aok := numericRefValue(a); aok {
		bn, bok := numericRefValue(b)
		return bok && an.Equal(bn) == types.True
	}

	switch ak := a.GetKind().(type) {
	case nil, *expr.Value_NullValue:
		_, null := b.GetKind().(*expr.Value_NullValue)
		return null || b.GetKind() == nil
	case *expr.Value_StringValue:
		bk, ok := b.GetKind().(*expr.Value_StringValue)
		return ok && ak.StringValue == bk.StringValue
	case *expr.Value_BoolValue:
		bk, ok := b.GetKind().(*expr.Value_BoolValue)
		return ok && ak.BoolValue == bk.BoolValue
	case *expr.Value_BytesValue:
		bk, ok := b.GetKind().(*expr.Value_BytesValue)
		return ok && string(ak.BytesValue) == string(bk.BytesValue)
	default:
		// Lists and maps are not scalars a case may hold — the validator
		// refuses them — and a discriminant of one matches nothing rather than
		// inventing a structural equality the doc above does not promise.
		return false
	}
}

// numericRefValue reports a literal's cel-go runtime value when it holds a
// number, so numeric matching is cel-go's own [ref.Val] Equal rather than a
// spelling of it here.
//
// This used to convert both sides to float64, which conflates every pair of
// integers a float64 cannot tell apart: 9007199254740992 and 9007199254740993
// compared equal, though `x == 9007199254740993` in the `if:` a switch replaces
// distinguishes them. cel-go compares same-typed integers exactly, int against
// uint by value with sign and range checks, and an integer against a double by
// CEL's numeric equality (common/types/compare.go) — delegating means a switch
// answers precisely what the expression language answers, including at the
// margins nobody thought to write down.
func numericRefValue(v *expr.Value) (ref.Val, bool) {
	switch kind := v.GetKind().(type) {
	case *expr.Value_Int64Value:
		return types.Int(kind.Int64Value), true
	case *expr.Value_Uint64Value:
		return types.Uint(kind.Uint64Value), true
	case *expr.Value_DoubleValue:
		return types.Double(kind.DoubleValue), true
	default:
		return nil, false
	}
}
