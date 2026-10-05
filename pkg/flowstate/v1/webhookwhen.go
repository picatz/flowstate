package flowstatev1

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/cel-go/common/types/ref"
)

// The admission predicate: a webhook's `when:`.
//
// A trigger could verify a delivery, name it and shape it, and could not say no
// to it. `when:` is that, and the whole of its semantics is one sentence: only a
// clean `true` admits. Everything else — `false`, an evaluation error, a result
// that is not a bool, an exceeded cost or time bound — keeps the delivery from
// starting a run or answering a gate, and the two classes of "everything else"
// are told apart because they mean different things to the party reading them:
//
//   - [ErrWebhookDeclined] is the predicate working. The workflow was asked a
//     question about a verified delivery and answered no. The receiver answers
//     `204` so the provider does not retry, records one bounded decision, and
//     starts nothing.
//   - [ErrWebhookWhenFailed] is the predicate broken, or the delivery not
//     carrying what it reads. Fail closed — there is no "accept on error" — and
//     answered as a refusal rather than as a decline, so an author whose
//     `event.body.action` is absent from some event type sees a non-2xx and a
//     different audit code instead of a silent filter that hides the mistake.
//
// It is evaluated by the one function both binding paths call
// ([BindWebhookTriggerInputs], [BindWebhookTriggerSignal]) in the one scope they
// share, after verification and before `idempotency_key:`, so a declined
// delivery computes no key, addresses no run and binds no input (invariant 2).
// The same evaluator and the same cost and deadline limits as its siblings bound
// it (invariant 5); `flow test` replays it through those two functions, so a
// rehearsal declines exactly what the receiver declines (invariant 3).

// ErrWebhookDeclined is what a binding returns when a trigger's `when:` answered
// false: the delivery is well-formed and was verified, and the workflow does not
// want it. It is a decision, not a failure; match it with [errors.Is].
var ErrWebhookDeclined = errors.New("declined by `when:`")

// ErrWebhookWhenFailed is what a binding returns when a trigger's `when:` could
// not be answered with a bool: it errored, exceeded its bound, or produced
// something other than a bool. Match it with [errors.Is]; [WebhookWhenError]
// carries the cause for an operator's log.
var ErrWebhookWhenFailed = errors.New("`when:` could not be answered")

// WebhookWhenError is a `when:` that could not be answered.
//
// Its message is a fixed sentence naming the trigger and nothing about the
// delivery: an evaluator's own error can quote a value out of the body, and this
// text is what the receiver sends back to the sender, so the cause is kept apart
// in [WebhookWhenError.Cause] for the operator rather than echoed.
type WebhookWhenError struct {
	// Webhook is the trigger's name, which is the file's own word.
	Webhook string

	cause error
}

func (e *WebhookWhenError) Error() string {
	return fmt.Sprintf("webhook %q: `when:` could not be answered over this delivery, so it is refused; a "+
		"predicate that cannot be evaluated never admits", e.Webhook)
}

// Is makes the error match [ErrWebhookWhenFailed].
func (e *WebhookWhenError) Is(target error) bool { return target == ErrWebhookWhenFailed }

// Cause is why the predicate could not be answered, for a log line and never for
// a response: it may quote the delivery.
func (e *WebhookWhenError) Cause() error { return e.cause }

// CheckWebhookWhen reports a `when:` that is written and cannot work.
//
// Absent is valid: no predicate admits every verified delivery, which is what a
// trigger has always done. Present, it must be an expression that names the
// delivery, for [CheckWebhookIdempotencyKey]'s reason arrived at from the other
// side: a predicate with no free `event` cannot vary with the delivery, so it
// admits all of them or none, and either is better said where it is written.
// Secrets are refused where the file compiles, and the evaluator binds no
// `secret(...)` at all, so there is nothing for a predicate to resolve here.
func CheckWebhookWhen(name string, when *Value) error {
	if when == nil {
		return nil
	}

	expression, computed := when.GetKind().(*Value_Expr)
	if !computed || !celExprReferencesIdentifier(expression.Expr.GetExpr(), EventRoot) {
		return fmt.Errorf("webhook %q writes a `when:` that does not depend on the delivery, so it would "+
			"admit every delivery or none; write a boolean expression over `%s`, such as "+
			"`${%s.%s.action == \"opened\"}`", name, EventRoot, EventRoot, EventBodyField)
	}

	return nil
}

// admitWebhookDelivery evaluates the trigger's `when:` in the scope the other
// trigger expressions share, and returns nil only for a clean `true`.
func admitWebhookDelivery(ctx context.Context, evaluator *Evaluator, profile string, trigger *WebhookTrigger, activation any) error {
	when := trigger.GetWhen()
	if when == nil {
		return nil
	}

	out, err := evaluator.EvalParsedBase(ctx, profile, when.GetExpr(), activation)
	if err != nil {
		return &WebhookWhenError{Webhook: trigger.GetName(), cause: err}
	}

	admitted, err := webhookWhenVerdict(out)
	if err != nil {
		return &WebhookWhenError{Webhook: trigger.GetName(), cause: err}
	}
	if !admitted {
		return fmt.Errorf("webhook %q: %w", trigger.GetName(), ErrWebhookDeclined)
	}

	return nil
}

// webhookWhenVerdict reads a predicate's result as a bool and nothing else: a
// truthy string, a non-zero int or a null is not an answer.
func webhookWhenVerdict(out ref.Val) (bool, error) {
	verdict, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("`when:` evaluated to %s rather than to a bool", out.Type())
	}

	return verdict, nil
}
