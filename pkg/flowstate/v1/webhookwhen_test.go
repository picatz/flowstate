package flowstatev1_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The admission predicate: only a clean `true` admits, and everything else
// starts nothing. The two binding functions share one evaluation of it, so each
// claim is asserted through both the start and the bridge.

// withWhen is a start trigger carrying the predicate, over the stripe fixture.
func withWhen(expression string) *v1.WebhookTrigger {
	trigger := stripeTrigger()
	trigger.When = v1.NewExpr(expression)

	return trigger
}

// bridgeWithWhen is the bridge fixture carrying the predicate.
func bridgeWithWhen(expression string) (*v1.Workflow, *v1.WebhookTrigger) {
	wf := bridged(namesTheTrigger())
	trigger := wf.GetTriggers().GetWebhooks()[0]
	trigger.When = v1.NewExpr(expression)

	return wf, trigger
}

func bridgeDelivery(action string) v1.WebhookDelivery {
	return v1.WebhookDelivery{
		Body: map[string]any{
			"trigger_id": "evt-1",
			"kind":       action,
			"actions":    []any{map[string]any{"value": "order-4471", "action_id": "approve"}},
		},
		Verified: true,
	}
}

func TestAWhenThatHoldsAdmitsTheDelivery(t *testing.T) {
	t.Parallel()

	t.Run("a start", func(t *testing.T) {
		t.Parallel()

		inputs, key, err := v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(),
			withWhen(`event.body.id.startsWith("evt_")`), stripeDelivery(true))
		require.NoError(t, err)
		assert.Equal(t, "evt_3PqLd2X1", key)
		assert.Equal(t, "ord_H1x9", inputs["order_id"].GetLiteral().GetStringValue())
	})

	t.Run("a bridge", func(t *testing.T) {
		t.Parallel()

		wf, trigger := bridgeWithWhen(`event.body.kind == "approve"`)
		entity, _, key, err := v1.BindWebhookTriggerSignal(t.Context(), wf, trigger, bridgeDelivery("approve"))
		require.NoError(t, err)
		assert.Equal(t, "order-4471", entity)
		assert.Equal(t, "evt-1", key)
	})
}

// TestAWhenThatIsFalseDeclinesAndComputesNothingElse: the decline is the
// predicate working, so it is [v1.ErrWebhookDeclined] and not a failure, and it
// is decided before the key, the arguments or `correlate:` run — the trigger
// here would fail all three on this delivery, and the answer is still a decline.
func TestAWhenThatIsFalseDeclinesAndComputesNothingElse(t *testing.T) {
	t.Parallel()

	t.Run("a start", func(t *testing.T) {
		t.Parallel()

		trigger := withWhen(`event.body.id == "something-else"`)
		trigger.IdempotencyKey = v1.NewExpr(`event.body.nosuchfield.deeper`)
		trigger.Arguments["order_id"] = v1.NewExpr(`event.body.nosuchfield.deeper`)

		inputs, key, err := v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(), trigger, stripeDelivery(true))
		require.ErrorIs(t, err, v1.ErrWebhookDeclined)
		assert.NotErrorIs(t, err, v1.ErrWebhookWhenFailed)
		assert.Nil(t, inputs)
		assert.Empty(t, key, "a declined delivery computed a key")
	})

	t.Run("a bridge", func(t *testing.T) {
		t.Parallel()

		wf, trigger := bridgeWithWhen(`event.body.kind == "reject"`)
		trigger.GetSignal().Correlate = v1.NewExpr(`event.body.nosuchfield.deeper`)

		entity, payload, key, err := v1.BindWebhookTriggerSignal(t.Context(), wf, trigger, bridgeDelivery("approve"))
		require.ErrorIs(t, err, v1.ErrWebhookDeclined)
		assert.Empty(t, entity, "a declined delivery addressed a run")
		assert.Nil(t, payload)
		assert.Empty(t, key)
	})
}

// TestAWhenThatCannotBeAnsweredFailsClosed: an error, a non-bool and a field the
// delivery does not carry each refuse the delivery with [v1.ErrWebhookWhenFailed]
// — never a decline (which says the author's rule worked) and never an
// admission. The sentence is fixed: the evaluator's own error can quote a value
// out of the body, and this is the text a sender is sent.
func TestAWhenThatCannotBeAnsweredFailsClosed(t *testing.T) {
	t.Parallel()

	for name, expression := range map[string]string{
		"an evaluation error":                `event.body.data.object.amount / 0 == 1`,
		"a field the delivery does not have": `event.body.nosuchfield == "secret-looking-value"`,
		"a missing key under a present one":  `event.body.data.object.metadata.absent == "x"`,
		"a string":                           `event.body.id`,
		"an int":                             `event.body.data.object.amount`,
		"a null":                             `event.body.id == "" ? null : null`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, _, err := v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(),
				withWhen(expression), stripeDelivery(true))
			require.ErrorIs(t, err, v1.ErrWebhookWhenFailed)
			assert.NotErrorIs(t, err, v1.ErrWebhookDeclined)

			var failed *v1.WebhookWhenError
			require.ErrorAs(t, err, &failed)
			assert.Equal(t, "stripe", failed.Webhook)
			require.Error(t, failed.Cause(), "the operator's log has no cause")
			assert.NotContains(t, err.Error(), "secret-looking-value")
			assert.NotContains(t, err.Error(), "evt_3PqLd2X1", "the refusal echoes the delivery")
			assert.NotContains(t, err.Error(), failed.Cause().Error(), "the refusal echoes the evaluator's error")

			wf, trigger := bridgeWithWhen(expression)
			_, _, _, err = v1.BindWebhookTriggerSignal(t.Context(), wf, trigger, bridgeDelivery("approve"))
			require.ErrorIs(t, err, v1.ErrWebhookWhenFailed)
		})
	}
}

// TestAWhenIsBoundedByTheSameLimitsAsItsSiblings: an expression that spends the
// cost budget, and one run past a deadline, fail closed rather than hang or
// admit.
func TestAWhenIsBoundedByTheSameLimitsAsItsSiblings(t *testing.T) {
	t.Parallel()

	const blowup = `size([1,2,3,4,5,6,7,8,9,10].map(a, [1,2,3,4,5,6,7,8,9,10].map(b, ` +
		`[1,2,3,4,5,6,7,8,9,10].map(c, [1,2,3,4,5,6,7,8,9,10].map(d, ` +
		`[1,2,3,4,5,6,7,8,9,10].map(e, [1,2,3,4,5,6,7,8,9,10].map(f, a+b+c+d+e+f)))))))` +
		` > 0 && event.body.id != ""`

	t.Run("the cost budget", func(t *testing.T) {
		t.Parallel()

		_, _, err := v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(), withWhen(blowup), stripeDelivery(true))
		require.ErrorIs(t, err, v1.ErrWebhookWhenFailed)

		var failed *v1.WebhookWhenError
		require.ErrorAs(t, err, &failed)
		assert.Contains(t, failed.Cause().Error(), "cost limit exceeded")
	})

	t.Run("a deadline", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()

		// Enough steps to reach the evaluator's periodic interrupt check, and
		// few enough to stay well inside the cost budget: it is the deadline
		// that stops this one.
		const slow = `size([1,2,3,4,5,6,7,8,9,10].map(a, [1,2,3,4,5,6,7,8,9,10].map(b, ` +
			`[1,2,3,4,5,6,7,8,9,10].map(c, [1,2,3,4,5,6,7,8,9,10].map(d, a+b+c+d))))) > 0`

		_, _, err := v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(), withWhen(slow+` && event.body.id != ""`), stripeDelivery(true))
		require.NoError(t, err, "the control: this expression admits when nothing interrupts it")

		_, _, err = v1.BindWebhookTriggerInputs(ctx, orderWorkflow(), withWhen(slow+` && event.body.id != ""`), stripeDelivery(true))
		require.ErrorIs(t, err, v1.ErrWebhookWhenFailed, "an expired context admitted a delivery")
	})
}

// TestAnAbsentFieldIsAnswerableWithTheOptionalSpelling: the `.?`/`has` form is
// how an author says "no such field means no", and it declines instead of
// failing.
func TestAnAbsentFieldIsAnswerableWithTheOptionalSpelling(t *testing.T) {
	t.Parallel()

	_, _, err := v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(),
		withWhen(`event.body.?action.orValue("") == "opened"`), stripeDelivery(true))
	require.ErrorIs(t, err, v1.ErrWebhookDeclined)

	_, _, err = v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(),
		withWhen(`has(event.body.action) && event.body.action == "opened"`), stripeDelivery(true))
	require.ErrorIs(t, err, v1.ErrWebhookDeclined)
}

// TestAWhenIsOnlyEvaluatedAfterVerification: an unverified delivery is refused
// as unverified, with the one sentence, before the predicate can say anything
// about it.
func TestAWhenIsOnlyEvaluatedAfterVerification(t *testing.T) {
	t.Parallel()

	_, _, err := v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(),
		withWhen(`event.body.id == "nope"`), stripeDelivery(false))
	require.Error(t, err)
	assert.NotErrorIs(t, err, v1.ErrWebhookDeclined)
	assert.Contains(t, err.Error(), "did not verify")
}

func TestCheckWebhookWhen(t *testing.T) {
	t.Parallel()

	assert.NoError(t, v1.CheckWebhookWhen("w", nil), "no predicate admits everything, as before")
	assert.NoError(t, v1.CheckWebhookWhen("w", v1.NewExpr(`event.body.action == "opened"`)))

	for name, when := range map[string]*v1.Value{
		"a constant expression": v1.NewExpr(`true`),
		"a literal":             v1.NewLiteral("opened"),
		"a shadowed event":      v1.NewExpr(`[1].exists(event, event == 1)`),
	} {
		err := v1.CheckWebhookWhen("w", when)
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "does not depend on the delivery", name)
	}

	trigger := stripeTrigger()
	trigger.When = v1.NewExpr(`true`)
	assert.Error(t, v1.CheckWebhookTrigger(trigger), "the trigger's own check misses a broken `when:`")
}
