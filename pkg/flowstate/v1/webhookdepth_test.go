package flowstatev1_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The binders are what bound a delivery's depth (#1770), and they are pure
// functions of a stored delivery — which is what lets `flow test` replay one
// offline and why the bound lives here rather than in the receiver. These pin
// both binders at the boundary in both directions, so the start path and the
// signal bridge cannot come to disagree about how deep a body may be.

// nestedBody nests a decoded JSON object levels deep around one leaf.
func nestedBody(levels int) any {
	var body any = "leaf"
	for range levels {
		body = map[string]any{"k": body}
	}

	return body
}

// withDoc adds a `doc` beside the fields a trigger reads, nested so that the
// body as a whole is levels deep: the body's own object is the first level.
func withDoc(body map[string]any, levels int) map[string]any {
	body["doc"] = nestedBody(levels - 1)

	return body
}

func bodyDepthRefusal(webhook string, levels int) string {
	return fmt.Sprintf("the body delivered to webhook %q nests %d levels deep, over the %d levels this "+
		"server can walk cheaply while evaluating an expression over it", webhook, levels, v1.MaxStructureDepth)
}

func TestADeliveryBodyIsBoundedInDepthBeforeWithIsEvaluated(t *testing.T) {
	t.Parallel()

	t.Run("at the bound binds", func(t *testing.T) {
		t.Parallel()

		delivery := stripeDelivery(true)
		delivery.Body = withDoc(delivery.Body.(map[string]any), v1.MaxStructureDepth)

		inputs, _, err := v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(), stripeTrigger(), delivery)
		require.NoError(t, err)
		require.Equal(t, "ord_H1x9", inputs["order_id"].GetLiteral().GetStringValue())
	})

	t.Run("one past the bound is refused in the submit door's words", func(t *testing.T) {
		t.Parallel()

		delivery := stripeDelivery(true)
		delivery.Body = withDoc(delivery.Body.(map[string]any), v1.MaxStructureDepth+1)

		_, _, err := v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(), stripeTrigger(), delivery)
		require.Error(t, err)
		require.Contains(t, err.Error(), bodyDepthRefusal("stripe", v1.MaxStructureDepth+1))
	})

	t.Run("an unverified delivery is still refused before its depth is measured", func(t *testing.T) {
		t.Parallel()

		// The order is the fail-closed one: verification first, so a prober
		// learns nothing about the bound from an unsigned body.
		delivery := stripeDelivery(false)
		delivery.Body = withDoc(delivery.Body.(map[string]any), v1.MaxStructureDepth+1)

		_, _, err := v1.BindWebhookTriggerInputs(t.Context(), orderWorkflow(), stripeTrigger(), delivery)
		require.Error(t, err)
		require.Contains(t, err.Error(), "did not verify")
		require.NotContains(t, err.Error(), "levels deep")
	})
}

func TestABridgedDeliveryBodyIsBoundedInDepth(t *testing.T) {
	t.Parallel()

	wf := bridged(namesTheTrigger())
	trigger := wf.GetTriggers().GetWebhooks()[0]

	delivery := func(levels int) v1.WebhookDelivery {
		return v1.WebhookDelivery{
			Body: withDoc(map[string]any{
				"trigger_id": "evt-1",
				"actions":    []any{map[string]any{"value": "order-4471", "action_id": "approve"}},
			}, levels),
			Verified: true,
		}
	}

	t.Run("at the bound binds", func(t *testing.T) {
		t.Parallel()

		key, payload, _, err := v1.BindWebhookTriggerSignal(t.Context(), wf, trigger, delivery(v1.MaxStructureDepth))
		require.NoError(t, err)
		require.Equal(t, "order-4471", key)
		require.True(t, payload.GetNamedValues()["approved"].GetLiteral().GetBoolValue())
	})

	t.Run("one past the bound is refused in the submit door's words", func(t *testing.T) {
		t.Parallel()

		_, _, _, err := v1.BindWebhookTriggerSignal(t.Context(), wf, trigger, delivery(v1.MaxStructureDepth+1))
		require.Error(t, err)
		require.Contains(t, err.Error(), bodyDepthRefusal("slack-approval", v1.MaxStructureDepth+1))
	})
}
