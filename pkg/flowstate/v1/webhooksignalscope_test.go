package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestADeliveryIDIsScopedToItsSource is the review finding a per-run consumed
// set makes load-bearing.
//
// An `idempotency_key:` names a delivery *within the source that issued it* —
// `event.body.order_id` is a promise one provider makes about its own
// redeliveries — while `RunState.consumed_delivery_ids` is one set per run
// across every source that can answer it. Hashing the key alone therefore made
// two bridges whose keys legitimately coincide produce one digest, and the
// second genuine delivery was dropped as a redelivery of the first: a dedupe
// silently refusing approvals nobody duplicated.
func TestADeliveryIDIsScopedToItsSource(t *testing.T) {
	t.Parallel()

	const key = "order-4471"

	same := v1.WebhookDeliveryID("deploy-gate", "slack-approval", key)

	assert.Equal(t, same, v1.WebhookDeliveryID("deploy-gate", "slack-approval", key),
		"one source's key must name one delivery, or a redelivery is not recognizable at all")

	assert.NotEqual(t, same, v1.WebhookDeliveryID("deploy-gate", "pagerduty-ack", key),
		"two triggers on one workflow whose keys coincide are two deliveries; this is the "+
			"collision that made the second one disappear")
	assert.NotEqual(t, same, v1.WebhookDeliveryID("other-workflow", "slack-approval", key),
		"the workflow is half of a source's identity, exactly as it is half of the principal")

	// And it is still a digest: the key is frequently a signature header, and
	// this value reaches durable state.
	assert.NotContains(t, same, key)
}

// TestABridgedDeliveryIDMatchesTheReceiversScope pins the two callers to one
// answer, since `flow test` asserting on `${trigger.delivery_id}` has to name
// what production names.
func TestABridgedDeliveryIDMatchesTheReceiversScope(t *testing.T) {
	t.Parallel()

	wf := bridged(namesTheTrigger())
	trigger := wf.GetTriggers().GetWebhooks()[0]

	_, _, key, err := v1.BindWebhookTriggerSignal(context.Background(), wf, trigger, v1.WebhookDelivery{
		Body: map[string]any{
			"trigger_id": "evt-1",
			"actions":    []any{map[string]any{"value": "order-4471", "action_id": "approve"}},
		},
		Verified: true,
	})
	require.NoError(t, err)

	assert.Equal(t,
		v1.WebhookDeliveryID(wf.GetName(), trigger.GetName(), key),
		v1.WebhookDeliveryID("deploy-gate", "slack-approval", "evt-1"),
		"the id a receiver computes and the id a rehearsal computes are one value")
}

// TestASpecCarryingBothConstructsIsRefused is the other review finding: the
// contradiction is judged by presence, not by length.
//
// A non-nil `arguments` map with nothing in it is still both constructs in one
// specification, and it is exactly what protojson produces for a
// written-but-empty mapping and what a hand-built request can carry. Judged by
// `len` it was accepted, through the one door that never passes a Flowfile.
func TestASpecCarryingBothConstructsIsRefused(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		arguments map[string]*v1.Value
		refused   bool
	}{
		{name: "no arguments at all is an ordinary bridge", arguments: nil},
		{name: "an empty but present map is still both constructs", arguments: map[string]*v1.Value{}, refused: true},
		{name: "a populated map is the obvious case", refused: true, arguments: map[string]*v1.Value{
			"order_id": v1.NewExpr(`event.body.order`),
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wf := bridged(namesTheTrigger())
			wf.GetTriggers().GetWebhooks()[0].Arguments = test.arguments

			err := v1.CheckWebhookSignalBridges(wf)
			if !test.refused {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Contains(t, err.Error(), "declares both `with:` and `signal:`")
		})
	}
}

// TestABridgeCannotAddressARunFromUnsignedHeaders is the security review's
// MEDIUM finding.
//
// `hmac_sha256` signs the body; `stripe` signs `<timestamp>.<body>`. Neither
// covers arbitrary request headers, so anybody who has once seen a valid
// delivery — a proxy log, mirrored traffic — can replay that exact body and
// signature with a header rewritten. For a bridge that is not cosmetic: a
// header-derived `correlate:` picks which parked run is answered, and a
// header-derived `idempotency_key:` mints a delivery id the replay ring has
// never seen, so the same approval can be replayed onto a different gate
// indefinitely.
func TestABridgeCannotAddressARunFromUnsignedHeaders(t *testing.T) {
	t.Parallel()

	body := v1.NewExpr(`event.body.order`)

	for _, test := range []struct {
		name       string
		scheme     string
		correlate  *v1.Value
		idempotent *v1.Value
		refused    string
	}{
		{
			name:       "a body-derived address is what a signature covers",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  body,
			idempotent: v1.NewExpr(`event.body.trigger_id`),
		},
		{
			name:       "a header-derived correlation is refused",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`event.headers["x-order"]`),
			idempotent: v1.NewExpr(`event.body.trigger_id`),
			refused:    "`signal.correlate:` over `event.headers`",
		},
		{
			name:       "a header-derived delivery id is refused too",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  body,
			idempotent: v1.NewExpr(`event.headers["x-request-id"]`),
			refused:    "`idempotency_key:` over `event.headers`",
		},
		{
			name:       "the index spelling of the same read is refused",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`event["headers"]["x-order"]`),
			idempotent: v1.NewExpr(`event.body.trigger_id`),
			refused:    "`signal.correlate:` over `event.headers`",
		},
		{
			name:      "stripe is decided the same way, since it signs no other header",
			scheme:    v1.WebhookSchemeStripe,
			correlate: v1.NewExpr(`event.headers["stripe-signature"]`),
			// The signed timestamp is a component of the signature header, not
			// a header an expression may read as though it were attested.
			idempotent: v1.NewExpr(`event.body.trigger_id`),
			refused:    "stripe does not sign a delivery's headers",
		},
		{
			name:       "stripe over the body is accepted",
			scheme:     v1.WebhookSchemeStripe,
			correlate:  body,
			idempotent: v1.NewExpr(`event.body.id`),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wf := bridged(namesTheTrigger())
			trigger := wf.GetTriggers().GetWebhooks()[0]
			trigger.Verify = map[string]*v1.Value{
				test.scheme: {Kind: &v1.Value_SecretRef{
					SecretRef: &v1.SecretRef{Scheme: "env", Name: "WEBHOOK_SECRET"},
				}},
			}
			trigger.IdempotencyKey = test.idempotent
			trigger.GetSignal().Correlate = test.correlate

			err := v1.CheckWebhookSignalBridges(wf)
			if test.refused == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Containsf(t, err.Error(), test.refused,
				"the refusal has to name which expression and which scheme; got %q", err.Error())
		})
	}
}

// TestAStartingTriggerMayStillReadHeaders keeps the rule where the hazard is.
//
// A trigger that starts a run has always been allowed to key on a header, and
// `examples/webhook-trigger` does exactly that with Stripe's signature header.
// The asymmetry is real rather than an inconsistency: a start's key moves only
// the run's own id and its inputs, all of which the key holder could have sent
// anyway, while a bridge's addresses somebody else's parked gate.
func TestAStartingTriggerMayStillReadHeaders(t *testing.T) {
	t.Parallel()

	wf := bridged(namesTheTrigger())
	trigger := wf.GetTriggers().GetWebhooks()[0]
	trigger.Signal = nil
	trigger.IdempotencyKey = v1.NewExpr(`event.headers["stripe-signature"]`)

	require.NoError(t, v1.CheckWebhookSignalBridges(wf))
	require.NoError(t, v1.CheckWebhookTriggers(wf.GetTriggers()))
}
