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

// TestABridgeProvesItsAddressCameFromSignedBytes is the security review's
// MEDIUM finding and the P1 that followed it.
//
// `hmac_sha256` signs the body; `stripe` signs `<timestamp>.<body>`. Neither
// covers arbitrary request headers, so anybody who has once seen a valid
// delivery can replay that exact body and signature with a header rewritten.
// On a bridge that is not cosmetic: a header-derived `correlate:` picks which
// parked run is answered, and a header-derived `idempotency_key:` mints a
// delivery id the replay ring has never seen.
//
// The rule was first written as a search for `event.headers`, and the aliasing
// case below is why that was wrong rather than merely incomplete: it reaches a
// header with no `headers` selection over the `event` identifier anywhere in
// the expression, so the search proved nothing about what it accepted. The rule
// is now an allow-list — every `event` must be a `body` read — which is why the
// refused table has entries that touch no header at all.
func TestABridgeProvesItsAddressCameFromSignedBytes(t *testing.T) {
	t.Parallel()

	body := v1.NewExpr(`event.body.order`)
	key := v1.NewExpr(`event.body.trigger_id`)

	for _, test := range []struct {
		name       string
		scheme     string
		correlate  *v1.Value
		idempotent *v1.Value
		construct  string
	}{
		{
			name:       "a body-derived address is what a signature covers",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  body,
			idempotent: key,
		},
		{
			name:       "a deeper body path is still a body read",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`event.body.order.id`),
			idempotent: key,
		},
		{
			name:       "the index spelling of a body read is accepted",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`event.body["order"]["id"]`),
			idempotent: key,
		},
		{
			name:       "the index spelling of the root is accepted when it names the body",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`event["body"].order`),
			idempotent: key,
		},
		{
			name:       "a body read inside an expression is accepted",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`event.body.retry ? event.body.original : event.body.id`),
			idempotent: key,
		},
		{
			name:       "a header-derived correlation is refused",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`event.headers["x-order"]`),
			idempotent: key,
			construct:  "`event.headers`",
		},
		{
			name:       "a header-derived delivery id is refused too",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  body,
			idempotent: v1.NewExpr(`event.headers["x-request-id"]`),
			construct:  "`event.headers`",
		},
		{
			name:       "the index spelling of a header read is refused",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`event["headers"]["x-order"]`),
			idempotent: key,
			construct:  "an indexed `event[…]`",
		},
		// The aliasing class, which is the whole reason this rule is an
		// allow-list. Every entry below reaches an unsigned header — or could
		// — without any node in it being a `headers` selection over the
		// `event` identifier, so the deny-list this replaced found nothing to
		// refuse and accepted all of them. They are listed one spelling per
		// case rather than folded together because the point is that the rule
		// does not depend on which spelling somebody picks: it refuses the
		// *root* escaping, and the escape is what every one of these has in
		// common.
		{
			// Codex's spelling: aliased through a comprehension variable.
			name:       "a header reached through a comprehension alias is refused",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`[event].map(e, e.headers["x-order"])[0]`),
			idempotent: key,
			construct:  "`event` inside a list",
		},
		{
			// Aliased through a ternary, which needs no comprehension at all.
			name:       "a header reached through a conditional is refused",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`(true ? event : event).headers["x-order"]`),
			idempotent: key,
			construct:  "`event` passed to an operator",
		},
		{
			// Aliased through a map literal.
			name:       "a header reached through a map literal is refused",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`{"e": event}["e"].headers["x-order"]`),
			idempotent: key,
			construct:  "`event` inside a map",
		},
		{
			// Aliased through a list, read back by index.
			name:       "a header reached through a list index is refused",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`[event][0].headers["x-order"]`),
			idempotent: key,
			construct:  "`event` inside a list",
		},
		{
			// A presence test is a selection under another name, and the rule
			// reads it as one.
			name:       "a presence test over a header is refused",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`has(event.headers) ? "a" : "b"`),
			idempotent: key,
			construct:  "`event.headers`",
		},
		{
			name:       "the delivery root as a comprehension's range is refused",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`event.map(k, k)[0]`),
			idempotent: key,
			construct:  "`event` as a comprehension's range",
		},
		{
			name:       "the delivery root passed to a function is refused",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`string(event)`),
			idempotent: key,
			construct:  "`event` passed to `string(...)`",
		},
		{
			// Refused even though this one reaches the body in the end: the
			// root left the rule's sight, and what comes back cannot be proved
			// to be what went in. That is the cost of proving provenance rather
			// than searching for its absence, and it is the right one.
			name:       "the delivery root inside a list is refused even reading the body",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`[event][0].body.order`),
			idempotent: key,
			construct:  "`event` inside a list",
		},
		{
			name:       "the delivery root inside a map is refused even reading the body",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  v1.NewExpr(`{"d": event}["d"].body.order`),
			idempotent: key,
			construct:  "`event` inside a map",
		},
		{
			// The other half of the aliasing class, on the other expression:
			// an `idempotency_key:` on a bridge mints the delivery id the
			// replay ring recognizes, so it is addressed by the same rule.
			name:       "an aliased delivery id is refused too",
			scheme:     v1.WebhookSchemeHMACSHA256,
			correlate:  body,
			idempotent: v1.NewExpr(`[event].map(e, e.headers["x-request-id"])[0]`),
			construct:  "`event` inside a list",
		},
		{
			name:       "stripe is decided the same way, since it signs no other header",
			scheme:     v1.WebhookSchemeStripe,
			correlate:  v1.NewExpr(`event.headers["stripe-signature"]`),
			idempotent: key,
			construct:  "`event.headers`",
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
			if test.construct == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Containsf(t, err.Error(), test.construct,
				"the refusal has to name the construct an author must rewrite; got %q", err.Error())
			require.Containsf(t, err.Error(), test.scheme,
				"the refusal has to name the scheme whose signature does not cover this")
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
