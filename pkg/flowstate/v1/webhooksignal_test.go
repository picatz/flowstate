package flowstatev1_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The bridge's file-level rules, which are the only place the signal zero case
// is closed.
//
// `flow validate` reports these against a line and a column and
// [v1.BindRunInputs] refuses them for a specification that never was a
// Flowfile; both reach the functions below, so this is where the rules
// themselves are pinned rather than either caller's rendering of them.

// bridged builds a workflow whose one webhook answers a gate, with policy as
// its `signals:` entry for that name — nil for the zero case.
func bridged(policy *v1.SignalPolicy) *v1.Workflow {
	wf := &v1.Workflow{
		Name: "deploy-gate",
		Triggers: &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{
			Name: "slack-approval",
			Verify: map[string]*v1.Value{
				v1.WebhookSchemeHMACSHA256: {Kind: &v1.Value_SecretRef{
					SecretRef: &v1.SecretRef{Scheme: "env", Name: "SLACK_SIGNING_SECRET"},
				}},
			},
			IdempotencyKey: v1.NewExpr(`event.body.trigger_id`),
			Signal: &v1.WebhookTrigger_Signal{
				Name:      "stage-approved",
				Correlate: v1.NewExpr(`event.body.actions[0].value`),
				Arguments: map[string]*v1.Value{
					"approved": v1.NewExpr(`event.body.actions[0].action_id == "approve"`),
				},
			},
		}}},
		Steps: []*v1.Node{{
			Id: "gate",
			Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Timeout: durationpb.New(durationpb.New(0).AsDuration()),
				Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "stage-approved"}},
			}},
		}},
	}
	if policy != nil {
		wf.Signals = map[string]*v1.SignalPolicy{"stage-approved": policy}
	}

	return wf
}

// namesTheTrigger is the rule an author has to write for the bridge to compile.
func namesTheTrigger() *v1.SignalPolicy {
	return &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{
		Subject: v1.QualifiedSubject(v1.WebhookPrincipalIssuer,
			v1.WebhookTriggerSubject("deploy-gate", "slack-approval")),
	}}}
}

// TestABridgeNeedsAPolicyThatCouldAdmitItsTrigger is the zero-case rule, in
// both directions.
//
// The negative direction is the one that matters and the one a mutation removes
// first: without it a `signal:` naming an unpoliced gate compiles, and one
// leaked signing key answers every unpoliced gate the deployment serves.
func TestABridgeNeedsAPolicyThatCouldAdmitItsTrigger(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		policy  *v1.SignalPolicy
		refused string
	}{
		{
			name:    "no policy at all is the zero case, and it is refused",
			policy:  nil,
			refused: "declares no `signals:` policy",
		},
		{
			name:   "a rule naming the trigger admits it",
			policy: namesTheTrigger(),
		},
		{
			name: "a rule naming a person cannot admit a webhook",
			policy: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{
				Subject: v1.QualifiedSubject("https://issuer.example.com", "sre-lead@example.com"),
			}}},
			refused: "none of that signal's `allow:` rules can admit a webhook delivery",
		},
		{
			name: "a rule naming another trigger on this workflow cannot admit this one",
			policy: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{
				Subject: v1.QualifiedSubject(v1.WebhookPrincipalIssuer,
					v1.WebhookTriggerSubject("deploy-gate", "pagerduty-ack")),
			}}},
			refused: "none of that signal's `allow:` rules can admit a webhook delivery",
		},
		{
			name: "a rule requiring a claim can never be reached from this route",
			policy: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{
				Subject: v1.QualifiedSubject(v1.WebhookPrincipalIssuer,
					v1.WebhookTriggerSubject("deploy-gate", "slack-approval")),
				Claims: map[string]string{"team": "release-managers"},
			}}},
			refused: "carries no claims",
		},
		{
			name: "a namespace-only rule is left to the deployment to satisfy",
			policy: &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{{
				Namespace: "release-managers",
			}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := v1.CheckWebhookSignalBridges(bridged(test.policy))
			if test.refused == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Containsf(t, err.Error(), test.refused,
				"the refusal has to name what to write; got %q", err.Error())
		})
	}
}

// TestABridgeCannotAlsoStartARun pins the mutual exclusion, which is refused
// rather than ordered by precedence.
func TestABridgeCannotAlsoStartARun(t *testing.T) {
	t.Parallel()

	wf := bridged(namesTheTrigger())
	wf.GetTriggers().GetWebhooks()[0].Arguments = map[string]*v1.Value{
		"order_id": v1.NewExpr(`event.body.order`),
	}

	err := v1.CheckWebhookSignalBridges(wf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "declares both `with:` and `signal:`")
}

// TestABridgeNamesASignalSomethingWaitsFor is the refusal `SignalWithStart`
// makes out loud, applied to a file: a delivery to a name nothing waits for is
// carried by the run forever.
func TestABridgeNamesASignalSomethingWaitsFor(t *testing.T) {
	t.Parallel()

	wf := bridged(namesTheTrigger())
	wf.GetTriggers().GetWebhooks()[0].GetSignal().Name = "stage-aproved"
	wf.Signals = map[string]*v1.SignalPolicy{"stage-aproved": namesTheTrigger()}

	err := v1.CheckWebhookSignalBridges(wf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "which no `wait_for_signal:` in this workflow waits for")
	require.Containsf(t, err.Error(), "stage-approved",
		"the diagnostic has to say what the file does wait for, since this is nearly always a misspelling")
}

// TestACorrelationMustReadTheDelivery is [v1.CheckWebhookIdempotencyKey]'s
// question asked of the other expression a delivery is addressed by: a
// constant `correlate:` sends every approval anybody makes to one run.
func TestACorrelationMustReadTheDelivery(t *testing.T) {
	t.Parallel()

	wf := bridged(namesTheTrigger())
	wf.GetTriggers().GetWebhooks()[0].GetSignal().Correlate = v1.NewExpr(`"order-4471"`)

	err := v1.CheckWebhookSignalBridges(wf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not depend on the delivery")
}

// TestABridgedDeliveryBindsWhatItCarries walks the mapping the receiver calls,
// including the two refusals that keep an attacker-shaped payload from
// addressing whatever it likes.
func TestABridgedDeliveryBindsWhatItCarries(t *testing.T) {
	t.Parallel()

	wf := bridged(namesTheTrigger())
	trigger := wf.GetTriggers().GetWebhooks()[0]

	delivery := func(value any) v1.WebhookDelivery {
		return v1.WebhookDelivery{
			Body: map[string]any{
				"trigger_id": "evt-1",
				"actions":    []any{map[string]any{"value": value, "action_id": "approve"}},
			},
			Verified: true,
		}
	}

	t.Run("a verified delivery yields its run and its payload", func(t *testing.T) {
		t.Parallel()

		key, payload, idempotency, err := v1.BindWebhookTriggerSignal(
			context.Background(), wf, trigger, delivery("order-4471"))
		require.NoError(t, err)
		require.Equal(t, "order-4471", key)
		require.Equal(t, "evt-1", idempotency)
		require.True(t, payload.GetNamedValues()["approved"].GetLiteral().GetBoolValue())
	})

	t.Run("an unverified delivery is refused before anything is evaluated", func(t *testing.T) {
		t.Parallel()

		unverified := delivery("order-4471")
		unverified.Verified = false

		_, _, _, err := v1.BindWebhookTriggerSignal(context.Background(), wf, trigger, unverified)
		require.Error(t, err)
		require.Contains(t, err.Error(), "did not verify")
	})

	t.Run("a correlation outside the entity key grammar is refused", func(t *testing.T) {
		t.Parallel()

		// Refused where a workflow id would have been accepted: the key enters a
		// composed address, so the grammar it enters under is the one
		// [v1.RunRequest.entity_key] is held to and not whatever a payload spells.
		_, _, _, err := v1.BindWebhookTriggerSignal(context.Background(), wf, trigger,
			delivery("Order_4471"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "which is not an entity key")
	})

	t.Run("a workflow-id-shaped correlation still addresses an entity", func(t *testing.T) {
		t.Parallel()

		// The shape an attacker reaches for, and the reason lifting a workflow
		// id out of a payload was refused: this string is legal *as an entity
		// key*, and what keeps it from naming somebody else's run is that the
		// receiver composes an address rather than using one. The composed id
		// lands in the entity namespace, which no other addressing scheme
		// reaches.
		key, _, _, err := v1.BindWebhookTriggerSignal(context.Background(), wf, trigger,
			delivery("flowstate-workflow-2f1c"))
		require.NoError(t, err)

		id, err := v1.EntityWorkflowID("", key)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(id, "flowstate-entity-"),
			"a delivery reaches no address a caller could not have reached: %s", id)
	})

	t.Run("a correlation that is not a string is refused", func(t *testing.T) {
		t.Parallel()

		_, _, _, err := v1.BindWebhookTriggerSignal(context.Background(), wf, trigger, delivery(int64(7)))
		require.Error(t, err)
		require.Contains(t, err.Error(), "rather than to a string")
	})
}

// TestTheConsumedSetIsAddOnlyAndRingBounded pins the three properties the
// dedupe rests on, including the eviction that keeps a long-lived run's state
// from growing without bound.
func TestTheConsumedSetIsAddOnlyAndRingBounded(t *testing.T) {
	t.Parallel()

	t.Run("an id is fresh once", func(t *testing.T) {
		t.Parallel()

		consumed, fresh := v1.ConsumeDeliveryID(nil, "click-a")
		require.True(t, fresh)

		_, fresh = v1.ConsumeDeliveryID(consumed, "click-a")
		require.False(t, fresh)
	})

	t.Run("an empty id is always fresh and never recorded", func(t *testing.T) {
		t.Parallel()

		// Every sender that is not a webhook carries none, so recording it
		// would make the first `flow signal` a run ever received suppress every
		// one after it.
		consumed, fresh := v1.ConsumeDeliveryID(nil, "")
		require.True(t, fresh)
		require.Empty(t, consumed)

		_, fresh = v1.ConsumeDeliveryID(consumed, "")
		require.True(t, fresh)
	})

	t.Run("the oldest is evicted at the bound", func(t *testing.T) {
		t.Parallel()

		var consumed []string
		for i := range v1.MaxPendingSignals + 1 {
			var fresh bool
			consumed, fresh = v1.ConsumeDeliveryID(consumed, "click-"+strings.Repeat("x", i))
			require.True(t, fresh)
		}

		require.Len(t, consumed, v1.MaxPendingSignals,
			"the set is a ring: evicting the oldest narrows the window a replay is caught in, "+
				"and unsays no delivery")
		require.False(t, v1.DeliveryWasConsumed(consumed, "click-"),
			"the first id in is the first one evicted")
		require.True(t, v1.DeliveryWasConsumed(consumed, "click-"+strings.Repeat("x", v1.MaxPendingSignals)),
			"the most recent id is still in the window")
	})
}
