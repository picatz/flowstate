package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// The two properties of the receiver that cannot be seen from outside the
// package: what it does when more deliveries arrive than it will process at once,
// and what a workflow id is derived from.

// oneKey is a backend resolving every reference in every tenant to one value.
type oneKey struct{ value string }

func (k oneKey) Scheme() string { return "env" }

func (k oneKey) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	return secrets.NewSecret(req.Ref, k.value), nil
}

// oneKeyStore is the store a receiver in these tests is handed.
func oneKeyStore(t *testing.T) *secrets.Store {
	t.Helper()

	store, err := secrets.NewStore(oneKey{value: "k"})
	require.NoError(t, err)

	return store
}

func servedWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "order-webhook",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{Webhooks: []*v1.WebhookTrigger{{
			Name: "storefront",
			Verify: map[string]*v1.Value{
				v1.WebhookSchemeHMACSHA256: {Kind: &v1.Value_SecretRef{
					SecretRef: &v1.SecretRef{Scheme: "env", Name: "K"},
				}},
			},
			IdempotencyKey: v1.NewExpr(`event.body.id`),
		}}},
		Steps: []*v1.Node{{Id: "record", Kind: &v1.Node_Value{Value: v1.NewLiteral("ok")}}},
	}
}

// TestADeliveryPastTheConcurrencyBoundIsShed pins the bound on what else a
// hostile sender controls: how many deliveries are in flight at once.
//
// Written by occupying the budget rather than by racing real deliveries into it,
// because the assertion is about the arm taken when the budget is full, and a test
// that had to *win* a race to reach that arm would be a flake rather than a check.
func TestADeliveryPastTheConcurrencyBoundIsShed(t *testing.T) {
	t.Parallel()

	receiver, err := New(nil).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{servedWorkflow()}, oneKeyStore(t), WithWebhookConcurrency(1))
	require.NoError(t, err)

	// The one token this receiver has, taken and not returned.
	receiver.inFlight <- struct{}{}

	req := httptest.NewRequest(http.MethodPost, "/webhooks/order-webhook/storefront",
		strings.NewReader(`{"id":"evt_1"}`))
	recorder := httptest.NewRecorder()
	receiver.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusServiceUnavailable, recorder.Code,
		"a delivery past the concurrency bound was processed rather than shed")
	assert.Equal(t, "1", recorder.Header().Get("Retry-After"),
		"a shed delivery was not told to retry, so a provider reads it as a failure")
}

// TestAWorkflowIdSeparatesWhatMustNotShareARun is the negative direction of the
// dedupe key, and the one a test asserting "the same key gives the same id"
// cannot see.
//
// A key is issued by one sender, for one trigger, in one tenant. If any of those
// were left out of the derivation, an id would be addressable by somebody else's
// delivery — which for a run keyed on it means joining, and joining somebody
// else's run is worse than starting a duplicate.
func TestAWorkflowIdSeparatesWhatMustNotShareARun(t *testing.T) {
	t.Parallel()

	base := webhookWorkflowID("team-a", "order-webhook", "storefront", "evt_1")

	assert.Equal(t, base, webhookWorkflowID("team-a", "order-webhook", "storefront", "evt_1"),
		"the same delivery derived two different ids, so a redelivery would start a second run")

	for name, id := range map[string]string{
		"another tenant":  webhookWorkflowID("team-b", "order-webhook", "storefront", "evt_1"),
		"another webhook": webhookWorkflowID("team-a", "order-webhook", "payments", "evt_1"),
		"another workflow": webhookWorkflowID("team-a", "refund-webhook", "storefront",
			"evt_1"),
		"another event": webhookWorkflowID("team-a", "order-webhook", "storefront", "evt_2"),
	} {
		assert.NotEqual(t, base, id, "%s derives the same run id", name)
	}

	// And the key itself is not in the id: the usual key is a signature header,
	// and a workflow id is durable, listed, and readable by anyone who can see
	// the namespace.
	assert.NotContains(t, webhookWorkflowID("team-a", "order-webhook", "storefront", "t=1,v1=deadbeef"),
		"deadbeef", "the raw idempotency key was interpolated into the run's id")
}
