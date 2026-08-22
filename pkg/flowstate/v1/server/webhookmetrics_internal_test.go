package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// What #526 asks the receiver's metrics to answer, and what this file pins.
//
// The question is a ratio, not a count: "delivery rate dropped because the
// sender stopped" and "delivery rate dropped because we are refusing
// everything" produce the same zero accepted deliveries, and today an operator
// tells them apart by reading logs. So the assertion below is that every
// refusal arm reaches the counter under *its own* reason — one member per
// refusal site, so that two arms answering the same HTTP status are still two
// series — and that nothing outside the schema's allowlist rides along.
//
// These are the arms decided before a run would start, which is what lets them
// run with no Temporal behind the receiver at all. The accepted and joined
// outcomes are asserted in webhook_test.go, where there is a cluster.

// signedBy is the signature a sender holding this deployment's key computes.
//
// [oneKey] resolves every reference to "k", so this is the genuine article for
// any delivery to [servedWorkflow]'s trigger.
func signedBy(body string) string {
	return v1.SignWebhookBody(secrets.NewSecret(secrets.NewRef("env", "K"), "k"), []byte(body))
}

// TestEveryWebhookRefusalIsCountedUnderItsOwnReason drives each pre-run refusal
// arm and asserts the reason it was counted under.
//
// No t.Parallel: the reader is installed as the process's meter provider, the
// same posture the engine-level metrics tests take.
func TestEveryWebhookRefusalIsCountedUnderItsOwnReason(t *testing.T) {
	const path = "/webhooks/order-webhook/storefront"
	const body = `{"id":"evt_1"}`

	reader := conformance.RecordMetrics(t)

	receiver, err := mustNew(t, nil).NewWebhookReceiver(t.Context(),
		"", []*v1.Workflow{servedWorkflow()}, oneKeyStore(t), WithWebhookConcurrency(1))
	require.NoError(t, err)

	// A GET, refused before the concurrency token is even taken.
	serve(t, receiver, httptest.NewRequest(http.MethodGet, path, nil))

	// The one token this receiver has, taken and not returned, so the next
	// delivery meets a full budget. Occupied rather than raced, for the reason
	// [TestADeliveryPastTheConcurrencyBoundIsShed] gives.
	receiver.inFlight <- struct{}{}
	serve(t, receiver, signedRequest(path, body))
	<-receiver.inFlight

	// A body past the bound, which is refused while it is being read rather
	// than after.
	oversized := strings.Repeat("a", v1.MaxWebhookPayloadBytes+1)
	serve(t, receiver, signedRequest(path, oversized))

	// A path nothing serves, and a signature computed under a key this
	// deployment does not hold. Both answer 404 with one sentence, deliberately
	// — and are counted apart, because an operator reading their own receiver
	// is not the prober that indistinguishability protects against.
	serve(t, receiver, signedRequest("/webhooks/order-webhook/nowhere", body))

	forged := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	forged.Header.Set(v1.WebhookSignatureHeader, signedBy("a different body"))
	serve(t, receiver, forged)

	collected := conformance.CollectFlowstateMetrics(t, reader)

	points, ok := collected[metricschema.InstrumentWebhookDeliveries]
	require.Truef(t, ok, "the receiver recorded nothing on %s — every instrument touched: %v",
		metricschema.InstrumentWebhookDeliveries, collected)

	counted := map[string]uint64{}
	for _, point := range points {
		require.Equalf(t, metricschema.WebhookOutcomeRefused, point.Attributes[metricschema.WebhookOutcome],
			"a refused delivery was counted as %v", point.Attributes)
		counted[point.Attributes[metricschema.WebhookRefusal]] += point.Count
	}

	require.Equal(t, map[string]uint64{
		metricschema.WebhookRefusalMethod:        1,
		metricschema.WebhookRefusalInFlightLimit: 1,
		metricschema.WebhookRefusalBodyTooLarge:  1,
		metricschema.WebhookRefusalUnrouted:      1,
		metricschema.WebhookRefusalUnverified:    1,
	}, counted,
		"each refusal arm must count under its own reason: two arms sharing one member make an operator's alert "+
			"fire for an incident that is not happening")

	// The half that guards something: no label outside what the instrument
	// declares, and nothing the schema refuses outright — a delivery id above
	// all, which the sender mints one of per delivery.
	conformance.AssertDeclaredAttributesOnly(t, collected)
}

// signedRequest is a genuine sender's delivery: a POST carrying a signature
// computed under this deployment's key.
func signedRequest(path, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(v1.WebhookSignatureHeader, signedBy(body))

	return req
}

// serve runs one delivery through the receiver and discards the answer: these
// assertions are about what was counted, and the statuses are already pinned by
// webhook_test.go.
func serve(t *testing.T, receiver *WebhookReceiver, req *http.Request) {
	t.Helper()

	receiver.ServeHTTP(httptest.NewRecorder(), req)
}
