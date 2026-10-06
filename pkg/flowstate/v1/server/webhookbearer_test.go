package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// A trigger that declares `verify: {jwt: <name>}` is served only by a deployment
// that holds the trust policy the name points into, and a delivery to it is
// admitted only for the sender that entry vouches for.
//
// What these tests stop short of is a run: the receiver here has no Temporal,
// and what is decided before one starts is the claim. A 404 is a refusal at
// verification; anything else means verification passed and the next arm, which
// needs a cluster, took over.

const bearerToken = "good-token"

// trustVerifier vouches for one token, as the entry named by name.
type trustVerifier struct{ name, ns string }

func (v trustVerifier) Verify(_ context.Context, raw string) (auth.Principal, error) {
	if raw != bearerToken {
		return auth.Principal{}, errors.New("not the token")
	}

	return auth.Principal{
		Issuer: "https://token.actions.githubusercontent.com", IssuerName: v.name, Subject: "repo:acme/app",
		Namespace: v.ns, Kind: auth.PrincipalKindWorkload,
	}, nil
}

func trustPolicy(entries ...auth.TrustedIssuer) *auth.Policy {
	return &auth.Policy{Issuers: entries}
}

func bearerWorkflow() *v1.Workflow {
	workflow := orderWebhookWorkflow()
	workflow.Triggers.Webhooks[0].Verify = map[string]*v1.Value{v1.WebhookSchemeJWT: v1.NewLiteral("github-actions")}

	return workflow
}

func bearerDelivery(t *testing.T, receiver http.Handler, authorization string) *http.Response {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/webhooks/order-webhook/storefront",
		strings.NewReader(deliveryBody("evt_bearer")))
	req.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}

	recorder := httptest.NewRecorder()
	receiver.ServeHTTP(recorder, req)

	return recorder.Result()
}

func TestABearerWebhookIsRefusedAtStartupWithoutATrustPolicy(t *testing.T) {
	t.Parallel()

	_, err := mustNew(t, nil).NewWebhookReceiver(t.Context(), "",
		[]*v1.Workflow{bearerWorkflow()}, keyStore(t, webhookSecret))
	require.Error(t, err, "a trigger naming a trusted issuer was served by a deployment with no trust policy")
	assert.Contains(t, err.Error(), "--auth-policy")
}

func TestABearerWebhookIsRefusedAtStartupForAnEntryItCannotUse(t *testing.T) {
	t.Parallel()

	tests := map[string]*auth.Policy{
		"no such entry": trustPolicy(auth.TrustedIssuer{Name: "gitlab"}),
		"a certificate": trustPolicy(auth.TrustedIssuer{Name: "github-actions", Kind: auth.IssuerKindMTLS}),
	}
	for name, policy := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := mustNew(t, nil).NewWebhookReceiver(t.Context(), "",
				[]*v1.Workflow{bearerWorkflow()}, keyStore(t, webhookSecret),
				server.WithWebhookTrust(trustVerifier{name: "github-actions"}, policy))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "github-actions")
		})
	}
}

func TestABearerWebhookAdmitsOnlyTheSenderItsEntryVouchesFor(t *testing.T) {
	t.Parallel()

	policy := trustPolicy(auth.TrustedIssuer{Name: "github-actions"}, auth.TrustedIssuer{Name: "gitlab"})

	build := func(verifier auth.Verifier) http.Handler {
		receiver, err := mustNew(t, unreachableTemporal(t)).NewWebhookReceiver(t.Context(), "",
			[]*v1.Workflow{bearerWorkflow()}, keyStore(t, webhookSecret), server.WithWebhookTrust(verifier, policy))
		require.NoError(t, err)

		return receiver
	}

	right := build(trustVerifier{name: "github-actions"})

	assert.NotEqual(t, http.StatusNotFound, bearerDelivery(t, right, "Bearer "+bearerToken).StatusCode,
		"the sender the named entry vouches for was refused at verification")

	for name, authorization := range map[string]string{
		"no credential": "", "another scheme": "Basic " + bearerToken, "a wrong token": "Bearer nope",
	} {
		assert.Equal(t, http.StatusNotFound, bearerDelivery(t, right, authorization).StatusCode, name)
	}

	// A token the policy trusts, vouched for by a different entry: another
	// sender under another rule.
	wrongEntry := build(trustVerifier{name: "gitlab"})
	assert.Equal(t, http.StatusNotFound, bearerDelivery(t, wrongEntry, "Bearer "+bearerToken).StatusCode,
		"a token another trust policy entry vouched for was admitted by this webhook")

	// And one belonging to another tenant than the route's.
	elsewhere := build(trustVerifier{name: "github-actions", ns: "other-tenant"})
	assert.Equal(t, http.StatusNotFound, bearerDelivery(t, elsewhere, "Bearer "+bearerToken).StatusCode,
		"a token from another tenant was admitted by this tenant's route")
}
