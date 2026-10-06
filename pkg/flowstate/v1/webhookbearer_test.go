package flowstatev1_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// stubBearer is a trust policy that vouches for exactly one token.
type stubBearer struct {
	token     string
	principal auth.Principal
	calls     int
}

func (s *stubBearer) Verify(_ context.Context, raw string) (auth.Principal, error) {
	s.calls++
	if raw != s.token {
		return auth.Principal{}, errors.New("not the token")
	}

	return s.principal, nil
}

func jwtTrigger(issuer string) *v1.WebhookTrigger {
	return &v1.WebhookTrigger{
		Name:           "deploy",
		Verify:         map[string]*v1.Value{v1.WebhookSchemeJWT: v1.NewLiteral(issuer)},
		IdempotencyKey: v1.NewExpr(`event.body.id`),
	}
}

func actions() *stubBearer {
	return &stubBearer{
		token: "tok",
		principal: auth.Principal{
			Issuer: "https://token.actions.githubusercontent.com", IssuerName: "github-actions",
			Subject: "repo:acme/app:ref:refs/heads/main", Kind: auth.PrincipalKindWorkload,
		},
	}
}

func bearerHeaders(value string) map[string]string {
	return map[string]string{v1.WebhookAuthorizationHeader: value}
}

// The accepted case also pins what the caller receives: the sender, not just a
// yes, because the receiver acts as it.
func TestAVerifiedBearerTokenYieldsTheSender(t *testing.T) {
	t.Parallel()

	stub := actions()
	sender, err := v1.VerifyWebhookDeliveryAs(t.Context(), jwtTrigger("github-actions"), nil, stub, "",
		bearerHeaders("Bearer tok"), []byte(`{}`), time.Now())
	require.NoError(t, err)
	require.NotNil(t, sender)
	assert.Equal(t, "repo:acme/app:ref:refs/heads/main", sender.Subject)
	assert.Equal(t, auth.PrincipalKindWorkload, sender.Kind)
}

func TestTheBearerSchemeNameIsCaseInsensitive(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"bearer tok", "BEARER tok", "Bearer   tok"} {
		_, err := v1.VerifyWebhookDeliveryAs(t.Context(), jwtTrigger("github-actions"), nil, actions(), "",
			bearerHeaders(value), nil, time.Now())
		assert.NoError(t, err, value)
	}
}

// Every way a delivery can fail to carry a usable credential, plus the three
// ways a verified one can still be the wrong one.
func TestABearerDeliveryThatDoesNotVerifyIsRefused(t *testing.T) {
	t.Parallel()

	other := actions()
	other.principal.IssuerName = "gitlab"

	elsewhere := actions()
	elsewhere.principal.Namespace = "acme"

	tests := map[string]struct {
		headers map[string]string
		bearer  auth.Verifier
		want    error
	}{
		"no header":       {nil, actions(), v1.ErrWebhookSignatureMissing},
		"another scheme":  {bearerHeaders("Basic tok"), actions(), v1.ErrWebhookSignatureMissing},
		"empty token":     {bearerHeaders("Bearer "), actions(), v1.ErrWebhookSignatureMissing},
		"two tokens":      {bearerHeaders("Bearer tok tok"), actions(), v1.ErrWebhookSignatureMissing},
		"wrong token":     {bearerHeaders("Bearer nope"), actions(), v1.ErrWebhookSignatureInvalid},
		"another issuer":  {bearerHeaders("Bearer tok"), other, v1.ErrWebhookSignatureInvalid},
		"another tenant":  {bearerHeaders("Bearer tok"), elsewhere, v1.ErrWebhookSignatureInvalid},
		"no trust policy": {bearerHeaders("Bearer tok"), nil, v1.ErrWebhookBearerUnchecked},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sender, err := v1.VerifyWebhookDeliveryAs(t.Context(), jwtTrigger("github-actions"), nil, test.bearer, "",
				test.headers, nil, time.Now())
			require.ErrorIs(t, err, test.want)
			assert.Nil(t, sender)
		})
	}
}

// The hazard this guards: the signing-only entry point returning nil for a
// trigger whose only scheme it cannot check.
func TestTheSigningOnlyEntryPointRefusesABearerTrigger(t *testing.T) {
	t.Parallel()

	err := v1.VerifyWebhookDelivery(jwtTrigger("github-actions"), nil, bearerHeaders("Bearer tok"), nil, time.Now())
	require.ErrorIs(t, err, v1.ErrWebhookBearerUnchecked)
}

// Every declared scheme must hold: a valid token does not stand in for a bad
// signature, and a good signature does not stand in for a bad token.
func TestBearerAndSigningSchemesMustBothVerify(t *testing.T) {
	t.Parallel()

	key := secrets.NewSecret(secrets.NewRef("env", "WEBHOOK_SECRET"), "whsec_test")
	body := []byte(`{"id":"evt_1"}`)
	trigger := jwtTrigger("github-actions")
	trigger.Verify[v1.WebhookSchemeHMACSHA256] = hmacTrigger().Verify[v1.WebhookSchemeHMACSHA256]
	keys := map[string]secrets.Secret{v1.WebhookSchemeHMACSHA256: key}

	signed := bearerHeaders("Bearer tok")
	signed[v1.WebhookSignatureHeader] = v1.SignWebhookBody(key, body)

	_, err := v1.VerifyWebhookDeliveryAs(t.Context(), trigger, keys, actions(), "", signed, body, time.Now())
	require.NoError(t, err)

	badSignature := bearerHeaders("Bearer tok")
	badSignature[v1.WebhookSignatureHeader] = v1.SignWebhookBody(key, []byte(`{"id":"other"}`))
	_, err = v1.VerifyWebhookDeliveryAs(t.Context(), trigger, keys, actions(), "", badSignature, body, time.Now())
	require.ErrorIs(t, err, v1.ErrWebhookSignatureInvalid)

	badToken := bearerHeaders("Bearer nope")
	badToken[v1.WebhookSignatureHeader] = v1.SignWebhookBody(key, body)
	_, err = v1.VerifyWebhookDeliveryAs(t.Context(), trigger, keys, actions(), "", badToken, body, time.Now())
	require.ErrorIs(t, err, v1.ErrWebhookSignatureInvalid)
}

func TestATriggerWithNoBearerSchemeNeverConsultsTheTrustPolicy(t *testing.T) {
	t.Parallel()

	key := signingKey("whsec_test")
	body := []byte(`{"id":"evt_1"}`)
	stub := actions()

	sender, err := v1.VerifyWebhookDeliveryAs(t.Context(), hmacTrigger(),
		map[string]secrets.Secret{v1.WebhookSchemeHMACSHA256: key}, stub, "",
		map[string]string{v1.WebhookSignatureHeader: v1.SignWebhookBody(key, body)}, body, time.Now())
	require.NoError(t, err)
	assert.Nil(t, sender, "a delivery with no bearer scheme acts as its trigger")
	assert.Zero(t, stub.calls)
}

func TestTheBearerSchemeNamesAnIssuerNotASecret(t *testing.T) {
	t.Parallel()

	require.NoError(t, v1.CheckWebhookTrigger(jwtTrigger("github-actions")))

	empty := v1.CheckWebhookTrigger(jwtTrigger(""))
	require.Error(t, empty)
	assert.Contains(t, empty.Error(), "names no trusted issuer")
}

// The credential must not become an input, an idempotency key or a signal
// payload, all of which land in history.
func TestTheAuthorizationHeaderIsNotPartOfTheEvent(t *testing.T) {
	t.Parallel()

	event := v1.NewWebhookEvent(map[string]string{
		"Authorization": "Bearer secret-token", "X-Request-Id": "r1",
	}, map[string]any{})

	rendered := event.String()
	assert.NotContains(t, rendered, "secret-token")
	assert.Contains(t, rendered, "x-request-id")
}

// A bearer token authenticates a sender and signs nothing, so the signer that
// builds outgoing deliveries refuses the scheme rather than inventing a header.
func TestTheSignerRefusesTheBearerScheme(t *testing.T) {
	t.Parallel()

	_, _, err := v1.SignWebhookDelivery(v1.WebhookSchemeJWT, signingKey("k"), []byte(`{}`), time.Now())
	require.Error(t, err)
}
