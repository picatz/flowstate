package flowstatev1_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// Signature checking, held to the properties that make it worth having.
//
// Two of these are the reason a webhook is verifiable at all — a genuine delivery
// is accepted, a tampered one is not — and the rest are the ways an implementation
// of this passes while being wrong: signing the wrong payload, accepting a replay,
// accepting because a scheme was unknown, accepting because the deployment held no
// key for it.

// signingKey is a resolved key, built the way a provider builds one so that
// nothing here depends on a test-only shape of [secrets.Secret].
func signingKey(value string) secrets.Secret {
	return secrets.NewSecret(secrets.NewRef("env", "WEBHOOK_SECRET"), value)
}

// hmacTrigger is a well-formed trigger declaring the generic scheme.
func hmacTrigger() *v1.WebhookTrigger {
	return &v1.WebhookTrigger{
		Name: "storefront",
		Verify: map[string]*v1.Value{
			v1.WebhookSchemeHMACSHA256: {Kind: &v1.Value_SecretRef{
				SecretRef: &v1.SecretRef{Scheme: "env", Name: "WEBHOOK_SECRET"},
			}},
		},
		IdempotencyKey: v1.NewExpr(`event.body.id`),
	}
}

// stripeSignedTrigger is the same, declaring the named scheme.
func stripeSignedTrigger() *v1.WebhookTrigger {
	return &v1.WebhookTrigger{
		Name: "stripe",
		Verify: map[string]*v1.Value{
			v1.WebhookSchemeStripe: {Kind: &v1.Value_SecretRef{
				SecretRef: &v1.SecretRef{Scheme: "env", Name: "WEBHOOK_SECRET"},
			}},
		},
		IdempotencyKey: v1.NewExpr(`event.body.id`),
	}
}

func TestAGenuineDeliveryVerifies(t *testing.T) {
	t.Parallel()

	key := signingKey("whsec_test")
	body := []byte(`{"id":"evt_1"}`)

	headers := map[string]string{
		// Spelled the way a sender would, with the header's canonical casing,
		// which the lookup has to match case-insensitively.
		v1.WebhookSignatureHeader: "sha256=" + v1.SignWebhookBody(key, body),
	}

	require.NoError(t, v1.VerifyWebhookDelivery(hmacTrigger(),
		map[string]secrets.Secret{v1.WebhookSchemeHMACSHA256: key}, headers, body, time.Now()))
}

// TestABodyChangedAfterSigningIsRefused is the property the whole scheme exists
// for, and the one a test asserting only the happy path cannot see.
func TestABodyChangedAfterSigningIsRefused(t *testing.T) {
	t.Parallel()

	key := signingKey("whsec_test")
	signed := []byte(`{"id":"evt_1","amount":100}`)
	tampered := []byte(`{"id":"evt_1","amount":100000}`)

	headers := map[string]string{v1.WebhookSignatureHeader: v1.SignWebhookBody(key, signed)}

	err := v1.VerifyWebhookDelivery(hmacTrigger(),
		map[string]secrets.Secret{v1.WebhookSchemeHMACSHA256: key}, headers, tampered, time.Now())
	require.Error(t, err, "a body changed after it was signed verified anyway")
}

func TestADeliveryWithNoSignatureHeaderIsRefused(t *testing.T) {
	t.Parallel()

	key := signingKey("whsec_test")

	err := v1.VerifyWebhookDelivery(hmacTrigger(),
		map[string]secrets.Secret{v1.WebhookSchemeHMACSHA256: key}, nil, []byte(`{"id":"evt_1"}`), time.Now())
	require.Error(t, err, "a delivery carrying no signature at all was accepted")
}

// TestADeliveryIsRefusedWhenTheDeploymentHoldsNoKey is the fail-closed direction:
// unverifiable is refused, never allowed because it could not be checked.
func TestADeliveryIsRefusedWhenTheDeploymentHoldsNoKey(t *testing.T) {
	t.Parallel()

	key := signingKey("whsec_test")
	body := []byte(`{"id":"evt_1"}`)
	headers := map[string]string{v1.WebhookSignatureHeader: v1.SignWebhookBody(key, body)}

	err := v1.VerifyWebhookDelivery(hmacTrigger(), nil, headers, body, time.Now())
	require.Error(t, err, "a delivery was accepted against a scheme this deployment resolved no key for")

	err = v1.VerifyWebhookDelivery(hmacTrigger(),
		map[string]secrets.Secret{v1.WebhookSchemeHMACSHA256: {}}, headers, body, time.Now())
	require.Error(t, err, "a zero key verified a delivery")
}

// TestEveryDeclaredSchemeMustVerify is the reading a `verify:` block with two
// entries has to have: adding a scheme must not be a way to weaken a webhook.
func TestEveryDeclaredSchemeMustVerify(t *testing.T) {
	t.Parallel()

	key := signingKey("whsec_test")
	body := []byte(`{"id":"evt_1"}`)

	trigger := hmacTrigger()
	trigger.Verify[v1.WebhookSchemeStripe] = &v1.Value{Kind: &v1.Value_SecretRef{
		SecretRef: &v1.SecretRef{Scheme: "env", Name: "WEBHOOK_SECRET"},
	}}

	// The generic half is signed correctly and the Stripe half is absent.
	headers := map[string]string{v1.WebhookSignatureHeader: v1.SignWebhookBody(key, body)}

	err := v1.VerifyWebhookDelivery(trigger, map[string]secrets.Secret{
		v1.WebhookSchemeHMACSHA256: key,
		v1.WebhookSchemeStripe:     key,
	}, headers, body, time.Now())
	require.Error(t, err, "a delivery satisfying one of two declared schemes was accepted")
}

func TestAStripeDeliveryVerifies(t *testing.T) {
	t.Parallel()

	key := signingKey("whsec_stripe")
	body := []byte(`{"id":"evt_1","data":{}}`)
	at := time.Unix(1755043200, 0)

	headers := map[string]string{v1.StripeSignatureHeader: v1.SignStripeBody(key, body, at)}

	require.NoError(t, v1.VerifyWebhookDelivery(stripeSignedTrigger(),
		map[string]secrets.Secret{v1.WebhookSchemeStripe: key}, headers, body, at.Add(30*time.Second)))
}

// TestAStripeSignatureIsOverTheTimestampAndTheBody is the arithmetic itself, and
// the reason `stripe` is a named scheme rather than the generic one pointed at
// Stripe's header: signing the body alone is a check that passes on a forged
// timestamp, which is how a replay window comes to mean nothing.
func TestAStripeSignatureIsOverTheTimestampAndTheBody(t *testing.T) {
	t.Parallel()

	key := signingKey("whsec_stripe")
	body := []byte(`{"id":"evt_1"}`)
	at := time.Unix(1755043200, 0)

	// A signature over the body alone, presented in Stripe's header shape.
	headers := map[string]string{
		v1.StripeSignatureHeader: "t=1755043200,v1=" + v1.SignWebhookBody(key, body),
	}

	err := v1.VerifyWebhookDelivery(stripeSignedTrigger(),
		map[string]secrets.Secret{v1.WebhookSchemeStripe: key}, headers, body, at)
	require.Error(t, err, "a signature over the body alone was accepted as a Stripe signature, so the "+
		"timestamp is not part of what is signed and a captured delivery can be re-timestamped")
}

// TestAStaleStripeDeliveryIsRefused pins the replay window in both directions.
func TestAStaleStripeDeliveryIsRefused(t *testing.T) {
	t.Parallel()

	key := signingKey("whsec_stripe")
	body := []byte(`{"id":"evt_1"}`)
	at := time.Unix(1755043200, 0)
	headers := map[string]string{v1.StripeSignatureHeader: v1.SignStripeBody(key, body, at)}

	keys := map[string]secrets.Secret{v1.WebhookSchemeStripe: key}

	err := v1.VerifyWebhookDelivery(stripeSignedTrigger(), keys, headers, body,
		at.Add(v1.WebhookReplayWindow+time.Second))
	require.Error(t, err, "a delivery older than the replay window was accepted")

	err = v1.VerifyWebhookDelivery(stripeSignedTrigger(), keys, headers, body,
		at.Add(-(v1.WebhookReplayWindow + time.Second)))
	require.Error(t, err, "a delivery timestamped in the future beyond the replay window was accepted")
}

// TestASchemeThisBuildCannotVerifyIsRefused covers the arm that must never fall
// through to acceptance.
func TestASchemeThisBuildCannotVerifyIsRefused(t *testing.T) {
	t.Parallel()

	trigger := hmacTrigger()
	trigger.Verify = map[string]*v1.Value{
		"paypal_v2": {Kind: &v1.Value_SecretRef{
			SecretRef: &v1.SecretRef{Scheme: "env", Name: "WEBHOOK_SECRET"},
		}},
	}

	err := v1.VerifyWebhookDelivery(trigger,
		map[string]secrets.Secret{"paypal_v2": signingKey("k")}, nil, nil, time.Now())
	require.Error(t, err, "a scheme nothing implements verified a delivery")
}

// TestEveryDeclarableSchemeIsImplemented keeps the closed set a file may name and
// the set this build can check from drifting apart.
//
// The drift is silent in the worst direction: a scheme `flow validate` accepts and
// no verifier implements is a webhook that compiles, deploys, and refuses every
// genuine delivery forever.
func TestEveryDeclarableSchemeIsImplemented(t *testing.T) {
	t.Parallel()

	key := signingKey("whsec_test")
	body := []byte(`{"id":"evt_1"}`)

	for _, scheme := range v1.WebhookVerificationSchemes() {
		trigger := hmacTrigger()
		trigger.Verify = map[string]*v1.Value{
			scheme: {Kind: &v1.Value_SecretRef{
				SecretRef: &v1.SecretRef{Scheme: "env", Name: "WEBHOOK_SECRET"},
			}},
		}

		err := v1.VerifyWebhookDelivery(trigger,
			map[string]secrets.Secret{scheme: key}, nil, body, time.Now())
		require.Error(t, err, "scheme %q accepted a delivery with no signature at all", scheme)
		assert.NotContains(t, err.Error(), "is not one this build can verify",
			"scheme %q is declarable in a Flowfile and has no verifier, so every genuine delivery to a "+
				"webhook naming it would be refused", scheme)
	}
}

// TestAVerificationErrorNamesNoSecret is the containment shape, on the one error
// that is produced with a key in scope.
func TestAVerificationErrorNamesNoSecret(t *testing.T) {
	t.Parallel()

	const value = "whsec_super_secret_value"

	key := signingKey(value)
	err := v1.VerifyWebhookDelivery(hmacTrigger(),
		map[string]secrets.Secret{v1.WebhookSchemeHMACSHA256: key},
		map[string]string{v1.WebhookSignatureHeader: "00"}, []byte(`{}`), time.Now())
	require.Error(t, err)

	assert.False(t, strings.Contains(err.Error(), value),
		"a refusal carried the signing key, and a refusal is logged")
}
