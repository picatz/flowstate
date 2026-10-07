package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// Provider schemes, held to each provider's own published vector where one
// exists, so the arithmetic is checked against the provider's documentation
// rather than against this file's reading of it.

func schemeTrigger(scheme string) *v1.WebhookTrigger {
	trigger := hmacTrigger()
	trigger.Verify = map[string]*v1.Value{scheme: {Kind: &v1.Value_SecretRef{
		SecretRef: &v1.SecretRef{Scheme: "env", Name: "WEBHOOK_SECRET"},
	}}}

	return trigger
}

func TestProviderSchemesAcceptTheirPublishedVectors(t *testing.T) {
	t.Parallel()

	const slackBody = "token=xyzz0WbapA4vBCDEFasx0q6G&team_id=T1DC2JH3J&team_domain=testteamnow&channel_id=G8PSS9T3V&" +
		"channel_name=foobar&user_id=U2CERLKJA&user_name=roadrunner&command=%2Fwebhook-collect&text=&" +
		"response_url=https%3A%2F%2Fhooks.slack.com%2Fcommands%2FT1DC2JH3J%2F397700885554%2F96rGlfmibIGlgcZRskXaIFfN&" +
		"trigger_id=398738663015.47445629121.803a0bc887a14d10d2c447fce8b6703c"

	tests := []struct {
		name    string
		scheme  string
		key     string
		body    string
		headers map[string]string
		now     time.Time
	}{
		{
			// docs.github.com: "Validating webhook deliveries".
			name: "github", scheme: v1.WebhookSchemeGitHub,
			key: "It's a Secret to Everybody", body: "Hello, World!",
			headers: map[string]string{
				"x-hub-signature-256": "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
			},
			now: time.Now(),
		},
		{
			// api.slack.com: "Verifying requests from Slack".
			name: "slack", scheme: v1.WebhookSchemeSlack,
			key: "8f742231b10e8888abcd99yyyzzz85a5", body: slackBody,
			headers: map[string]string{
				"x-slack-signature":         "v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503",
				"x-slack-request-timestamp": "1531420618",
			},
			now: time.Unix(1531420618, 0).Add(time.Minute),
		},
		{
			// Cross-checked with Python's hmac and base64; Shopify publishes no vector.
			name: "shopify", scheme: v1.WebhookSchemeShopify,
			key: "shop-secret", body: `{"id":1}`,
			headers: map[string]string{"x-shopify-hmac-sha256": "plW8VGqq0nDMIEhDqaNNW99asdUCy7EIqLZVcQwtAZ0="},
			now:     time.Now(),
		},
		{
			name: "linear", scheme: v1.WebhookSchemeLinear,
			key: "shop-secret", body: `{"id":1}`,
			headers: map[string]string{"linear-signature": "a655bc546aaad270cc204843a9a34d5bdf5ab1d502cbb108a8b655710c2d019d"},
			now:     time.Now(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			keys := map[string]secrets.Secret{tt.scheme: signingKey(tt.key)}
			trigger := schemeTrigger(tt.scheme)

			require.NoError(t, v1.VerifyWebhookDelivery(trigger, keys, tt.headers, []byte(tt.body), tt.now))
			require.Error(t, v1.VerifyWebhookDelivery(trigger, keys, tt.headers, []byte(tt.body+" "), tt.now),
				"a tampered body verified")
			require.Error(t, v1.VerifyWebhookDelivery(trigger,
				map[string]secrets.Secret{tt.scheme: signingKey("wrong")}, tt.headers, []byte(tt.body), tt.now),
				"a different key verified")
		})
	}
}

// A signature made for one provider is not another's: the generic header is not
// read by a GitHub route, and a Slack-signed body presented in the generic header
// is refused.
func TestProviderSchemesReadOnlyTheirOwnHeader(t *testing.T) {
	t.Parallel()

	key := signingKey("k")
	body := []byte(`{"id":1}`)
	now := time.Now()

	signed, err := v1.SignWebhookDelivery(v1.WebhookSchemeHMACSHA256, key, body, now)
	require.NoError(t, err)

	require.Error(t, v1.VerifyWebhookDelivery(schemeTrigger(v1.WebhookSchemeGitHub),
		map[string]secrets.Secret{v1.WebhookSchemeGitHub: key}, signed, body, now))

	slack, err := v1.SignWebhookDelivery(v1.WebhookSchemeSlack, key, body, now)
	require.NoError(t, err)
	require.Contains(t, slack, "X-Slack-Request-Timestamp")
	require.Error(t, v1.VerifyWebhookDelivery(schemeTrigger(v1.WebhookSchemeHMACSHA256),
		map[string]secrets.Secret{v1.WebhookSchemeHMACSHA256: key},
		map[string]string{v1.WebhookSignatureHeader: slack["X-Slack-Signature"]}, body, now))
}

// Slack's timestamp is signed, so replaying a delivery outside the window, or
// moving the timestamp, is refused.
func TestSlackSchemeRefusesAStaleOrMovedTimestamp(t *testing.T) {
	t.Parallel()

	key := signingKey("k")
	body := []byte(`payload=x`)
	at := time.Unix(1755043200, 0)
	keys := map[string]secrets.Secret{v1.WebhookSchemeSlack: key}
	trigger := schemeTrigger(v1.WebhookSchemeSlack)

	headers, err := v1.SignWebhookDelivery(v1.WebhookSchemeSlack, key, body, at)
	require.NoError(t, err)
	require.NoError(t, v1.VerifyWebhookDelivery(trigger, keys, headers, body, at))

	require.ErrorIs(t, v1.VerifyWebhookDelivery(trigger, keys, headers, body, at.Add(v1.WebhookReplayWindow+time.Minute)),
		v1.ErrWebhookReplayWindow)

	moved := map[string]string{
		"X-Slack-Signature":         headers["X-Slack-Signature"],
		"X-Slack-Request-Timestamp": "1755043201",
	}
	require.ErrorIs(t, v1.VerifyWebhookDelivery(trigger, keys, moved, body, at), v1.ErrWebhookSignatureInvalid)

	missing := map[string]string{"X-Slack-Signature": headers["X-Slack-Signature"]}
	require.Error(t, v1.VerifyWebhookDelivery(trigger, keys, missing, body, at))
}

// A timestamp so far from now that the duration saturates is outside the window
// in both directions, not inside it by overflow.
func TestReplayWindowHoldsAtTheExtremes(t *testing.T) {
	t.Parallel()

	key := signingKey("k")
	body := []byte(`x`)
	now := time.Unix(1755043200, 0)

	slackKeys := map[string]secrets.Secret{v1.WebhookSchemeSlack: key}
	stripeKeys := map[string]secrets.Secret{v1.WebhookSchemeStripe: key}

	for _, at := range []time.Time{time.Unix(1<<62, 0), time.Unix(-(1 << 62), 0)} {
		slack, err := v1.SignWebhookDelivery(v1.WebhookSchemeSlack, key, body, at)
		require.NoError(t, err)
		require.ErrorIs(t, v1.VerifyWebhookDelivery(schemeTrigger(v1.WebhookSchemeSlack), slackKeys, slack, body, now),
			v1.ErrWebhookReplayWindow, "slack at %v", at.Unix())

		stripe, err := v1.SignWebhookDelivery(v1.WebhookSchemeStripe, key, body, at)
		require.NoError(t, err)
		require.ErrorIs(t, v1.VerifyWebhookDelivery(schemeTrigger(v1.WebhookSchemeStripe), stripeKeys, stripe, body, now),
			v1.ErrWebhookReplayWindow, "stripe at %v", at.Unix())
	}
}
