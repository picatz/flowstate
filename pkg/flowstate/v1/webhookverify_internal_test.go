package flowstatev1

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// countedSigning replaces [signWebhookPayload] for the duration of one test and
// reports how many times a verification hashed, and over how many bytes.
//
// Both numbers matter and they answer different questions. The count says a
// refusal did not skip the work — the property the ordering in this file exists
// to keep. The byte total says the work did not *grow* with something a sender
// chose, which is the sharper version of the same oracle.
func countedSigning(t *testing.T) (calls, bytes *int) {
	t.Helper()

	var c, b int
	original := signWebhookPayload
	signWebhookPayload = func(key secrets.Secret, payload []byte) []byte {
		c++
		b += len(payload)

		return original(key, payload)
	}
	t.Cleanup(func() { signWebhookPayload = original })

	return &c, &b
}

// TestEveryWebhookRefusalHashesExactlyOnce pins the property
// server/webhook.go's decoy exists to pair with: an unrouted delivery spends one
// body-sized HMAC under a decoy key, so a *routed* delivery must spend one too,
// whatever is wrong with it. Before this, a missing or malformed signature
// header returned before hashing at all, which made "no such webhook" and "this
// route exists but your signature is wrong" cost visibly different amounts.
//
// It counts rather than times, deliberately. A timing assertion on a shared CI
// runner is a flake generator, and the thing actually being claimed is not "these
// take equally long" but "these do the same work" — which is countable, stable,
// and fails for the one reason worth failing for.
func TestEveryWebhookRefusalHashesExactlyOnce(t *testing.T) {
	key := secrets.NewSecret(secrets.NewRef("env", "WEBHOOK_SECRET"), "shh")
	body := []byte(`{"id":"evt_1"}`)
	now := time.Now()

	t.Run("hmac-sha256", func(t *testing.T) {
		for name, headers := range map[string]map[string]string{
			"a valid signature":   {WebhookSignatureHeader: "sha256=" + SignWebhookBody(key, body)},
			"a wrong signature":   {WebhookSignatureHeader: "sha256=" + strings.Repeat("00", 32)},
			"no signature header": nil,
			"an unparseable hex":  {WebhookSignatureHeader: "sha256=nothexatall"},
			"an empty value":      {WebhookSignatureHeader: ""},
		} {
			t.Run(name, func(t *testing.T) {
				calls, hashed := countedSigning(t)
				_ = verifyHMACSHA256(key, headers, body)

				require.Equal(t, 1, *calls, "a refusal that skips the HMAC is a route oracle")
				require.Equal(t, len(body), *hashed, "the work must be the body's size and nothing else")
			})
		}
	})

	t.Run("stripe", func(t *testing.T) {
		signed := SignStripeBody(key, body, now)
		for name, headers := range map[string]map[string]string{
			"a valid signature":           {StripeSignatureHeader: signed},
			"a wrong signature":           {StripeSignatureHeader: "t=1700000000,v1=" + strings.Repeat("00", 32)},
			"no signature header":         nil,
			"a header of the wrong shape": {StripeSignatureHeader: "not-a-stripe-header"},
			"an unparseable timestamp":    {StripeSignatureHeader: "t=whenever,v1=" + strings.Repeat("00", 32)},
			"a stale timestamp":           {StripeSignatureHeader: "t=1,v1=" + strings.Repeat("00", 32)},
		} {
			t.Run(name, func(t *testing.T) {
				calls, hashed := countedSigning(t)
				_ = verifyStripe(key, headers, body, now)

				require.Equal(t, 1, *calls, "a refusal that skips the HMAC is a route oracle")
				require.LessOrEqual(t, *hashed, len(body)+32,
					"the work must be the body plus a timestamp, not something a sender sized")
			})
		}
	})
}

// TestStripeVerificationDoesNotHashASenderSizedTimestamp is the finding Copilot
// raised on this change, and it is the sharper half of it.
//
// Hashing before the shape checks is right; keeping the header's own text as the
// signed timestamp when it does not parse is not. `t=` carries whatever a sender
// wrote, bounded only by the server's MaxHeaderBytes, so a megabyte of it would
// have made a configured Stripe route hash roughly twice what the decoy hashes —
// a bigger and more precise oracle than the present/absent one being removed.
func TestStripeVerificationDoesNotHashASenderSizedTimestamp(t *testing.T) {
	key := secrets.NewSecret(secrets.NewRef("env", "WEBHOOK_SECRET"), "shh")
	body := []byte(`{"id":"evt_1"}`)
	huge := strings.Repeat("9", 1<<20) + "x" // long, and deliberately not a number

	calls, hashed := countedSigning(t)
	_ = verifyStripe(key, map[string]string{
		StripeSignatureHeader: "t=" + huge + ",v1=" + strings.Repeat("00", 32),
	}, body, time.Now())

	require.Equal(t, 1, *calls)
	require.Less(t, *hashed, len(huge),
		"the signed payload grew with a value the sender chose")
	require.LessOrEqual(t, *hashed, len(body)+32)
}
