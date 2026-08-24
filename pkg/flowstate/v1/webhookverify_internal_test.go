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
	signWebhookPayload = func(key secrets.Secret, parts ...[]byte) []byte {
		c++
		for _, part := range parts {
			b += len(part)
		}

		return original(key, parts...)
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

// TestWebhookVerificationHashesTheBodyWithoutCopyingIt pins the half of the
// uniformity claim that counting bytes cannot see.
//
// Both schemes hash the same number of bytes they always did — the counting
// test above says so — but Stripe used to reach that number by building
// `timestamp + "." + body` first, a second body-sized pass spent copying bytes
// the hash was about to read anyway. Against the decoy an unrouted request
// spends, which signs the body alone, that is a real difference in the same
// shape as the present/absent signal #955 removed: smaller, and the kind that
// grows back once nobody is looking (#973).
//
// Asserted by identity rather than by timing or by allocation count. A slice
// sharing its backing array with the caller's body was not copied, which is
// exactly the claim; `testing.AllocsPerRun` answers a nearby question flakily,
// and a timing assertion on a shared runner answers none of them.
func TestWebhookVerificationHashesTheBodyWithoutCopyingIt(t *testing.T) {
	key := secrets.NewSecret(secrets.NewRef("env", "WEBHOOK_SECRET"), "shh")
	body := []byte(`{"id":"evt_1","data":"a body long enough to be worth not copying"}`)
	now := time.Now()

	hashedTheBodyItself := func(t *testing.T) *bool {
		t.Helper()

		var seen bool
		original := signWebhookPayload
		signWebhookPayload = func(key secrets.Secret, parts ...[]byte) []byte {
			for _, part := range parts {
				if len(part) == len(body) && &part[0] == &body[0] {
					seen = true
				}
			}

			return original(key, parts...)
		}
		t.Cleanup(func() { signWebhookPayload = original })

		return &seen
	}

	t.Run("hmac-sha256", func(t *testing.T) {
		seen := hashedTheBodyItself(t)
		require.NoError(t, verifyHMACSHA256(key,
			map[string]string{WebhookSignatureHeader: "sha256=" + SignWebhookBody(key, body)}, body))

		require.True(t, *seen, "the body reached the hash as a copy")
	})

	t.Run("stripe", func(t *testing.T) {
		seen := hashedTheBodyItself(t)
		require.NoError(t, verifyStripe(key,
			map[string]string{StripeSignatureHeader: SignStripeBody(key, body, now)}, body, now))

		require.True(t, *seen,
			"the body reached the hash as a copy, so this delivery paid a second body-sized pass the decoy does not")
	})
}
