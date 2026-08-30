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

// TestStripeVerificationBoundsCommaDelimitedFields pins the field-count bound,
// not merely the signature-count bound. strings.Split used to allocate and walk
// every empty field before the verifier retained at most eight v1 signatures, so
// a comma-filled header could turn the listener's 1 MiB byte allowance into a
// million-element allocation on every routed or decoy verification.
func TestStripeVerificationBoundsCommaDelimitedFields(t *testing.T) {
	key := secrets.NewSecret(secrets.NewRef("env", "WEBHOOK_SECRET"), "shh")
	body := []byte(`{"id":"evt_1"}`)
	now := time.Now()
	header := SignStripeBody(key, body, now) + strings.Repeat(",", 1<<20)

	err := verifyStripe(key, map[string]string{StripeSignatureHeader: header}, body, now)
	require.ErrorContains(t, err, "contains more than")

	allocations := testing.AllocsPerRun(10, func() {
		_ = verifyStripe(key, map[string]string{StripeSignatureHeader: header}, body, now)
	})
	require.Less(t, allocations, float64(100),
		"verification allocated in proportion to the comma-delimited field count")
}

// verifyTrigger builds a well-formed trigger declaring exactly the schemes
// named, each bound to key, for [TestVerifyWebhookDeliverySpendsConstantWork].
func verifyTrigger(schemes ...string) *WebhookTrigger {
	verify := make(map[string]*Value, len(schemes))
	for _, scheme := range schemes {
		verify[scheme] = &Value{Kind: &Value_SecretRef{
			SecretRef: &SecretRef{Scheme: "env", Name: "WEBHOOK_SECRET"},
		}}
	}

	return &WebhookTrigger{
		Name:           "counted",
		Verify:         verify,
		IdempotencyKey: NewExpr(`event.body.id`),
	}
}

// TestVerifyWebhookDeliverySpendsConstantWork pins residual 2 of #973: the
// number of schemes a trigger declares, and whether a declared scheme's key
// resolved, must not change how much [VerifyWebhookDelivery] hashes. Before
// this, a trigger declaring two schemes hashed twice what one declaring a
// single scheme did, and a scheme with no resolved key hashed nothing at all
// — both readable from outside by timing a delivery to a route whose
// `verify:` shape is otherwise unknown.
//
// Counted rather than timed, for the reason [TestEveryWebhookRefusalHashesExactlyOnce]
// gives: the claim is about work, and work is countable without a flake.
func TestVerifyWebhookDeliverySpendsConstantWork(t *testing.T) {
	key := secrets.NewSecret(secrets.NewRef("env", "WEBHOOK_SECRET"), "shh")
	body := []byte(`{"id":"evt_1"}`)
	now := time.Now()

	spend := func(t *testing.T, trigger *WebhookTrigger, keys map[string]secrets.Secret) (calls, hashed int) {
		t.Helper()

		c, b := countedSigning(t)
		_ = VerifyWebhookDelivery(trigger, keys, nil, body, now)

		return *c, *b
	}

	baseline := func(t *testing.T) (int, int) {
		t.Helper()

		return spend(t, verifyTrigger(WebhookSchemeHMACSHA256), map[string]secrets.Secret{
			WebhookSchemeHMACSHA256: key,
		})
	}

	wantCalls, wantHashed := baseline(t)
	require.Equal(t, len(webhookVerificationSchemes), wantCalls,
		"one HMAC per scheme this build knows, not per scheme declared")

	for name, run := range map[string]func(t *testing.T) (int, int){
		"one scheme declared, resolved": baseline,

		"the other scheme declared, resolved": func(t *testing.T) (int, int) {
			return spend(t, verifyTrigger(WebhookSchemeStripe), map[string]secrets.Secret{
				WebhookSchemeStripe: key,
			})
		},

		"both schemes declared, both resolved": func(t *testing.T) (int, int) {
			return spend(t, verifyTrigger(WebhookSchemeHMACSHA256, WebhookSchemeStripe), map[string]secrets.Secret{
				WebhookSchemeHMACSHA256: key,
				WebhookSchemeStripe:     key,
			})
		},

		"one scheme declared, no key resolved at all": func(t *testing.T) (int, int) {
			return spend(t, verifyTrigger(WebhookSchemeHMACSHA256), nil)
		},

		"one scheme declared, a zero-value key": func(t *testing.T) (int, int) {
			return spend(t, verifyTrigger(WebhookSchemeHMACSHA256), map[string]secrets.Secret{
				WebhookSchemeHMACSHA256: {},
			})
		},

		"both schemes declared, one key unresolved": func(t *testing.T) (int, int) {
			return spend(t, verifyTrigger(WebhookSchemeHMACSHA256, WebhookSchemeStripe), map[string]secrets.Secret{
				WebhookSchemeHMACSHA256: key,
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			calls, hashed := run(t)
			require.Equal(t, wantCalls, calls,
				"the number of schemes declared or resolved changed how many times this hashed")
			require.Equal(t, wantHashed, hashed,
				"the number of schemes declared or resolved changed how many bytes this hashed")
		})
	}
}

// TestSpendWebhookVerificationWorkMatchesADeclaredDelivery pins the other half
// of residual 2: an unrouted delivery ([SpendWebhookVerificationWork], what
// the receiver calls when no route matched) must cost exactly what a routed
// one does, whatever that route would have declared — otherwise routed and
// unrouted are a route-existence oracle in the shape #955 already removed
// once.
func TestSpendWebhookVerificationWorkMatchesADeclaredDelivery(t *testing.T) {
	key := secrets.NewSecret(secrets.NewRef("env", "WEBHOOK_SECRET"), "shh")
	body := []byte(`{"id":"evt_1"}`)
	now := time.Now()

	declaredCalls, declaredHashed := func() (int, int) {
		c, b := countedSigning(t)
		_ = VerifyWebhookDelivery(verifyTrigger(WebhookSchemeHMACSHA256, WebhookSchemeStripe),
			map[string]secrets.Secret{WebhookSchemeHMACSHA256: key, WebhookSchemeStripe: key}, nil, body, now)

		return *c, *b
	}()

	unroutedCalls, unroutedHashed := func() (int, int) {
		c, b := countedSigning(t)
		SpendWebhookVerificationWork(nil, body, now)

		return *c, *b
	}()

	require.Equal(t, declaredCalls, unroutedCalls,
		"an unrouted delivery hashed a different number of times than a routed one")
	require.Equal(t, declaredHashed, unroutedHashed,
		"an unrouted delivery hashed a different number of bytes than a routed one")
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
