package flowstatev1

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// The arithmetic behind `verify:`, which is the half of a webhook that decides
// whether anything else happens at all.
//
// It lives here rather than in the receiver for the reason [CheckWebhookTrigger]
// does: the declaration and the check have to agree about what a scheme *is*, and
// a rule with two implementations eventually has two meanings. A scheme named in
// [webhookVerificationSchemes] and not implemented here is a delivery that can
// never be accepted, so the two lists are asserted against each other by
// [TestEveryDeclarableSchemeIsImplemented] rather than kept in step by hand.
//
// Everything here is a pure function of the raw body, the headers and a key. It
// performs no I/O, resolves nothing, and is deliberately callable with a stored
// delivery: a signature is either arithmetic somebody can reproduce offline or it
// is a thing only production can check.
//
// # What is compared, and how
//
// Every comparison of a computed digest against a supplied one goes through
// [hmac.Equal], which is constant-time. Nothing here compares a *secret* to
// anything — the key is an input to the digest, never an operand of a comparison —
// so the only value an attacker can vary the timing of is their own signature
// against a digest they cannot predict.
//
// The refusals below are worded for an operator reading a log, and are never the
// sentence a sender is given: see the receiver, which answers every refusal with
// one response so that "wrong signature" and "no such webhook" are not
// distinguishable from outside.

const (
	// WebhookSignatureHeader is where the generic [WebhookSchemeHMACSHA256]
	// scheme reads a delivery's signature.
	//
	// Named for Flowstate rather than for a provider because the generic scheme
	// is the one an unfamiliar sender is configured against: whoever is pointing
	// their system at this endpoint chooses the header, and choosing it here
	// means there is exactly one to document. A provider with a header of its
	// own gets a named scheme instead, which is what [WebhookSchemeStripe] is.
	//
	// The value is a hex-encoded HMAC-SHA256 of the raw body, optionally
	// prefixed `sha256=` — the spelling GitHub, Shopify and most others use, so
	// a sender already emitting one needs no adapter.
	WebhookSignatureHeader = "X-Flowstate-Signature"

	// StripeSignatureHeader is where [WebhookSchemeStripe] reads its signature,
	// and is Stripe's own spelling.
	StripeSignatureHeader = "Stripe-Signature"

	// hmacPrefix is the optional algorithm marker on a generic signature.
	hmacPrefix = "sha256="
)

// WebhookReplayWindow is how far a signed timestamp may be from now.
//
// It applies to [WebhookSchemeStripe], which is the scheme that signs one: a
// signature with no timestamp in it is replayable forever by whoever captured it,
// and the window is the whole reason the named scheme exists rather than the
// generic one being pointed at Stripe's header.
//
// Five minutes is Stripe's own documented default tolerance. It bounds the value
// of a captured delivery rather than the cost of one, so it is deliberately not a
// knob: a deployment that widened it would be widening the replay window for an
// attacker rather than for itself.
const WebhookReplayWindow = 5 * time.Minute

// maxSignaturesPerHeader bounds how many candidate signatures one header may
// offer.
//
// Stripe sends more than one while a signing secret is being rotated, so the
// number cannot be one — and the work of checking them is an HMAC over the whole
// body each, chosen by the sender, which is the shape of an amplifier. Eight is
// far past any rotation and short of anything that costs a receiver.
const maxSignaturesPerHeader = 8

// VerifyWebhookDelivery reports whether a delivery satisfies what its trigger
// declared.
//
// Every declared scheme must verify, not merely one of them. A `verify:` block
// with two entries is a sender that signs twice, and accepting a delivery that
// satisfied only the weaker half would make adding a scheme a way to *weaken* a
// webhook — the opposite of what somebody writing a second entry means. It is
// also the fail-closed reading of a set of requirements, which is the standing
// rule when a design has not settled the question.
//
// keys holds the resolved signing material, one entry per scheme the trigger
// names, resolved when configuration loaded rather than when this delivery
// arrived. A scheme with no key refuses: an unverifiable delivery is refused,
// never accepted on the grounds that it could not be checked.
//
// now is passed rather than read so that the replay window is testable and so
// that a receiver and a rehearsal cannot disagree about what "recent" means.
//
// The error is for the operator's log. It names which scheme failed and why,
// which is exactly what must not reach the sender.
//
// # Constant work, regardless of what is declared
//
// This spends exactly one verification of every scheme this build knows —
// [webhookVerificationSchemes], currently two — on every call, whatever the
// trigger declares and whatever keys resolved. A trigger naming one scheme, a
// trigger naming both, and a trigger whose one scheme resolved no key all cost
// the same: an outside party timing a known route could otherwise count how
// many schemes it requires, or notice that a misconfigured route (no key
// resolved) answers *faster* than a working one, from the delivery it sends
// itself (#973). A scheme not declared, or declared with no key, is still
// verified — against [webhookDecoyKeys], a key nobody holds — and the result
// discarded; only a declared, resolved scheme's outcome decides the return.
// [SpendWebhookVerificationWork] is the same work with nothing to decide,
// which is what an unrouted delivery spends so routed and unrouted cost alike.
//
// The loop never returns before every scheme has been tried, for the same
// reason: stopping at the first declared scheme that fails would make a
// two-scheme trigger cost one HMAC when its first scheme is wrong and two when
// its first scheme is right — the identical leak in a different shape.
func VerifyWebhookDelivery(trigger *WebhookTrigger, keys map[string]secrets.Secret, headers map[string]string, body []byte, now time.Time) error {
	// The declaration's own rules first, because a malformed trigger cannot be a
	// basis for accepting anything — the same order [BindWebhookTriggerInputs]
	// takes, and the reason a receiver cannot skip this by having checked at load:
	// checking twice costs nothing and a missing check costs everything.
	if err := CheckWebhookTrigger(trigger); err != nil {
		return err
	}

	verify := trigger.GetVerify()

	var (
		failed     error
		unresolved string
	)

	for _, scheme := range webhookVerificationSchemes {
		_, declared := verify[scheme]
		key, held := keys[scheme]
		usable := held && !key.IsZero()

		effectiveKey := key
		if !declared || !usable {
			// Not this delivery's business either way: a scheme it never named,
			// or one it named that this deployment cannot check. Verified anyway,
			// against a key nobody has, so the work spent does not depend on
			// which case this is.
			effectiveKey = webhookDecoyKeys[scheme]
		}

		err := verifyScheme(scheme, effectiveKey, headers, body, now)

		if !declared {
			continue
		}
		if !usable {
			if unresolved == "" {
				unresolved = scheme
			}
			continue
		}
		if err != nil && failed == nil {
			failed = fmt.Errorf("webhook %q: %w", trigger.GetName(), err)
		}
	}

	if unresolved != "" {
		return fmt.Errorf("webhook %q verifies with %q and this deployment resolved no key for it, "+
			"so the delivery cannot be checked and is refused", trigger.GetName(), unresolved)
	}

	return failed
}

// verifyScheme dispatches one scheme's arithmetic. Shared by
// [VerifyWebhookDelivery], which uses the result, and
// [SpendWebhookVerificationWork], which spends the same work and discards it.
func verifyScheme(scheme string, key secrets.Secret, headers map[string]string, body []byte, now time.Time) error {
	switch scheme {
	case WebhookSchemeHMACSHA256:
		return verifyHMACSHA256(key, headers, body)
	case WebhookSchemeStripe:
		return verifyStripe(key, headers, body, now)
	default:
		// Unreachable: both callers range over [webhookVerificationSchemes],
		// which this switch covers exhaustively. Kept because the alternative to
		// an arm here is a switch that falls through to acceptance, which is the
		// one way this function must never be wrong.
		return fmt.Errorf("scheme %q is not one this build can verify", scheme)
	}
}

// webhookDecoyKeys are keys nobody holds, one per scheme this build knows,
// generated once per process.
//
// [VerifyWebhookDelivery] verifies against one of these whenever a scheme is
// not this delivery's to answer for — not declared, or declared with no key
// resolved — so that the work spent does not reveal which case it was.
// [SpendWebhookVerificationWork] uses the same keys for the same reason, for a
// delivery that matched no route at all.
//
// Generated per process rather than fixed, for the reason
// [WebhookReceiver]'s own decoy was: a constant would be a value an attacker
// could compute against, and a zero key would skip the work this exists to
// spend.
var webhookDecoyKeys = func() map[string]secrets.Secret {
	keys := make(map[string]secrets.Secret, len(webhookVerificationSchemes))
	for _, scheme := range webhookVerificationSchemes {
		buf := make([]byte, sha256.Size)
		if _, err := rand.Read(buf); err != nil {
			// rand.Read only fails when the system's entropy source is broken,
			// which nothing downstream can recover from either.
			panic("webhookverify: generating a decoy key: " + err.Error())
		}
		keys[scheme] = secrets.NewSecret(secrets.NewRef("internal", "webhook-verify-decoy-"+scheme),
			hex.EncodeToString(buf))
	}

	return keys
}()

// SpendWebhookVerificationWork performs the same total hashing
// [VerifyWebhookDelivery] spends on any delivery whose route exists, against
// keys nobody holds, and reports nothing.
//
// It is what a receiver calls for a delivery that matched no route at all, so
// that an unrouted delivery costs what a routed one costs regardless of how
// many schemes that route would have declared — [VerifyWebhookDelivery] spends
// a constant amount of work per call (#973), and this is that same amount with
// no trigger to decide against.
func SpendWebhookVerificationWork(headers map[string]string, body []byte, now time.Time) {
	for _, scheme := range webhookVerificationSchemes {
		_ = verifyScheme(scheme, webhookDecoyKeys[scheme], headers, body, now)
	}
}

// verifyHMACSHA256 checks the generic scheme: an HMAC-SHA256 of the raw body.
//
// The *raw* body, before any decoding, which is the whole reason a receiver reads
// the bytes once and keeps them: a signature over a re-encoded document is a
// signature over whatever the encoder happened to produce, and JSON has enough
// freedom (key order, escaping, number spelling) that a re-encoded body differs
// from the signed one for reasons nobody can see.
func verifyHMACSHA256(key secrets.Secret, headers map[string]string, body []byte) error {
	supplied := webhookHeader(headers, WebhookSignatureHeader)
	// Compute the digest before inspecting the attacker-controlled header. An
	// unrouted request spends this same body-sized work under a decoy key; an
	// early return here would therefore reveal that this route exists.
	expected := signWebhookPayload(key, body)
	if supplied == "" {
		return fmt.Errorf("the delivery carried no %s header", WebhookSignatureHeader)
	}

	for _, candidate := range splitSignatures(strings.TrimPrefix(supplied, hmacPrefix)) {
		digest, err := hex.DecodeString(candidate)
		if err != nil {
			continue
		}
		if hmac.Equal(digest, expected) {
			return nil
		}
	}

	return fmt.Errorf("no signature in %s matched the body under this deployment's key", WebhookSignatureHeader)
}

// verifyStripe checks Stripe's `Stripe-Signature` construction.
//
// A named scheme rather than the generic one pointed at Stripe's header, because
// the payload that is signed is `<timestamp>.<body>` rather than the body. Getting
// that wrong is not a check that fails, it is a check that *passes on a forged
// body* whenever an attacker can choose the part the implementation happened to
// hash — which is why this is written down once here rather than configured.
func verifyStripe(key secrets.Secret, headers map[string]string, body []byte, now time.Time) error {
	supplied := webhookHeader(headers, StripeSignatureHeader)

	var (
		timestamp  string
		signatures []string
	)
	for _, part := range strings.Split(supplied, ",") {
		name, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			continue
		}
		switch name {
		case "t":
			timestamp = value
		case "v1":
			if len(signatures) < maxSignaturesPerHeader {
				signatures = append(signatures, value)
			}
		}
	}

	seconds, secondsErr := strconv.ParseInt(timestamp, 10, 64)
	// The signed payload is built from the *parsed* seconds, so a padded or
	// oddly-spelled timestamp cannot make the payload something other than what
	// the window was checked against. When it does not parse there is no such
	// number, and the header's own text must not stand in for one: `t=` carries
	// whatever a sender wrote, bounded only by MaxHeaderBytes, so hashing it
	// would make this route's work scale with a value the sender chooses — a
	// larger and more precise oracle than the one this ordering removes.
	var signingTimestamp string
	if secondsErr == nil {
		signingTimestamp = strconv.FormatInt(seconds, 10)
	}

	// Spend the body-sized authentication work before any header-shape refusal.
	// Unknown routes do the same under a decoy key, so a missing or malformed
	// header must not turn a configured Stripe route into a timing oracle.
	//
	// Written into the hash in pieces rather than joined first. Joining copied
	// the whole body to hash bytes it already had, which cost a second
	// body-sized pass on every delivery — small beside a full HMAC of the same
	// bytes, but the same *shape* of signal as the one this ordering removes,
	// and measurable against the decoy, which signs the body alone (#973).
	expected := signWebhookPayload(key, []byte(signingTimestamp), stripeSignedSeparator, body)

	if supplied == "" {
		return fmt.Errorf("the delivery carried no %s header", StripeSignatureHeader)
	}

	if timestamp == "" || len(signatures) == 0 {
		return fmt.Errorf("the %s header is not `t=<unix seconds>,v1=<hex>`", StripeSignatureHeader)
	}

	if secondsErr != nil {
		return fmt.Errorf("the %s header's timestamp is not a whole number of seconds", StripeSignatureHeader)
	}

	// Both directions. A delivery from the future is as much a sign of a forged
	// or replayed timestamp as one from last week, and bounding only the past
	// would let a captured delivery be replayed indefinitely by re-signing it
	// with a timestamp far ahead — which the attacker cannot do without the key,
	// but a clock that has jumped can hand them for free.
	skew := now.Sub(time.Unix(seconds, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > WebhookReplayWindow {
		return fmt.Errorf("the %s header's timestamp is %s away from now, outside the %s replay window",
			StripeSignatureHeader, skew.Round(time.Second), WebhookReplayWindow)
	}

	// signingTimestamp was built from the parsed seconds rather than from the
	// header's text so that a padded or oddly-spelled timestamp cannot make the
	// payload something other than what the window was checked against.
	for _, candidate := range signatures {
		digest, err := hex.DecodeString(candidate)
		if err != nil {
			continue
		}
		if hmac.Equal(digest, expected) {
			return nil
		}
	}

	return fmt.Errorf("no v1 signature in %s matched the signed payload under this deployment's key",
		StripeSignatureHeader)
}

// signWebhookPayload is [signHMACSHA256], reached through a variable so that
// one internal test can count what a verification hashes.
//
// The ordering in [verifyHMACSHA256] and [verifyStripe] is load-bearing rather
// than incidental: every refusal spends the same body-sized work an unrouted
// delivery spends under the receiver's decoy key, so that "no such webhook" and
// "wrong signature" cost the same. That is a claim about *work*, and a claim
// about work that nothing measures is one the next tidy-up silently repeals by
// restoring an early return. This is the same instrument, and the same reason,
// as pathChecker.ownerOf in cmd/flow.
// stripeSignedSeparator is the byte between Stripe's timestamp and the body in
// the payload it signs. A package-level slice so that hashing it allocates
// nothing per delivery.
var stripeSignedSeparator = []byte{'.'}

var signWebhookPayload = signHMACSHA256

// signHMACSHA256 is the one place a key is revealed, and it reveals it into an
// HMAC and nothing else.
//
// Exported nowhere: the value goes from [secrets.Secret.Reveal] into
// [hmac.New] within one statement, so there is no variable holding it for a later
// log line or error to reach. What comes back is a digest, which is safe to
// compare, print and return.
func signHMACSHA256(key secrets.Secret, parts ...[]byte) []byte {
	mac := hmac.New(sha256.New, []byte(key.Reveal()))
	for _, part := range parts {
		mac.Write(part)
	}

	return mac.Sum(nil)
}

// SignWebhookBody returns the hex signature a sender would put in
// [WebhookSignatureHeader] for this body under this key.
//
// It exists so that a test, a fixture generator or an operator reproducing a
// refusal computes the signature with the same code that checks it. A helper that
// re-derived the arithmetic would be a second implementation of the thing this
// file exists to have only one of.
func SignWebhookBody(key secrets.Secret, body []byte) string {
	return hex.EncodeToString(signHMACSHA256(key, body))
}

// SignStripeBody returns the `Stripe-Signature` header value a sender would send
// for this body at this time, for the same reason [SignWebhookBody] exists.
func SignStripeBody(key secrets.Secret, body []byte, at time.Time) string {
	seconds := strconv.FormatInt(at.Unix(), 10)
	signed := make([]byte, 0, len(seconds)+1+len(body))
	signed = append(signed, seconds...)
	signed = append(signed, '.')
	signed = append(signed, body...)

	return fmt.Sprintf("t=%s,v1=%s", seconds, hex.EncodeToString(signHMACSHA256(key, signed)))
}

// splitSignatures reads the candidates one header may offer, bounded.
//
// A single value is the ordinary case; a comma-separated list is what a sender
// mid-rotation emits. Bounded by [maxSignaturesPerHeader] because each candidate
// costs an HMAC over the whole body and the count is the sender's choice.
func splitSignatures(value string) []string {
	parts := strings.Split(value, ",")
	if len(parts) > maxSignaturesPerHeader {
		parts = parts[:maxSignaturesPerHeader]
	}
	for i, part := range parts {
		parts[i] = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(part), hmacPrefix))
	}

	return parts
}

// webhookHeader reads a header case-insensitively.
//
// The same normalization [NewWebhookEvent] performs on the way into `event`, and
// performed here for the same reason: HTTP header names are case-insensitive, so
// a receiver that matched them exactly would refuse a genuine delivery from a
// sender that capitalized differently — a refusal nobody could debug from either
// end.
func webhookHeader(headers map[string]string, name string) string {
	if value, ok := headers[strings.ToLower(name)]; ok {
		return value
	}
	for key, value := range headers {
		if strings.EqualFold(key, name) {
			return value
		}
	}

	return ""
}
