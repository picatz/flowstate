package flowstatev1

import (
	"encoding/base64"
	"encoding/hex"
)

// webhookSchemeKind is how a scheme's signature is computed and read. A handful
// of constructions cover every provider Flowstate names; a provider is a row in
// [webhookSchemes] that picks one, so a new service whose signature is a known
// construction under a different header is data rather than a new code path.
//
// The constructions stay code on purpose. What is signed is not something a
// Flowfile may spell: a wrong basestring is a check that passes on a forged
// body (see verifyStripe), so it is chosen here, once, and tested against the
// provider's published vectors.
type webhookSchemeKind int

const (
	// kindBodyHMAC: HMAC-SHA256 of the raw body, carried in one header.
	kindBodyHMAC webhookSchemeKind = iota

	// kindStripe: `t=<seconds>,v1=<hex>` over `<t>.<body>`.
	kindStripe

	// kindSlack: `v0=<hex>` over `v0:<timestamp>:<body>`, the timestamp in its
	// own header.
	kindSlack
)

// webhookScheme is one row of the table every direction reads: verification,
// signing, and the header names an outbound task refuses to let a caller set.
type webhookScheme struct {
	name string
	kind webhookSchemeKind

	// header carries the signature.
	header string

	// timestampHeader carries the signed timestamp for kinds that sign one
	// separately from the signature, and is empty otherwise.
	timestampHeader string

	// prefix is what a sender writes before the encoded digest. The body-HMAC
	// kind accepts the digest with or without it; [kindSlack] requires it.
	prefix string

	// base64 selects standard base64 over hex for the digest.
	base64 bool

	// answerEmpty marks a sender that reads anything but a bodyless 200 as a
	// failure of the delivery it just made. Slack shows a non-200, or a body it
	// did not ask for, to the person who clicked; the others read any 2xx.
	answerEmpty bool
}

// webhookSchemes is the one table. Its order is the order a diagnostic lists the
// schemes in: the generic one first, because it is what an unfamiliar provider
// is spelled with.
var webhookSchemes = []webhookScheme{
	{name: WebhookSchemeHMACSHA256, kind: kindBodyHMAC, header: WebhookSignatureHeader},
	{name: WebhookSchemeGitHub, kind: kindBodyHMAC, header: "X-Hub-Signature-256", prefix: hmacPrefix},
	{name: WebhookSchemeShopify, kind: kindBodyHMAC, header: "X-Shopify-Hmac-Sha256", base64: true},
	{name: WebhookSchemeLinear, kind: kindBodyHMAC, header: "Linear-Signature"},
	{name: WebhookSchemeSlack, kind: kindSlack, header: "X-Slack-Signature", timestampHeader: slackTimestampHeader, prefix: slackSignaturePrefix, answerEmpty: true},
	{name: WebhookSchemeStripe, kind: kindStripe, header: StripeSignatureHeader},
}

const (
	slackTimestampHeader  = "X-Slack-Request-Timestamp"
	slackSignaturePrefix  = "v0="
	slackSignedVersionTag = "v0:"
)

func webhookSchemeNames() []string {
	names := make([]string, len(webhookSchemes))
	for i, scheme := range webhookSchemes {
		names[i] = scheme.name
	}

	return names
}

func lookupWebhookScheme(name string) (webhookScheme, bool) {
	for _, scheme := range webhookSchemes {
		if scheme.name == name {
			return scheme, true
		}
	}

	return webhookScheme{}, false
}

// WebhookAnswersEmpty reports whether the trigger is verified under a scheme
// whose sender takes only a bodyless 200 as success (Slack's interactivity and
// events URLs). The receiver then answers an accepted or joined delivery with
// that, in place of 202 and the run's address. A decline or a refusal keeps its
// own status: a failure is the truthful answer there.
func WebhookAnswersEmpty(trigger *WebhookTrigger) bool {
	for name := range trigger.GetVerify() {
		if scheme, ok := lookupWebhookScheme(name); ok && scheme.answerEmpty {
			return true
		}
	}

	return false
}

// WebhookSignatureHeaders returns every header a signing scheme writes, so an
// outbound task can refuse a caller-supplied header of the same name.
func WebhookSignatureHeaders() []string {
	var headers []string
	for _, scheme := range webhookSchemes {
		headers = append(headers, scheme.header)
		if scheme.timestampHeader != "" {
			headers = append(headers, scheme.timestampHeader)
		}
	}

	return headers
}

func (s webhookScheme) decodeDigest(text string) ([]byte, error) {
	if s.base64 {
		return base64.StdEncoding.DecodeString(text)
	}

	return hex.DecodeString(text)
}

func (s webhookScheme) encodeDigest(digest []byte) string {
	if s.base64 {
		return base64.StdEncoding.EncodeToString(digest)
	}

	return hex.EncodeToString(digest)
}
