package main

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"

	webhookv1 "github.com/picatz/flowstate/plugins/webhook/gen/webhook/v1"
)

const (
	maxURLBytes            = 2048
	maxBodyBytes           = 1 << 20
	maxKeyBytes            = 4096
	maxHeaders             = 16
	maxHeaderValueBytes    = 1024
	maxIdempotencyKeyBytes = 255
	maxResponseBytes       = 64 << 10

	// requestTimeout caps one delivery in addition to the operator policy's own
	// bound, so a policy that left requests unbounded does not leave this task
	// unbounded.
	requestTimeout = 30 * time.Second

	idempotencyHeader = "Idempotency-Key"
	redacted          = "[redacted]"
)

var headerNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,63}$`)

// ownedHeaders are the request headers a caller may not set through `headers`.
// The credential headers would put a secret in the recorded input; the rest
// are the task's own (the signature, the idempotency key) or framing the
// transport decides. Compared case-insensitively.
var ownedHeaders = []string{
	"authorization", "proxy-authorization", "cookie",
	"host", "content-length", "transfer-encoding", "connection", "expect", "te", "trailer", "upgrade",
	strings.ToLower(idempotencyHeader),
	strings.ToLower(flowstatev1.WebhookSignatureHeader),
	strings.ToLower(flowstatev1.StripeSignatureHeader),
}

func webhookSend(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if egressPolicy == nil {
		return nil, sdk.PermissionDenied(
			"webhook.send has no usable egress policy, so no destination is authorized: %v", egressRefusal)
	}

	var in webhookv1.SendInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}
	key, err := keyFromValue(in.GetSigningKey())
	if err != nil {
		return nil, err
	}
	if err := validateSend(&in); err != nil {
		return nil, err
	}

	// The SDK's client: the operator's egress policy plus the credential
	// marking. The signature header is a credential no header name shows, so
	// deliver marks the request itself.
	governed, err := sdk.HTTPClient()
	if err != nil {
		return nil, sdk.PermissionDenied("webhook.send has no usable egress policy: %v", err)
	}

	out, err := deliver(ctx, governed, &in, key, time.Now())
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(out)
}

func keyFromValue(v *flowstatev1.Value) (secrets.Secret, error) {
	if v == nil {
		return secrets.Secret{}, sdk.InvalidInput("signing_key is required")
	}
	switch kind := v.GetKind().(type) {
	case *flowstatev1.Value_Literal:
		s, ok := kind.Literal.GetKind().(*expr.Value_StringValue)
		if !ok || s.StringValue == "" || len(s.StringValue) > maxKeyBytes {
			return secrets.Secret{}, sdk.InvalidInput("signing_key must resolve to a non-empty string no longer than %d bytes", maxKeyBytes)
		}
		return secrets.NewSecret(secrets.NewRef("webhook", "signing_key"), s.StringValue), nil
	case *flowstatev1.Value_SecretRef:
		return secrets.Secret{}, sdk.Failed("signing_key reached webhook.send as an unresolved secret reference; the host must resolve required secret inputs before plugin execution")
	default:
		return secrets.Secret{}, sdk.InvalidInput("signing_key must resolve to a string")
	}
}

func validateSend(in *webhookv1.SendInputs) error {
	if len(in.GetUrl()) > maxURLBytes {
		return sdk.InvalidInput("url must be no longer than %d bytes", maxURLBytes)
	}
	u, err := url.Parse(in.GetUrl())
	if err != nil || !u.IsAbs() || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return sdk.InvalidInput("url must be an absolute http or https URL")
	}
	if u.User != nil {
		return sdk.InvalidInput("url must not carry credentials; a secret in a URL is recorded with the step's input")
	}
	if len(in.GetBody()) > maxBodyBytes {
		return sdk.InvalidInput("body must be no longer than %d bytes", maxBodyBytes)
	}
	if !slices.Contains(flowstatev1.WebhookSigningSchemes(), schemeOf(in)) {
		return sdk.InvalidInput("scheme must be one of %s", strings.Join(flowstatev1.WebhookSigningSchemes(), ", "))
	}
	if k := in.GetIdempotencyKey(); len(k) > maxIdempotencyKeyBytes || !printableASCII(k) {
		return sdk.InvalidInput("idempotency_key must be at most %d printable ASCII characters", maxIdempotencyKeyBytes)
	}
	if len(in.GetHeaders()) > maxHeaders {
		return sdk.InvalidInput("headers must name at most %d headers", maxHeaders)
	}
	for name, value := range in.GetHeaders() {
		if !headerNamePattern.MatchString(name) {
			return sdk.InvalidInput("a header name must be 1 to 64 letters, digits or hyphens")
		}
		if slices.Contains(ownedHeaders, strings.ToLower(name)) {
			return sdk.InvalidInput("header %q cannot be set here: credentials do not belong in recorded inputs, and the signature, idempotency key and framing headers belong to the task", name)
		}
		if len(value) > maxHeaderValueBytes || !utf8.ValidString(value) || strings.ContainsAny(value, "\r\n\x00") {
			return sdk.InvalidInput("the value of header %q must be at most %d bytes of text without line breaks", name, maxHeaderValueBytes)
		}
	}
	return nil
}

func schemeOf(in *webhookv1.SendInputs) string {
	return cmp.Or(in.GetScheme(), flowstatev1.WebhookSchemeHMACSHA256)
}

func printableASCII(s string) bool {
	for i := range len(s) {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// deliver signs the body with the engine's own signer and sends it once. There
// are no hidden retries: the workflow's retry policy is the one mechanism, and
// only a definite no-delivery outcome is classified as retryable.
func deliver(ctx context.Context, client *http.Client, in *webhookv1.SendInputs, key secrets.Secret, now time.Time) (*webhookv1.SendOutputs, error) {
	// The shared signer: the arithmetic the inbound verifier checks against, so
	// there is no second implementation here to drift from it.
	sigHeader, sigValue, err := flowstatev1.SignWebhookDelivery(schemeOf(in), key, []byte(in.GetBody()), now)
	if err != nil {
		return nil, sdk.InvalidInput("signing the delivery: %v", err)
	}

	// A signature is what the receiver authenticates by, so the operator's
	// `credentials` rules must see this request as credentialed.
	ctx, cancel := context.WithTimeout(sdk.WithCredentials(ctx), requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, in.GetUrl(), bytes.NewReader([]byte(in.GetBody())))
	if err != nil {
		return nil, sdk.InvalidInput("building the webhook request: url is not usable")
	}
	for name, value := range in.GetHeaders() {
		req.Header.Set(name, value)
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(sigHeader, sigValue)
	if k := in.GetIdempotencyKey(); k != "" {
		req.Header.Set(idempotencyHeader, k)
	}

	// A signed delivery is addressed to the receiver the author named; it is
	// not followed to another host, whatever the policy would allow there.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	resp, err := client.Do(req)
	if err != nil {
		return nil, classifyTransportError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		delay := retryAfter(resp.Header.Get("Retry-After"))
		return nil, sdk.UnavailableAfter(delay, "the receiver rate-limited the delivery; retry after %s", delay)
	}

	// Read past the cap by the longest thing to redact, so a secret that
	// straddles the cut is redacted whole before the cut is made.
	scrub := scrubTargets(key, sigValue)
	limit := int64(maxResponseBytes + maxKeyBytes + 1)
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, limit))
	truncated := int64(len(raw)) == limit
	if readErr != nil {
		if _, tooLarge := errors.AsType[*netpolicy.BodyTooLargeError](readErr); !tooLarge {
			return nil, sdk.OutcomeUnknown("the receiver's response could not be read after the delivery was sent; it may have been received, so it is not retried automatically")
		}
		truncated = true
	}

	switch {
	case resp.StatusCode >= 500:
		return nil, sdk.OutcomeUnknown("the receiver answered HTTP %d after the delivery was sent; it may have been received, so it is not retried automatically", resp.StatusCode)
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return nil, sdk.Failed("the receiver refused the delivery with HTTP %d", resp.StatusCode)
	}

	// Case-insensitively: a receiver that echoes a hex digest upper-cased has
	// still returned a signature that is replayable for this body.
	text := string(raw)
	for _, target := range scrub {
		text = regexp.MustCompile("(?i)"+regexp.QuoteMeta(target)).ReplaceAllLiteralString(text, redacted)
	}
	if len(text) > maxResponseBytes {
		text, truncated = text[:maxResponseBytes], true
	}

	return &webhookv1.SendOutputs{
		Status:            int32(resp.StatusCode),
		Response:          strings.ToValidUTF8(text, "�"),
		ResponseTruncated: truncated,
	}, nil
}

// scrubTargets lists what a receiver's echo must not return: the key, the
// whole signature header value, and the digest inside it, longest first so a
// containing value is replaced before its parts.
func scrubTargets(key secrets.Secret, sigValue string) []string {
	targets := []string{key.Reveal(), sigValue}
	if _, digest, ok := strings.Cut(sigValue, "v1="); ok {
		targets = append(targets, digest)
	}
	slices.SortFunc(targets, func(a, b string) int { return cmp.Compare(len(b), len(a)) })
	return slices.DeleteFunc(targets, func(s string) bool { return s == "" })
}

func classifyTransportError(err error) error {
	if limited, ok := errors.AsType[*netpolicy.RateLimitedError](err); ok {
		if limited.AfterRedirect {
			return sdk.OutcomeUnknown("the delivery was redirected before the operator egress policy rate-limited the next hop; it may have been received, so it is not retried automatically")
		}
		delay := boundedRetryAfter(limited.RetryAfter)
		return sdk.UnavailableAfter(delay, "operator egress policy rate-limited webhook.send before it was sent; retry after %s", delay)
	}
	if _, ok := errors.AsType[*netpolicy.DenyError](err); ok {
		return sdk.PermissionDenied("deployment egress policy denied webhook.send")
	}
	if _, ok := errors.AsType[net.Error](err); ok || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return sdk.OutcomeUnknown("the connection failed after the delivery began; it may have been received, so it is not retried automatically")
	}
	return sdk.OutcomeUnknown("the delivery failed after the request began; it may have been received, so it is not retried automatically")
}

func retryAfter(value string) time.Duration {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
	if err != nil || seconds <= 0 {
		return time.Second
	}
	return boundedRetryAfter(time.Duration(seconds) * time.Second)
}

func boundedRetryAfter(delay time.Duration) time.Duration {
	return min(max(delay, time.Second), 5*time.Minute)
}
