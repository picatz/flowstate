package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	decisionv1 "github.com/picatz/flowstate/pkg/flowstate/decision/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	openaiv1 "github.com/picatz/flowstate/plugins/openai/gen/openai/v1"
)

const (
	decisionsURL = "https://api.openai.com/v1/decisions"

	maxKeyBytes      = 4096
	maxModelBytes    = 256
	maxEvidenceBytes = 256 << 10

	// maxResponseBytes bounds what is read back. A reply is at most 32 answers
	// of at most 32 probabilities each, a few tens of kilobytes, so this is
	// generous for any honest answer and still small enough that a hostile or
	// broken endpoint cannot make a worker buffer more than a fraction of a
	// megabyte per call.
	maxResponseBytes = 256 << 10

	// requestTimeout bounds one call end to end, including a model that is slow
	// to start. The operator's egress policy may bound it tighter; it cannot be
	// raised here.
	requestTimeout = 2 * time.Minute

	maxErrorBytes = 256
)

var (
	modelPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	errorTypePattern = regexp.MustCompile(`^[a-z_]{1,64}$`)
)

// decisionsResponse is the subset of a reply this plugin reads. Fields it does
// not read, such as an id or a usage count, are ignored rather than refused, so
// a field the beta adds does not stop every call; the fields it does read are
// checked strictly in [answersFromReply].
type decisionsResponse struct {
	Answers []wireAnswer `json:"answers"`
	Error   *struct {
		Type string          `json:"type"`
		Code json.RawMessage `json:"code"`
	} `json:"error"`
}

func openaiDecide(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if egressPolicy == nil {
		return nil, sdk.PermissionDenied(
			"openai.decide has no usable egress policy, so no destination is authorized: %v", egressRefusal)
	}

	var in openaiv1.DecideInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}
	key, err := keyFromValue(in.GetApiKey())
	if err != nil {
		return nil, err
	}
	set, err := validateInputs(&in)
	if err != nil {
		return nil, err
	}

	governed, err := sdk.HTTPClient()
	if err != nil {
		return nil, sdk.PermissionDenied("openai.decide has no usable egress policy: %v", err)
	}

	answers, err := decide(ctx, governed, decisionsURL, key, &in, set)
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(&openaiv1.DecideOutputs{Answers: answers})
}

func keyFromValue(v *flowstatev1.Value) (string, error) {
	if v == nil {
		return "", sdk.InvalidInput("api_key is required")
	}
	switch kind := v.GetKind().(type) {
	case *flowstatev1.Value_Literal:
		s, ok := kind.Literal.GetKind().(*expr.Value_StringValue)
		if !ok || s.StringValue == "" || len(s.StringValue) > maxKeyBytes {
			return "", sdk.InvalidInput("api_key must resolve to a non-empty string no longer than %d bytes", maxKeyBytes)
		}
		// A header value cannot carry a control character, and a line break in a
		// key would be a header injection rather than a credential.
		if strings.IndexFunc(s.StringValue, unicode.IsControl) >= 0 {
			return "", sdk.InvalidInput("api_key must resolve to a single-line string without control characters")
		}
		return s.StringValue, nil
	case *flowstatev1.Value_SecretRef:
		return "", sdk.Failed("api_key reached openai.decide as an unresolved secret reference; the host must resolve required secret inputs before plugin execution")
	default:
		return "", sdk.InvalidInput("api_key must resolve to a string")
	}
}

func validateInputs(in *openaiv1.DecideInputs) (*decisionv1.QuestionSet, error) {
	if len(in.GetModel()) > maxModelBytes || !modelPattern.MatchString(in.GetModel()) {
		return nil, sdk.InvalidInput("model must be a model identifier of at most %d bytes", maxModelBytes)
	}
	if in.GetEvidence() == "" {
		return nil, sdk.InvalidInput("evidence is required")
	}
	if len(in.GetEvidence()) > maxEvidenceBytes || !utf8.ValidString(in.GetEvidence()) {
		return nil, sdk.InvalidInput("evidence must be valid UTF-8 no longer than %d bytes", maxEvidenceBytes)
	}
	return checkQuestionSet(in.GetQuestionSet())
}

// decide sends the request and turns the reply into validated answers.
func decide(ctx context.Context, client *http.Client, endpoint, key string, in *openaiv1.DecideInputs, set *decisionv1.QuestionSet) ([]*decisionv1.Answer, error) {
	body, err := json.Marshal(decisionsRequest{
		Model: in.GetModel(),
		// The evidence is the request's input as given. The API separates it
		// from the questions structurally, so it is not wrapped in markup here.
		Input:     in.GetEvidence(),
		Questions: requestQuestions(set),
	})
	if err != nil {
		return nil, sdk.Failed("encoding the OpenAI request: %v", err)
	}

	// The key is in the standard Authorization header, which the SDK's
	// transport recognizes as a credential on its own, so an operator rule that
	// keeps credentials away from an unapproved host still decides it.
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	// Whether any byte of the request could have left this process. Before
	// that a failure proves nothing was processed; after it a lost connection
	// or a timeout does not, and the provider may be generating a paid answer.
	var sent atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteHeaders: func() { sent.Store(true) },
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, sdk.Failed("building the OpenAI request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, classifyTransportError(err, sent.Load())
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		var tooLarge *netpolicy.BodyTooLargeError
		if errors.As(err, &tooLarge) {
			return nil, sdk.Failed("OpenAI's response exceeded the %d-byte limit", tooLarge.Limit)
		}
		return nil, sdk.OutcomeUnknown("OpenAI's response could not be read after the request was sent; the call may have been processed, so it is not retried automatically")
	}
	if len(raw) > maxResponseBytes {
		return nil, sdk.Failed("OpenAI's response exceeded the %d-byte limit", maxResponseBytes)
	}

	var reply decisionsResponse
	decodeErr := json.Unmarshal(raw, &reply)

	if resp.StatusCode != http.StatusOK {
		return nil, classifyStatus(resp, &reply)
	}
	if decodeErr != nil {
		return nil, sdk.Failed("OpenAI's response was not valid JSON")
	}
	return answersFromReply(set, reply.Answers)
}

// classifyStatus maps a non-200 reply onto the retry contract. A decision has no
// effect beyond the model's own cost, so a refusal that is about capacity is
// retryable; one about the request or the credential is not, because the same
// request would fail the same way.
//
// Only the error's type is reported. Its message is the provider's prose, which
// can quote the request, and the request holds the key.
func classifyStatus(resp *http.Response, reply *decisionsResponse) error {
	kind := "unspecified_error"
	if reply.Error != nil && errorTypePattern.MatchString(reply.Error.Type) {
		kind = reply.Error.Type
	}

	// A 429 that says the account has no quota left is not a rate limit: no
	// wait makes the same request succeed, so it is not retried.
	if resp.StatusCode == http.StatusTooManyRequests && (kind == quotaExhausted || codeIs(reply, quotaExhausted)) {
		return sdk.PermissionDenied("OpenAI refused the request: HTTP 429 %s", quotaExhausted)
	}

	switch code := resp.StatusCode; {
	case code == http.StatusTooManyRequests:
		delay := retryAfter(resp.Header.Get("Retry-After"))
		return sdk.UnavailableAfter(delay, "OpenAI rate-limited the request (%s); retry after %s", kind, delay)
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		return sdk.PermissionDenied("OpenAI refused the credential: HTTP %d %s", code, kind)
	case code == http.StatusBadRequest || code == http.StatusNotFound || code == http.StatusRequestEntityTooLarge || code == http.StatusUnprocessableEntity:
		return sdk.InvalidInput("OpenAI refused the request: HTTP %d %s", code, kind)
	case code >= 500 || code == http.StatusRequestTimeout:
		return sdk.Unavailable("OpenAI was unavailable: HTTP %d %s", code, kind)
	default:
		return sdk.Failed("OpenAI returned HTTP %d %s", code, kind)
	}
}

// quotaExhausted is the error type or code OpenAI uses for an account that is
// out of quota or credit.
const quotaExhausted = "insufficient_quota"

// codeIs reports whether the error's code is the given word. The code is a
// string, a number or null depending on the error, so anything else is not it.
func codeIs(reply *decisionsResponse, want string) bool {
	if reply.Error == nil {
		return false
	}
	var code string
	return json.Unmarshal(reply.Error.Code, &code) == nil && code == want
}

func classifyTransportError(err error, sent bool) error {
	var limited *netpolicy.RateLimitedError
	if errors.As(err, &limited) {
		if limited.AfterRedirect {
			return sdk.OutcomeUnknown("operator egress policy rate-limited a redirect hop after the request was sent; the call may have been processed, so it is not retried automatically")
		}
		delay := boundedRetryAfter(limited.RetryAfter)
		return sdk.UnavailableAfter(delay, "operator egress policy rate-limited openai.decide; retry after %s", delay)
	}
	var deny *netpolicy.DenyError
	if errors.As(err, &deny) {
		return sdk.PermissionDenied("deployment egress policy denied openai.decide")
	}
	var tooLarge *netpolicy.BodyTooLargeError
	if errors.As(err, &tooLarge) {
		return sdk.Failed("OpenAI's response exceeded the %d-byte limit", tooLarge.Limit)
	}
	// The transport's own error text names the URL and can wrap a proxy's
	// message, so none of it is repeated. A failure before any byte was written
	// (dial, DNS, TLS) proves nothing was processed and is retryable. After the
	// request was written, a reset or a timeout does not: the provider may still
	// answer and bill for it, so the outcome is unknown and is not retried
	// automatically, as with any call that may have taken effect.
	if sent {
		return sdk.OutcomeUnknown("the connection to OpenAI failed or timed out after the request was sent; the call may have been processed, so it is not retried automatically")
	}
	if errors.Is(err, context.Canceled) {
		return sdk.Unavailable("the request to OpenAI was canceled before it was sent")
	}
	return sdk.Unavailable("the connection to OpenAI could not be established before the request was sent")
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

// bounded caps text taken from a parser's own error before it is repeated.
func bounded(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > maxErrorBytes {
		// A cut can land inside a multi-byte rune; dropping the broken tail keeps
		// the text valid UTF-8 at the plugin's error boundary.
		return strings.ToValidUTF8(value[:maxErrorBytes], "") + "…"
	}
	return value
}
