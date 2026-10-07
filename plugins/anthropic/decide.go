package main

import (
	"bytes"
	"cmp"
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
	"unicode/utf8"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	anthropicv1 "github.com/picatz/flowstate/plugins/anthropic/gen/anthropic/v1"
)

const (
	messagesURL      = "https://api.anthropic.com/v1/messages"
	anthropicVersion = "2023-06-01"

	maxKeyBytes      = 4096
	maxModelBytes    = 256
	maxEvidenceBytes = 256 << 10

	defaultMaxTokens = 1024
	maxMaxTokens     = 8192

	// maxResponseBytes bounds what is read back. A reply is one tool call whose
	// size is bounded by max_tokens, so this is generous for any honest answer
	// and still small enough that a hostile or broken endpoint cannot make a
	// worker buffer more than a fraction of a megabyte per call.
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

// messagesRequest is the subset of the Messages API this plugin sends.
type messagesRequest struct {
	Model      string    `json:"model"`
	MaxTokens  int32     `json:"max_tokens"`
	System     string    `json:"system"`
	Messages   []message `json:"messages"`
	Tools      []object  `json:"tools"`
	ToolChoice object    `json:"tool_choice"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// messagesResponse is the subset of a reply this plugin reads.
type messagesResponse struct {
	StopReason string         `json:"stop_reason"`
	Content    []contentBlock `json:"content"`
	Error      *struct {
		Type string `json:"type"`
	} `json:"error"`
}

type contentBlock struct {
	Type  string          `json:"type"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

func anthropicDecide(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if egressPolicy == nil {
		return nil, sdk.PermissionDenied(
			"anthropic.decide has no usable egress policy, so no destination is authorized: %v", egressRefusal)
	}

	var in anthropicv1.DecideInputs
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
		return nil, sdk.PermissionDenied("anthropic.decide has no usable egress policy: %v", err)
	}

	answers, err := decide(ctx, governed, messagesURL, key, &in, set)
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(&anthropicv1.DecideOutputs{Answers: answers})
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
		// A header value cannot carry a line break, and one in a key would be a
		// header injection rather than a credential.
		if strings.ContainsAny(s.StringValue, "\r\n\x00") {
			return "", sdk.InvalidInput("api_key must resolve to a single-line string")
		}
		return s.StringValue, nil
	case *flowstatev1.Value_SecretRef:
		return "", sdk.Failed("api_key reached anthropic.decide as an unresolved secret reference; the host must resolve required secret inputs before plugin execution")
	default:
		return "", sdk.InvalidInput("api_key must resolve to a string")
	}
}

func validateInputs(in *anthropicv1.DecideInputs) (*flowstatev1.QuestionSet, error) {
	if len(in.GetModel()) > maxModelBytes || !modelPattern.MatchString(in.GetModel()) {
		return nil, sdk.InvalidInput("model must be a model identifier of at most %d bytes", maxModelBytes)
	}
	if in.GetEvidence() == "" {
		return nil, sdk.InvalidInput("evidence is required")
	}
	if len(in.GetEvidence()) > maxEvidenceBytes || !utf8.ValidString(in.GetEvidence()) {
		return nil, sdk.InvalidInput("evidence must be valid UTF-8 no longer than %d bytes", maxEvidenceBytes)
	}
	if in.GetMaxTokens() < 0 || in.GetMaxTokens() > maxMaxTokens {
		return nil, sdk.InvalidInput("max_tokens must be between 0 and %d", maxMaxTokens)
	}
	return parseQuestionSet(in.GetQuestionSet())
}

// decide sends the request and turns the reply into validated answers.
func decide(ctx context.Context, client *http.Client, endpoint, key string, in *anthropicv1.DecideInputs, set *flowstatev1.QuestionSet) ([]*flowstatev1.Answer, error) {
	body, err := json.Marshal(messagesRequest{
		Model:     in.GetModel(),
		MaxTokens: cmp.Or(in.GetMaxTokens(), defaultMaxTokens),
		System:    systemPrompt,
		Messages: []message{{
			Role:    "user",
			Content: "<evidence>\n" + in.GetEvidence() + "\n</evidence>",
		}},
		Tools:      []object{toolDefinition(set, in.GetReportConfidence())},
		ToolChoice: object{{"type", "tool"}, {"name", toolName}},
	})
	if err != nil {
		return nil, sdk.Failed("encoding the Anthropic request: %v", err)
	}

	// The key is in a custom header the SDK's transport cannot recognize as a
	// credential, so the request says so, and an operator rule that keeps
	// credentials away from an unapproved host still decides it.
	ctx, cancel := context.WithTimeout(sdk.WithCredentials(ctx), requestTimeout)
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
		return nil, sdk.Failed("building the Anthropic request: %v", err)
	}
	req.Header.Set("x-api-key", key)
	req.Header.Set("anthropic-version", anthropicVersion)
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
			return nil, sdk.Failed("Anthropic's response exceeded the %d-byte limit", tooLarge.Limit)
		}
		return nil, sdk.OutcomeUnknown("Anthropic's response could not be read after the request was sent; the call may have been processed, so it is not retried automatically")
	}
	if len(raw) > maxResponseBytes {
		return nil, sdk.Failed("Anthropic's response exceeded the %d-byte limit", maxResponseBytes)
	}

	var reply messagesResponse
	decodeErr := json.Unmarshal(raw, &reply)

	if resp.StatusCode != http.StatusOK {
		return nil, classifyStatus(resp, &reply)
	}
	if decodeErr != nil {
		return nil, sdk.Failed("Anthropic's response was not valid JSON")
	}

	var call *contentBlock
	for i := range reply.Content {
		if reply.Content[i].Type == "tool_use" && reply.Content[i].Name == toolName {
			if call != nil {
				return nil, sdk.Failed("Anthropic's response called %s more than once", toolName)
			}
			call = &reply.Content[i]
		}
	}
	if call == nil {
		return nil, sdk.Failed("Anthropic's response did not call the %s tool (stop reason %s)", toolName, token(reply.StopReason))
	}
	if reply.StopReason == "max_tokens" {
		return nil, sdk.Failed("Anthropic's answer was cut off at max_tokens; raise max_tokens")
	}
	return answersFromToolInput(set, call.Input, in.GetReportConfidence())
}

// classifyStatus maps a non-200 reply onto the retry contract. A decision has no
// effect beyond the model's own cost, so a refusal that is about capacity is
// retryable; one about the request or the credential is not, because the same
// request would fail the same way.
//
// Only the error's type is reported. Its message is the provider's prose, which
// can quote the request, and the request holds the key.
func classifyStatus(resp *http.Response, reply *messagesResponse) error {
	kind := "unspecified_error"
	if reply.Error != nil && errorTypePattern.MatchString(reply.Error.Type) {
		kind = reply.Error.Type
	}

	switch code := resp.StatusCode; {
	case code == http.StatusTooManyRequests:
		delay := retryAfter(resp.Header.Get("Retry-After"))
		return sdk.UnavailableAfter(delay, "Anthropic rate-limited the request (%s); retry after %s", kind, delay)
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		return sdk.PermissionDenied("Anthropic refused the credential: HTTP %d %s", code, kind)
	case code == http.StatusBadRequest || code == http.StatusNotFound || code == http.StatusRequestEntityTooLarge || code == http.StatusUnprocessableEntity:
		return sdk.InvalidInput("Anthropic refused the request: HTTP %d %s", code, kind)
	case code >= 500 || code == http.StatusRequestTimeout:
		return sdk.Unavailable("Anthropic was unavailable: HTTP %d %s", code, kind)
	default:
		return sdk.Failed("Anthropic returned HTTP %d %s", code, kind)
	}
}

func classifyTransportError(err error, sent bool) error {
	var limited *netpolicy.RateLimitedError
	if errors.As(err, &limited) {
		if limited.AfterRedirect {
			return sdk.OutcomeUnknown("operator egress policy rate-limited a redirect hop after the request was sent; the call may have been processed, so it is not retried automatically")
		}
		delay := boundedRetryAfter(limited.RetryAfter)
		return sdk.UnavailableAfter(delay, "operator egress policy rate-limited anthropic.decide; retry after %s", delay)
	}
	var deny *netpolicy.DenyError
	if errors.As(err, &deny) {
		return sdk.PermissionDenied("deployment egress policy denied anthropic.decide")
	}
	var tooLarge *netpolicy.BodyTooLargeError
	if errors.As(err, &tooLarge) {
		return sdk.Failed("Anthropic's response exceeded the %d-byte limit", tooLarge.Limit)
	}
	// The transport's own error text names the URL and can wrap a proxy's
	// message, so none of it is repeated. A failure before any byte was written
	// (dial, DNS, TLS) proves nothing was processed and is retryable. After the
	// request was written, a reset or a timeout does not: the provider may still
	// answer and bill for it, so the outcome is unknown and is not retried
	// automatically, as with any call that may have taken effect.
	if sent {
		return sdk.OutcomeUnknown("the connection to Anthropic failed or timed out after the request was sent; the call may have been processed, so it is not retried automatically")
	}
	if errors.Is(err, context.Canceled) {
		return sdk.Unavailable("the request to Anthropic was canceled before it was sent")
	}
	return sdk.Unavailable("the connection to Anthropic could not be established before the request was sent")
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

// token reports a provider-chosen word only when it is a short identifier, so a
// stop reason can be named without repeating arbitrary provider text.
func token(value string) string {
	if errorTypePattern.MatchString(value) {
		return value
	}
	return "unspecified"
}

// bounded caps text taken from a parser's own error before it is repeated.
func bounded(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > maxErrorBytes {
		return value[:maxErrorBytes] + "…"
	}
	return value
}
