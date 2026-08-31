package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
)

const (
	chatPostMessageURL = "https://slack.com/api/chat.postMessage"
	maxTextCharacters  = 4000
	maxTokenBytes      = 4096
	maxChannelBytes    = 255
	maxThreadTSBytes   = 32
	maxErrorBytes      = 256
	maxResponseBytes   = 64 << 10
)

var (
	channelPattern = regexp.MustCompile(`^[CDG][A-Z0-9]{1,254}$`)
	uuidPattern    = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	threadPattern  = regexp.MustCompile(`^[0-9]{1,16}\.[0-9]{6}$`)
)

type postRequest struct {
	Channel     string `json:"channel"`
	Text        string `json:"text"`
	ClientMsgID string `json:"client_msg_id"`
	ThreadTS    string `json:"thread_ts,omitempty"`
	UnfurlLinks bool   `json:"unfurl_links"`
	UnfurlMedia bool   `json:"unfurl_media"`
}

type postResponse struct {
	OK      bool   `json:"ok"`
	Error   string `json:"error"`
	Channel string `json:"channel"`
	TS      string `json:"ts"`
}

func slackPost(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	caller, ok := sdk.CallerFromContext(ctx)
	if !ok || caller.Mode() != flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION {
		return nil, sdk.PermissionDenied(
			"slack.post performs an external write and requires a production execution identity; local rehearsals and unknown execution modes are refused")
	}
	if egressPolicy == nil {
		return nil, sdk.PermissionDenied(
			"slack.post requires an operator egress policy passed with --egress-policy; network access is denied when it is absent")
	}

	var in slackv1.PostInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}
	token, err := tokenFromValue(in.GetToken())
	if err != nil {
		return nil, err
	}
	if err := validatePost(&in); err != nil {
		return nil, err
	}

	response, err := sendPost(ctx, egressPolicy.Client(), chatPostMessageURL, token, &in)
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(&slackv1.PostOutputs{Channel: response.Channel, Ts: response.TS})
}

func tokenFromValue(v *flowstatev1.Value) (string, error) {
	if v == nil {
		return "", sdk.InvalidInput("token is required")
	}
	switch kind := v.GetKind().(type) {
	case *flowstatev1.Value_Literal:
		s, ok := kind.Literal.GetKind().(*expr.Value_StringValue)
		if !ok || s.StringValue == "" || len(s.StringValue) > maxTokenBytes {
			return "", sdk.InvalidInput("token must resolve to a non-empty string no longer than %d bytes", maxTokenBytes)
		}
		return s.StringValue, nil
	case *flowstatev1.Value_SecretRef:
		return "", sdk.Failed("token reached slack.post as an unresolved secret reference; the host must resolve required secret inputs before plugin execution")
	default:
		return "", sdk.InvalidInput("token must resolve to a string")
	}
}

func validatePost(in *slackv1.PostInputs) error {
	if len(in.GetChannel()) > maxChannelBytes || !channelPattern.MatchString(in.GetChannel()) {
		return sdk.InvalidInput("channel must be a Slack conversation ID beginning C, D, or G and no longer than %d bytes", maxChannelBytes)
	}
	if in.GetText() == "" {
		return sdk.InvalidInput("text is required")
	}
	if !utf8.ValidString(in.GetText()) || utf8.RuneCountInString(in.GetText()) > maxTextCharacters {
		return sdk.InvalidInput("text must be valid UTF-8 and no longer than %d Unicode characters", maxTextCharacters)
	}
	if !uuidPattern.MatchString(in.GetMessageKey()) {
		return sdk.InvalidInput("message_key must be a canonical lowercase UUID chosen once for this logical notification")
	}
	if len(in.GetThreadTs()) > maxThreadTSBytes || (in.GetThreadTs() != "" && !threadPattern.MatchString(in.GetThreadTs())) {
		return sdk.InvalidInput("thread_ts must be empty or a Slack message timestamp such as 1503435956.000247")
	}
	return nil
}

func sendPost(ctx context.Context, client *http.Client, endpoint, token string, in *slackv1.PostInputs) (*postResponse, error) {
	caller, _ := sdk.CallerFromContext(ctx)
	identity := caller.Identity
	ctx = netpolicy.ContextWithIdentity(ctx, netpolicy.Identity{
		Subject: identity.GetSubject(), Issuer: identity.GetIssuer(),
		Namespace: identity.GetNamespace(), Claims: identity.GetClaims(),
	})
	ctx = netpolicy.ContextWithCredentials(ctx, true)

	body, err := json.Marshal(postRequest{
		Channel: in.GetChannel(), Text: in.GetText(), ClientMsgID: in.GetMessageKey(), ThreadTS: in.GetThreadTs(),
		UnfurlLinks: false, UnfurlMedia: false,
	})
	if err != nil {
		return nil, sdk.Failed("encoding the bounded Slack request: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, sdk.Failed("building the Slack request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := client.Do(req)
	if err != nil {
		return nil, classifyTransportError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		delay := retryAfter(resp.Header.Get("Retry-After"))
		return nil, sdk.UnavailableAfter(delay, "Slack rate-limited chat.postMessage; retry after %s", delay)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		var tooLarge *netpolicy.BodyTooLargeError
		if errors.As(err, &tooLarge) {
			return nil, sdk.OutcomeUnknown("Slack's response exceeded the operator egress policy's %d-byte limit after chat.postMessage was sent; the message may already exist, so it is not retried automatically", tooLarge.Limit)
		}
		return nil, sdk.OutcomeUnknown("Slack's response could not be read after chat.postMessage was sent; the message may already exist, so it is not retried automatically")
	}
	if len(raw) > maxResponseBytes {
		return nil, sdk.OutcomeUnknown("Slack's response exceeded the %d-byte limit after chat.postMessage was sent; the message may already exist, so it is not retried automatically", maxResponseBytes)
	}
	var answer postResponse
	if err := json.Unmarshal(raw, &answer); err != nil {
		return nil, sdk.OutcomeUnknown("Slack's response could not be decoded after chat.postMessage was sent; the message may already exist, so it is not retried automatically")
	}
	if resp.StatusCode >= 500 {
		return nil, sdk.OutcomeUnknown("Slack returned HTTP %d after receiving chat.postMessage; Slack documents that some server errors may still have applied the operation, so it is not retried automatically", resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, sdk.Failed("Slack returned HTTP %d: %s", resp.StatusCode, bounded(answer.Error))
	}
	if !answer.OK {
		return nil, classifySlackError(answer.Error, resp.Header.Get("Retry-After"))
	}
	if answer.Channel != in.GetChannel() || !channelPattern.MatchString(answer.Channel) || answer.TS == "" || !threadPattern.MatchString(answer.TS) {
		return nil, sdk.OutcomeUnknown("Slack acknowledged chat.postMessage without a valid channel and timestamp; the message may exist, so it is not retried automatically")
	}
	return &answer, nil
}

func classifyTransportError(err error) error {
	var limited *netpolicy.RateLimitedError
	if errors.As(err, &limited) {
		if limited.AfterRedirect {
			return sdk.OutcomeUnknown("Slack redirected chat.postMessage before the operator egress policy rate-limited the next hop; the original request may already have taken effect, so it is not retried automatically")
		}
		delay := boundedRetryAfter(limited.RetryAfter)
		return sdk.UnavailableAfter(delay, "operator egress policy rate-limited slack.post before it was sent; retry after %s", delay)
	}
	var deny *netpolicy.DenyError
	if errors.As(err, &deny) {
		return sdk.PermissionDenied("deployment egress policy denied slack.post")
	}
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return sdk.OutcomeUnknown("the Slack connection failed after chat.postMessage began; the message may already exist, so it is not retried automatically")
	}
	return sdk.OutcomeUnknown("chat.postMessage failed after the request began; the message may already exist, so it is not retried automatically")
}

func classifySlackError(code, retryHeader string) error {
	code = bounded(code)
	switch code {
	case "ratelimited", "rate_limited", "service_unavailable", "request_timeout":
		delay := retryAfter(retryHeader)
		return sdk.UnavailableAfter(delay, "Slack refused chat.postMessage with %s; retry after %s", code, delay)
	case "invalid_auth", "not_authed", "account_inactive", "token_expired", "token_revoked", "missing_scope", "not_allowed_token_type":
		return sdk.PermissionDenied("Slack refused the credential: %s", code)
	case "channel_not_found", "no_text", "invalid_arguments", "invalid_arg_name", "invalid_post_type", "is_archived", "duplicate_channel_not_found", "duplicate_message_not_found":
		return sdk.InvalidInput("Slack refused chat.postMessage: %s", code)
	case "internal_error", "fatal_error":
		return sdk.OutcomeUnknown("Slack returned %s and documents that the operation may have succeeded; the message is not retried automatically", code)
	default:
		return sdk.Failed("Slack refused chat.postMessage: %s", code)
	}
}

func retryAfter(value string) time.Duration {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
	if err != nil || seconds <= 0 {
		return time.Second
	}
	return boundedRetryAfter(time.Duration(seconds) * time.Second)
}

func boundedRetryAfter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return time.Second
	}
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

func bounded(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > maxErrorBytes {
		return value[:maxErrorBytes] + "…"
	}
	if value == "" {
		return "unspecified_error"
	}
	return value
}
