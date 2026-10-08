package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	"github.com/picatz/flowstate/plugins/slack/render"
)

const (
	// apiBase is Slack's Web API. The egress policy, not this constant, decides
	// whether the worker may reach it.
	apiBase          = "https://slack.com/api/"
	maxTokenBytes    = 4096
	maxErrorBytes    = 256
	maxResponseBytes = 64 << 10
)

var (
	channelPattern = regexp.MustCompile(`^[CDG][A-Z0-9]{1,254}$`)
	userPattern    = regexp.MustCompile(`^[UW][A-Z0-9]{1,20}$`)
	uuidPattern    = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	// tsPattern is a Slack message timestamp, which is also what thread_ts is.
	tsPattern = regexp.MustCompile(`^[0-9]{1,16}\.[0-9]{6}$`)
)

// operation is one Slack Web API method this plugin calls, and the posture its
// unknown outcomes take.
type operation struct {
	// task is the Flowfile spelling, used in errors: "slack.post".
	task string
	// method is the Slack method: "chat.postMessage".
	method string
	// idempotent is true when repeating the call with the same input leaves the
	// same state, which makes an unknown outcome safe to retry. chat.update
	// names its target; chat.postMessage creates one.
	idempotent bool
}

var (
	opPost      = operation{task: "slack.post", method: "chat.postMessage"}
	opEphemeral = operation{task: "slack.post", method: "chat.postEphemeral"}
	opUpdate    = operation{task: "slack.update", method: "chat.update", idempotent: true}
)

// unknown classifies a failure after the request may have taken effect. A post
// is never retried automatically, because the message may already exist and a
// retry would post it twice; an update is retryable, because applying the same
// content to the same message twice is the same as once.
func (o operation) unknown(what string) error {
	if o.idempotent {
		return sdk.Unavailable("%s; %s is idempotent on its target message, so the step may be retried", what, o.method)
	}
	return sdk.OutcomeUnknown("%s; the message may already exist, so it is not retried automatically", what)
}

// plan is a validated request, ready to send. Building one is pure: every
// cross-field and limit rule has already run, so nothing reaches the network
// with an input the plugin would refuse.
type plan struct {
	op   operation
	body any
	// channel and ts are what Slack's acknowledgement must echo.
	channel string
	ts      string
}

// slackResponse is the subset of Slack's answer the plugin reads.
type slackResponse struct {
	OK               bool   `json:"ok"`
	Error            string `json:"error"`
	Channel          string `json:"channel"`
	TS               string `json:"ts"`
	MessageTS        string `json:"message_ts"`
	ResponseMetadata struct {
		Messages []string `json:"messages"`
	} `json:"response_metadata"`
}

// guard is every task's first decision: production mode, then a usable egress
// policy, before any input or credential is decoded.
func guard(ctx context.Context, task string) error {
	caller, ok := sdk.CallerFromContext(ctx)
	if err := requireProductionMode(task, caller, ok); err != nil {
		return err
	}
	if egressPolicy == nil {
		return sdk.PermissionDenied(
			"%s has no usable egress policy, so no destination is authorized: %v", task, egressRefusal)
	}
	return nil
}

// requireProductionMode is a Slack write's side-effect posture, not an
// authorization decision. Task policy, secret release, and egress policy grant
// the authorities the call spends; the host-attested mode only keeps a local
// rehearsal from being mistaken for a notification preview.
func requireProductionMode(task string, caller sdk.Caller, ok bool) error {
	if !ok || caller.Mode() != flowstatev1.WorkloadIdentityMode_WORKLOAD_IDENTITY_MODE_PRODUCTION {
		return sdk.PermissionDenied(
			"%s performs an external write and requires a production execution identity; local rehearsals and unknown execution modes are refused", task)
	}
	return nil
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
		return "", sdk.Failed("token reached the Slack plugin as an unresolved secret reference; the host must resolve required secret inputs before plugin execution")
	default:
		return "", sdk.InvalidInput("token must resolve to a string")
	}
}

// send performs one planned call. base is the API root, ending in a slash.
func send(ctx context.Context, client *http.Client, base, token string, p *plan) (*slackResponse, error) {
	// Nothing is marked here: the calling workload's identity is installed where
	// netpolicy looks when the task call is delivered, and the bearer token
	// below travels in an Authorization header, which sdk.HTTPClient's transport
	// marks as a credential before the policy is evaluated. A second install of
	// either would be one fact written twice.
	body, err := render.JSON(p.body, false)
	if err != nil {
		return nil, sdk.Failed("encoding the bounded Slack request: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+p.op.method, bytes.NewReader(body))
	if err != nil {
		return nil, sdk.Failed("building the Slack request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := client.Do(req)
	if err != nil {
		return nil, classifyTransportError(p.op, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		delay := retryAfter(resp.Header.Get("Retry-After"))
		return nil, sdk.UnavailableAfter(delay, "Slack rate-limited %s; retry after %s", p.op.method, delay)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		var tooLarge *netpolicy.BodyTooLargeError
		if errors.As(err, &tooLarge) {
			return nil, p.op.unknown(fmt.Sprintf("Slack's response exceeded the operator egress policy's %d-byte limit after %s was sent", tooLarge.Limit, p.op.method))
		}
		return nil, p.op.unknown(fmt.Sprintf("Slack's response could not be read after %s was sent", p.op.method))
	}
	if len(raw) > maxResponseBytes {
		return nil, p.op.unknown(fmt.Sprintf("Slack's response exceeded the %d-byte limit after %s was sent", maxResponseBytes, p.op.method))
	}
	var answer slackResponse
	if err := json.Unmarshal(raw, &answer); err != nil {
		return nil, p.op.unknown(fmt.Sprintf("Slack's response could not be decoded after %s was sent", p.op.method))
	}
	if resp.StatusCode >= 500 {
		return nil, p.op.unknown(fmt.Sprintf("Slack returned HTTP %d after receiving %s; Slack documents that some server errors may still have applied the operation", resp.StatusCode, p.op.method))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, sdk.Failed("Slack returned HTTP %d: %s", resp.StatusCode, bounded(answer.Error))
	}
	if !answer.OK {
		return nil, classifySlackError(p.op, answer, resp.Header.Get("Retry-After"))
	}
	if err := checkAcknowledgement(p, &answer); err != nil {
		return nil, err
	}
	return &answer, nil
}

// checkAcknowledgement refuses a success that does not name the message the
// plan addressed, because an acknowledgement that cannot be tied to the request
// is not evidence it took effect as asked.
func checkAcknowledgement(p *plan, a *slackResponse) error {
	bad := func(what string) error {
		return p.op.unknown(fmt.Sprintf("Slack acknowledged %s without %s", p.op.method, what))
	}
	switch p.op {
	case opEphemeral:
		if !tsPattern.MatchString(a.MessageTS) {
			return bad("a valid message_ts")
		}
	case opUpdate:
		if a.Channel != p.channel || a.TS != p.ts {
			return bad("echoing the channel and timestamp that were updated")
		}
	default:
		if a.Channel != p.channel || !channelPattern.MatchString(a.Channel) || !tsPattern.MatchString(a.TS) {
			return bad("a valid channel and timestamp")
		}
	}
	return nil
}

func classifyTransportError(op operation, err error) error {
	var limited *netpolicy.RateLimitedError
	if errors.As(err, &limited) {
		if limited.AfterRedirect {
			return op.unknown(fmt.Sprintf("Slack redirected %s before the operator egress policy rate-limited the next hop; the original request may already have taken effect", op.method))
		}
		delay := boundedRetryAfter(limited.RetryAfter)
		return sdk.UnavailableAfter(delay, "operator egress policy rate-limited %s before it was sent; retry after %s", op.task, delay)
	}
	var deny *netpolicy.DenyError
	if errors.As(err, &deny) {
		return sdk.PermissionDenied("deployment egress policy denied %s", op.task)
	}
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return op.unknown(fmt.Sprintf("the Slack connection failed after %s began", op.method))
	}
	return op.unknown(fmt.Sprintf("%s failed after the request began", op.method))
}

// classifySlackError maps a Slack `ok:false` to how the engine should treat it.
// Slack's own detail for a rejected layout (response_metadata.messages) is
// appended, bounded, so an `invalid_blocks` names the offending block.
func classifySlackError(op operation, a slackResponse, retryHeader string) error {
	code := bounded(a.Error)
	detail := ""
	if len(a.ResponseMetadata.Messages) > 0 {
		detail = " (" + bounded(strings.Join(a.ResponseMetadata.Messages, "; ")) + ")"
	}
	switch code {
	case "ratelimited", "rate_limited", "service_unavailable", "request_timeout":
		delay := retryAfter(retryHeader)
		return sdk.UnavailableAfter(delay, "Slack refused %s with %s; retry after %s", op.method, code, delay)
	case "invalid_auth", "not_authed", "account_inactive", "token_expired", "token_revoked", "missing_scope", "not_allowed_token_type", "no_permission", "restricted_action":
		return sdk.PermissionDenied("Slack refused the credential for %s: %s%s", op.method, code, detail)
	case "not_in_channel":
		return sdk.PermissionDenied("Slack refused %s: the bot is not a member of the channel; invite it, or grant chat:write.public for public channels", op.method)
	case "channel_not_found", "no_text", "invalid_arguments", "invalid_arg_name", "invalid_post_type", "is_archived",
		"duplicate_channel_not_found", "duplicate_message_not_found", "invalid_blocks", "invalid_blocks_format",
		"invalid_metadata_format", "invalid_metadata_schema", "metadata_too_large", "msg_too_long", "message_not_found",
		"cant_update_message", "edit_window_closed", "thread_not_found", "user_not_in_channel", "user_not_found", "invalid_ts":
		return sdk.InvalidInput("Slack refused %s: %s%s", op.method, code, detail)
	case "internal_error", "fatal_error":
		return op.unknown(fmt.Sprintf("Slack returned %s and documents that the operation may have succeeded", code))
	default:
		return sdk.Failed("Slack refused %s: %s%s", op.method, code, detail)
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
	return min(delay, 5*time.Minute)
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
