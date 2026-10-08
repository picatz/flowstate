package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
	"github.com/picatz/flowstate/plugins/slack/render"
)

const (
	howReplace   = "replace"
	howDelete    = "delete"
	howEphemeral = "ephemeral"
	howInChannel = "in_channel"

	responseHost = "hooks.slack.com"
	// maxResponseURLBytes bounds an address that arrives in a delivery body, so
	// it is another party's input and not trusted for its length.
	maxResponseURLBytes = 512
)

// opRespond answers through a response_url. Replacing or deleting names its
// target, so repeating it leaves the same state; the two new-message forms
// create one, and are not retried.
var (
	opRespond      = operation{task: "slack.respond", method: "response_url", idempotent: true}
	opRespondFresh = operation{task: "slack.respond", method: "response_url"}
)

// respondRequest is the body Slack reads at a response_url.
type respondRequest struct {
	Text           string         `json:"text,omitempty"`
	Blocks         []render.Block `json:"blocks,omitempty"`
	ResponseType   string         `json:"response_type,omitempty"`
	ReplaceOrig    *bool          `json:"replace_original,omitempty"`
	DeleteOriginal *bool          `json:"delete_original,omitempty"`
}

// respondPlan is a validated answer: the pinned address and the body to send.
type respondPlan struct {
	op   operation
	url  string
	how  string
	body respondRequest
}

// checkResponseURL accepts only the address shape Slack issues. A response_url
// arrives inside a delivery, and the delivery's signature proves Slack sent it,
// but the plugin does not rest on that: a URL that names any other host would
// make this task a request forwarder for whoever controls a payload, so the
// host, scheme, port and path family are pinned here, before the egress policy
// decides anything.
func checkResponseURL(raw string) (*url.URL, error) {
	if raw == "" || len(raw) > maxResponseURLBytes {
		return nil, sdk.InvalidInput("response_url must be 1 to %d bytes", maxResponseURLBytes)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, sdk.InvalidInput("response_url is not a URL")
	}
	switch {
	case u.Scheme != "https",
		u.Host != responseHost,
		u.User != nil,
		u.RawQuery != "" || u.Fragment != "",
		!strings.HasPrefix(u.Path, "/actions/") && !strings.HasPrefix(u.Path, "/commands/"):
		return nil, sdk.InvalidInput("response_url must be an https://%s/actions/... or /commands/... address, as Slack sends it", responseHost)
	}
	return u, nil
}

func planRespond(in *slackv1.RespondInputs) (*respondPlan, error) {
	u, err := checkResponseURL(in.GetResponseUrl())
	if err != nil {
		return nil, err
	}
	how := in.GetHow()
	if how == "" {
		how = howReplace
	}
	p := &respondPlan{op: opRespond, url: u.String(), how: how}
	yes := true
	if how == howDelete {
		if in.GetText() != "" || in.GetCard() != nil || len(in.GetBlocks()) > 0 {
			return nil, sdk.InvalidInput("how: delete removes the message and takes no text, card or blocks")
		}
		p.body.DeleteOriginal = &yes
		return p, nil
	}
	msg, err := buildBody(in.GetText(), in.GetCard(), in.GetBlocks())
	if err != nil {
		return nil, err
	}
	p.body.Text, p.body.Blocks = msg.text, msg.blocks
	switch how {
	case howReplace:
		p.body.ReplaceOrig = &yes
	case howEphemeral:
		p.op = opRespondFresh
		p.body.ResponseType = "ephemeral"
		no := false
		p.body.ReplaceOrig = &no
	case howInChannel:
		p.op = opRespondFresh
		p.body.ResponseType = "in_channel"
		no := false
		p.body.ReplaceOrig = &no
	}
	return p, nil
}

func slackRespond(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if err := guard(ctx, opRespond.task); err != nil {
		return nil, err
	}
	var in slackv1.RespondInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}
	p, err := planRespond(&in)
	if err != nil {
		return nil, err
	}
	governed, err := sdk.HTTPClient()
	if err != nil {
		return nil, sdk.PermissionDenied("%s has no usable egress policy: %v", opRespond.task, err)
	}
	if err := sendRespond(ctx, governed, p); err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(&slackv1.RespondOutputs{How: p.how})
}

// sendRespond posts the body to the plan's address. Slack answers a response_url
// with a bare "ok" (or a JSON error), not the Web API's envelope, so success is
// the 200 itself.
func sendRespond(ctx context.Context, client *http.Client, p *respondPlan) error {
	body, err := render.JSON(p.body, false)
	if err != nil {
		return sdk.Failed("encoding the bounded Slack request: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return sdk.Failed("building the Slack request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	// A response_url is final: a redirect would carry the body to a host this
	// task never pinned.
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	resp, err := c.Do(req)
	if err != nil {
		return classifyTransportError(p.op, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		delay := retryAfter(resp.Header.Get("Retry-After"))
		return sdk.UnavailableAfter(delay, "Slack rate-limited the response_url; retry after %s", delay)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes+1))
	if err != nil {
		var tooLarge *netpolicy.BodyTooLargeError
		if errors.As(err, &tooLarge) {
			return p.op.unknown("Slack's answer exceeded the operator egress policy's response limit after the response_url was posted")
		}
		return p.op.unknown("Slack's answer could not be read after the response_url was posted")
	}
	detail := bounded(string(raw))
	switch {
	case resp.StatusCode >= 500:
		return p.op.unknown(fmt.Sprintf("Slack returned HTTP %d after receiving the response_url post", resp.StatusCode))
	case resp.StatusCode == http.StatusNotFound, resp.StatusCode == http.StatusGone:
		return sdk.InvalidInput("Slack no longer accepts this response_url (%s); each lasts 30 minutes and five uses", detail)
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return sdk.Failed("Slack returned HTTP %d for the response_url: %s", resp.StatusCode, detail)
	}
	return nil
}
