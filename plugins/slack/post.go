package main

import (
	"context"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
	"github.com/picatz/flowstate/plugins/slack/render"
)

// postRequest is the body of chat.postMessage. Link and media unfurling are
// always off, so message content cannot make Slack fetch an arbitrary URL, and
// icon_url and username are never sent, because chat:write.customize would make
// Slack fetch an author-chosen image.
type postRequest struct {
	Channel        string           `json:"channel"`
	Text           string           `json:"text"`
	Blocks         []render.Block   `json:"blocks,omitempty"`
	ClientMsgID    string           `json:"client_msg_id"`
	ThreadTS       string           `json:"thread_ts,omitempty"`
	ReplyBroadcast bool             `json:"reply_broadcast,omitempty"`
	Metadata       *requestMetadata `json:"metadata,omitempty"`
	UnfurlLinks    bool             `json:"unfurl_links"`
	UnfurlMedia    bool             `json:"unfurl_media"`
}

// ephemeralRequest is the body of chat.postEphemeral. Slack's method has no
// client_msg_id, metadata or unfurl controls, so none are sent.
type ephemeralRequest struct {
	Channel  string         `json:"channel"`
	User     string         `json:"user"`
	Text     string         `json:"text"`
	Blocks   []render.Block `json:"blocks,omitempty"`
	ThreadTS string         `json:"thread_ts,omitempty"`
}

func slackPost(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if err := guard(ctx, opPost.task); err != nil {
		return nil, err
	}
	var in slackv1.PostInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}
	token, err := tokenFromValue(in.GetToken())
	if err != nil {
		return nil, err
	}
	p, err := planPost(&in)
	if err != nil {
		return nil, err
	}

	// The SDK's client, not egressPolicy.Client(): it is the same policy, plus
	// the credential marking this request needs. The policy checked in guard is
	// what the boundary reads; this is what the request crosses.
	governed, err := sdk.HTTPClient()
	if err != nil {
		return nil, sdk.PermissionDenied("%s has no usable egress policy: %v", opPost.task, err)
	}
	answer, err := send(ctx, governed, apiBase, token, p)
	if err != nil {
		return nil, err
	}
	if p.op == opEphemeral {
		return sdk.EncodeOutputs(&slackv1.PostOutputs{Channel: p.channel, MessageTs: answer.MessageTS})
	}
	return sdk.EncodeOutputs(&slackv1.PostOutputs{Channel: answer.Channel, Ts: answer.TS})
}

// planPost enforces every rule of slack.post that needs more than one field, or
// the text after rendering, before any request: protovalidate carries the
// single-field rules to `flow validate`, but the host strips cross-field rules
// from a plugin's descriptor, so they live here.
func planPost(in *slackv1.PostInputs) (*plan, error) {
	if err := requireChannel(in.GetChannel()); err != nil {
		return nil, err
	}
	if !uuidPattern.MatchString(in.GetIdempotencyKey()) {
		return nil, sdk.InvalidInput("idempotency_key must be a canonical lowercase UUID chosen once for this logical message; got %q", in.GetIdempotencyKey())
	}
	if in.GetThreadTs() != "" {
		if err := requireTS("thread_ts", in.GetThreadTs()); err != nil {
			return nil, err
		}
	}
	if in.GetReplyBroadcast() && in.GetThreadTs() == "" {
		return nil, sdk.InvalidInput("reply_broadcast needs thread_ts: it also shows a threaded reply in the channel, so there must be a thread to reply to")
	}
	if in.GetToUser() != "" {
		if !userPattern.MatchString(in.GetToUser()) {
			return nil, sdk.InvalidInput("to_user must be a Slack user ID such as U0123ABCD; got %q", in.GetToUser())
		}
		if in.GetReplyBroadcast() {
			return nil, sdk.InvalidInput("to_user and reply_broadcast exclude each other: an ephemeral message is visible to one person and cannot be broadcast")
		}
		if in.GetMetadata() != nil {
			return nil, sdk.InvalidInput("to_user and metadata exclude each other: Slack's chat.postEphemeral stores no metadata")
		}
	}
	msg, err := buildBody(in.GetText(), in.GetCard(), in.GetBlocks())
	if err != nil {
		return nil, err
	}
	meta, err := buildMetadata(in.GetMetadata())
	if err != nil {
		return nil, err
	}

	if in.GetToUser() != "" {
		return &plan{op: opEphemeral, channel: in.GetChannel(), body: ephemeralRequest{
			Channel: in.GetChannel(), User: in.GetToUser(), Text: msg.text, Blocks: msg.blocks, ThreadTS: in.GetThreadTs(),
		}}, nil
	}
	return &plan{op: opPost, channel: in.GetChannel(), body: postRequest{
		Channel: in.GetChannel(), Text: msg.text, Blocks: msg.blocks, ClientMsgID: in.GetIdempotencyKey(),
		ThreadTS: in.GetThreadTs(), ReplyBroadcast: in.GetReplyBroadcast(), Metadata: meta,
	}}, nil
}
