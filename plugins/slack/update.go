package main

import (
	"context"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
	"github.com/picatz/flowstate/plugins/slack/render"
)

// updateRequest is the body of chat.update. Blocks is always sent, as an empty
// list for a text-only update: omitted, Slack keeps the blocks the message
// already had, and a "text" update that leaves the old layout in place is the
// surprise the empty list prevents.
type updateRequest struct {
	Channel  string           `json:"channel"`
	TS       string           `json:"ts"`
	Text     string           `json:"text"`
	Blocks   []render.Block   `json:"blocks"`
	Metadata *requestMetadata `json:"metadata,omitempty"`
}

func slackUpdate(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if err := guard(ctx, opUpdate.task); err != nil {
		return nil, err
	}
	var in slackv1.UpdateInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}
	token, err := tokenFromValue(in.GetToken())
	if err != nil {
		return nil, err
	}
	p, err := planUpdate(&in)
	if err != nil {
		return nil, err
	}
	governed, err := sdk.HTTPClient()
	if err != nil {
		return nil, sdk.PermissionDenied("%s has no usable egress policy: %v", opUpdate.task, err)
	}
	answer, err := send(ctx, governed, apiBase, token, p)
	if err != nil {
		return nil, err
	}
	return sdk.EncodeOutputs(&slackv1.UpdateOutputs{Channel: answer.Channel, Ts: answer.TS})
}

// planUpdate enforces slack.update's cross-field rules before any request.
func planUpdate(in *slackv1.UpdateInputs) (*plan, error) {
	if err := requireChannel(in.GetChannel()); err != nil {
		return nil, err
	}
	if err := requireTS("ts", in.GetTs()); err != nil {
		return nil, err
	}
	msg, err := buildBody(in.GetText(), in.GetCard(), in.GetBlocks())
	if err != nil {
		return nil, err
	}
	meta, err := buildMetadata(in.GetMetadata())
	if err != nil {
		return nil, err
	}
	blocks := msg.blocks
	if blocks == nil {
		blocks = []render.Block{}
	}
	return &plan{op: opUpdate, channel: in.GetChannel(), ts: in.GetTs(), body: updateRequest{
		Channel: in.GetChannel(), TS: in.GetTs(), Text: msg.text, Blocks: blocks, Metadata: meta,
	}}, nil
}
