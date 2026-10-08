package main

import (
	"unicode/utf8"

	chatv1 "github.com/picatz/flowstate/pkg/flowstate/chat/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
	"github.com/picatz/flowstate/plugins/slack/render"
)

// body is a message's content, rendered and checked: the notification text and,
// for a card or blocks, the layout.
type body struct {
	// text is what Slack shows in notifications and to screen readers. It is
	// always plain: escaped here, so it can never open a mention.
	text string
	// blocks is the layout, nil for a text-only message.
	blocks []render.Block
}

// buildBody applies the rules shared by slack.post and slack.update: a message
// is text, a card, or blocks, and text beside a card or blocks is its fallback.
// It is pure, so the rules run before any request and are tested without one.
func buildBody(text string, card *chatv1.Card, blocks []*slackv1.Block) (*body, error) {
	if !utf8.ValidString(text) || utf8.RuneCountInString(render.Escape(text)) > render.MaxMessageText {
		return nil, sdk.InvalidInput("text must be valid UTF-8 and, once &, < and > are escaped, no longer than %d characters", render.MaxMessageText)
	}
	if card != nil && len(blocks) > 0 {
		return nil, sdk.InvalidInput("card and blocks are alternatives: use card for a preset layout, or blocks for native Block Kit, not both")
	}
	switch {
	case card != nil:
		// The host validates literals, but an expression builds its value at
		// run time, so the rules are repeated here before rendering.
		if err := flowstatev1.Validate(card); err != nil {
			return nil, sdk.InvalidInput("card: %v", err)
		}
		rendered, err := render.Card(card)
		if err != nil {
			return nil, sdk.InvalidInput("%v", err)
		}
		return withFallback(text, rendered), nil
	case len(blocks) > 0:
		for i, b := range blocks {
			if err := flowstatev1.Validate(b); err != nil {
				return nil, sdk.InvalidInput("blocks[%d]: %v", i, err)
			}
		}
		rendered, err := render.Blocks(blocks)
		if err != nil {
			return nil, sdk.InvalidInput("%v", err)
		}
		return withFallback(text, rendered), nil
	case text == "":
		return nil, sdk.InvalidInput("a message needs one of text, card or blocks")
	}
	return &body{text: render.Escape(text)}, nil
}

func withFallback(text string, blocks []render.Block) *body {
	if text == "" {
		return &body{text: render.Fallback(blocks), blocks: blocks}
	}
	return &body{text: render.Escape(text), blocks: blocks}
}

// metadataLimit is the size of a message's metadata payload, keys and values
// together. Slack documents a 3 KiB cap on the whole metadata object; the
// plugin keeps a tighter 1 KiB so a message is never refused for its metadata
// after being rendered.
const metadataLimit = 1024

// requestMetadata is Slack's message metadata object.
type requestMetadata struct {
	EventType    string            `json:"event_type"`
	EventPayload map[string]string `json:"event_payload,omitempty"`
}

func buildMetadata(m *slackv1.Metadata) (*requestMetadata, error) {
	if m == nil {
		return nil, nil
	}
	if n := utf8.RuneCountInString(m.GetEventType()); n < 1 || n > 64 {
		return nil, sdk.InvalidInput("metadata.event_type must be 1 to 64 characters")
	}
	if len(m.GetEventPayload()) > 16 {
		return nil, sdk.InvalidInput("metadata.event_payload has %d keys, at most 16 are allowed", len(m.GetEventPayload()))
	}
	size := 0
	for k, v := range m.GetEventPayload() {
		size += len(k) + len(v)
	}
	if size > metadataLimit {
		return nil, sdk.InvalidInput("metadata.event_payload is %d bytes, at most %d are allowed", size, metadataLimit)
	}
	return &requestMetadata{EventType: m.GetEventType(), EventPayload: m.GetEventPayload()}, nil
}

func requireChannel(channel string) error {
	if !channelPattern.MatchString(channel) {
		return sdk.InvalidInput("channel must be a Slack conversation ID such as C0123ABCD (beginning C, D or G), not a name; got %q", channel)
	}
	return nil
}

func requireTS(field, ts string) error {
	if !tsPattern.MatchString(ts) {
		return sdk.InvalidInput("%s must be a Slack message timestamp such as 1503435956.000247; got %q", field, ts)
	}
	return nil
}
