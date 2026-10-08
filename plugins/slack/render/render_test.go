package render_test

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	chatv1 "github.com/picatz/flowstate/pkg/flowstate/chat/v1"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
	"github.com/picatz/flowstate/plugins/slack/render"
)

// update rewrites the golden files: `go test ./render -update`, then review the
// diff like any other change to what operators see in Slack.
var update = flag.Bool("update", false, "rewrite testdata/*.golden.json")

func golden(t *testing.T, name string, v any) {
	t.Helper()
	got, err := render.JSON(v, true)
	if err != nil {
		t.Fatalf("encoding %s: %v", name, err)
	}
	got = append(got, '\n')
	path := filepath.Join("testdata", name+".golden.json")
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading golden (run with -update to create it): %v", err)
	}
	if string(want) != string(got) {
		t.Errorf("%s differs from %s (run with -update to accept):\n--- want\n%s\n--- got\n%s", name, path, want, got)
	}
}

// Constructors keep the cases readable: they are the same shapes a Flowfile
// writes.
func plain(s string) *chatv1.Text { return &chatv1.Text{Kind: &chatv1.Text_Plain{Plain: s}} }

func emoji(s string) *chatv1.Text {
	return &chatv1.Text{Kind: &chatv1.Text_Plain{Plain: s}, Emoji: true}
}

func tmpl(template string, args map[string]string, mentions ...*chatv1.Mention) *chatv1.Text {
	return &chatv1.Text{Kind: &chatv1.Text_Markup{Markup: &chatv1.Markup{Template: template, Args: args, Mentions: mentions}}}
}

func btn(label, action, value string) *chatv1.Button {
	return &chatv1.Button{Label: plain(label), Callback: &chatv1.Callback{Action: action, Value: value}}
}

func field(label, value string) *chatv1.Field {
	return &chatv1.Field{Label: label, Value: plain(value)}
}

func sbtn(text, id string) *slackv1.Element {
	return &slackv1.Element{Kind: &slackv1.Element_Button{Button: &slackv1.Button{Text: plain(text), ActionId: id}}}
}

func opt(text, value string) *slackv1.Option { return &slackv1.Option{Text: plain(text), Value: value} }

func section(text string) *slackv1.Block {
	return &slackv1.Block{Kind: &slackv1.Block_Section{Section: &slackv1.Section{Text: plain(text)}}}
}

func TestCardGoldens(t *testing.T) {
	deploy := "01HZ-run:deploy"
	for name, card := range map[string]*chatv1.Card{
		"card_approval": {Kind: &chatv1.Card_Approval{Approval: &chatv1.Approval{
			Title:   plain("Production deploy"),
			Summary: tmpl("*{change}* requested by {who}", map[string]string{"change": "api v1.4.2", "who": "alice@example.com"}),
			Fields:  []*chatv1.Field{field("Service", "api"), field("Version", "v1.4.2"), field("Region", "us-east-1"), field("Risk", "low")},
			Approve: btn("Approve", "approve", deploy),
			Reject: &chatv1.Button{
				Label:    plain("Reject"),
				Callback: &chatv1.Callback{Action: "reject", Value: deploy},
				Confirm:  &chatv1.Confirm{Title: plain("Reject deploy?"), Text: plain("This cannot be undone.")},
			},
			Extra:  []*chatv1.Button{{Label: plain("Runbook"), Url: "https://example.com/runbook"}},
			Footer: plain("Expires in 24h"),
		}}},
		"card_status_running": {Kind: &chatv1.Card_Status{Status: &chatv1.Status{
			State: chatv1.State_STATE_RUNNING, Title: emoji(":rocket: Deploying api"),
			Detail:   plain("Stage 2 of 3: canary"),
			Fields:   []*chatv1.Field{field("Environment", "production"), field("Started", "12:04 UTC")},
			Progress: &chatv1.Progress{Current: 2, Total: 3},
			Link:     "https://example.com/runs/42",
		}}},
		"card_status_failed": {Kind: &chatv1.Card_Status{Status: &chatv1.Status{
			State: chatv1.State_STATE_FAILED, Title: plain("Deploy api"), Detail: plain("health check failed"),
		}}},
		"card_log_error": {Kind: &chatv1.Card_Log{Log: &chatv1.Log{
			Level:   chatv1.Level_LEVEL_ERROR,
			Message: plain("migration 0042 failed"),
			Code:    &chatv1.Code{Text: "ERROR: relation \"users\" does not exist", Lang: "text"},
			Context: []*chatv1.Text{plain("attempt 2 of 3"), plain("db: primary")},
		}}},
		"card_log_info": {Kind: &chatv1.Card_Log{Log: &chatv1.Log{
			Level: chatv1.Level_LEVEL_INFO, Message: plain("cache warmed"),
		}}},
		"card_notice": {Kind: &chatv1.Card_Notice{Notice: &chatv1.Notice{
			Title:   plain("Maintenance window"),
			Body:    []*chatv1.Text{plain("The database will be read-only for 10 minutes."), tmpl("Details: <https://example.com/{id}|status page>", map[string]string{"id": "m-17"})},
			Fields:  []*chatv1.Field{field("Starts", "02:00 UTC")},
			Buttons: []*chatv1.Button{btn("Acknowledge", "ack", "m-17"), {Label: plain("Status page"), Url: "https://example.com/status"}},
		}}},
	} {
		t.Run(name, func(t *testing.T) {
			blocks, err := render.Card(card)
			if err != nil {
				t.Fatalf("Card: %v", err)
			}
			golden(t, name, blocks)
		})
	}
}

func TestBlockGoldens(t *testing.T) {
	img := &slackv1.Image{Url: "https://example.com/a.png", AltText: "diagram"}
	confirm := &chatv1.Confirm{Title: plain("Sure?"), Text: plain("Really."), Confirm: plain("Do it"), Deny: plain("Back")}
	for name, b := range map[string]*slackv1.Block{
		"block_header":  {BlockId: "top", Kind: &slackv1.Block_Header{Header: &slackv1.Header{Text: emoji(":tada: Done")}}},
		"block_divider": {Kind: &slackv1.Block_Divider{Divider: &slackv1.Divider{}}},
		"block_image":   {Kind: &slackv1.Block_Image{Image: &slackv1.Image{Url: img.Url, AltText: img.AltText, Title: plain("Architecture")}}},
		"block_section_fields": {Kind: &slackv1.Block_Section{Section: &slackv1.Section{
			Text:   tmpl("*{n}* things", map[string]string{"n": "3"}),
			Fields: []*chatv1.Text{plain("one"), plain("two")},
		}}},
		"block_context": {Kind: &slackv1.Block_Context{Context: &slackv1.Context{Elements: []*slackv1.ContextElement{
			{Kind: &slackv1.ContextElement_Image{Image: img}},
			{Kind: &slackv1.ContextElement_Text{Text: plain("posted by flowstate")}},
		}}}},
		"block_actions": {Kind: &slackv1.Block_Actions{Actions: &slackv1.Actions{Elements: []*slackv1.Element{
			{Kind: &slackv1.Element_Button{Button: &slackv1.Button{
				Text: plain("Roll back"), ActionId: "rollback", Value: "run-1", Style: chatv1.Style_STYLE_DANGER,
				Confirm: confirm, AccessibilityLabel: "Roll back the deploy",
			}}},
			{Kind: &slackv1.Element_Button{Button: &slackv1.Button{Text: plain("Docs"), ActionId: "docs", Url: "https://example.com/docs"}}},
			{Kind: &slackv1.Element_StaticSelect{StaticSelect: &slackv1.StaticSelect{
				ActionId: "env", Placeholder: plain("Environment"),
				Options:       []*slackv1.Option{opt("Staging", "staging"), {Text: plain("Production"), Value: "prod", Description: plain("careful")}},
				InitialOption: opt("Staging", "staging"), Confirm: confirm,
			}}},
			{Kind: &slackv1.Element_Overflow{Overflow: &slackv1.Overflow{ActionId: "more", Options: []*slackv1.Option{opt("Snooze", "snooze"), opt("Mute", "mute")}}}},
			{Kind: &slackv1.Element_DatePicker{DatePicker: &slackv1.DatePicker{ActionId: "when", Placeholder: plain("Pick a day"), InitialDate: "2026-10-07"}}},
		}}}},
		"block_section_accessory_button": {Kind: &slackv1.Block_Section{Section: &slackv1.Section{
			Text: plain("Open the dashboard"), Accessory: sbtn("Open", "open"),
		}}},
		"block_section_accessory_image": {Kind: &slackv1.Block_Section{Section: &slackv1.Section{
			Text: plain("With a picture"), Accessory: &slackv1.Element{Kind: &slackv1.Element_Image{Image: img}},
		}}},
	} {
		t.Run(name, func(t *testing.T) {
			blocks, err := render.Blocks([]*slackv1.Block{b})
			if err != nil {
				t.Fatalf("Blocks: %v", err)
			}
			golden(t, name, blocks)
		})
	}
}

func TestFallbackDerivation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		blocks []*slackv1.Block
		want   string
	}{
		{"first header wins and is escaped", []*slackv1.Block{section("body"), {Kind: &slackv1.Block_Header{Header: &slackv1.Header{Text: plain("Title <!channel> & co")}}}}, "Title &lt;!channel&gt; &amp; co"},
		{"section when no header", []*slackv1.Block{section("only body")}, "only body"},
		{"markup stays as rendered", []*slackv1.Block{{Kind: &slackv1.Block_Section{Section: &slackv1.Section{Text: tmpl("*{a}*", map[string]string{"a": "<b>"})}}}}, "*&lt;b&gt;*"},
		{"nothing to quote", []*slackv1.Block{{Kind: &slackv1.Block_Divider{Divider: &slackv1.Divider{}}}}, "New message"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blocks, err := render.Blocks(tc.blocks)
			if err != nil {
				t.Fatal(err)
			}
			if got := render.Fallback(blocks); got != tc.want {
				t.Errorf("Fallback = %q, want %q", got, tc.want)
			}
		})
	}

	t.Run("capped at 4000 characters", func(t *testing.T) {
		long := strings.Repeat("é", 5000)
		got := render.Fallback([]render.Block{render.SectionBlock{Type: "section", Text: &render.TextObject{Type: "mrkdwn", Text: long}}})
		if n := len([]rune(got)); n != render.MaxMessageText || !strings.HasSuffix(got, "…") {
			t.Errorf("fallback has %d characters (suffix %q), want %d ending in an ellipsis", n, got[len(got)-3:], render.MaxMessageText)
		}
	})
}
