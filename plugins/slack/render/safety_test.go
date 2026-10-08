package render_test

import (
	"fmt"
	"strings"
	"testing"

	chatv1 "github.com/picatz/flowstate/pkg/flowstate/chat/v1"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
	"github.com/picatz/flowstate/plugins/slack/render"
)

func sectionOf(t *chatv1.Text) []*slackv1.Block {
	return []*slackv1.Block{{Kind: &slackv1.Block_Section{Section: &slackv1.Section{Text: t}}}}
}

// renderedText returns the one section's text, failing the test on error.
func renderedText(t *testing.T, text *chatv1.Text) render.TextObject {
	t.Helper()
	blocks, err := render.Blocks(sectionOf(text))
	if err != nil {
		t.Fatalf("Blocks: %v", err)
	}
	return *blocks[0].(render.SectionBlock).Text
}

// TestArgsStayLiteral is the injection claim of the design: no value in args
// can become a broadcast, a mention or a link.
func TestArgsStayLiteral(t *testing.T) {
	for _, hostile := range []string{"<!channel>", "<!here>", "<@U1>", "<#C1>", "<https://x|y>", "<!subteam^S1>", "a&b", "&lt;!channel&gt;"} {
		got := renderedText(t, tmpl("hello {who}", map[string]string{"who": hostile}))
		if got.Type != "mrkdwn" || !got.Verbatim {
			t.Errorf("markup rendered as %+v, want verbatim mrkdwn", got)
		}
		body := strings.TrimPrefix(got.Text, "hello ")
		if strings.ContainsAny(body, "<>") {
			t.Errorf("arg %q left a raw angle bracket: %q", hostile, got.Text)
		}
		if want := render.Escape(hostile); body != want {
			t.Errorf("arg %q rendered %q, want %q", hostile, body, want)
		}
	}
	// And through the plain-text paths a card uses inside mrkdwn.
	blocks, err := render.Card(&chatv1.Card{Kind: &chatv1.Card_Notice{Notice: &chatv1.Notice{
		Title:  plain("<!channel>"),
		Fields: []*chatv1.Field{field("<@U1>", "<https://x|y>")},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	json, _ := render.JSON(blocks, false)
	if strings.Contains(string(json), "<@") || strings.Contains(string(json), "<https") {
		t.Errorf("a card field carried live markup: %s", json)
	}
}

// TestInjectionThroughAPlaceholderInsideBrackets covers the sneakier shape: the
// template supplies the brackets and the value supplies what is inside.
func TestInjectionThroughAPlaceholderInsideBrackets(t *testing.T) {
	for _, tc := range []struct {
		name, template string
	}{
		{"mention", "<{x}>"},
		{"mention after bang", "<!{x}>"},
		{"link with a chosen scheme", "<{x}|click>"},
		{"link with a chosen host", "<https://{x}/|click>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := render.Blocks(sectionOf(tmpl(tc.template, map[string]string{"x": "channel"})))
			if err == nil || !strings.Contains(err.Error(), "sits inside <...>") && !strings.Contains(err.Error(), "does not declare") {
				t.Fatalf("error = %v, want a refusal naming the placeholder inside <...>", err)
			}
		})
	}
	// An argument inside a link whose host the author wrote is the supported shape.
	got := renderedText(t, tmpl("<https://example.com/runs/{id}|run {id}>", map[string]string{"id": "a>b|c"}))
	if want := "<https://example.com/runs/a&gt;b|c|run a&gt;b|c>"; got.Text != want {
		t.Errorf("link = %q, want %q", got.Text, want)
	}
}

func TestMentionsMustBeDeclared(t *testing.T) {
	for _, template := range []string{"ping <@U1>", "ping <#C1>", "ping <!here>", "ping <!channel>", "ping <!everyone>", "ping <!subteam^S1>", "ping <@U1|alice>"} {
		_, err := render.Blocks(sectionOf(tmpl(template, nil)))
		if err == nil || !strings.Contains(err.Error(), "does not declare") {
			t.Errorf("template %q: error = %v, want an undeclared-mention refusal", template, err)
		}
	}

	user := &chatv1.Mention{Kind: &chatv1.Mention_User{User: "U0ALICE"}}
	channel := &chatv1.Mention{Kind: &chatv1.Mention_Channel{Channel: "C0OPS"}}
	here := &chatv1.Mention{Kind: &chatv1.Mention_Broadcast{Broadcast: chatv1.Broadcast_BROADCAST_HERE}}
	all := &chatv1.Mention{Kind: &chatv1.Mention_Broadcast{Broadcast: chatv1.Broadcast_BROADCAST_CHANNEL}}
	got := renderedText(t, tmpl("<@U0ALICE> see <#C0OPS>: <!here> <!channel>", nil, user, channel, here, all))
	if want := "<@U0ALICE> see <#C0OPS>: <!here> <!channel>"; got.Text != want {
		t.Errorf("declared mentions rendered %q, want %q", got.Text, want)
	}

	// A declared mention that does not match the template token is not a pass
	// for a different one.
	_, err := render.Blocks(sectionOf(tmpl("ping <@U0BOB>", nil, user)))
	if err == nil {
		t.Error("declaring U0ALICE allowed the template to mention U0BOB")
	}
	// Declared but never written.
	_, err = render.Blocks(sectionOf(tmpl("no ping", nil, user)))
	if err == nil || !strings.Contains(err.Error(), "never contains it") {
		t.Errorf("unused mention: error = %v", err)
	}
	// Only here and channel exist.
	for _, m := range []*chatv1.Mention{
		{Kind: &chatv1.Mention_Broadcast{Broadcast: chatv1.Broadcast_BROADCAST_UNSPECIFIED}},
		{Kind: &chatv1.Mention_User{User: "alice"}},
		{Kind: &chatv1.Mention_Channel{Channel: "general"}},
	} {
		if _, err := render.Blocks(sectionOf(tmpl("x", nil, m))); err == nil {
			t.Errorf("mention %v was accepted", m)
		}
	}
}

func TestTemplateGrammarErrors(t *testing.T) {
	for _, tc := range []struct {
		template string
		args     map[string]string
		want     string
	}{
		{"hi {nam}", map[string]string{"name": "x"}, "did you mean {name}?"},
		{"hi {who}", nil, "there are no args"},
		{"hi {who", map[string]string{"who": "x"}, "never closed"},
		{"hi }", nil, "closes nothing"},
		{"hi {Who}", map[string]string{"who": "x"}, "not a placeholder name"},
		{"hi", map[string]string{"extra": "x"}, `"extra" is never used`},
	} {
		_, err := render.Blocks(sectionOf(tmpl(tc.template, tc.args)))
		if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.HasPrefix(err.Error(), "blocks[0].section.text.markup.template") && !strings.HasPrefix(err.Error(), "blocks[0].section.text.markup.args") {
			t.Errorf("template %q: error = %v, want path-prefixed message containing %q", tc.template, err, tc.want)
		}
	}
	got := renderedText(t, tmpl("{{literal}} {a}", map[string]string{"a": "1"}))
	if got.Text != "{literal} 1" {
		t.Errorf("brace escapes rendered %q", got.Text)
	}
}

func TestPlainTextIsNeverInterpreted(t *testing.T) {
	got := renderedText(t, plain("<!channel> *bold* <@U1>"))
	if got.Type != "plain_text" || got.Text != "<!channel> *bold* <@U1>" || got.Verbatim {
		t.Errorf("plain rendered as %+v, want literal plain_text", got)
	}
}

func TestPlainOnlyPositionsRefuseMarkup(t *testing.T) {
	blocks := []*slackv1.Block{{Kind: &slackv1.Block_Header{Header: &slackv1.Header{Text: tmpl("*x*", nil)}}}}
	_, err := render.Blocks(blocks)
	if err == nil || !strings.Contains(err.Error(), "blocks[0].header.text") || !strings.Contains(err.Error(), "plain text only") {
		t.Errorf("markup header: error = %v", err)
	}
}

func TestCardRefusalsNameTheCardPath(t *testing.T) {
	_, err := render.Card(&chatv1.Card{Kind: &chatv1.Card_Approval{Approval: &chatv1.Approval{
		Title:   plain("t"),
		Approve: &chatv1.Button{Label: plain("Yes")},
		Reject:  btn("No", "reject", "v"),
	}}})
	if err == nil || !strings.Contains(err.Error(), "card.approval.approve: needs a `callback`") {
		t.Errorf("error = %v", err)
	}

	fields := make([]*chatv1.Field, 0, 3)
	for i := range 3 {
		fields = append(fields, field(fmt.Sprintf("k%d", i), strings.Repeat("v", 2000)))
	}
	_, err = render.Card(&chatv1.Card{Kind: &chatv1.Card_Status{Status: &chatv1.Status{Title: plain("t"), Fields: fields}}})
	if err == nil || !strings.Contains(err.Error(), "card.blocks[1].section.fields[0]") {
		t.Errorf("escaped field over the limit: error = %v", err)
	}
}

func TestCodeCannotEscapeItsFence(t *testing.T) {
	blocks, err := render.Card(&chatv1.Card{Kind: &chatv1.Card_Log{Log: &chatv1.Log{
		Level: chatv1.Level_LEVEL_WARN, Message: plain("m"),
		Code: &chatv1.Code{Text: "a ``` <!channel> b"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	code := blocks[1].(render.SectionBlock).Text.Text
	if strings.Count(code, "```") != 2 || strings.Contains(code, "<!channel>") {
		t.Errorf("code block = %q, want exactly its own fence and no live broadcast", code)
	}
}

func TestProgressBar(t *testing.T) {
	for _, tc := range []struct {
		cur, total int32
		want       string
	}{
		{0, 10, "▱▱▱▱▱▱▱▱▱▱  0 of 10 · 0%"},
		{3, 10, "▰▰▰▱▱▱▱▱▱▱  3 of 10 · 30%"},
		{1, 3, "▰▰▰▱▱▱▱▱▱▱  1 of 3 · 33%"},
		{10, 10, "▰▰▰▰▰▰▰▰▰▰  10 of 10 · 100%"},
		{50, 10, "▰▰▰▰▰▰▰▰▰▰  10 of 10 · 100%"},
		{-4, 10, "▱▱▱▱▱▱▱▱▱▱  0 of 10 · 0%"},
	} {
		if got := render.ProgressBar(tc.cur, tc.total); got != tc.want {
			t.Errorf("ProgressBar(%d, %d) = %q, want %q", tc.cur, tc.total, got, tc.want)
		}
	}
}
