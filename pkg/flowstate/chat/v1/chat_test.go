package chatv1_test

import (
	"strings"
	"testing"

	chatv1 "github.com/picatz/flowstate/pkg/flowstate/chat/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

func plain(s string) *chatv1.Text {
	return &chatv1.Text{Kind: &chatv1.Text_Plain{Plain: s}}
}

func button(action string) *chatv1.Button {
	return &chatv1.Button{Label: plain(action), Callback: &chatv1.Callback{Action: action, Value: "run:1"}}
}

func approval() *chatv1.Approval {
	return &chatv1.Approval{
		Title:   plain("Deploy?"),
		Summary: plain("Ship build 42 to production."),
		Approve: button("approve"),
		Reject:  button("reject"),
	}
}

func approvalCard(a *chatv1.Approval) *chatv1.Card {
	return &chatv1.Card{Kind: &chatv1.Card_Approval{Approval: a}}
}

// requireField asserts err is a validation failure with a violation on field.
func requireField(t *testing.T, err error, field string) {
	t.Helper()
	var invalid *flowstatev1.ValidationError
	require.ErrorAsf(t, err, &invalid, "want a *flowstatev1.ValidationError, got %[1]T: %[1]v", err)
	fields := make([]string, 0, len(invalid.Violations))
	for _, v := range invalid.Violations {
		fields = append(fields, v.Field)
	}
	require.Containsf(t, fields, field, "violations: %v", invalid)
}

func TestChatRules(t *testing.T) {
	t.Parallel()

	// The accepted baseline, so each case below is the one change that
	// breaks it.
	require.NoError(t, flowstatev1.Validate(approvalCard(approval())))

	markup := func(m *chatv1.Markup) *chatv1.Card {
		a := approval()
		a.Summary = &chatv1.Text{Kind: &chatv1.Text_Markup{Markup: m}}
		return approvalCard(a)
	}
	fields := func(n int) []*chatv1.Field {
		out := make([]*chatv1.Field, n)
		for i := range out {
			out[i] = &chatv1.Field{Label: "k", Value: plain("v")}
		}
		return out
	}
	with := func(f func(*chatv1.Approval)) *chatv1.Card {
		a := approval()
		f(a)
		return approvalCard(a)
	}

	for _, tc := range []struct {
		name  string
		valid bool
		card  *chatv1.Card
		field string
	}{
		{"plain text at the limit", true, with(func(a *chatv1.Approval) { a.Summary = plain(strings.Repeat("a", 3000)) }), ""},
		{"plain text over the limit", false, with(func(a *chatv1.Approval) { a.Summary = plain(strings.Repeat("a", 3001)) }), "approval.summary.plain"},
		{"text with neither plain nor markup", false, with(func(a *chatv1.Approval) { a.Summary = &chatv1.Text{} }), "approval.summary.kind"},
		{"ten fields", true, with(func(a *chatv1.Approval) { a.Fields = fields(10) }), ""},
		{"eleven fields", false, with(func(a *chatv1.Approval) { a.Fields = fields(11) }), "approval.fields"},
		{"field label over the limit", false, with(func(a *chatv1.Approval) {
			a.Fields = []*chatv1.Field{{Label: strings.Repeat("l", 49), Value: plain("v")}}
		}), "approval.fields[0].label"},
		{"four extra buttons", false, with(func(a *chatv1.Approval) {
			a.Extra = []*chatv1.Button{button("a"), button("b"), button("c"), button("d")}
		}), "approval.extra"},
		{"missing title", false, with(func(a *chatv1.Approval) { a.Title = nil }), "approval.title"},
		{"missing approve button", false, with(func(a *chatv1.Approval) { a.Approve = nil }), "approval.approve"},
		{"empty card", false, &chatv1.Card{}, "kind"},
		{"eight mentions", true, markup(&chatv1.Markup{Template: "hi", Mentions: make([]*chatv1.Mention, 8)}), ""},
		{"nine mentions", false, markup(&chatv1.Markup{Template: "hi", Mentions: make([]*chatv1.Mention, 9)}), "approval.summary.markup.mentions"},
		{"empty template", false, markup(&chatv1.Markup{}), "approval.summary.markup.template"},
		{"valid arg key", true, markup(&chatv1.Markup{Template: "{who}", Args: map[string]string{"who_1": "x"}}), ""},
		{"arg key with a capital", false, markup(&chatv1.Markup{Template: "{Who}", Args: map[string]string{"Who": "x"}}), "approval.summary.markup.args[\"Who\"]"},
		{"arg value over the limit", false, markup(&chatv1.Markup{Template: "{a}", Args: map[string]string{"a": strings.Repeat("v", 3001)}}), "approval.summary.markup.args[\"a\"]"},
		{"callback value with a space", false, with(func(a *chatv1.Approval) { a.Approve.Callback.Value = "run 1" }), "approval.approve.callback.value"},
		{"callback action in the wrong case", false, with(func(a *chatv1.Approval) { a.Approve.Callback.Action = "Approve" }), "approval.approve.callback.action"},
		{"callback action empty", false, with(func(a *chatv1.Approval) { a.Approve.Callback.Action = "" }), "approval.approve.callback.action"},
		{"button url over http", false, with(func(a *chatv1.Approval) { a.Approve.Url = "http://example.com" }), "approval.approve.url"},
		{"button url over https", true, with(func(a *chatv1.Approval) { a.Approve.Url = "https://example.com/x" }), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := flowstatev1.Validate(tc.card)
			if tc.valid {
				require.NoError(t, err)
				return
			}
			requireField(t, err, tc.field)
		})
	}
}

func TestChatStatusLogNoticeRules(t *testing.T) {
	t.Parallel()

	status := func(s *chatv1.Status) *chatv1.Card { return &chatv1.Card{Kind: &chatv1.Card_Status{Status: s}} }
	log := func(l *chatv1.Log) *chatv1.Card { return &chatv1.Card{Kind: &chatv1.Card_Log{Log: l}} }
	notice := func(n *chatv1.Notice) *chatv1.Card { return &chatv1.Card{Kind: &chatv1.Card_Notice{Notice: n}} }

	for _, tc := range []struct {
		name  string
		card  *chatv1.Card
		field string // empty when the card is valid
	}{
		{"progress mid-way", status(&chatv1.Status{State: chatv1.State_STATE_RUNNING, Title: plain("t"), Progress: &chatv1.Progress{Current: 3, Total: 10}}), ""},
		{"progress total zero", status(&chatv1.Status{Title: plain("t"), Progress: &chatv1.Progress{Total: 0}}), "status.progress.total"},
		{"progress total over the limit", status(&chatv1.Status{Title: plain("t"), Progress: &chatv1.Progress{Total: 10001}}), "status.progress.total"},
		{"progress current negative", status(&chatv1.Status{Title: plain("t"), Progress: &chatv1.Progress{Current: -1, Total: 1}}), "status.progress.current"},
		{"status link over http", status(&chatv1.Status{Title: plain("t"), Link: "http://example.com"}), "status.link"},
		{"status without a title", status(&chatv1.Status{}), "status.title"},
		{"log with code", log(&chatv1.Log{Level: chatv1.Level_LEVEL_ERROR, Message: plain("m"), Code: &chatv1.Code{Text: "x", Lang: "go"}}), ""},
		{"log code over the limit", log(&chatv1.Log{Message: plain("m"), Code: &chatv1.Code{Text: strings.Repeat("c", 2801)}}), "log.code.text"},
		{"log code lang over the limit", log(&chatv1.Log{Message: plain("m"), Code: &chatv1.Code{Lang: strings.Repeat("l", 17)}}), "log.code.lang"},
		{"log with six context lines", log(&chatv1.Log{Message: plain("m"), Context: []*chatv1.Text{plain("a"), plain("a"), plain("a"), plain("a"), plain("a"), plain("a")}}), "log.context"},
		{"notice with eleven paragraphs", notice(&chatv1.Notice{Title: plain("t"), Body: make([]*chatv1.Text, 11)}), "notice.body"},
		{"notice with six buttons", notice(&chatv1.Notice{Title: plain("t"), Buttons: make([]*chatv1.Button, 6)}), "notice.buttons"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := flowstatev1.Validate(tc.card)
			if tc.field == "" {
				require.NoError(t, err)
				return
			}
			requireField(t, err, tc.field)
		})
	}
}

func TestInteractionFormIsBounded(t *testing.T) {
	t.Parallel()

	form := func(n int) map[string]string {
		m := make(map[string]string, n)
		for i := range n {
			m[strings.Repeat("k", i+1)] = "v"
		}
		return m
	}
	require.NoError(t, flowstatev1.Validate(&chatv1.Interaction{Kind: chatv1.Interaction_KIND_BUTTON, Form: form(64)}))
	requireField(t, flowstatev1.Validate(&chatv1.Interaction{Form: form(65)}), "form")
}
