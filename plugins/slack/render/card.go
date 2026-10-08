package render

import (
	"fmt"
	"strings"

	chatv1 "github.com/picatz/flowstate/pkg/flowstate/chat/v1"
)

// Card lays out a vendor-neutral card as Slack blocks and checks the result
// against Slack's limits. The four presets share one visual grammar: a bold
// header carries the title, a section carries the explanation with its fields
// in two columns, a row of buttons carries the actions, and muted context text
// carries the footer. Block IDs are fixed (`fs_header`, `fs_body`, `fs_actions`,
// `fs_footer`) so a later slack.update replaces like with like.
func Card(c *chatv1.Card) ([]Block, error) {
	var (
		blocks []Block
		err    error
	)
	switch k := c.GetKind().(type) {
	case *chatv1.Card_Approval:
		blocks, err = approval("card.approval", k.Approval)
	case *chatv1.Card_Status:
		blocks, err = status("card.status", k.Status)
	case *chatv1.Card_Log:
		blocks, err = logCard("card.log", k.Log)
	case *chatv1.Card_Notice:
		blocks, err = notice("card.notice", k.Notice)
	default:
		return nil, fmt.Errorf("card: needs one of approval, status, log or notice")
	}
	if err != nil {
		return nil, err
	}
	if err := Validate("card", blocks); err != nil {
		return nil, err
	}
	return blocks, nil
}

// header renders a title as a Slack header. Slack headers are plain text, so a
// markup title is refused with the reason.
func header(path string, t *chatv1.Text) (Block, error) {
	text, err := Plain(path, t)
	if err != nil {
		return nil, err
	}
	return HeaderBlock{Type: "header", BlockID: "fs_header", Text: text}, nil
}

// fieldList renders labelled values as section fields: a bold label over its
// value, laid out by Slack in two columns. A field's inline flag is advisory
// here: Slack always lays fields out two to a row.
func fieldList(path string, fields []*chatv1.Field) ([]TextObject, error) {
	out := make([]TextObject, 0, len(fields))
	for i, f := range fields {
		fp := fmt.Sprintf("%s[%d]", path, i)
		value, err := Mrkdwn(fp+".value", f.GetValue())
		if err != nil {
			return nil, err
		}
		text := value
		if label := f.GetLabel(); label != "" {
			text = "*" + Escape(label) + "*\n" + value
		}
		out = append(out, TextObject{Type: "mrkdwn", Text: text, Verbatim: true})
	}
	return out, nil
}

func footer(path string, t *chatv1.Text) (Block, error) {
	text, err := Text(path, t)
	if err != nil {
		return nil, err
	}
	return ContextBlock{Type: "context", BlockID: "fs_footer", Elements: []ContextElement{text}}, nil
}

// button renders a card button. A callback becomes action_id (the decision) and
// value (the correlation token, such as a run's entity key); a url makes it a
// link button, which still needs an action_id, so one is derived from its
// position. def is the style used when the author gave none.
func button(path string, b *chatv1.Button, def chatv1.Style, position int) (Element, error) {
	if b == nil {
		return nil, fmt.Errorf("%s: is required", path)
	}
	label, err := Plain(path+".label", b.GetLabel())
	if err != nil {
		return nil, err
	}
	confirm, err := confirmObject(path+".confirm", b.GetConfirm())
	if err != nil {
		return nil, err
	}
	out := ButtonElement{Type: "button", Text: label, URL: b.GetUrl(), Confirm: confirm}
	switch cb := b.GetCallback(); {
	case cb != nil:
		out.ActionID, out.Value = cb.GetAction(), cb.GetValue()
	case b.GetUrl() != "":
		out.ActionID = fmt.Sprintf("link_%d", position)
	default:
		return nil, fmt.Errorf("%s: needs a `callback` (what pressing it means) or a `url`", path)
	}
	style := b.GetStyle()
	if style == chatv1.Style_STYLE_UNSPECIFIED {
		style = def
	}
	out.Style = styleName(style)
	return out, nil
}

func buttonRow(path string, buttons []*chatv1.Button, styles []chatv1.Style) (Block, error) {
	row := ActionsBlock{Type: "actions", BlockID: "fs_actions", Elements: []Element{}}
	for i, b := range buttons {
		def := chatv1.Style_STYLE_UNSPECIFIED
		if i < len(styles) {
			def = styles[i]
		}
		e, err := button(fmt.Sprintf("%s[%d]", path, i), b, def, i)
		if err != nil {
			return nil, err
		}
		row.Elements = append(row.Elements, e)
	}
	return row, nil
}

// approval: header, summary with fields, the approve/reject/extra row, footer.
// Approve defaults to the primary style and reject to danger.
func approval(path string, a *chatv1.Approval) ([]Block, error) {
	h, err := header(path+".title", a.GetTitle())
	if err != nil {
		return nil, err
	}
	blocks := []Block{h}
	body, err := bodySection(path, a.GetSummary(), a.GetFields())
	if err != nil {
		return nil, err
	}
	if body != nil {
		blocks = append(blocks, *body)
	}
	buttons := append([]*chatv1.Button{a.GetApprove(), a.GetReject()}, a.GetExtra()...)
	// Paths name the author's fields, not the slice built here.
	row := ActionsBlock{Type: "actions", BlockID: "fs_actions", Elements: []Element{}}
	for i, b := range buttons {
		var bp string
		def := chatv1.Style_STYLE_UNSPECIFIED
		switch i {
		case 0:
			bp, def = path+".approve", chatv1.Style_STYLE_PRIMARY
		case 1:
			bp, def = path+".reject", chatv1.Style_STYLE_DANGER
		default:
			bp = fmt.Sprintf("%s.extra[%d]", path, i-2)
		}
		e, err := button(bp, b, def, i)
		if err != nil {
			return nil, err
		}
		row.Elements = append(row.Elements, e)
	}
	blocks = append(blocks, row)
	if a.GetFooter() != nil {
		f, err := footer(path+".footer", a.GetFooter())
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, f)
	}
	return blocks, nil
}

// bodySection is the section most presets share: an optional paragraph and an
// optional two-column grid of fields. It returns nil when both are absent.
func bodySection(path string, text *chatv1.Text, fields []*chatv1.Field) (*SectionBlock, error) {
	s := SectionBlock{Type: "section", BlockID: "fs_body"}
	if text != nil {
		t, err := Text(path+".summary", text)
		if err != nil {
			return nil, err
		}
		s.Text = &t
	}
	fs, err := fieldList(path+".fields", fields)
	if err != nil {
		return nil, err
	}
	s.Fields = fs
	if s.Text == nil && len(s.Fields) == 0 {
		return nil, nil
	}
	return &s, nil
}

// stateLabel is how a status state reads: an emoji and a short bold word.
func stateLabel(s chatv1.State) string {
	switch s {
	case chatv1.State_STATE_PENDING:
		return ":hourglass_flowing_sand: *Pending*"
	case chatv1.State_STATE_RUNNING:
		return ":arrows_counterclockwise: *In progress*"
	case chatv1.State_STATE_OK:
		return ":white_check_mark: *Succeeded*"
	case chatv1.State_STATE_WARN:
		return ":warning: *Needs attention*"
	case chatv1.State_STATE_FAILED:
		return ":x: *Failed*"
	}
	return ""
}

// ProgressBar draws completion as ten cells and a count, such as
// `▰▰▰▱▱▱▱▱▱▱  3 of 10 · 30%`. A current beyond the total draws full.
func ProgressBar(current, total int32) string {
	const cells = 10
	cur, tot := int64(max(current, 0)), int64(max(total, 1))
	cur = min(cur, tot)
	filled := (cur*cells + tot/2) / tot
	return fmt.Sprintf("%s%s  %d of %d · %d%%",
		strings.Repeat("▰", int(filled)), strings.Repeat("▱", int(cells-filled)), cur, tot, cur*100/tot)
}

// status: header, then one section holding the state, the detail and the
// progress bar with the fields beside it, then a link line.
func status(path string, s *chatv1.Status) ([]Block, error) {
	h, err := header(path+".title", s.GetTitle())
	if err != nil {
		return nil, err
	}
	blocks := []Block{h}

	var lines []string
	if label := stateLabel(s.GetState()); label != "" {
		lines = append(lines, label)
	}
	if s.GetDetail() != nil {
		d, err := Mrkdwn(path+".detail", s.GetDetail())
		if err != nil {
			return nil, err
		}
		lines = append(lines, d)
	}
	if p := s.GetProgress(); p != nil {
		lines = append(lines, ProgressBar(p.GetCurrent(), p.GetTotal()))
	}
	fields, err := fieldList(path+".fields", s.GetFields())
	if err != nil {
		return nil, err
	}
	if len(lines) > 0 || len(fields) > 0 {
		section := SectionBlock{Type: "section", BlockID: "fs_body", Fields: fields}
		if len(lines) > 0 {
			section.Text = &TextObject{Type: "mrkdwn", Text: strings.Join(lines, "\n"), Verbatim: true}
		}
		blocks = append(blocks, section)
	}
	if link := s.GetLink(); link != "" {
		if strings.ContainsAny(link, "<>| \t\r\n") || !strings.HasPrefix(link, "https://") {
			return nil, fmt.Errorf("%s.link: %q must be an https URL without spaces or any of < > |", path, link)
		}
		blocks = append(blocks, ContextBlock{Type: "context", BlockID: "fs_footer", Elements: []ContextElement{
			TextObject{Type: "mrkdwn", Text: "<" + Escape(link) + "|View details>", Verbatim: true},
		}})
	}
	return blocks, nil
}

func levelLabel(l chatv1.Level) string {
	switch l {
	case chatv1.Level_LEVEL_DEBUG:
		return ":mag: *DEBUG*"
	case chatv1.Level_LEVEL_INFO:
		return ":information_source: *INFO*"
	case chatv1.Level_LEVEL_WARN:
		return ":warning: *WARN*"
	case chatv1.Level_LEVEL_ERROR:
		return ":rotating_light: *ERROR*"
	}
	return ""
}

// logCard: a compact leveled line, the code in a preformatted section, and
// muted context lines. It has no header, so a stream of them in a thread stays
// quiet.
func logCard(path string, l *chatv1.Log) ([]Block, error) {
	msg, err := Mrkdwn(path+".message", l.GetMessage())
	if err != nil {
		return nil, err
	}
	line := msg
	if label := levelLabel(l.GetLevel()); label != "" {
		line = label + "  " + msg
	}
	blocks := []Block{SectionBlock{Type: "section", BlockID: "fs_body", Text: &TextObject{Type: "mrkdwn", Text: line, Verbatim: true}}}
	if code := l.GetCode(); code != nil && code.GetText() != "" {
		// A fence inside the code would end the block early and let the rest be
		// read as markup, so it is broken with a zero-width space.
		body := strings.ReplaceAll(Escape(code.GetText()), "```", "`\u200b``")
		blocks = append(blocks, SectionBlock{Type: "section", BlockID: "fs_code", Text: &TextObject{Type: "mrkdwn", Text: "```\n" + body + "\n```", Verbatim: true}})
	}
	if len(l.GetContext()) > 0 {
		ctx := ContextBlock{Type: "context", BlockID: "fs_footer", Elements: []ContextElement{}}
		for i, t := range l.GetContext() {
			s, err := Mrkdwn(fmt.Sprintf("%s.context[%d]", path, i), t)
			if err != nil {
				return nil, err
			}
			ctx.Elements = append(ctx.Elements, TextObject{Type: "mrkdwn", Text: s, Verbatim: true})
		}
		blocks = append(blocks, ctx)
	}
	return blocks, nil
}

// notice: header, one section per paragraph, the fields, then the buttons.
func notice(path string, n *chatv1.Notice) ([]Block, error) {
	h, err := header(path+".title", n.GetTitle())
	if err != nil {
		return nil, err
	}
	blocks := []Block{h}
	for i, p := range n.GetBody() {
		t, err := Text(fmt.Sprintf("%s.body[%d]", path, i), p)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, SectionBlock{Type: "section", BlockID: fmt.Sprintf("fs_body_%d", i), Text: &t})
	}
	if fields, err := fieldList(path+".fields", n.GetFields()); err != nil {
		return nil, err
	} else if len(fields) > 0 {
		blocks = append(blocks, SectionBlock{Type: "section", BlockID: "fs_fields", Fields: fields})
	}
	if len(n.GetButtons()) > 0 {
		row, err := buttonRow(path+".buttons", n.GetButtons(), nil)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, row)
	}
	return blocks, nil
}
