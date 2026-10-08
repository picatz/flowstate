package render

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	chatv1 "github.com/picatz/flowstate/pkg/flowstate/chat/v1"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
)

// Block is one rendered Slack block. The concrete types below are plain structs
// with ordered fields, so a value always marshals to the same bytes.
type Block interface{ isBlock() }

// Element is a rendered block element, such as a button.
type Element interface{ isElement() }

// ContextElement is a rendered context element: a [TextObject] or an
// [ImageElement].
type ContextElement interface{ contextElement() }

// SectionBlock is Slack's section block.
type SectionBlock struct {
	Type      string       `json:"type"`
	BlockID   string       `json:"block_id,omitempty"`
	Text      *TextObject  `json:"text,omitempty"`
	Fields    []TextObject `json:"fields,omitempty"`
	Accessory Element      `json:"accessory,omitempty"`
}

// HeaderBlock is Slack's header block.
type HeaderBlock struct {
	Type    string     `json:"type"`
	BlockID string     `json:"block_id,omitempty"`
	Text    TextObject `json:"text"`
}

// DividerBlock is Slack's divider block.
type DividerBlock struct {
	Type    string `json:"type"`
	BlockID string `json:"block_id,omitempty"`
}

// ContextBlock is Slack's context block.
type ContextBlock struct {
	Type     string           `json:"type"`
	BlockID  string           `json:"block_id,omitempty"`
	Elements []ContextElement `json:"elements"`
}

// ActionsBlock is Slack's actions block.
type ActionsBlock struct {
	Type     string    `json:"type"`
	BlockID  string    `json:"block_id,omitempty"`
	Elements []Element `json:"elements"`
}

// ImageBlock is Slack's image block.
type ImageBlock struct {
	Type     string      `json:"type"`
	BlockID  string      `json:"block_id,omitempty"`
	ImageURL string      `json:"image_url"`
	AltText  string      `json:"alt_text"`
	Title    *TextObject `json:"title,omitempty"`
}

// ButtonElement is Slack's button.
type ButtonElement struct {
	Type               string         `json:"type"`
	Text               TextObject     `json:"text"`
	ActionID           string         `json:"action_id"`
	Value              string         `json:"value,omitempty"`
	URL                string         `json:"url,omitempty"`
	Style              string         `json:"style,omitempty"`
	Confirm            *ConfirmObject `json:"confirm,omitempty"`
	AccessibilityLabel string         `json:"accessibility_label,omitempty"`
}

// ImageElement is Slack's image element, for context blocks and accessories.
type ImageElement struct {
	Type     string `json:"type"`
	ImageURL string `json:"image_url"`
	AltText  string `json:"alt_text"`
}

// StaticSelectElement is Slack's static select menu.
type StaticSelectElement struct {
	Type          string         `json:"type"`
	ActionID      string         `json:"action_id"`
	Placeholder   TextObject     `json:"placeholder"`
	Options       []OptionObject `json:"options"`
	InitialOption *OptionObject  `json:"initial_option,omitempty"`
	Confirm       *ConfirmObject `json:"confirm,omitempty"`
}

// OverflowElement is Slack's overflow menu.
type OverflowElement struct {
	Type     string         `json:"type"`
	ActionID string         `json:"action_id"`
	Options  []OptionObject `json:"options"`
	Confirm  *ConfirmObject `json:"confirm,omitempty"`
}

// DatePickerElement is Slack's date picker.
type DatePickerElement struct {
	Type        string         `json:"type"`
	ActionID    string         `json:"action_id"`
	Placeholder *TextObject    `json:"placeholder,omitempty"`
	InitialDate string         `json:"initial_date,omitempty"`
	Confirm     *ConfirmObject `json:"confirm,omitempty"`
}

// OptionObject is Slack's option object.
type OptionObject struct {
	Text        TextObject  `json:"text"`
	Value       string      `json:"value"`
	Description *TextObject `json:"description,omitempty"`
}

// ConfirmObject is Slack's confirmation dialog object.
type ConfirmObject struct {
	Title   TextObject `json:"title"`
	Text    TextObject `json:"text"`
	Confirm TextObject `json:"confirm"`
	Deny    TextObject `json:"deny"`
}

func (SectionBlock) isBlock()          {}
func (HeaderBlock) isBlock()           {}
func (DividerBlock) isBlock()          {}
func (ContextBlock) isBlock()          {}
func (ActionsBlock) isBlock()          {}
func (ImageBlock) isBlock()            {}
func (ButtonElement) isElement()       {}
func (ImageElement) isElement()        {}
func (StaticSelectElement) isElement() {}
func (OverflowElement) isElement()     {}
func (DatePickerElement) isElement()   {}
func (ImageElement) contextElement()   {}

// JSON encodes v the way the plugin sends it to Slack and the goldens record
// it: HTML characters are not escaped, so `&lt;` stays readable. Indent is two
// spaces when pretty is set.
func JSON(v any, pretty bool) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if pretty {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// Blocks renders native blocks and checks the result against Slack's limits.
// Every error names the path an author wrote.
func Blocks(in []*slackv1.Block) ([]Block, error) {
	out := make([]Block, 0, len(in))
	for i, b := range in {
		rendered, err := block(fmt.Sprintf("blocks[%d]", i), b)
		if err != nil {
			return nil, err
		}
		out = append(out, rendered)
	}
	if err := Validate("blocks", out); err != nil {
		return nil, err
	}
	return out, nil
}

func block(path string, b *slackv1.Block) (Block, error) {
	id := b.GetBlockId()
	switch k := b.GetKind().(type) {
	case *slackv1.Block_Section:
		path += ".section"
		s := SectionBlock{Type: "section", BlockID: id}
		if k.Section.GetText() != nil {
			t, err := Text(path+".text", k.Section.GetText())
			if err != nil {
				return nil, err
			}
			s.Text = &t
		}
		for i, f := range k.Section.GetFields() {
			t, err := Text(fmt.Sprintf("%s.fields[%d]", path, i), f)
			if err != nil {
				return nil, err
			}
			s.Fields = append(s.Fields, t)
		}
		if a := k.Section.GetAccessory(); a != nil {
			e, err := element(path+".accessory", a)
			if err != nil {
				return nil, err
			}
			s.Accessory = e
		}
		return s, nil
	case *slackv1.Block_Header:
		t, err := Plain(path+".header.text", k.Header.GetText())
		if err != nil {
			return nil, err
		}
		return HeaderBlock{Type: "header", BlockID: id, Text: t}, nil
	case *slackv1.Block_Divider:
		return DividerBlock{Type: "divider", BlockID: id}, nil
	case *slackv1.Block_Context:
		path += ".context"
		c := ContextBlock{Type: "context", BlockID: id, Elements: []ContextElement{}}
		for i, e := range k.Context.GetElements() {
			ep := fmt.Sprintf("%s.elements[%d]", path, i)
			switch ek := e.GetKind().(type) {
			case *slackv1.ContextElement_Text:
				t, err := Text(ep+".text", ek.Text)
				if err != nil {
					return nil, err
				}
				c.Elements = append(c.Elements, t)
			case *slackv1.ContextElement_Image:
				c.Elements = append(c.Elements, imageElement(ek.Image))
			default:
				return nil, fmt.Errorf("%s: needs `text` or `image`", ep)
			}
		}
		return c, nil
	case *slackv1.Block_Actions:
		path += ".actions"
		a := ActionsBlock{Type: "actions", BlockID: id, Elements: []Element{}}
		for i, e := range b.GetActions().GetElements() {
			ep := fmt.Sprintf("%s.elements[%d]", path, i)
			rendered, err := element(ep, e)
			if err != nil {
				return nil, err
			}
			if _, isImage := rendered.(ImageElement); isImage {
				return nil, fmt.Errorf("%s.image: Slack does not allow an image in an actions block; use a section accessory or a context block", ep)
			}
			a.Elements = append(a.Elements, rendered)
		}
		return a, nil
	case *slackv1.Block_Image:
		path += ".image"
		img := ImageBlock{Type: "image", BlockID: id, ImageURL: k.Image.GetUrl(), AltText: k.Image.GetAltText()}
		if k.Image.GetTitle() != nil {
			t, err := Plain(path+".title", k.Image.GetTitle())
			if err != nil {
				return nil, err
			}
			img.Title = &t
		}
		return img, nil
	}
	return nil, fmt.Errorf("%s: needs one of section, header, divider, context, actions or image", path)
}

func imageElement(i *slackv1.Image) ImageElement {
	return ImageElement{Type: "image", ImageURL: i.GetUrl(), AltText: i.GetAltText()}
}

func element(path string, e *slackv1.Element) (Element, error) {
	switch k := e.GetKind().(type) {
	case *slackv1.Element_Button:
		b := k.Button
		path += ".button"
		text, err := Plain(path+".text", b.GetText())
		if err != nil {
			return nil, err
		}
		confirm, err := confirmObject(path+".confirm", b.GetConfirm())
		if err != nil {
			return nil, err
		}
		if b.GetActionId() == "" {
			return nil, fmt.Errorf("%s.action_id: is required; name what pressing the button means, such as `approve`", path)
		}
		return ButtonElement{
			Type: "button", Text: text, ActionID: b.GetActionId(), Value: b.GetValue(), URL: b.GetUrl(),
			Style: styleName(b.GetStyle()), Confirm: confirm, AccessibilityLabel: b.GetAccessibilityLabel(),
		}, nil
	case *slackv1.Element_StaticSelect:
		s := k.StaticSelect
		path += ".static_select"
		placeholder, err := Plain(path+".placeholder", s.GetPlaceholder())
		if err != nil {
			return nil, err
		}
		opts, err := options(path, s.GetOptions())
		if err != nil {
			return nil, err
		}
		confirm, err := confirmObject(path+".confirm", s.GetConfirm())
		if err != nil {
			return nil, err
		}
		out := StaticSelectElement{Type: "static_select", ActionID: s.GetActionId(), Placeholder: placeholder, Options: opts, Confirm: confirm}
		if s.GetInitialOption() != nil {
			o, err := option(path+".initial_option", s.GetInitialOption())
			if err != nil {
				return nil, err
			}
			out.InitialOption = &o
		}
		return out, nil
	case *slackv1.Element_Overflow:
		o := k.Overflow
		path += ".overflow"
		opts, err := options(path, o.GetOptions())
		if err != nil {
			return nil, err
		}
		confirm, err := confirmObject(path+".confirm", o.GetConfirm())
		if err != nil {
			return nil, err
		}
		return OverflowElement{Type: "overflow", ActionID: o.GetActionId(), Options: opts, Confirm: confirm}, nil
	case *slackv1.Element_DatePicker:
		d := k.DatePicker
		path += ".date_picker"
		out := DatePickerElement{Type: "datepicker", ActionID: d.GetActionId(), InitialDate: d.GetInitialDate()}
		if d.GetPlaceholder() != nil {
			p, err := Plain(path+".placeholder", d.GetPlaceholder())
			if err != nil {
				return nil, err
			}
			out.Placeholder = &p
		}
		confirm, err := confirmObject(path+".confirm", d.GetConfirm())
		if err != nil {
			return nil, err
		}
		out.Confirm = confirm
		return out, nil
	case *slackv1.Element_Image:
		return imageElement(k.Image), nil
	}
	return nil, fmt.Errorf("%s: needs one of button, static_select, overflow, date_picker or image", path)
}

func options(path string, in []*slackv1.Option) ([]OptionObject, error) {
	out := make([]OptionObject, 0, len(in))
	for i, o := range in {
		rendered, err := option(fmt.Sprintf("%s.options[%d]", path, i), o)
		if err != nil {
			return nil, err
		}
		out = append(out, rendered)
	}
	return out, nil
}

func option(path string, o *slackv1.Option) (OptionObject, error) {
	text, err := Plain(path+".text", o.GetText())
	if err != nil {
		return OptionObject{}, err
	}
	out := OptionObject{Text: text, Value: o.GetValue()}
	if o.GetDescription() != nil {
		d, err := Plain(path+".description", o.GetDescription())
		if err != nil {
			return OptionObject{}, err
		}
		out.Description = &d
	}
	return out, nil
}

// confirmObject renders a confirmation dialog. Slack requires all four parts,
// so the two button labels default to "Confirm" and "Cancel"; the title and
// body are the author's to write, because a dialog that does not say what is
// being confirmed is worse than none.
func confirmObject(path string, c *chatv1.Confirm) (*ConfirmObject, error) {
	if c == nil {
		return nil, nil
	}
	if c.GetTitle() == nil || c.GetText() == nil {
		return nil, fmt.Errorf("%s: needs a `title` and a `text` saying what is being confirmed", path)
	}
	title, err := Plain(path+".title", c.GetTitle())
	if err != nil {
		return nil, err
	}
	text, err := Text(path+".text", c.GetText())
	if err != nil {
		return nil, err
	}
	out := &ConfirmObject{
		Title: title, Text: text,
		Confirm: TextObject{Type: "plain_text", Text: "Confirm"},
		Deny:    TextObject{Type: "plain_text", Text: "Cancel"},
	}
	if c.GetConfirm() != nil {
		if out.Confirm, err = Plain(path+".confirm", c.GetConfirm()); err != nil {
			return nil, err
		}
	}
	if c.GetDeny() != nil {
		if out.Deny, err = Plain(path+".deny", c.GetDeny()); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func styleName(s chatv1.Style) string {
	switch s {
	case chatv1.Style_STYLE_PRIMARY:
		return "primary"
	case chatv1.Style_STYLE_DANGER:
		return "danger"
	}
	return ""
}

// Fallback derives the notification text for blocks that come without any: the
// first header, else the first section's text, else the first field, capped at
// [MaxMessageText] characters. Plain text is escaped, because Slack parses a
// message's top-level text as mrkdwn; mrkdwn is already safe by construction.
func Fallback(blocks []Block) string {
	pick := func() (TextObject, bool) {
		for _, b := range blocks {
			if h, ok := b.(HeaderBlock); ok {
				return h.Text, true
			}
		}
		for _, b := range blocks {
			if s, ok := b.(SectionBlock); ok {
				if s.Text != nil {
					return *s.Text, true
				}
				if len(s.Fields) > 0 {
					return s.Fields[0], true
				}
			}
		}
		for _, b := range blocks {
			if c, ok := b.(ContextBlock); ok {
				for _, e := range c.Elements {
					if t, ok := e.(TextObject); ok {
						return t, true
					}
				}
			}
		}
		return TextObject{}, false
	}
	t, ok := pick()
	if !ok {
		return "New message"
	}
	text := t.Text
	if t.Type == "plain_text" {
		text = Escape(text)
	}
	return Truncate(text, MaxMessageText)
}

// Truncate cuts s to at most max characters, ending in an ellipsis when it cut.
func Truncate(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	runes := []rune(s)
	return string(runes[:max-1]) + "…"
}
