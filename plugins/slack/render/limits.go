package render

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Slack's published limits for a message, in one place so a change at Slack is
// one edit here and one golden refresh. The values were read from the Block
// Kit reference (https://docs.slack.dev/reference/block-kit) and the
// chat.postMessage method reference on 2026-10-07; a Slack-side change is a
// reviewed edit to these constants, never a silent drift. The .proto files
// mirror the counts that protovalidate can express so `flow validate` reports
// them early; this file is the authority the plugin enforces on the rendered
// result, where escaping can make text longer than the author wrote.
const (
	// MaxBlocks is the number of blocks in one message.
	MaxBlocks = 50
	// MaxSectionText is the characters of a section's text.
	MaxSectionText = 3000
	// MaxSectionFields is the fields of one section.
	MaxSectionFields = 10
	// MaxFieldText is the characters of one section field.
	MaxFieldText = 2000
	// MaxHeaderText is the characters of a header.
	MaxHeaderText = 150
	// MaxContextElements is the elements of one context block.
	MaxContextElements = 10
	// MaxContextText is the characters of a context text element.
	MaxContextText = 2000
	// MaxActionsElements is the elements of one actions block.
	MaxActionsElements = 25
	// MaxButtonText is the characters of a button label.
	MaxButtonText = 75
	// MaxButtonValue is the characters of a button's value.
	MaxButtonValue = 2000
	// MaxURL is the characters of any URL.
	MaxURL = 3000
	// MaxID is the characters of a block_id or action_id.
	MaxID = 255
	// MaxAccessibilityLabel is the characters of a button's accessibility label.
	MaxAccessibilityLabel = 75
	// MaxImageAlt is the characters of image alt text, and of an image title.
	MaxImageAlt = 2000
	// MaxSelectOptions is the options of a static select.
	MaxSelectOptions = 100
	// MinOverflowOptions and MaxOverflowOptions bound an overflow menu.
	MinOverflowOptions = 2
	// MaxOverflowOptions is the most options an overflow menu holds.
	MaxOverflowOptions = 5
	// MaxOptionText is the characters of an option label or description.
	MaxOptionText = 75
	// MaxOptionValue is the characters of an option value.
	MaxOptionValue = 150
	// MaxPlaceholder is the characters of a menu or picker placeholder.
	MaxPlaceholder = 150
	// MaxConfirmTitle is the characters of a confirmation dialog's title.
	MaxConfirmTitle = 100
	// MaxConfirmText is the characters of a confirmation dialog's body.
	MaxConfirmText = 300
	// MaxConfirmLabel is the characters of a confirmation dialog's buttons.
	MaxConfirmLabel = 30
	// MaxMessageText is the characters of a message's top-level text, Slack's
	// documented recommendation before truncation begins.
	MaxMessageText = 4000
)

// maxReported bounds how many violations one Validate names, so a pathological
// input produces a readable error and bounded work.
const maxReported = 8

var (
	idPattern   = regexp.MustCompile(`^[A-Za-z0-9_-]*$`)
	datePattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
)

// Validate checks rendered blocks against Slack's limits and returns an error
// naming every violation (up to eight) by the path an author wrote, such as
// `blocks[2].section.text: 3001 characters, Slack allows at most 3000`. root is
// the first path segment: "blocks" for native blocks, "card" for a preset,
// whose paths then read `card.blocks[1].section.fields[3]`.
func Validate(root string, blocks []Block) error {
	v := &validator{}
	prefix := root
	if root != "blocks" {
		prefix = root + ".blocks"
	}
	if len(blocks) > MaxBlocks {
		v.add(prefix, "%d blocks, Slack allows at most %d", len(blocks), MaxBlocks)
	}
	seen := map[string]int{}
	for i, b := range blocks {
		path := fmt.Sprintf("%s[%d]", prefix, i)
		if id := blockID(b); id != "" {
			v.id(path+".block_id", id)
			if first, dup := seen[id]; dup {
				v.add(path+".block_id", "%q is already used by %s[%d]; block_ids are unique within a message", id, prefix, first)
			}
			seen[id] = i
		}
		v.block(path, b)
	}
	return v.err()
}

type validator struct {
	errs []string
	more int
}

func (v *validator) add(path, format string, args ...any) {
	if len(v.errs) >= maxReported {
		v.more++
		return
	}
	v.errs = append(v.errs, path+": "+fmt.Sprintf(format, args...))
}

func (v *validator) err() error {
	if len(v.errs) == 0 {
		return nil
	}
	msg := strings.Join(v.errs, "\n")
	if v.more > 0 {
		msg += fmt.Sprintf("\n... and %d more", v.more)
	}
	return fmt.Errorf("%s", msg)
}

// text checks a text object: valid UTF-8, non-empty, and at most max
// characters.
func (v *validator) text(path string, t TextObject, max int) {
	v.str(path, t.Text, 1, max)
}

func (v *validator) str(path, s string, min, max int) {
	if !utf8.ValidString(s) {
		v.add(path, "is not valid UTF-8")
		return
	}
	switch n := utf8.RuneCountInString(s); {
	case n < min:
		v.add(path, "is empty; Slack requires at least %d character", min)
	case n > max:
		v.add(path, "%d characters, Slack allows at most %d", n, max)
	}
}

func (v *validator) id(path, id string) {
	if utf8.RuneCountInString(id) > MaxID {
		v.add(path, "%d characters, Slack allows at most %d", utf8.RuneCountInString(id), MaxID)
	} else if !idPattern.MatchString(id) {
		v.add(path, "%q may contain only letters, digits, '_' and '-'", id)
	}
}

func (v *validator) url(path, u string) {
	v.str(path, u, 1, MaxURL)
	if !strings.HasPrefix(u, "https://") {
		v.add(path, "%q must be an https URL", u)
	}
}

func (v *validator) block(path string, b Block) {
	switch b := b.(type) {
	case SectionBlock:
		path += ".section"
		if b.Text == nil && len(b.Fields) == 0 {
			v.add(path, "needs text or fields")
		}
		if b.Text != nil {
			v.text(path+".text", *b.Text, MaxSectionText)
		}
		if len(b.Fields) > MaxSectionFields {
			v.add(path+".fields", "%d fields, Slack allows at most %d", len(b.Fields), MaxSectionFields)
		}
		for i, f := range b.Fields {
			v.text(fmt.Sprintf("%s.fields[%d]", path, i), f, MaxFieldText)
		}
		if b.Accessory != nil {
			v.element(path+".accessory", b.Accessory, map[string]bool{})
		}
	case HeaderBlock:
		v.text(path+".header.text", b.Text, MaxHeaderText)
	case DividerBlock:
	case ContextBlock:
		path += ".context"
		if n := len(b.Elements); n < 1 || n > MaxContextElements {
			v.add(path+".elements", "%d elements, Slack requires 1 to %d", n, MaxContextElements)
		}
		for i, e := range b.Elements {
			ep := fmt.Sprintf("%s.elements[%d]", path, i)
			switch e := e.(type) {
			case TextObject:
				v.text(ep+".text", e, MaxContextText)
			case ImageElement:
				v.image(ep+".image", e.ImageURL, e.AltText)
			}
		}
	case ActionsBlock:
		path += ".actions"
		if n := len(b.Elements); n < 1 || n > MaxActionsElements {
			v.add(path+".elements", "%d elements, Slack requires 1 to %d", n, MaxActionsElements)
		}
		ids := map[string]bool{}
		for i, e := range b.Elements {
			v.element(fmt.Sprintf("%s.elements[%d]", path, i), e, ids)
		}
	case ImageBlock:
		path += ".image"
		v.image(path, b.ImageURL, b.AltText)
		if b.Title != nil {
			v.text(path+".title", *b.Title, MaxImageAlt)
		}
	}
}

func (v *validator) image(path, url, alt string) {
	v.url(path+".url", url)
	v.str(path+".alt_text", alt, 1, MaxImageAlt)
}

// element checks one control. ids carries the action_ids already used in the
// enclosing block, because Slack requires them to be unique there.
func (v *validator) element(path string, e Element, ids map[string]bool) {
	action := func(p, id string) {
		v.str(p, id, 1, MaxID)
		if ids[id] && id != "" {
			v.add(p, "%q is already used by another control in this block; action_ids are unique within a block", id)
		}
		ids[id] = true
	}
	switch e := e.(type) {
	case ButtonElement:
		path += ".button"
		v.text(path+".text", e.Text, MaxButtonText)
		action(path+".action_id", e.ActionID)
		v.str(path+".value", e.Value, 0, MaxButtonValue)
		if e.URL != "" {
			v.url(path+".url", e.URL)
		}
		v.str(path+".accessibility_label", e.AccessibilityLabel, 0, MaxAccessibilityLabel)
		v.confirm(path+".confirm", e.Confirm)
	case StaticSelectElement:
		path += ".static_select"
		action(path+".action_id", e.ActionID)
		v.text(path+".placeholder", e.Placeholder, MaxPlaceholder)
		if n := len(e.Options); n < 1 || n > MaxSelectOptions {
			v.add(path+".options", "%d options, Slack requires 1 to %d", n, MaxSelectOptions)
		}
		v.options(path, e.Options)
		if e.InitialOption != nil && !hasOption(e.Options, e.InitialOption.Value) {
			v.add(path+".initial_option", "value %q is not one of the options", e.InitialOption.Value)
		}
		v.confirm(path+".confirm", e.Confirm)
	case OverflowElement:
		path += ".overflow"
		action(path+".action_id", e.ActionID)
		if n := len(e.Options); n < MinOverflowOptions || n > MaxOverflowOptions {
			v.add(path+".options", "%d options, Slack requires %d to %d", n, MinOverflowOptions, MaxOverflowOptions)
		}
		v.options(path, e.Options)
		v.confirm(path+".confirm", e.Confirm)
	case DatePickerElement:
		path += ".date_picker"
		action(path+".action_id", e.ActionID)
		if e.Placeholder != nil {
			v.text(path+".placeholder", *e.Placeholder, MaxPlaceholder)
		}
		if e.InitialDate != "" && !datePattern.MatchString(e.InitialDate) {
			v.add(path+".initial_date", "%q must be written YYYY-MM-DD", e.InitialDate)
		}
		v.confirm(path+".confirm", e.Confirm)
	case ImageElement:
		v.image(path+".image", e.ImageURL, e.AltText)
	}
}

func (v *validator) options(path string, opts []OptionObject) {
	values := map[string]bool{}
	for i, o := range opts {
		op := fmt.Sprintf("%s.options[%d]", path, i)
		v.text(op+".text", o.Text, MaxOptionText)
		v.str(op+".value", o.Value, 1, MaxOptionValue)
		if values[o.Value] {
			v.add(op+".value", "%q repeats an earlier option; values are unique within a menu", o.Value)
		}
		values[o.Value] = true
		if o.Description != nil {
			v.text(op+".description", *o.Description, MaxOptionText)
		}
	}
}

func hasOption(opts []OptionObject, value string) bool {
	for _, o := range opts {
		if o.Value == value {
			return true
		}
	}
	return false
}

func (v *validator) confirm(path string, c *ConfirmObject) {
	if c == nil {
		return
	}
	v.text(path+".title", c.Title, MaxConfirmTitle)
	v.text(path+".text", c.Text, MaxConfirmText)
	v.text(path+".confirm", c.Confirm, MaxConfirmLabel)
	v.text(path+".deny", c.Deny, MaxConfirmLabel)
}

// blockID returns the block_id a block carries.
func blockID(b Block) string {
	switch b := b.(type) {
	case SectionBlock:
		return b.BlockID
	case HeaderBlock:
		return b.BlockID
	case DividerBlock:
		return b.BlockID
	case ContextBlock:
		return b.BlockID
	case ActionsBlock:
		return b.BlockID
	case ImageBlock:
		return b.BlockID
	}
	return ""
}
