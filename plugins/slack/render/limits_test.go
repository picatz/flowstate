package render_test

import (
	"strings"
	"testing"

	chatv1 "github.com/picatz/flowstate/pkg/flowstate/chat/v1"

	slackv1 "github.com/picatz/flowstate/plugins/slack/gen/slack/v1"
	"github.com/picatz/flowstate/plugins/slack/render"
)

// TestEveryLimitIsEnforcedWithItsPath builds one over-limit input per rule and
// requires a refusal that names the path the author wrote and the limit.
func TestEveryLimitIsEnforcedWithItsPath(t *testing.T) {
	long := func(n int) string { return strings.Repeat("x", n) }
	header := func(s string) *slackv1.Block {
		return &slackv1.Block{Kind: &slackv1.Block_Header{Header: &slackv1.Header{Text: plain(s)}}}
	}
	actions := func(els ...*slackv1.Element) *slackv1.Block {
		return &slackv1.Block{Kind: &slackv1.Block_Actions{Actions: &slackv1.Actions{Elements: els}}}
	}
	button := func(text, id, value string) *slackv1.Element {
		return &slackv1.Element{Kind: &slackv1.Element_Button{Button: &slackv1.Button{Text: plain(text), ActionId: id, Value: value}}}
	}
	many := func(n int, f func(i int) *slackv1.Block) []*slackv1.Block {
		out := make([]*slackv1.Block, n)
		for i := range out {
			out[i] = f(i)
		}
		return out
	}
	var twentySix []*slackv1.Element
	for i := range 26 {
		twentySix = append(twentySix, button("b", "a"+strings.Repeat("i", i+1), ""))
	}
	elevenFields := make([]*chatv1.Text, 11)
	for i := range elevenFields {
		elevenFields[i] = plain("f")
	}
	section := func(s *slackv1.Section) *slackv1.Block {
		return &slackv1.Block{Kind: &slackv1.Block_Section{Section: s}}
	}

	for _, tc := range []struct {
		name   string
		blocks []*slackv1.Block
		want   string
	}{
		{"51 blocks", many(51, func(int) *slackv1.Block { return header("h") }), "blocks: 51 blocks, Slack allows at most 50"},
		{"3001-char section", []*slackv1.Block{header("h"), header("h"), section(&slackv1.Section{Text: plain(long(3001))})}, "blocks[2].section.text: 3001 characters, Slack allows at most 3000"},
		{"26 actions", []*slackv1.Block{actions(twentySix...)}, "blocks[0].actions.elements: 26 elements, Slack requires 1 to 25"},
		{"11 fields", []*slackv1.Block{section(&slackv1.Section{Fields: elevenFields})}, "blocks[0].section.fields: 11 fields, Slack allows at most 10"},
		{"2001-char field", []*slackv1.Block{section(&slackv1.Section{Fields: []*chatv1.Text{plain(long(2001))}})}, "blocks[0].section.fields[0]: 2001 characters, Slack allows at most 2000"},
		{"76-char button", []*slackv1.Block{actions(button(long(76), "a", ""))}, "blocks[0].actions.elements[0].button.text: 76 characters, Slack allows at most 75"},
		{"2001-char value", []*slackv1.Block{actions(button("b", "a", long(2001)))}, "blocks[0].actions.elements[0].button.value: 2001 characters, Slack allows at most 2000"},
		{"151-char header", []*slackv1.Block{header("h"), header(long(151))}, "blocks[1].header.text: 151 characters, Slack allows at most 150"},
		{"256-char action_id", []*slackv1.Block{actions(button("b", long(256), ""))}, "blocks[0].actions.elements[0].button.action_id: 256 characters, Slack allows at most 255"},
		{"256-char block_id", []*slackv1.Block{{BlockId: long(256), Kind: &slackv1.Block_Divider{Divider: &slackv1.Divider{}}}}, "blocks[0].block_id: 256 characters, Slack allows at most 255"},
		{"bad block_id", []*slackv1.Block{{BlockId: "a b", Kind: &slackv1.Block_Divider{Divider: &slackv1.Divider{}}}}, "blocks[0].block_id"},
		{"duplicate block_id", []*slackv1.Block{{BlockId: "a", Kind: &slackv1.Block_Divider{Divider: &slackv1.Divider{}}}, {BlockId: "a", Kind: &slackv1.Block_Divider{Divider: &slackv1.Divider{}}}}, "blocks[1].block_id: \"a\" is already used by blocks[0]"},
		{"duplicate action_id", []*slackv1.Block{actions(button("a", "go", ""), button("b", "go", ""))}, "blocks[0].actions.elements[1].button.action_id: \"go\" is already used"},
		{"empty section", []*slackv1.Block{section(&slackv1.Section{})}, "blocks[0].section: needs text or fields"},
		{"empty button text", []*slackv1.Block{actions(button("", "a", ""))}, "blocks[0].actions.elements[0].button.text: is empty"},
		{"11 context elements", []*slackv1.Block{{Kind: &slackv1.Block_Context{Context: &slackv1.Context{Elements: func() []*slackv1.ContextElement {
			out := make([]*slackv1.ContextElement, 11)
			for i := range out {
				out[i] = &slackv1.ContextElement{Kind: &slackv1.ContextElement_Text{Text: plain("c")}}
			}
			return out
		}()}}}}, "blocks[0].context.elements: 11 elements, Slack requires 1 to 10"},
		{"6 overflow options", []*slackv1.Block{section(&slackv1.Section{Text: plain("t"), Accessory: &slackv1.Element{Kind: &slackv1.Element_Overflow{Overflow: &slackv1.Overflow{
			ActionId: "o", Options: []*slackv1.Option{opt("1", "1"), opt("2", "2"), opt("3", "3"), opt("4", "4"), opt("5", "5"), opt("6", "6")},
		}}}})}, "blocks[0].section.accessory.overflow.options: 6 options, Slack requires 2 to 5"},
		{"initial option outside options", []*slackv1.Block{actions(&slackv1.Element{Kind: &slackv1.Element_StaticSelect{StaticSelect: &slackv1.StaticSelect{
			ActionId: "s", Placeholder: plain("p"), Options: []*slackv1.Option{opt("a", "a")}, InitialOption: opt("z", "z"),
		}}})}, "blocks[0].actions.elements[0].static_select.initial_option"},
		{"initial option sharing only a value", []*slackv1.Block{actions(&slackv1.Element{Kind: &slackv1.Element_StaticSelect{StaticSelect: &slackv1.StaticSelect{
			ActionId: "s", Placeholder: plain("p"), Options: []*slackv1.Option{opt("A", "a")}, InitialOption: opt("Other", "a"),
		}}})}, "blocks[0].actions.elements[0].static_select.initial_option"},
		{"image in actions", []*slackv1.Block{actions(&slackv1.Element{Kind: &slackv1.Element_Image{Image: &slackv1.Image{Url: "https://x.test/a.png", AltText: "a"}}})}, "blocks[0].actions.elements[0].image"},
		{"http image", []*slackv1.Block{{Kind: &slackv1.Block_Image{Image: &slackv1.Image{Url: "http://x.test/a.png", AltText: "a"}}}}, "blocks[0].image.url"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := render.Blocks(tc.blocks)
			if err == nil {
				t.Fatalf("accepted; want an error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q\nwant it to contain %q", err, tc.want)
			}
		})
	}
}

func TestSeveralViolationsAreReportedTogether(t *testing.T) {
	blocks := make([]*slackv1.Block, 3)
	for i := range blocks {
		blocks[i] = &slackv1.Block{Kind: &slackv1.Block_Header{Header: &slackv1.Header{Text: plain(strings.Repeat("h", 151))}}}
	}
	_, err := render.Blocks(blocks)
	if err == nil {
		t.Fatal("accepted")
	}
	for _, path := range []string{"blocks[0].header.text", "blocks[1].header.text", "blocks[2].header.text"} {
		if !strings.Contains(err.Error(), path) {
			t.Errorf("error does not name %s:\n%s", path, err)
		}
	}
}

func TestEscapingCountsTowardTheLimit(t *testing.T) {
	// 1500 ampersands are 1500 characters as written and 7500 once escaped.
	_, err := render.Blocks(sectionOf(tmpl("{a}", map[string]string{"a": strings.Repeat("&", 1500)})))
	if err == nil || !strings.Contains(err.Error(), "blocks[0].section.text: 7500 characters") {
		t.Errorf("error = %v, want the rendered length refused", err)
	}
}
