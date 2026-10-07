// Package render turns flowstate.chat.v1 cards and slack.v1 blocks into Slack
// Block Kit JSON, and holds every Slack limit the plugin enforces.
//
// Three properties are the point of the package, and each has a test:
//
//   - Safe by construction. Free text is plain_text. Markup is a template whose
//     arguments are escaped, so a value that came from an event can never
//     become a mention, a broadcast or a link ([Text], [Escape]).
//   - Deterministic. Output is built from structs with ordered fields, never
//     maps, so one input is always the same bytes and the golden files in
//     testdata can be compared exactly.
//   - Bounded before any request. [Validate] names the path of every violated
//     limit, such as `blocks[2].section.text`, so the author fixes it from the
//     error alone rather than from a Slack 400.
package render

import (
	"fmt"
	"regexp"
	"strings"

	chatv1 "github.com/picatz/flowstate/pkg/flowstate/chat/v1"
)

// TextObject is Slack's text composition object.
type TextObject struct {
	// Type is "plain_text" or "mrkdwn".
	Type string `json:"type"`
	// Text is the content: literal for plain_text, markup for mrkdwn.
	Text string `json:"text"`
	// Emoji asks Slack to turn :shortcodes: into emoji; plain_text only.
	Emoji bool `json:"emoji,omitempty"`
	// Verbatim stops Slack from auto-linking channel names and @-mentions
	// inside mrkdwn text, so only what the template spells out is linked.
	Verbatim bool `json:"verbatim,omitempty"`
}

func (TextObject) contextElement() {}

var entities = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// Escape makes s literal inside Slack mrkdwn: the three characters Slack
// reserves for its own markup, & < >, become entities, so the text can no
// longer open a mention (`<@U1>`), a broadcast (`<!channel>`) or a link
// (`<https://x|y>`). Every markup argument passes through it.
func Escape(s string) string { return entities.Replace(s) }

var (
	argName      = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
	userID       = regexp.MustCompile(`^[UW][A-Z0-9]{1,20}$`)
	channelID    = regexp.MustCompile(`^[CG][A-Z0-9]{1,254}$`)
	linkPrefix   = regexp.MustCompile(`^<(https?://[^/\s|>]+/|mailto:)`)
	specialToken = regexp.MustCompile(`^<[@#!]`)
)

// Text renders t as a Slack text object: `plain` becomes plain_text (with the
// emoji flag when asked), and `markup` becomes mrkdwn with verbatim set. path
// names t in errors, such as `blocks[0].section.text`.
func Text(path string, t *chatv1.Text) (TextObject, error) {
	switch k := t.GetKind().(type) {
	case *chatv1.Text_Plain:
		return TextObject{Type: "plain_text", Text: k.Plain, Emoji: t.GetEmoji()}, nil
	case *chatv1.Text_Markup:
		s, err := markup(path+".markup", k.Markup)
		if err != nil {
			return TextObject{}, err
		}
		return TextObject{Type: "mrkdwn", Text: s, Verbatim: true}, nil
	}
	return TextObject{}, fmt.Errorf("%s: needs `plain` or `markup`", path)
}

// Plain renders t where Slack accepts plain text only (headers, button labels,
// menu options, placeholders). A markup text is refused by name rather than
// silently flattened.
func Plain(path string, t *chatv1.Text) (TextObject, error) {
	if _, ok := t.GetKind().(*chatv1.Text_Markup); ok {
		return TextObject{}, fmt.Errorf("%s: Slack shows this as plain text only, so it cannot be `markup`; write `plain:`", path)
	}
	return Text(path, t)
}

// Mrkdwn renders t as a string to embed in mrkdwn the renderer is composing
// (a card field, a status line): plain text is escaped, markup is expanded.
func Mrkdwn(path string, t *chatv1.Text) (string, error) {
	switch k := t.GetKind().(type) {
	case *chatv1.Text_Plain:
		return Escape(k.Plain), nil
	case *chatv1.Text_Markup:
		return markup(path+".markup", k.Markup)
	}
	return "", fmt.Errorf("%s: needs `plain` or `markup`", path)
}

// mentionToken is the exact text a declared mention renders as, and the only
// `<@`, `<#` or `<!` token a template may contain.
func mentionToken(path string, m *chatv1.Mention) (string, error) {
	switch k := m.GetKind().(type) {
	case *chatv1.Mention_User:
		if !userID.MatchString(k.User) {
			return "", fmt.Errorf("%s.user: %q is not a Slack user ID such as U0123ABCD", path, k.User)
		}
		return "<@" + k.User + ">", nil
	case *chatv1.Mention_Channel:
		if !channelID.MatchString(k.Channel) {
			return "", fmt.Errorf("%s.channel: %q is not a Slack channel ID such as C0123ABCD", path, k.Channel)
		}
		return "<#" + k.Channel + ">", nil
	case *chatv1.Mention_Broadcast:
		switch k.Broadcast {
		case chatv1.Broadcast_BROADCAST_HERE:
			return "<!here>", nil
		case chatv1.Broadcast_BROADCAST_CHANNEL:
			return "<!channel>", nil
		}
		return "", fmt.Errorf("%s.broadcast: must be BROADCAST_HERE or BROADCAST_CHANNEL; there is no way to notify the whole workspace", path)
	}
	return "", fmt.Errorf("%s: needs `user`, `channel` or `broadcast`", path)
}

// markup expands a template. The grammar is deliberately tiny: `{name}` is an
// argument, `{{` and `}}` are literal braces, and everything else is the
// author's mrkdwn. Arguments are escaped, a mention token is accepted only when
// declared in mentions, and an argument may sit inside a `<...>` link only
// after the link's host is spelled out by the author.
func markup(path string, m *chatv1.Markup) (string, error) {
	declared := map[string]int{} // token -> index in mentions
	for i, mention := range m.GetMentions() {
		tok, err := mentionToken(fmt.Sprintf("%s.mentions[%d]", path, i), mention)
		if err != nil {
			return "", err
		}
		declared[tok] = i
	}
	usedMention := map[string]bool{}
	usedArg := map[string]bool{}

	tmpl := m.GetTemplate()
	var out strings.Builder
	tokenStart := -1 // offset in out of an open literal '<', or -1

	closeToken := func() error {
		tok := out.String()[tokenStart:]
		tokenStart = -1
		if !specialToken.MatchString(tok) {
			return nil
		}
		if _, ok := declared[tok]; !ok {
			return fmt.Errorf("%s.template: %s is a mention the template does not declare; list it under `mentions:` (a template can only notify who it names there)", path, tok)
		}
		usedMention[tok] = true
		return nil
	}

	for i := 0; i < len(tmpl); i++ {
		c := tmpl[i]
		switch {
		case c == '{' && i+1 < len(tmpl) && tmpl[i+1] == '{':
			out.WriteByte('{')
			i++
		case c == '}' && i+1 < len(tmpl) && tmpl[i+1] == '}':
			out.WriteByte('}')
			i++
		case c == '{':
			end := strings.IndexByte(tmpl[i:], '}')
			if end < 0 {
				return "", fmt.Errorf("%s.template: '{' at offset %d is never closed; write {{ for a literal brace", path, i)
			}
			name := tmpl[i+1 : i+end]
			if !argName.MatchString(name) {
				return "", fmt.Errorf("%s.template: %q is not a placeholder name (lowercase letters, digits, '_'); write {{ for a literal brace", path, "{"+name+"}")
			}
			val, ok := m.GetArgs()[name]
			if !ok {
				return "", fmt.Errorf("%s.template: placeholder {%s} names no entry of `args:`%s", path, name, didYouMean(name, m.GetArgs()))
			}
			if tokenStart >= 0 && !linkPrefix.MatchString(out.String()[tokenStart:]) {
				return "", fmt.Errorf("%s.template: placeholder {%s} sits inside <...> before the link's host is written out; spell the target as literal text up to its host (for example <https://example.com/{%s}|label>) so a value cannot choose where the link points", path, name, name)
			}
			usedArg[name] = true
			out.WriteString(Escape(val))
			i += end
		case c == '}':
			return "", fmt.Errorf("%s.template: '}' at offset %d closes nothing; write }} for a literal brace", path, i)
		case c == '<':
			if tokenStart >= 0 && specialToken.MatchString(out.String()[tokenStart:]) {
				return "", fmt.Errorf("%s.template: %q is not a complete mention", path, out.String()[tokenStart:])
			}
			tokenStart = out.Len()
			out.WriteByte(c)
		case c == '>':
			out.WriteByte(c)
			if tokenStart >= 0 {
				if err := closeToken(); err != nil {
					return "", err
				}
			}
		default:
			out.WriteByte(c)
		}
	}
	if tokenStart >= 0 && specialToken.MatchString(out.String()[tokenStart:]) {
		return "", fmt.Errorf("%s.template: %q is not a complete mention", path, out.String()[tokenStart:])
	}
	for name := range m.GetArgs() {
		if !usedArg[name] {
			return "", fmt.Errorf("%s.args: %q is never used by the template; remove it or add {%s}", path, name, name)
		}
	}
	for i, mention := range m.GetMentions() {
		tok, _ := mentionToken("", mention)
		if !usedMention[tok] {
			return "", fmt.Errorf("%s.mentions[%d]: %s is declared but the template never contains it; write it in the template where it should appear", path, i, tok)
		}
	}
	return out.String(), nil
}

// didYouMean suggests an existing arg when a placeholder is a near miss, so a
// typo is fixed from the error alone.
func didYouMean(name string, args map[string]string) string {
	if len(args) == 0 {
		return " (there are no args)"
	}
	best, bestScore := "", 0
	for have := range args {
		if s := commonPrefix(name, have); s > bestScore || (s == bestScore && best != "" && have < best) {
			best, bestScore = have, s
		}
	}
	if bestScore >= 2 {
		return fmt.Sprintf("; did you mean {%s}?", best)
	}
	return ""
}

func commonPrefix(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}
