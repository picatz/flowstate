package flowdebug

import (
	"iter"
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokenKind classifies a run of text [ValueTokens] found in a rendered value.
type TokenKind int

const (
	// TokenPlain is punctuation, indentation and anything not classified.
	TokenPlain TokenKind = iota

	// TokenKey is a map key or a list index: the label of a child.
	TokenKey

	// TokenString is a quoted string value.
	TokenString

	// TokenLiteral is a number, `true`, `false` or `null`.
	TokenLiteral

	// TokenRedacted is the text of a withheld value's marker, kept apart so a
	// reader can find it. It is recognised by its bytes: a string that spells
	// the marker is, by construction, indistinguishable from one the redactor
	// wrote, here and in the plain text alike.
	TokenRedacted

	// TokenElision is what a layout left out: `… 12 more keys`, `{… 7 keys}`.
	TokenElision
)

// redactedMarker is what a withheld leaf is written as, with or without the
// quotes its string form carries.
const redactedMarker = "[redacted]"

// ValueTokens splits text written by [RenderValue], or the compact JSON it
// keeps for a small value, into classified runs, in order.
//
// It colours and decides nothing: the runs concatenate to exactly text, so a
// front that ignores the kinds writes the same bytes as one that styles them,
// and nothing here can add, drop or reorder a character of what was redacted
// before it got here. It reads only the shapes the renderer writes, so text of
// another origin comes out as plain runs rather than as a guess at a grammar.
func ValueTokens(text string) iter.Seq2[TokenKind, string] {
	return func(yield func(TokenKind, string) bool) {
		for line := range strings.SplitAfterSeq(text, "\n") {
			if line != "" && !tokenizeLine(line, yield) {
				return
			}
		}
	}
}

// tokenizeLine reports whether the caller wants more.
func tokenizeLine(line string, yield func(TokenKind, string) bool) bool {
	body := strings.TrimLeft(line, " ")
	indent := line[:len(line)-len(body)]
	if indent != "" && !yield(TokenPlain, indent) {
		return false
	}
	if strings.HasPrefix(body, "…") {
		return yield(TokenElision, strings.TrimSuffix(body, "\n")) && yieldNewline(body, yield)
	}

	// A tree line begins with a bare key (`name:`) or an index (`[3]`); a
	// compact one begins with whatever JSON does and is handled below.
	if key, rest, ok := treeLabel(body); ok {
		if !yield(TokenKey, key) {
			return false
		}
		body = rest
	}

	for body != "" {
		var (
			kind TokenKind
			n    int
		)
		switch r, _ := utf8.DecodeRuneInString(body); {
		case r == '"':
			n = quotedLen(body)
			kind = TokenString
			switch {
			case strings.HasPrefix(body[n:], ":"):
				kind = TokenKey
			case body[:n] == `"`+redactedMarker+`"`:
				kind = TokenRedacted
			}
		case strings.HasPrefix(body, redactedMarker):
			n, kind = len(redactedMarker), TokenRedacted
		case (r == '{' || r == '[') && strings.HasPrefix(body[1:], "…"):
			n = max(strings.IndexByte(body, byte(closer(r))), 0) + 1
			kind = TokenElision
		case isLiteralStart(r):
			n = wordLen(body)
			kind = TokenLiteral
			if !isLiteral(body[:n]) {
				kind = TokenPlain
			}
		default:
			n = plainLen(body)
			kind = TokenPlain
		}
		if !yield(kind, body[:n]) {
			return false
		}
		body = body[n:]
	}

	return true
}

func yieldNewline(body string, yield func(TokenKind, string) bool) bool {
	return !strings.HasSuffix(body, "\n") || yield(TokenPlain, "\n")
}

func closer(open rune) rune {
	if open == '{' {
		return '}'
	}

	return ']'
}

// treeLabel reports a line's leading key and what follows it, where the line
// is `key:` or `[n]` as [RenderValue] writes a tree.
func treeLabel(body string) (label, rest string, ok bool) {
	if strings.HasPrefix(body, "[") {
		end := strings.IndexByte(body, ']')
		if end < 0 || end == 1 || strings.Trim(body[1:end], "0123456789") != "" {
			return "", body, false
		}

		return body[:end+1], body[end+1:], true
	}
	if strings.HasPrefix(body, `"`) {
		n := quotedLen(body)
		if strings.HasPrefix(body[n:], ":") {
			return body[:n], body[n:], true
		}

		return "", body, false
	}
	end := strings.IndexByte(body, ':')
	if end <= 0 || strings.ContainsFunc(body[:end], func(r rune) bool {
		return unicode.IsSpace(r) || strings.ContainsRune(`{}[]",`, r)
	}) {
		return "", body, false
	}

	return body[:end+1], body[end+1:], true
}

// quotedLen is the length of the JSON string at the start of s, through its
// closing quote, or of all of s when it never closes.
func quotedLen(s string) int {
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case '"':
			return i + 1
		}
	}

	return len(s)
}

func isLiteralStart(r rune) bool {
	return r == '-' || r >= '0' && r <= '9' || r == 't' || r == 'f' || r == 'n'
}

func wordLen(s string) int {
	for i, r := range s {
		if !(r == '-' || r == '+' || r == '.' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
			return max(i, 1)
		}
	}

	return len(s)
}

func isLiteral(word string) bool {
	switch word {
	case "true", "false", "null":
		return true
	}

	return word != "" && strings.Trim(word, "-+.0123456789eE") == "" && strings.ContainsAny(word, "0123456789")
}

// plainLen is the run up to the next character a token could begin with.
func plainLen(s string) int {
	for i, r := range s {
		if i > 0 && (r == '"' || r == '[' || r == '{' || isLiteralStart(r)) {
			return i
		}
	}

	return len(s)
}
