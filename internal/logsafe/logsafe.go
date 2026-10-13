// Package logsafe is the one place a value another party chose is made safe to
// write into a log line.
//
// A request path, a peer address, a rejection reason, or an error that quotes
// a delivery is controlled by whoever sent the request. Written bare, a line
// break in it forges a second log entry and any other control character
// reaches whatever terminal or line reader the operator reads the log with
// (CWE-117). [String] rewrites every rune [termsafe.Unsafe] names as a visible
// escape, so a log line stays one line and says what it was sent; [Value] does
// the same for the values of a logger's key/value arguments.
//
// The rewrite is applied where a value is logged rather than inside a log
// handler, because the handler cannot know which attributes carry another
// party's text: the call site can.
package logsafe

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/picatz/flowstate/internal/termsafe"
)

// String returns s with every [termsafe.Unsafe] rune replaced by a visible
// escape: \n and \r for the line breaks, otherwise Go's own quoting (\x1b,
// \u202e). A backslash is left alone, so the escapes read as
// escapes only to a human; the rewrite is for reading, not for decoding.
// A string with nothing to escape is returned unchanged and unallocated.
// Bytes that are not valid UTF-8 are written as \xXX, so they cannot
// masquerade as a character either.
func String(s string) string {
	// The line breaks first, by name: they are the characters that end a log
	// entry, so they are the ones an operator most needs to see spelled out.
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, "\r", `\r`)

	var out strings.Builder
	copied := 0

	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])

		var escape string

		switch {
		case r == utf8.RuneError && size <= 1:
			escape = `\x` + hex2(s[i])
		case termsafe.Unsafe(r):
			escape = strconv.QuoteToASCII(string(r))
			escape = escape[1 : len(escape)-1]
		default:
			i += size

			continue
		}

		out.WriteString(s[copied:i])
		out.WriteString(escape)

		i += size
		copied = i
	}

	if copied == 0 {
		return s
	}

	out.WriteString(s[copied:])

	return out.String()
}

// Value returns v made safe to log: a string through [String], an error as
// [String] of its message, and everything else unchanged. It exists for the
// variadic key/value arguments of a [log/slog.Logger], where a value's type is
// not known at the call site.
func Value(v any) any {
	switch v := v.(type) {
	case string:
		return String(v)
	case error:
		if v == nil {
			return v
		}

		return String(v.Error())
	default:
		return v
	}
}

// Args returns args with the value of each key/value pair passed through
// [Value]. Keys are the call site's own constants and are kept as written, as
// are a [log/slog.Attr] and any odd trailing element.
func Args(args ...any) []any {
	out := make([]any, len(args))

	for i, arg := range args {
		if i%2 == 1 {
			out[i] = Value(arg)

			continue
		}

		out[i] = arg
	}

	return out
}

func hex2(b byte) string {
	const digits = "0123456789abcdef"

	return string([]byte{digits[b>>4], digits[b&0xf]})
}
