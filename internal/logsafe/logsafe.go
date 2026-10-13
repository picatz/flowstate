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
	"log/slog"
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
// not known at the call site. An error whose Error method panics, as a nil
// pointer implementing error does, is written as "<nil>" the way
// [log/slog] writes it, rather than taking the caller down.
func Value(v any) (safe any) {
	switch v := v.(type) {
	case string:
		return String(v)
	case error:
		defer func() {
			if recover() != nil {
				safe = "<nil>"
			}
		}()

		return String(v.Error())
	default:
		return v
	}
}

// Args returns args with the value of each key/value pair passed through
// [Value]. It walks the arguments as [log/slog.Logger.Log] does: a string key
// takes the next argument as its value, and a [log/slog.Attr], or anything else
// where a key belongs, stands alone and is kept as written (an Attr's own
// string value is rewritten, as a pair's is).
func Args(args ...any) []any {
	out := make([]any, 0, len(args))

	for i := 0; i < len(args); i++ {
		switch key := args[i].(type) {
		case string:
			out = append(out, key)

			if i+1 < len(args) {
				i++

				out = append(out, Value(args[i]))
			}
		case slog.Attr:
			out = append(out, safeAttr(key))
		default:
			out = append(out, key)
		}
	}

	return out
}

func safeAttr(a slog.Attr) slog.Attr {
	v := a.Value.Resolve()

	switch v.Kind() {
	case slog.KindString:
		return slog.String(a.Key, String(v.String()))
	case slog.KindAny:
		return slog.Any(a.Key, Value(v.Any()))
	case slog.KindGroup:
		group := v.Group()
		safe := make([]any, len(group))

		for i, member := range group {
			safe[i] = safeAttr(member)
		}

		return slog.Group(a.Key, safe...)
	default:
		return a
	}
}

func hex2(b byte) string {
	const digits = "0123456789abcdef"

	return string([]byte{digits[b>>4], digits[b&0xf]})
}
