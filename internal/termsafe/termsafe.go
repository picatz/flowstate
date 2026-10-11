// Package termsafe is the one definition of which runes must not reach a
// terminal bare, and the JSON rewrite that keeps them from doing so.
//
// Text a process did not write — a plugin's task name, a workload's failure
// message — is printed to a terminal by both the text views and `-o json`. The
// text views escape through [Unsafe]; the JSON paths through [EscapeJSON]. Both
// ask the same predicate, so the two surfaces cannot disagree about what is
// dangerous (#2691, #2693).
package termsafe

import (
	"unicode"
	"unicode/utf8"
)

// Unsafe reports whether a rune is written out rather than emitted: a control
// character (C0, DEL and the C1 range, which includes U+009B, the single-byte
// CSI), a bidirectional control such as U+202E that is not a control by
// Unicode's category yet reorders the text around it, and the line and
// paragraph separators U+2028 and U+2029, which some terminals and most line
// readers treat as a line break.
func Unsafe(r rune) bool {
	return unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r) || r == '\u2028' || r == '\u2029'
}

// EscapeJSON rewrites every [Unsafe] rune in an encoded JSON document as a
// \uXXXX escape, which decodes back to the same string.
//
// The document's structural bytes are all ASCII and so is every escape
// sequence it already holds, so a backslash-u spelling in the input passes
// through unchanged and is never escaped twice. The unsafe runes can only
// occur inside string tokens (a bare one outside is already invalid JSON), so
// the result is valid JSON exactly when the input was. ASCII controls other
// than DEL are already escaped by every encoder this is applied to; DEL is not,
// and is handled here. Bytes that are not valid UTF-8 are copied through.
func EscapeJSON(encoded []byte) []byte {
	var out []byte
	copied := 0

	for i := 0; i < len(encoded); {
		r, size := utf8.DecodeRune(encoded[i:])
		// ASCII belongs to the encoder (its whitespace and its escapes) except
		// DEL, which no encoder escapes.
		if (r == utf8.RuneError && size <= 1) || (r < utf8.RuneSelf && r != 0x7f) || !Unsafe(r) {
			i += size

			continue
		}

		if out == nil {
			out = make([]byte, 0, len(encoded)+32)
		}
		out = append(out, encoded[copied:i]...)
		out = append(out, '\\', 'u',
			hexDigits[r>>12&0xf], hexDigits[r>>8&0xf], hexDigits[r>>4&0xf], hexDigits[r&0xf])
		i += size
		copied = i
	}

	if out == nil {
		return encoded
	}

	return append(out, encoded[copied:]...)
}

const hexDigits = "0123456789abcdef"
