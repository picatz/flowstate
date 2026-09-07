package flowfile

import (
	"fmt"
	"strings"
)

// yamlMappingValue is the sentence goccy's parser gives for `a: b: c` — a
// second `: ` on a line that already has one — spelled once here so the
// recogniser below is anchored on the exact message rather than a substring
// of it. See parser.go's parseMapValue in the pinned goccy version.
const yamlMappingValue = "mapping value is not allowed in this context"

// offerQuotedFence rewrites the YAML parser's mapping-value refusal into this
// language's own sentence when the token it stopped on opens an unquoted
// `${...}` whose text holds a `: ` (#1683).
//
// The shape is a ternary written the way every other position lets an author
// write an expression:
//
//	value: ${inputs.priority == "express" ? steps.a.value : steps.b.value}
//
// YAML reads a plain scalar up to the first `: ` and takes what precedes it
// as a mapping key, so the value ends in the middle of the expression and the
// parser refuses the line with a sentence about mappings. Every verb stopped
// there — validate, lint, fmt, run, the language server — in goccy's voice,
// and nothing said that the fix is to quote the whole scalar, which is what
// the corpus does everywhere a ternary appears. This is the third YAML trap
// beside #1466's two, and it fires on the most common CEL construct after
// field access.
//
// Read from the source line rather than from the parser's token, because the
// token ends where YAML decided the key did: the text this names is the
// scalar the author wrote, fence to closing brace. The edit wraps it in single
// quotes, doubling any quote inside, which is the spelling `flow fmt` keeps
// and the one the shipped examples use; a trailing comment stays outside.
//
// Only that shape. A `: ` on a line whose value does not open with a fence is
// the mapping mistake goccy describes, and its sentence stands.
func offerQuotedFence(data []byte, d *Diagnostic) {
	if d.Message != yamlMappingValue || d.Line <= 0 || d.Column <= 0 {
		return
	}
	line, ok := sourceLine(data, d.Line)
	if !ok {
		return
	}
	runes := []rune(line)
	if d.Column > len(runes) {
		return
	}
	rest := string(runes[d.Column-1:])
	if !strings.HasPrefix(rest, "${") {
		return
	}
	scalar := plainScalarText(rest)
	if !strings.HasSuffix(scalar, "}") || !strings.Contains(scalar, ": ") {
		// Either the line carries text after the fence closes, in which case
		// the mapping goccy describes is outside the expression and quoting
		// would not be the fix, or the fence holds no `: ` and something else
		// on the line is the key.
		return
	}

	quoted := "'" + strings.ReplaceAll(scalar, "'", "''") + "'"
	d.Message = fmt.Sprintf(
		"an unquoted expression holding %q is read by YAML as a mapping key (a ternary's \"? a : b\" is the usual one), "+
			"so the value stopped before the fence closed; quote the whole value, '${...}'", ": ")
	start := Position{Line: d.Line, Column: d.Column}
	if edit := replaceSpan("quote the expression", Span{Start: start, End: advance(start, scalar)}, quoted); edit != nil {
		d.Edits = append(d.Edits, edit)
	}
}

// sourceLine returns the 1-based line of a document without its line ending,
// or false when the document has no such line.
func sourceLine(data []byte, n int) (string, bool) {
	lines := strings.SplitN(string(data), "\n", n+1)
	if n > len(lines) {
		return "", false
	}
	return strings.TrimSuffix(lines[n-1], "\r"), true
}

// plainScalarText is the part of a line's remainder that a plain scalar
// opening with a fence covers: through the last closing brace when only a
// comment or nothing follows it, and otherwise the whole remainder with the
// trailing space removed.
//
// A comment starts at a `#` YAML would read as one, which is a `#` after
// whitespace; `}#tail` is part of the scalar and stays inside the quotes.
func plainScalarText(rest string) string {
	if brace := strings.LastIndexByte(rest, '}'); brace >= 0 {
		after := rest[brace+1:]
		trimmed := strings.TrimLeft(after, " \t")
		if trimmed == "" || (len(trimmed) < len(after) && strings.HasPrefix(trimmed, "#")) {
			return rest[:brace+1]
		}
	}
	return strings.TrimRight(rest, " \t")
}
