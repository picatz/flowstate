package lsp

import (
	"strings"

	"github.com/sourcegraph/go-lsp"
)

// keyValueOnLine reads line as a "key: value" line — using the same [keyLine]
// pattern [keyAndPosition] matches for the workflow grammar — and returns the
// key name, the value's text (quotes and a trailing comment trimmed by
// [unquote]), and the value's range: from just after the colon and any
// spaces to the end of the line. False when line declares no key at all.
//
// Character offsets are UTF-16 code units, converted with the package's own
// [utf16Len] over the line's prefix — LSP's Position.Character counts code
// units, and a byte count past a non-ASCII case name (`name: café`) ends the
// range beyond the text, which an editor may misplace or refuse outright
// (Codex, #1173). The first version argued bytes were close enough; the
// converter needs only the line string, which is exactly what this has.
//
// Shared by [testDocumentSymbols] (filtering on key == "name") and
// [hoverTestDocument] (filtering on key == "task"): both want the same two
// things about a "key: value" line, one to build a symbol's range and name,
// the other to build a hover's range and registry lookup.
func keyValueOnLine(line string, lineNo int) (key, value string, rng lsp.Range, ok bool) {
	m := keyLine.FindStringSubmatch(line)
	if m == nil {
		return "", "", lsp.Range{}, false
	}
	after := len(m[1]) + len(m[2]) + len(m[3])
	colon := strings.Index(line[after:], ":")
	if colon < 0 {
		// Unreachable given the pattern matched: keyLine requires a colon
		// after the key. A negative index here would silently place the
		// value at the key's own start.
		return "", "", lsp.Range{}, false
	}
	start := after + colon + 1
	for start < len(line) && line[start] == ' ' {
		start++
	}
	rng = lsp.Range{
		Start: lsp.Position{Line: lineNo, Character: utf16Len(line[:start])},
		End:   lsp.Position{Line: lineNo, Character: utf16Len(line)},
	}
	return m[3], unquote(line[start:]), rng, true
}
