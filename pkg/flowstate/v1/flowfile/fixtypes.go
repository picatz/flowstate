package flowfile

import (
	"cmp"
	"slices"
	"strings"

	"github.com/goccy/go-yaml/ast"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Edition v2026.4 retires the legacy `type:` words `list`, `struct` and `float`
// (see [CurrentEdition]); this is the rewrite that carries a file across.
//
// The rewrite is total and guesses nothing: `list` held any list, so it is
// `list(dyn)`; `struct` was an open map with no fields, so it is
// `map(string, dyn)`; and CEL spells `float` as `double`. An input or output's
// `type:` is the only place these words were types, so only those two blocks are
// read, and only a `type:` whose whole value is one of the three.

// retiredTypeWords maps each retired word to what it meant.
var retiredTypeWords = map[string]string{
	"list":   retiredTypeSpellings[v1.InputDeclaration_TYPE_LIST],
	"struct": retiredTypeSpellings[v1.InputDeclaration_TYPE_STRUCT],
	"float":  retiredTypeSpellings[v1.InputDeclaration_TYPE_FLOAT],
}

// declarationTypes rewrites the retired `type:` words in an `inputs:` or
// `outputs:` mapping.
func (f *fixer) declarationTypes(n ast.Node) {
	n = unwrapAnchor(n)
	declarations, ok := n.(*ast.MappingNode)
	if !ok {
		return
	}

	for _, entry := range declarations.Values {
		body, ok := unwrapAnchor(entry.Value).(*ast.MappingNode)
		if !ok {
			continue
		}

		for _, field := range body.Values {
			if name, ok := keyNameOf(field.Key); !ok || name != "type" {
				continue
			}

			f.retiredTypeWord(field.Value, body.IsFlowStyle)
		}
	}
}

// retiredTypeWord rewrites one `type:` value that is a retired word, in place on
// its source line so a trailing comment and the spacing survive.
//
// Inside a YAML flow mapping the comma in `map(string, dyn)` ends the value, so
// the replacement is quoted there, which is the form the diagnostic for the
// unquoted spelling asks for (#1466).
func (f *fixer) retiredTypeWord(value ast.Node, flow bool) {
	text, ok := scalarText(value)
	if !ok {
		return
	}
	now, retired := retiredTypeWords[text]
	if !retired {
		return
	}

	span := spanOfNode(value)
	if !span.IsValid() || span.Start.Line > len(f.lines) || span.Start.Line != span.End.Line {
		f.refuseRetired(value, text, now)

		return
	}

	line := f.lines[span.Start.Line-1]
	start, end, ok := scalarBounds(line, span.Start.Column-1, text)
	if !ok {
		// Not where the parser said: refusing beats an off-by-one edit.
		f.refuseRetired(value, text, now)

		return
	}

	written := now
	if flow && strings.Contains(now, ",") {
		written = `"` + now + `"`
	}

	f.replaceOnLine(span.Start.Line, typeSpan{start: start, end: end, text: written},
		"`type: "+text+"` is now `type: "+now+"`",
		"`type: "+text+"` would become `type: "+now+"`")
}

func (f *fixer) refuseRetired(value ast.Node, text, now string) {
	f.refuse(value, "`%s` is retired in edition %s and means %s, but this line is not shaped so it can be rewritten safely; change it by hand",
		text, CurrentEdition, now)
}

// scalarBounds finds the whole scalar `text` on a line, quotes included, given
// the column the parser reported (which may or may not point at the quote).
func scalarBounds(line string, at int, text string) (start, end int, ok bool) {
	for _, from := range []int{at, at - 1} {
		if from < 0 || from >= len(line) {
			continue
		}

		for _, quote := range []string{"", `"`, `'`} {
			token := quote + text + quote
			if strings.HasPrefix(line[from:], token) {
				return from, from + len(token), true
			}
		}
	}

	return 0, 0, false
}

// typeSpan is one replacement within a source line, as byte offsets.
type typeSpan struct {
	start, end int
	text       string
}

// replaceOnLine records a replacement and rebuilds the line's one edit from
// every replacement recorded for it so far. A flow-style mapping holds many
// declarations on one line, and the edit map keeps one edit per line, so
// recording each separately would spend a fix round per declaration.
func (f *fixer) replaceOnLine(line int, span typeSpan, message, pending string) {
	if f.typeSpans == nil {
		f.typeSpans = make(map[int][]typeSpan)
	}

	if _, ours := f.typeSpans[line]; !ours {
		if _, taken := f.edits[line]; taken {
			return
		}
	}

	spans := append(f.typeSpans[line], span)
	f.typeSpans[line] = spans

	sorted := slices.SortedFunc(slices.Values(spans), func(a, b typeSpan) int { return cmp.Compare(b.start, a.start) })
	rebuilt := f.lines[line-1]
	for _, s := range sorted {
		rebuilt = rebuilt[:s.start] + s.text + rebuilt[s.end:]
	}

	if f.edits == nil {
		f.edits = make(map[int]lineEdit)
	}
	f.edits[line] = lineEdit{through: line, replacement: []string{rebuilt}}
	f.changes = append(f.changes, FixChange{Line: line, Message: message, Pending: pending})
}
