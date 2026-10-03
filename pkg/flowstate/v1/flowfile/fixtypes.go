package flowfile

import (
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
		f.refuse(value, "`%s` is retired in edition %s and means %s, but this line is not shaped so it can be rewritten safely; change it by hand",
			text, CurrentEdition, now)

		return
	}

	line := f.lines[span.Start.Line-1]
	at := span.Start.Column - 1
	if at < 0 || at+len(text) > len(line) || line[at:at+len(text)] != text {
		// Quoted, or not where the parser said: refusing beats an off-by-one edit.
		f.refuse(value, "`%s` is retired in edition %s and means %s, but this line is not shaped so it can be rewritten safely; change it by hand",
			text, CurrentEdition, now)

		return
	}

	written := now
	if flow && strings.Contains(now, ",") {
		written = `"` + now + `"`
	}

	f.record(span.Start.Line, span.Start.Line,
		[]string{line[:at] + written + line[at+len(text):]},
		"`type: "+text+"` is now `type: "+now+"`",
		"`type: "+text+"` would become `type: "+now+"`")
}
