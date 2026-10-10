package flowfile

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"

	"github.com/picatz/flowstate/internal/strictyaml"
)

// yamlMappingValue is the sentence goccy's parser gives for `a: b: c` — a
// second `: ` on a line that already has one — spelled once here so the
// recogniser below is anchored on the exact message rather than a substring
// of it. See parser.go's parseMapValue in the pinned goccy version.
const yamlMappingValue = "mapping value is not allowed in this context"

// yamlFlowMappingEnd is the sentence goccy's parser gives when a flow mapping
// meets something other than a comma or its closing brace. A fence written
// inside one reaches it: the parser takes `$` as the entry's value and stops at
// the `{` after it, where it expected a comma or the closing brace (#1466).
const yamlFlowMappingEnd = "',' or '}' must be specified"

// yamlUnterminatedDouble and yamlUnterminatedSingle are the sentences goccy's
// scanner gives when the `: ` inside a string literal of an unquoted
// expression is followed by a quote: `message: ${x + "a: "}` reads `${x + "a`
// as a key and the closing `"` as the start of a quoted value that never ends
// (#2660). The same trap as the ternary, reached through a different token, so
// the same recogniser answers it.
const (
	yamlUnterminatedDouble = "could not find end character of double-quoted text"
	yamlUnterminatedSingle = "could not find end character of single-quoted text"
)

// fenceValue matches the head of a line whose value is a plain scalar opening
// with a fence: indentation and sequence dashes, an optional key (plain or
// quoted), then `${`. Group 1 is everything before the key or the scalar, which
// is the column the line's node starts at.
var fenceValue = regexp.MustCompile(`^(\s*(?:-\s+)*)(?:(?:"[^"]*"|'[^']*'|[^\s#'"{}\[\],:$][^:#${}]*?):\s+)?\$\{`)

// maxQuoteHintRunes bounds the corrected line a diagnostic quotes back.
const maxQuoteHintRunes = 160

// offerQuotedFence rewrites the YAML parser's refusal into this language's own
// sentence when the line it stopped on holds an unquoted `${...}` whose text
// holds a `: ` (#1683).
//
// The shape is a ternary written the way every other position lets an author
// write an expression:
//
//	value: ${inputs.priority == "express" ? steps.a.value : steps.b.value}
//
// YAML reads a plain scalar up to the first `: ` and takes what precedes it
// as a mapping key, so the value ends in the middle of the expression and the
// parser refuses the line with a sentence about mappings, or about a quote that
// never ends when a string literal holds the `: `. Every verb stopped there —
// validate, lint, fmt, run, the language server — in goccy's voice, and nothing
// said that the fix is to quote the whole scalar, which is what the corpus does
// everywhere a ternary appears. This is the third YAML trap beside #1466's two,
// and it fires on the most common CEL construct after field access.
//
// Read from the source line rather than from the parser's token, because the
// token ends where YAML decided the key did: the text this names is the
// scalar the author wrote, fence to closing brace. The edit wraps it in single
// quotes, doubling any quote inside, which is the spelling `flow fmt` keeps
// and the one the shipped examples use; a trailing comment stays outside. The
// message shows the corrected line, so the author reads what to write rather
// than a pattern.
//
// Only that shape, and only when the scalar is unambiguous: it ends at a closing
// brace on this line, and no deeper-indented line follows that YAML would have
// folded into it. A `: ` on a line whose value does not open with a fence is
// the mapping mistake goccy describes, and its sentence stands.
func offerQuotedFence(data []byte, d *Diagnostic) {
	switch d.Message {
	case yamlMappingValue, yamlUnterminatedDouble, yamlUnterminatedSingle:
	default:
		return
	}
	if d.Line <= 0 || d.Column <= 0 {
		return
	}
	line, ok := sourceLine(data, d.Line)
	if !ok {
		return
	}
	m := fenceValue.FindStringSubmatchIndex(line)
	if m == nil {
		return
	}
	// The scalar starts at the fence, the last two bytes of the match.
	fence := m[1] - len("${")
	rest := line[fence:]
	scalar := plainScalarText(rest)
	if scalar == "" || !strings.Contains(scalar, ": ") {
		// Either the line carries text after the fence closes (or a fence that
		// never closes here), in which case
		// the mapping goccy describes is outside the expression and quoting
		// would not be the fix, or the fence holds no `: ` and something else
		// on the line is the key.
		return
	}
	column := utf8.RuneCountInString(line[:fence]) + 1
	if d.Message == yamlMappingValue && column != d.Column {
		// goccy stopped somewhere other than the fence, so the fence is not
		// what it refused.
		return
	}
	if foldsContinuation(data, d.Line, m[3]-m[2]) {
		return
	}

	quoted := "'" + strings.ReplaceAll(scalar, "'", "''") + "'"
	corrected := strings.TrimSpace(line[:fence] + quoted + rest[len(scalar):])
	if r := []rune(corrected); len(r) > maxQuoteHintRunes {
		corrected = string(r[:maxQuoteHintRunes]) + "..."
	}
	d.Message = fmt.Sprintf(
		"an unquoted expression holding %q is read by YAML as a mapping key (a ternary's \"? a : b\" and a string literal "+
			"like \"a: \" are the usual ones), so the value stopped before the fence closed; quote the whole value, '${...}', so the line reads: %s",
		": ", corrected)
	start := Position{Line: d.Line, Column: column}
	d.Line, d.Column = start.Line, start.Column
	if edit := replaceSpan("quote the expression", Span{Start: start, End: advance(start, scalar)}, quoted); edit != nil {
		d.Edits = append(d.Edits, edit)
	}
}

// explainFenceInFlowMapping rewrites the YAML parser's refusal into this
// language's own sentence when the token it stopped on is the `{` of a `${`
// inside a flow-style mapping (#1466).
//
// `log: {message: ${steps.a.value}}` is a shape a person and a model both write.
// YAML reads the `$` as the whole value of the entry and then meets the fence's
// `{` where it expected a comma or the mapping's closing brace; it complains
// about a comma, in its own voice, at a column that names nothing the author
// wrote. The remedy is not quoting but block style, where a
// fence is an ordinary value, so that is what this names. No edit is offered: the
// rewrite reflows the author's mapping across lines, which is the reformatting
// `flow fix` refuses to do to flow style.
func explainFenceInFlowMapping(data []byte, d *Diagnostic) {
	if d.Message != yamlFlowMappingEnd || d.Line <= 0 || d.Column < len("${") {
		return
	}
	line, ok := sourceLine(data, d.Line)
	if !ok {
		return
	}
	// The token is the fence's `{`, so the `$` is the column before it.
	runes := []rune(line)
	if d.Column > len(runes) || !strings.HasSuffix(string(runes[:d.Column]), "${") {
		return
	}

	d.Column--
	d.Message = "a `${...}` expression cannot be written inside a flow-style mapping (`{...}`): YAML took " +
		"the `$` as the entry's value and stopped at the `{` after it; write the mapping in block " +
		"style instead, one `key: ${...}` per line, where an expression is an ordinary value"
}

// foldsContinuation reports whether a plain scalar on the given line could
// continue onto the next one: a following non-blank, non-comment line indented
// deeper than the node's own column is folded into it by YAML, so the text the
// author meant is not the one line and quoting that line would change it.
func foldsContinuation(data []byte, line, nodeColumn int) bool {
	for n := line + 1; ; n++ {
		next, ok := sourceLine(data, n)
		if !ok {
			return false
		}
		trimmed := strings.TrimSpace(next)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		return indentWidth(next) > nodeColumn
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

// fenceEnd returns the byte index of the brace that closes the fence opening
// rest, or -1 when there is none. Braces inside a string literal (single or
// double quoted, with backslash escapes) do not count and nested braces do, so
// a map literal or a `}` in a string is not mistaken for the end.
func fenceEnd(rest string) int {
	depth := 0
	var quote byte
	for i := 0; i < len(rest); i++ {
		c := rest[i]
		if quote != 0 {
			switch c {
			case '\\':
				i++
			case quote:
				quote = 0
			}
			continue
		}
		switch c {
		case '"', '\'':
			quote = c
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// plainScalarText is the part of a line's remainder that a plain scalar
// opening with a fence covers: through the fence's own closing brace, when only
// nothing or a YAML comment follows it. Anything else, a `#` with no space
// before it, more text, or a fence that never closes on this line, is not a
// shape quoting provably repairs, and the answer is empty.
//
// A comment starts at a `#` YAML would read as one, which is a `#` after
// whitespace; `}#tail` is part of the scalar, so quoting only the fence would
// change it.
func plainScalarText(rest string) string {
	end := fenceEnd(rest)
	if end < 0 {
		return ""
	}
	after := rest[end+1:]
	trimmed := strings.TrimLeft(after, " \t")
	if trimmed == "" || (len(trimmed) < len(after) && strings.HasPrefix(trimmed, "#")) {
		return rest[:end+1]
	}
	return ""
}

// maxQuoteRepairs bounds how many plain scalars [repairQuotedFences] quotes in
// one run. Each repair is one parse of the document, so the bound is the work
// a file somebody else wrote can cost, and a file past it is left as it was.
const maxQuoteRepairs = 32

// repairQuotedFences quotes the plain scalars that keep a document from
// parsing, for [Fix] (#2660). It returns the repaired document and what it
// changed, or false when it did nothing.
//
// It acts only where the diagnostic [offerQuotedFence] builds carries its one
// edit, which already refuses every ambiguous shape. On top of that, nothing is
// returned unless the repaired document parses and each quoted scalar reads
// back, to the byte, as the text the author wrote: a repair that makes the file
// parse by changing what an expression says is the failure this exists to
// avoid, and the check is cheaper than arguing it cannot happen. One scalar is
// quoted per parse, so a second trap on another line is repaired from its own
// diagnostic, and a document that still fails to parse afterwards, or needs more
// than [maxQuoteRepairs], is left exactly as it was.
func repairQuotedFences(data []byte) ([]byte, []FixChange, bool) {
	type site struct {
		line   int
		scalar string
	}
	var (
		source  = data
		changes []FixChange
		sites   []site
	)
	for range maxQuoteRepairs + 1 {
		file, err := strictyaml.ParseBytes(source, parser.ParseComments)
		if err == nil {
			if len(changes) == 0 {
				return nil, nil, false
			}
			for _, s := range sites {
				if !hasPlainValue(file, s.line, s.scalar) {
					return nil, nil, false
				}
			}
			slices.SortStableFunc(changes, func(a, b FixChange) int { return a.Line - b.Line })
			return source, changes, true
		}
		if len(changes) == maxQuoteRepairs {
			return nil, nil, false
		}

		ds := YAMLSyntaxDiagnostics(source, err)
		if len(ds) != 1 || len(ds[0].Edits) != 1 || len(ds[0].Edits[0].GetChanges()) != 1 {
			return nil, nil, false
		}
		change := ds[0].Edits[0].GetChanges()[0]
		r := change.GetRange()
		line := int(r.GetStartLine())
		if line != int(r.GetEndLine()) {
			return nil, nil, false
		}

		f := &fixer{
			lines:           splitLines(source),
			trailingNewline: strings.HasSuffix(string(source), "\n"),
			terminator:      lineTerminator(source),
		}
		text := []rune(f.line(line))
		start, end := int(r.GetStartColumn())-1, int(r.GetEndColumn())-1
		if start < 0 || end < start || end > len(text) {
			return nil, nil, false
		}
		scalar := string(text[start:end])
		// The read-back below compares against the text the author wrote, so
		// that text is established here from the source line rather than from
		// what the edit produced: one whole fence, quoted exactly.
		if !strings.HasPrefix(scalar, "${") || fenceEnd(scalar) != len(scalar)-1 ||
			change.GetNewText() != "'"+strings.ReplaceAll(scalar, "'", "''")+"'" {
			return nil, nil, false
		}
		replaced := string(text[:start]) + change.GetNewText() + string(text[end:])
		f.record(line, line, []string{replaced},
			"quoted the expression, which YAML read as a mapping key",
			"would quote the expression, which YAML reads as a mapping key")
		source = f.apply()
		changes = append(changes, f.changes...)
		sites = append(sites, site{line: line, scalar: scalar})
	}
	return nil, nil, false
}

// hasPlainValue reports whether the document holds a string on the given line
// whose value is exactly the text a repair quoted.
func hasPlainValue(file *ast.File, line int, text string) bool {
	v := &valueFinder{line: line, text: text}
	for _, doc := range file.Docs {
		ast.Walk(v, doc.Body)
	}
	return v.found
}

type valueFinder struct {
	line  int
	text  string
	found bool
}

func (v *valueFinder) Visit(n ast.Node) ast.Visitor {
	if s, ok := n.(*ast.StringNode); ok && s.Value == v.text {
		if tok := s.GetToken(); tok != nil && tok.Position != nil && tok.Position.Line == v.line {
			v.found = true
		}
	}
	return v
}
