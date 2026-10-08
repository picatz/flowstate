package lsp

import (
	"fmt"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/sourcegraph/go-lsp"
)

// hoverInputPath describes `inputs.<name>` and, for an input declared as a record,
// each `.<field>` after it: the field's type, whether it is required, what its
// description says and, when it is itself a record, the fields it holds.
//
// A workflow's own input is documented where it is declared, so an author reading
// `inputs.order.total.cents` is being told something they cannot see on the line in
// front of them. The answer is read from the compiled workflow rather than the
// positional model, because it is the same declaration the checker judges the
// expression against: a hover that disagreed with the squiggle would be worse than
// silence. A document that does not compile has no declaration to read and gets
// none.
//
// Nil when the cursor is not on a segment of such a chain, or the chain names
// nothing the workflow declares (the validator says so).
func hoverInputPath(doc *document, v *value, f fence, cursor int) *lsp.Hover {
	start, word, ok := wordAt(f.source, cursor)
	if !ok {
		return nil
	}

	segments := strings.Split(word, ".")
	if len(segments) < 2 || segments[0] != v1.InputsRoot || segments[1] == "" {
		return nil
	}

	// The segment under the cursor, which bounds both the answer and its range:
	// `inputs.order.id` hovered on `order` describes the input, not its field.
	end, upto := start, -1
	for i, s := range segments {
		end += len(s)
		if cursor <= end {
			upto = i
			break
		}
		end++ // the dot
	}
	if upto < 1 || segments[upto] == "" {
		return nil
	}

	wf := compiledWorkflow(doc)
	if wf == nil {
		return nil
	}

	declaration := declaredInput(wf, segments[1])
	if declaration == nil {
		return nil
	}

	table := v1.TypesOf(wf)
	var parent *v1.TypeDeclaration
	for _, name := range segments[2 : upto+1] {
		parent = table[declaration.DeclaredType().GetMessage()]
		if parent == nil {
			return nil
		}
		i := slices.IndexFunc(parent.GetFields(), func(d *v1.InputDeclaration) bool { return d.GetName() == name })
		if i < 0 {
			return nil
		}
		declaration = parent.GetFields()[i]
	}

	rng := v.fenceSpanOrWhole(doc.index, f, start, end)

	return markdownHover(inputPathDoc(strings.Join(segments[1:upto+1], "."), declaration, parent, table, declaredOnLine(doc, segments[1], parent)), rng)
}

// inputPathDoc renders one declaration reached by a path: the input itself, or a
// field of the record that holds it.
//
// line is the sentence naming where a top-level input is declared, empty for a
// field, which names its record instead.
func inputPathDoc(path string, declaration *v1.InputDeclaration, parent *v1.TypeDeclaration, table v1.TypeTable, line string) string {
	provenance := line
	if parent != nil {
		provenance = fmt.Sprintf("A field of the record `%s`.", parent.GetName())
	}

	var b strings.Builder
	b.WriteString(declarationDoc(path, declaration, provenance))
	typ := declaration.DeclaredType()

	if record := table[typ.GetMessage()]; record != nil {
		fmt.Fprintf(&b, "\n\nThe record `%s`", record.GetName())
		if description := record.GetDescription(); description != "" {
			fmt.Fprintf(&b, ": %s", description)
		}
		b.WriteString("\n")
		for _, field := range record.GetFields() {
			optional := ""
			if !field.GetRequired() {
				optional = " (optional)"
			}
			fmt.Fprintf(&b, "\n- `%s`: `%s`%s", field.GetName(), v1.TypeString(field.DeclaredType()), optional)
		}
	}

	return b.String()
}

// declaredInput finds the workflow input called name.
func declaredInput(wf *v1.Workflow, name string) *v1.InputDeclaration {
	i := slices.IndexFunc(wf.GetDeclaredInputs(), func(d *v1.InputDeclaration) bool { return d.GetName() == name })
	if i < 0 {
		return nil
	}

	return wf.GetDeclaredInputs()[i]
}

// compiledWorkflow compiles the document the way every other answer here does, and
// returns nil for text that does not compile.
func compiledWorkflow(doc *document) *v1.Workflow {
	if path, ok := doc.filesystemPath(); ok {
		wf, _, err := flowfile.ParseAt([]byte(doc.text), path)
		if err != nil {
			return nil
		}

		return wf
	}

	wf, err := flowfile.Unmarshal([]byte(doc.text))
	if err != nil {
		return nil
	}

	return wf
}

// wordAt returns where the run of identifier characters and dots around cursor
// starts, and the run.
func wordAt(src string, cursor int) (int, string, bool) {
	if cursor < 0 || cursor > len(src) {
		return 0, "", false
	}
	isWord := func(c byte) bool {
		return c == '_' || c == '.' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
	}

	start, end := cursor, cursor
	for start > 0 && isWord(src[start-1]) {
		start--
	}
	for end < len(src) && isWord(src[end]) {
		end++
	}

	return start, src[start:end], start < end
}

// hoverInputDeclaration describes the key of a workflow input where it is
// declared, in the words [hoverInputPath] uses for a reference to it, so the
// declaration and its uses cannot say it two ways. It is answered from the
// compiled workflow like they are, and a document that does not compile gets
// none.
func hoverInputDeclaration(doc *document, pos lsp.Position) *lsp.Hover {
	for _, top := range doc.parsed.entries {
		if top.key != "inputs" {
			continue
		}
		for _, in := range nestedEntries(top) {
			if !contains(in.keyRange, pos) {
				continue
			}
			wf := compiledWorkflow(doc)
			if wf == nil {
				return nil
			}
			declaration := declaredInput(wf, in.key)
			if declaration == nil {
				return nil
			}

			return markdownHover(inputPathDoc(in.key, declaration, nil, v1.TypesOf(wf), declaredOnLine(doc, in.key, nil)), in.keyRange)
		}
	}

	return nil
}

// hoverVarPath describes `vars.<name>` in an expression: the workflow var's
// expression and the line it is written on. The var is read from the document's
// own `vars:` block rather than the compiled workflow, since what an author
// wants to know is what they wrote.
func hoverVarPath(doc *document, v *value, f fence, cursor int) *lsp.Hover {
	start, word, ok := wordAt(f.source, cursor)
	if !ok {
		return nil
	}

	segments := strings.Split(word, ".")
	if len(segments) < 2 || segments[0] != v1.VarsRoot || segments[1] == "" || cursor > start+len(v1.VarsRoot)+1+len(segments[1]) {
		return nil
	}

	for _, e := range nestedEntries(doc.parsed.varsEntry) {
		if e.key != segments[1] {
			continue
		}
		rng := v.fenceSpanOrWhole(doc.index, f, start, start+len(v1.VarsRoot)+1+len(segments[1]))
		body := fmt.Sprintf("**`%s.%s`** · workflow var, declared on line %d", v1.VarsRoot, e.key, e.keyRange.Start.Line+1)
		if text := e.valueText(); text != "" {
			body += fmt.Sprintf("\n\nEvaluated once before the first step: `%s`", text)
		}

		return markdownHover(body, rng)
	}

	return nil
}

// declaredOnLine names the line a top-level input's key is written on, as the
// same-file counterpart of the path a callee's input is attributed to. Empty for
// a record field, whose provenance is its record, and for a name the document
// does not spell under `inputs:`.
func declaredOnLine(doc *document, name string, parent *v1.TypeDeclaration) string {
	if parent != nil {
		return ""
	}
	for _, top := range doc.parsed.entries {
		if top.key != "inputs" {
			continue
		}
		for _, in := range nestedEntries(top) {
			if in.key == name {
				return fmt.Sprintf("Declared on line %d.", in.keyRange.Start.Line+1)
			}
		}
	}

	return ""
}
