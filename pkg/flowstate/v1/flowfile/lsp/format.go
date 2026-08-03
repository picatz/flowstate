package lsp

import (
	"github.com/sourcegraph/go-lsp"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Formatting answers "what would `flow fmt` write here?" through the same path
// the command uses: [flowfile.Unmarshal] into a workflow, then [flowfile.Marshal]
// back out. There is exactly one edit, covering the whole document, because
// Marshal renders from the parsed model rather than editing source text — it has
// no notion of a partial change, so neither does this.
//
// That is also why a broken document draws nothing. There is no [v1.Workflow] to
// render when the file does not compile, and inventing one to patch around the
// break would be exactly the class of mistake `flow fix` refuses to make: a file
// that looks formatted and is not is worse than one formatting left alone.

// formatEdits returns the full-document edit that brings doc into the form
// [flowfile.Marshal] writes, or nil when the document does not compile.
//
// An empty (non-nil) slice is returned when the document already matches
// Marshal's output, so a client asking "is there anything to do here" gets a
// real answer rather than being unable to tell "nothing to change" from
// "couldn't tell".
func formatEdits(doc *document) []lsp.TextEdit {
	workflow, err := flowfile.Unmarshal([]byte(doc.text))
	if err != nil {
		return nil
	}

	formatted, err := flowfile.Marshal(workflow)
	if err != nil {
		// A workflow this build itself just compiled but cannot write back out —
		// an expression built from a macro, or a literal `${` — is not a shape a
		// formatter can produce an edit for either. `flow fmt` reports this as a
		// refusal on the file; here there is no stream to report it on, so the
		// honest answer is the same one a parse failure gets: nothing.
		return nil
	}

	if string(formatted) == doc.text {
		return []lsp.TextEdit{}
	}

	return []lsp.TextEdit{{
		Range:   wholeDocumentRange(doc),
		NewText: string(formatted),
	}}
}

// wholeDocumentRange spans every line of doc, which is what a full-document
// replacement edit has to cover under the protocol: an edit whose range does not
// reach the last character would leave whatever came after it in place, appended
// to the document Marshal just rendered.
func wholeDocumentRange(doc *document) lsp.Range {
	last := doc.index.lineCount() - 1
	return lsp.Range{
		Start: lsp.Position{Line: 0, Character: 0},
		End:   lsp.Position{Line: last, Character: utf16Len(doc.index.line(last))},
	}
}
