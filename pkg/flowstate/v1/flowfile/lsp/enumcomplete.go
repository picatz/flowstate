package lsp

import (
	"fmt"

	lsp "github.com/sourcegraph/go-lsp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celcomplete"
)

// enumValueCandidates are the enum values the outputs of the file's own tasks can
// hold, offered bare: the names an expression may write where the run holds a
// number (see [v1.OutputEnumsOf]). They come from the document's registry, the
// same descriptors the compiler resolves the names against, so an offered name
// is one the file compiles with. Nothing when the document names no task with an
// enum in its outputs.
func enumValueCandidates(doc *document, pos lsp.Position) []celcomplete.Candidate {
	wf := compiledWorkflow(doc)
	if wf == nil {
		// Completing inside a fence that does not parse yet: the declarations are
		// the part of the file the author is not editing, as for functions.
		wf = compiledText(doc, withoutFenceAt(doc, pos))
	}

	enums := v1.OutputEnumsOf(wf, doc.tasks)
	names := enums.Names()
	out := make([]celcomplete.Candidate, 0, len(names))
	for _, name := range names {
		value, _ := enums.Value(name)
		out = append(out, celcomplete.Candidate{
			Name:   name,
			Kind:   celcomplete.KindValue,
			Detail: fmt.Sprintf("%s = %d", value.Enum.Name(), value.Number),
			Docs: fmt.Sprintf("A value of the enum `%s`. A run stores it as the number %d, and the compiler writes the number for the name.",
				value.Enum.FullName(), value.Number),
		})
	}

	return out
}
