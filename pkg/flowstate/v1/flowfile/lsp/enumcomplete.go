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
//
// Read from [v1.DefaultRegistry] and not the document's own: the compiler lowers a
// name against the default registry (as every other check of a file does), so a
// name only an injected registry holds would be offered and then refused. A name
// something else already holds here (a local, a root, `now`) is not offered; taken
// is the names this position binds.
func enumValueCandidates(doc *document, pos lsp.Position, taken map[string]bool) []celcomplete.Candidate {
	wf := compiledWorkflow(doc)
	if wf == nil {
		// Completing inside a fence that does not parse yet: the declarations are
		// the part of the file the author is not editing, as for functions.
		wf = compiledText(doc, withoutFenceAt(doc, pos))
	}

	enums := v1.OutputEnumsOf(wf, nil)
	names := enums.Names()
	out := make([]celcomplete.Candidate, 0, len(names))
	for _, name := range names {
		if taken[name] || v1.IsDeclarationRoot(name) || name == v1.NowIdentifier {
			continue
		}
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
