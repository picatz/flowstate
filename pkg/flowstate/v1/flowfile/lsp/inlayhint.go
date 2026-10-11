package lsp

import (
	"fmt"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/sourcegraph/go-lsp"
)

// Inlay hints write down the types an author cannot see in the file.
//
// Two places have one and neither spells it: a `for_each` binds an item whose
// type follows from `items:`, and a task step produces outputs whose names and
// types live in the task's descriptor. Both are answers hover already gives on
// demand; a hint gives them without a cursor, so they come from the same
// sources — [flowfile.IteratorType], the checker's own table, and the
// descriptor's output fields — and there is no second evaluator here. Nothing
// is run: a hint reads declarations, never a secret and never a value.
//
// The answer is bounded three ways, because a hint request arrives on every
// scroll: a document above [maxDocumentBytes] has no parsed model and so no
// hints, only hints inside the visible range are built, and no more than
// [maxInlayHints] are returned.

// maxInlayHints caps one response. It is the work limit as well as the
// reporting limit: the walk stops at it rather than building and trimming.
const maxInlayHints = 200

// maxHintOutputs is how many of a step's outputs one hint names before it
// elides the rest, so a task with a long output list stays one short label.
const maxHintOutputs = 4

// inlayHintKindType is the LSP `InlayHintKind.Type`.
const inlayHintKindType = 1

// inlayHintParams is `textDocument/inlayHint`'s request.
type inlayHintParams struct {
	TextDocument lsp.TextDocumentIdentifier `json:"textDocument"`
	Range        lsp.Range                  `json:"range"`
}

// An inlayHint is the protocol's `InlayHint`, which go-lsp predates.
type inlayHint struct {
	Position    lsp.Position `json:"position"`
	Label       string       `json:"label"`
	Kind        int          `json:"kind,omitempty"`
	PaddingLeft bool         `json:"paddingLeft,omitempty"`
}

// inlayHints returns the hints inside visible, in document order. It is never
// nil, and it is empty for a document that does not parse: a hint positioned
// from a model the text no longer matches would sit on the wrong line.
func inlayHints(doc *document, visible lsp.Range) []inlayHint {
	hints := []inlayHint{}
	if doc == nil || doc.parsed == nil || !doc.speaksFlowfile() {
		return hints
	}

	// Compiled at most once, and only when a loop is in range: compiling is the
	// dear part and most ranges hold none.
	var (
		compiled bool
		wf       *v1.Workflow
	)
	workflow := func() *v1.Workflow {
		if !compiled {
			compiled = true
			wf = compiledWorkflow(doc)
		}
		return wf
	}

	// The checker binds an item in the expressions of the loop's body, not of the
	// loop, so the question is asked of the first step inside it. One pass keeps
	// that lookup linear in the steps.
	firstChild := map[*parsedStep]*parsedStep{}
	for _, step := range doc.parsed.steps {
		if step.parent != nil && firstChild[step.parent] == nil {
			firstChild[step.parent] = step
		}
	}

	for _, step := range doc.parsed.steps {
		if len(hints) >= maxInlayHints {
			break
		}

		// A for_each's item, after the `items:` that decides its type.
		if body := firstChild[step]; body != nil && step.forEachEntry != nil && step.itemsEntry != nil && step.itemsEntry.value != nil {
			at := step.itemsEntry.valueRange().End
			if contains(visible, at) {
				if w := workflow(); w != nil {
					name := step.iteratorName()
					if typed, ok := flowfile.IteratorType(w, body.id, name); ok {
						hints = append(hints, typeHint(at, fmt.Sprintf("%s: %s", name, typed)))
					}
				}
			}
		}

		// A task step's outputs, after its id. A shaped step has none to claim:
		// its `outputs:` replaces what the descriptor declares.
		if len(hints) >= maxInlayHints || step.taskEntry == nil || step.idEntry == nil || step.idEntry.value == nil {
			continue
		}
		def, known := doc.tasks.Lookup(step.taskName)
		if !known || def.Outputs == nil || step.shapingEntry(doc.tasks) != nil {
			continue
		}
		at := step.idEntry.valueRange().End
		fields := def.Outputs.Fields()
		if !contains(visible, at) || fields.Len() == 0 {
			continue
		}
		parts := make([]string, 0, maxHintOutputs+1)
		for i := range min(fields.Len(), maxHintOutputs) {
			fd := fields.Get(i)
			parts = append(parts, fmt.Sprintf("%s: %s", fd.Name(), typeName(fd)))
		}
		if fields.Len() > maxHintOutputs {
			parts = append(parts, "…")
		}
		hints = append(hints, typeHint(at, "→ "+strings.Join(parts, ", ")))
	}

	return hints[:min(len(hints), maxInlayHints)]
}

func typeHint(at lsp.Position, label string) inlayHint {
	return inlayHint{Position: at, Label: label, Kind: inlayHintKindType, PaddingLeft: true}
}
