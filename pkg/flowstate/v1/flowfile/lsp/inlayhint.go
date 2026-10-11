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
// A hint request arrives on every scroll, so the work is bounded: a document
// above [maxDocumentBytes] has no parsed model and so no hints, only hints
// inside the visible range are built, and the checker's table is built once per
// request however many loops are asked about. At most [maxInlayHints] are
// returned; that caps the response, not the work.

// maxInlayHints caps one response. It bounds what is reported, not the work: a
// loop that cannot be typed costs a lookup and yields no hint. The work is
// bounded by one type-table build per request and the document size limit.
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
	// Both the table's construction and each question it answers read task
	// definitions, so both run in the document's registry scope.
	return inlayHintsWith(doc, visible, func(wf *v1.Workflow) func(step, name string) (string, bool) {
		typer := underDoc(doc, func() func(step, name string) (string, bool) { return flowfile.IteratorTyper(wf) })
		return func(step, name string) (string, bool) {
			type typed struct {
				name string
				ok   bool
			}
			t := underDoc(doc, func() typed {
				name, ok := typer(step, name)
				return typed{name, ok}
			})
			return t.name, t.ok
		}
	})
}

// inlayHintsWith is [inlayHints] over a chosen iterator-type source, the seam a
// test uses to count how many times the checker table is built.
func inlayHintsWith(doc *document, visible lsp.Range, newTyper func(*v1.Workflow) func(step, name string) (string, bool)) []inlayHint {
	hints := []inlayHint{}
	if doc == nil || doc.parsed == nil || !doc.speaksFlowfile() {
		return hints
	}

	// Compiled and scanned at most once per request, and only when a loop is in
	// range: both are the dear part, and most ranges hold no loop. Every loop is
	// then answered from the one type table.
	var (
		built bool
		typer func(step, name string) (string, bool)
	)
	iteratorType := func(step, name string) (string, bool) {
		if !built {
			built = true
			if wf := compiledWorkflow(doc); wf != nil {
				typer = newTyper(wf)
			}
		}
		if typer == nil {
			return "", false
		}
		return typer(step, name)
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
			if inRange(visible, at) {
				name := step.iteratorName()
				if typed, ok := iteratorType(body.id, name); ok {
					hints = append(hints, typeHint(at, fmt.Sprintf("%s: %s", name, typed)))
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
		if !inRange(visible, at) || fields.Len() == 0 {
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

// inRange reports whether at is inside r the way LSP means a range: the start is
// in and the end is out. [contains] is end-inclusive, which is right for a
// cursor resting at the end of a token and wrong here, where a hint exactly at
// the visible range's end belongs to the next screen.
func inRange(r lsp.Range, at lsp.Position) bool {
	before := func(a, b lsp.Position) bool {
		return a.Line < b.Line || a.Line == b.Line && a.Character < b.Character
	}
	return !before(at, r.Start) && before(at, r.End)
}

func typeHint(at lsp.Position, label string) inlayHint {
	return inlayHint{Position: at, Label: label, Kind: inlayHintKindType, PaddingLeft: true}
}
