package lsp

import (
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/sourcegraph/go-lsp"
)

// Completion for a `*.test.yaml` and a `testdefaults.yaml` (#1110 item 8).
//
// completeAt dispatches here on doc.kind rather than widening speaksFlowfile:
// the workflow grammar's candidates (a step's `for_each:`, a task's own
// inputs) would be wrong answers with confidence in a document that is not a
// workflow at all. What is offered instead comes from testDocKeys, derived
// from the flowtest structs themselves (see testschema.go), and from the
// task registry for a stub's `task:` value — the identical registry a
// workflow step's own task-name completion reads, so a task registered
// tomorrow is completable here with no change to this file either.
//
// The position machinery is shared with the workflow side on purpose:
// [keyPath], [keyAndPosition], and [wordBefore] read a document's raw text
// and indentation, never the Flowfile-specific parsed model, so they say the
// same thing about a test document's YAML structure that they say about a
// workflow's.

// completeTestDocument returns the completion candidates for a position in a
// document [document.isTestDocument] reports true for.
func completeTestDocument(doc *document, pos lsp.Position) *lsp.CompletionList {
	empty := list(nil)
	if doc.tooLarge {
		return empty
	}

	line := doc.index.line(pos.Line)
	col := doc.index.byteOfUTF16(pos.Line, pos.Character)
	before := line[:min(col, len(line))]

	path := keyPath(doc.index, pos.Line)
	key, valuePos := keyAndPosition(line, col)
	word, replace := wordBefore(pos, before)

	// testdefaults.yaml's top level is a narrower table than a suite's own —
	// see [dirDefaultsTopLevelKeys] — everything nested below `defaults:` is
	// the identical [flowtest.Defaults] shape either file nests it in, so
	// everything below applies unchanged to both document kinds.
	if len(path) == 0 && !valuePos {
		if doc.kind == docTestDefaults {
			return list(keyCandidates(dirDefaultsTopLevelKeys, word, replace))
		}
		return list(testDSLCandidates(testLevelFile, word, replace))
	}

	// The whole chain from the root decides the level, not the innermost key.
	// A suffix match answered for any map whose key happened to spell a stanza
	// name — a case's `inputs: {expect: ...}` is the author's fixture data, and
	// it was offered `outputs`, `failed` and the rest with full confidence
	// (Codex, #1173). The walk refuses at the first key the grammar does not
	// open, so nothing inside the author's data is ever the DSL's to complete.
	level, structural := testDocLevelAt(doc.kind, path)
	if !structural {
		return empty
	}

	if valuePos {
		// The one value position this offers anything for: a stub's `task:`,
		// naming a task the same way a workflow step does.
		if key == "task" && level == testLevelStub {
			return list(testStubTaskCandidates(word, replace, doc.tasks))
		}
		return empty
	}

	return list(testDSLCandidates(level, word, replace))
}

// testDSLCandidates offers testDocKeys' keys at one level of nesting — the
// test-language analogue of [dslCandidates].
func testDSLCandidates(level testDocLevel, prefix string, replace lsp.Range) []lsp.CompletionItem {
	return keyCandidates(testDocKeys[level], prefix, replace)
}

// keyCandidates renders a table of [dslKey] as completion items, in the
// table's own order. Shared by [testDSLCandidates] and testdefaults.yaml's
// top level, which reads a table ([dirDefaultsTopLevelKeys]) too small to
// need its own map entry.
func keyCandidates(keys []dslKey, prefix string, replace lsp.Range) []lsp.CompletionItem {
	var items []lsp.CompletionItem
	for i, k := range keys {
		if !strings.HasPrefix(k.name, prefix) {
			continue
		}
		items = append(items, lsp.CompletionItem{
			Label:         k.name,
			Kind:          lsp.CIKKeyword,
			Detail:        k.detail,
			Documentation: plainText(k.docs),
			SortText:      sortAt(i*slotSpacing, k.name),
			TextEdit:      &lsp.TextEdit{Range: replace, NewText: k.name + ": "},
		})
	}
	return items
}

// testStubTaskCandidates offers every registered task as a stub's `task:`
// value — the same registry [taskCandidates] offers a workflow step's task
// name from, read as a plain value rather than a step key: no trailing
// colon, since the colon here belongs to `task:` itself and is already on
// the line.
func testStubTaskCandidates(prefix string, replace lsp.Range, tasks *v1.Registry) []lsp.CompletionItem {
	var items []lsp.CompletionItem
	for _, def := range tasks.All() {
		if !strings.HasPrefix(def.Name, prefix) {
			continue
		}
		items = append(items, lsp.CompletionItem{
			Label:         def.Name,
			Kind:          lsp.CIKValue,
			Detail:        def.Summary,
			Documentation: plainText(taskDoc(def)),
			SortText:      def.Name,
			TextEdit:      &lsp.TextEdit{Range: replace, NewText: def.Name},
		})
	}
	return items
}
