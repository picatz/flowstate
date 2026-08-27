package lsp

import (
	"github.com/sourcegraph/go-lsp"
)

// Hover for a `*.test.yaml` (#1110 item 8) is deliberately narrow: only a
// stub's `task:` name, because that is the one place in the test language
// this package already has real generated prose to show. [taskDoc] renders
// a task's summary and full typed signature from the registry's own
// descriptors — the identical function a workflow step's task-name hover
// calls (hover.go) — so answering here is reusing that answer for a second
// position, not writing a new one.
//
// `expect.` keys are the other candidate #1110 item 8 named, and deliberately
// do not get a hover yet: nothing in this build carries generated,
// per-field prose for [flowtest.Expectation] the way protodoc carries it for
// a proto message. The doc comments on that struct are real prose, but
// prose in a Go source comment is not prose this package can read at
// runtime without either parsing the flowtest package's source (which
// nothing here does, for any type) or retyping it by hand — and CLAUDE.md's
// rule is exactly against the second of those. Hand-copying those sentences
// into a table here would be the two-spellings bug this repository has
// already paid for elsewhere: the struct's doc comment and the hover's
// prose would each be a copy of the same fact, free to drift from each
// other the first time one of them is edited and the other is not.

// hoverTestDocument returns the hover for a position in a document
// [document.isTestDocument] reports true for, or nil when the position is
// not a stub's task name.
func hoverTestDocument(doc *document, pos lsp.Position) *lsp.Hover {
	if doc.tooLarge {
		return nil
	}

	line := doc.index.line(pos.Line)
	col := doc.index.byteOfUTF16(pos.Line, pos.Character)

	key, inValue := keyAndPosition(line, col)
	if !inValue || key != "task" {
		return nil
	}
	if !endsWith(keyPath(doc.index, pos.Line), "stubs") {
		return nil
	}

	_, name, rng, ok := keyValueOnLine(line, pos.Line)
	if !ok || name == "" {
		return nil
	}
	def, known := doc.tasks.Lookup(name)
	if !known {
		return nil
	}
	return markdownHover(taskDoc(def), rng)
}
