package lsp

import (
	"github.com/sourcegraph/go-lsp"
)

// The outline for a `*.test.yaml` (#1110 item 8): one symbol per runnable
// case, so the outline pane and breadcrumbs work the way they do for a
// workflow's steps.
//
// A `tests:` entry that declares `cases:` rows (#924 slice 2) is a template
// the rows are merged over and does not itself run — see [flowtest.Test.Cases]'s
// own doc, "An entry that declares rows does not itself run" — so only the
// rows are runnable symbols, named `<entry name>/<row name>` exactly as
// flow test's own report names them ([flowtest.Test.Cases] again: "Report
// identity is `<entry name>/<row name>`"). An entry with no rows is itself
// the runnable case, and gets the symbol.
//
// This reads the document by line, the same way [testDocumentSymbols]'s
// completion neighbor does, rather than through a decoded [flowtest.File]:
// the document is usually mid-edit, and a decode that fails on the first
// mistake would blank the whole outline rather than showing every case
// written correctly around it.

// testDocumentSymbols returns one symbol per case in doc, or an empty slice
// when the document is too large to have been analyzed.
func testDocumentSymbols(doc *document) []lsp.SymbolInformation {
	out := []lsp.SymbolInformation{}
	if doc.tooLarge {
		return out
	}

	// pending is the most recently seen `tests:`-level entry whose name has
	// been read but not yet emitted — held back because whether it is a
	// runnable case of its own, or only a template its `cases:` rows are
	// merged over, is not known until either the next entry starts or a
	// `cases:` row under this one is seen.
	type pendingEntry struct {
		name string
		rng  lsp.Range
	}
	var pending *pendingEntry
	hasRows := false

	flush := func() {
		if pending != nil && !hasRows {
			out = append(out, lsp.SymbolInformation{
				Name:     pending.name,
				Kind:     lsp.SKMethod,
				Location: lsp.Location{URI: doc.uri, Range: pending.rng},
			})
		}
		pending = nil
		hasRows = false
	}

	for i := range doc.index.lineCount() {
		key, value, rng, ok := keyValueOnLine(doc.index.line(i), i)
		if !ok || key != "name" {
			continue
		}
		// The exact shapes flow test's own report identity is built from, not
		// a suffix: `tests` for an entry, `tests`/`cases` for a row. A suffix
		// match minted a runnable case out of any nested mapping whose key
		// spelled `cases` — a fixture like `inputs: {cases: {name: bogus}}`
		// emitted `real/bogus` and, worse, set hasRows, which suppressed the
		// real case's own symbol (Codex, #1173).
		path := keyPath(doc.index, i)
		switch {
		case pathIs(path, "tests"):
			flush()
			pending = &pendingEntry{name: value, rng: rng}
		case pathIs(path, "tests", "cases"):
			hasRows = true
			name := value
			if pending != nil {
				name = pending.name + "/" + value
			}
			out = append(out, lsp.SymbolInformation{
				Name:     name,
				Kind:     lsp.SKMethod,
				Location: lsp.Location{URI: doc.uri, Range: rng},
			})
		}
	}
	flush()

	return out
}

// pathIs reports whether a key chain is exactly these segments, root first.
func pathIs(path []string, segments ...string) bool {
	if len(path) != len(segments) {
		return false
	}
	for i, segment := range segments {
		if path[i] != segment {
			return false
		}
	}
	return true
}
