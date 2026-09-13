package lsp

import (
	"strings"

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

	// Keep the enclosing keys as the scan advances. Calling keyPath for each
	// name would rescan every preceding sibling, making a large flat suite
	// quadratic even though the document's byte size is bounded.
	var path keyPathTracker
	for i := range doc.index.lineCount() {
		line := doc.index.line(i)
		enclosing := path.advance(line)
		key, value, rng, ok := keyValueOnLine(line, i)
		if !ok || key != "name" {
			continue
		}
		// The exact shapes flow test's own report identity is built from, not
		// a suffix: `tests` for an entry, `tests`/`cases` for a row. A suffix
		// match minted a runnable case out of any nested mapping whose key
		// spelled `cases` — a fixture like `inputs: {cases: {name: bogus}}`
		// emitted `real/bogus` and, worse, set hasRows, which suppressed the
		// real case's own symbol (Codex, #1173).
		switch {
		case trackedPathIs(enclosing, "tests"):
			flush()
			pending = &pendingEntry{name: value, rng: rng}
		case trackedPathIs(enclosing, "tests", "cases"):
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

type trackedKey struct {
	indent int
	key    string
}

// keyPathTracker maintains the same enclosing-key path as keyPath while a
// caller walks lines forwards. Each line is visited once and each key is pushed
// and popped at most once.
type keyPathTracker []trackedKey

func (p *keyPathTracker) advance(line string) []trackedKey {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return *p
	}

	indent := indentOf(line)
	for len(*p) > 0 && (*p)[len(*p)-1].indent >= indent {
		*p = (*p)[:len(*p)-1]
	}
	enclosing := *p
	if match := keyLine.FindStringSubmatch(line); match != nil {
		*p = append(*p, trackedKey{indent: indent, key: match[3]})
	}
	return enclosing
}

func trackedPathIs(path []trackedKey, segments ...string) bool {
	if len(path) != len(segments) {
		return false
	}
	for i, segment := range segments {
		if path[i].key != segment {
			return false
		}
	}
	return true
}
