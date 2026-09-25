package lsp

import (
	"github.com/sourcegraph/go-lsp"
)

// Hover for a test document uses the same semantic sources completion does:
// [testDocKeys] for language keys (whose key sets are mutation-guarded against
// flowtest's strict decoder), and [taskDoc] over the server's registry for a
// stub task name. It never falls through to the Flowfile grammar.

// hoverTestDocument returns the hover for a position in a document
// [document.isTestDocument] reports true for, or nil when the position is
// not a stub's task name.
func hoverTestDocument(doc *document, pos lsp.Position) *lsp.Hover {
	if doc.tooLarge {
		return nil
	}

	line := doc.index.line(pos.Line)
	col := doc.index.byteOfUTF16(pos.Line, pos.Character)

	m, ok := scanKeyLine(line)
	if !ok {
		return nil
	}
	level, structural := testDocLevelAt(doc.kind, keyPath(doc.index, pos.Line))
	if !structural {
		return nil
	}
	keyRange := lsp.Range{
		Start: lsp.Position{Line: pos.Line, Character: utf16Len(line[:m.keyStart])},
		End:   lsp.Position{Line: pos.Line, Character: utf16Len(line[:m.keyEnd])},
	}
	if contains(keyRange, pos) {
		keys := testDocKeys[level]
		if doc.kind == docTestDefaults && len(keyPath(doc.index, pos.Line)) == 0 {
			keys = dirDefaultsTopLevelKeys
		}
		for _, key := range keys {
			if key.name == m.key {
				return dslKeyHover(key, keyRange)
			}
		}
		return nil
	}

	if col <= m.colon || m.key != "task" || level != testLevelStub {
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
