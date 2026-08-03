package lsp

import (
	"sync"

	"github.com/sourcegraph/go-lsp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// maxDocumentBytes bounds how much of a document the server will analyze.
//
// Every feature parses the whole text, and an editor may hand over any file the
// user opened. A bound keeps a pathological or accidental file — a multi-megabyte
// generated manifest with a Flowfile language id — from turning each keystroke
// into a long parse while the editor waits.
const maxDocumentBytes = 1 << 20 // 1 MiB

// A document is an open Flowfile and everything derived from its text.
//
// A document is immutable. An edit builds a new one and swaps it into the store,
// so a request that has already taken a reference keeps reading a consistent
// snapshot while a newer version is being built beside it. That is what makes the
// concurrent handling implied by jsonrpc2.AsyncHandler safe without holding a lock
// across analysis.
type document struct {
	uri     lsp.DocumentURI
	version int
	text    string

	// tasks is the registry every answer about this document is derived from.
	// Never nil: [newDocument] substitutes [v1.DefaultRegistry] so that no
	// reader has to decide what an absent registry means.
	//
	// Carried on the document rather than read from the package, because the
	// registry a server answers from is the server's — see [FlowfileServer.Tasks].
	tasks *v1.Registry

	index *lineIndex

	// parsed is the positional model, nil when the text does not parse.
	parsed *parsedFile

	// parseErr is the YAML syntax error, when there is one.
	parseErr error

	// tooLarge reports that the text exceeded maxDocumentBytes and was not
	// parsed at all.
	tooLarge bool
}

// newDocument analyzes text once, so that each request reads results rather than
// recomputing them.
func newDocument(uri lsp.DocumentURI, version int, text string, tasks *v1.Registry) *document {
	if tasks == nil {
		tasks = v1.DefaultRegistry()
	}
	doc := &document{
		uri:      uri,
		version:  version,
		text:     text,
		tasks:    tasks,
		index:    newLineIndex(text),
		tooLarge: len(text) > maxDocumentBytes,
	}
	if doc.tooLarge {
		return doc
	}
	doc.parsed, doc.parseErr = parseFlowfile(text, doc.index)
	return doc
}

// A documentStore holds the documents an editor has open, keyed by URI.
type documentStore struct {
	mu   sync.RWMutex
	docs map[lsp.DocumentURI]*document
}

// open records a newly opened document and returns it.
func (s *documentStore) open(uri lsp.DocumentURI, version int, text string, tasks *v1.Registry) *document {
	doc := newDocument(uri, version, text, tasks)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.docs == nil {
		s.docs = make(map[lsp.DocumentURI]*document)
	}
	s.docs[uri] = doc
	return doc
}

// change applies content changes and returns the resulting document, or nil when
// the change is stale and was ignored.
//
// Both sync kinds are handled even though the server advertises full sync: a
// change carrying a range is applied as a splice. Honoring an incremental change
// costs a few lines and means a client that ignores the advertised kind gets
// correct results instead of a silently truncated document.
//
// The version guard matters because the connection wraps the handler in
// jsonrpc2.AsyncHandler, which starts a goroutine per message and so gives up the
// arrival ordering the protocol otherwise guarantees. Two edits landing out of
// order would leave the document holding the older text — and for an incremental
// change, applying a range computed against text the store never had would corrupt
// it outright. Document versions are monotonic per the spec, so comparing them
// restores the ordering the wrapper discarded.
func (s *documentStore) change(uri lsp.DocumentURI, version int, changes []lsp.TextDocumentContentChangeEvent, tasks *v1.Registry) *document {
	s.mu.Lock()
	defer s.mu.Unlock()

	text := ""
	if prev, ok := s.docs[uri]; ok {
		// A version of zero means the client does not track them, in which case
		// there is nothing to compare and last-write-wins is all that is on offer.
		if version > 0 && prev.version > 0 && version <= prev.version {
			return nil
		}
		text = prev.text
	}
	for _, c := range changes {
		if c.Range == nil {
			text = c.Text
			continue
		}
		ix := newLineIndex(text)
		start := ix.offsetOfPosition(c.Range.Start)
		end := ix.offsetOfPosition(c.Range.End)
		if end < start {
			start, end = end, start
		}
		text = text[:start] + c.Text + text[end:]
	}

	doc := newDocument(uri, version, text, tasks)
	if s.docs == nil {
		s.docs = make(map[lsp.DocumentURI]*document)
	}
	s.docs[uri] = doc
	return doc
}

// get returns the document open under uri, if any.
func (s *documentStore) get(uri lsp.DocumentURI) (*document, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	doc, ok := s.docs[uri]
	return doc, ok
}

// close forgets a document.
func (s *documentStore) close(uri lsp.DocumentURI) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.docs, uri)
}
