package lsp

import (
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
// documentKind says which of the languages this server speaks a document holds.
//
// A `*.test.yaml` is flowtest's suite format and `testdefaults.yaml` its shared
// directory fixture — real languages of this repository, but not the Flowfile
// grammar. Before the kind existed, a test file handed to this server was
// diagnosed *as a workflow*: `tests:` an unknown key, no `steps:`, every line a
// squiggle — false diagnostics on a correct file, the exact failure this
// package's doc names as worse than silence (#1110). The kind is what routes a
// document to checks that speak its language and gates off the ones that do not.
type documentKind int

const (
	docWorkflow documentKind = iota
	docTestFile
	docTestDefaults
)

// speaksFlowfile reports whether the workflow grammar's features may answer
// for this document. A test document never gets a workflow answer: a
// completion, hover, symbol tree or format computed from the workflow
// grammar would be confidently wrong in someone's editor, which is worse
// than the honest empty answer.
//
// This does not mean a test document gets nothing. #1110 item 8 gave the
// test language its own answers — completion, an outline of its cases, and
// hover on a stub's task name — each behind its own dispatch on doc.kind
// rather than behind this gate; see completeAt, testDocumentSymbols and
// hoverAt. speaksFlowfile stays the question "may the *workflow* grammar
// answer here", not "may anything answer here".
func (doc *document) speaksFlowfile() bool { return doc.kind == docWorkflow }

// isTestDocument reports whether doc is one of flowtest's own file kinds —
// a `*.test.yaml` suite or a directory's `testdefaults.yaml` — as opposed to
// a Flowfile. The two test kinds share a document shape at every level below
// the top (both nest a `defaults:` block of the same [flowtest.Defaults]
// shape), which is why features that answer for one often take doc.kind as
// a parameter rather than being written twice.
func (doc *document) isTestDocument() bool {
	return doc.kind == docTestFile || doc.kind == docTestDefaults
}

// kindOfURI reads the kind off the document's basename, which is also how the
// editor configurations decide what to attach: `**/*.test.yaml` and
// `testdefaults.yaml` name the test language, everything else the Flowfile.
func kindOfURI(uri lsp.DocumentURI) documentKind {
	base := string(uri)
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[i+1:]
	}
	switch {
	case strings.HasSuffix(base, ".test.yaml"), strings.HasSuffix(base, ".test.yml"):
		return docTestFile
	case base == "testdefaults.yaml":
		return docTestDefaults
	default:
		return docWorkflow
	}
}

type document struct {
	uri     lsp.DocumentURI
	version int
	text    string

	// kind routes the document to the language it actually holds; see
	// [documentKind].
	kind documentKind

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

// filesystemPath returns doc's location as a path a `call:` step can be
// resolved against, when its URI names one.
//
// Only the `file://` scheme has one: an untitled buffer, or a document an
// extension synthesizes over some other scheme, has no directory a relative
// path could mean anything against, and [flowfile.ValidateSourceAt] refuses a
// `call:` in that case with a diagnostic saying so — the same answer every
// other bytes-only caller of this package gives.
//
// Parsed as a URI rather than trimmed as a prefix, because a `file://` URI is
// not a path with a scheme stapled on the front: a real editor sends one
// percent-encoded — a space as `%20`, a `#` in a filename as `%23`, a
// non-ASCII character as its UTF-8 percent escapes — and trimming the prefix
// leaves every one of those still escaped, so a `call:` step beside a file
// whose name needs any of them resolves against a path that does not exist.
// An authority is legal too: `file://host/path` names a UNC-style location,
// and Windows spells a drive letter either `file:///C:/path` (the drive
// after an empty authority, with the leading slash net/url keeps) or, from
// some clients, `file://C:/path` (the drive letter parsed as the authority).
// Both are handled below; a genuine remote authority — anything else — is
// refused the same as no path at all, since nothing here can read it.
func (doc *document) filesystemPath() (string, bool) {
	u, err := url.Parse(string(doc.uri))
	if err != nil || u.Scheme != "file" {
		return "", false
	}

	host := u.Host
	path := u.Path

	// `file://C:/path`: net/url reads everything up to the next `/` as the
	// authority, and a single letter followed by `:` is not a host anything
	// speaks — it is a drive letter LSP clients sometimes emit this way.
	if isWindowsDrive(host) {
		return host + path, true
	}

	// An empty authority is the ordinary form; `localhost` names this
	// machine explicitly, which means the same thing. Anything else is a
	// real remote authority — a UNC share, another machine entirely — which
	// this process has no path to open regardless of what it decodes to.
	if host != "" && !strings.EqualFold(host, "localhost") {
		return "", false
	}
	if path == "" {
		return "", false
	}

	// `file:///C:/path`: the authority is empty (three slashes), and the
	// drive letter is the first path segment with net/url's leading slash
	// still in front of it — `/C:/path` rather than the `C:/path` a Windows
	// API takes.
	if len(path) >= 3 && isWindowsDrive(path[1:3]) {
		path = path[1:]
	}

	return path, true
}

// isWindowsDrive reports whether s is a bare drive letter and colon, `C:` —
// the whole of what distinguishes a Windows drive from an ordinary URI
// authority or path segment, in either position `filesystemPath` meets one.
func isWindowsDrive(s string) bool {
	if len(s) != 2 || s[1] != ':' {
		return false
	}
	c := s[0]
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
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
		kind:     kindOfURI(uri),
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

// A build is one document notification that has arrived and not yet landed.
//
// It exists so a request can tell "there is nothing here" from "there is
// something here and it is not finished yet". Those are the same answer to
// [documentStore.get] and they must not be the same answer to a client: the
// first is honestly null, and the second is #317: a hover returning null for a
// document the editor opened a microsecond earlier, which reads to a user as the
// feature having nothing to say about that position.
//
// The two waits below are different questions and so have different bounds.
const (
	// documentSettleGrace is how long a request waits for a build to be
	// *registered* when the store has never heard of the URI.
	//
	// Served through [NewHandler], the grace should never be what answers: a
	// build is announced on the connection's read loop before dispatch, so any
	// request sent after a document notification finds it registered already,
	// and a URI with no document and no build is one that genuinely was never
	// opened. The grace is the fallback for a server wired without that wrapper
	// — jsonrpc2.AsyncHandler alone starts a goroutine per message and lets the
	// scheduler order them, leaving a window between a didOpen being dispatched
	// and this store hearing about it. A tenth of a second is orders of
	// magnitude more than that window ordinarily is, and it is the whole cost
	// paid by a request for a URI that was never opened.
	documentSettleGrace = 100 * time.Millisecond

	// documentBuildTimeout is how long a request waits for a build it has seen
	// registered to finish. That is a bounded parse of at most
	// [maxDocumentBytes], so this is a ceiling and not an expectation.
	//
	// Reaching it does not fail the request: the caller answers from whatever
	// version it has, because a slightly stale answer is worth more to an author
	// than a null one.
	documentBuildTimeout = 2 * time.Second
)

// A documentStore holds the documents an editor has open, keyed by URI.
type documentStore struct {
	mu          sync.Mutex
	docs        map[lsp.DocumentURI]*document
	localByPath map[string]lsp.DocumentURI

	// building counts the document notifications in flight per URI: incremented
	// before the parse starts, decremented once the result is in docs.
	building map[lsp.DocumentURI]int

	// settled is closed and replaced on every change to docs or building, which
	// is how a waiter is woken without polling. Nil until someone waits.
	settled chan struct{}

	// awaitTrace, when set, is called once by every [documentStore.await] return
	// with whether that call gave up because its bound expired rather than
	// because it found a current document. It exists for tests that need to
	// tell "the wait never rode out the build bound" from "it did, and still
	// answered" — a distinction elapsed wall-clock time cannot make reliably
	// under contention, since a build that lands just under the deadline looks
	// identical to one answered from memory once you're only counting
	// milliseconds. Nil in production, where the call costs nothing.
	awaitTrace func(boundExpired bool)

	// closeGate, when set, is called by [documentStore.close] with uri before
	// the delete runs, and blocks until told to proceed. It exists for a test
	// that needs a same-URI didOpen provably held behind an in-flight didClose
	// rather than merely hoping the scheduler cooperates: hold the close here,
	// send the reopen, and show the store still has not changed until the gate
	// is released. Nil in production, where the call costs nothing.
	closeGate func(uri lsp.DocumentURI)

	// tail is, for each URI with a didOpen, didChange or didClose still queued
	// or in flight, the channel that operation closes once its handler
	// returns. The next same-URI notification's goroutine waits on it before
	// touching the store — see [documentStore.enqueue] — which is what makes
	// the three apply in the arrival order the protocol implies regardless of
	// how jsonrpc2.AsyncHandler schedules the goroutines behind them (#1986).
	// Deleted the moment nothing is queued behind it, so a long-running
	// connection's memory is bounded by URIs currently in flight rather than
	// by every URI it has ever seen.
	tail map[lsp.DocumentURI]chan struct{}

	// parseGate, when set, is called by [documentStore.change] with uri right
	// after s.mu is released and before the unlocked splice and
	// [newDocument]'s parse run, and blocks until told to proceed. It exists
	// for a test that needs a slow parse for one URI provably not holding
	// s.mu while it runs, rather than hoping a concurrent enqueue/beginBuild
	// for a different URI never contends with it by coincidence: hold this
	// URI's change here, show the other URI's enqueue/beginBuild still
	// completes, and only then release it (#2071 review, F3). Nil in
	// production, where the call costs nothing.
	parseGate func(uri lsp.DocumentURI)
}

// changeVersionWins reports whether version supersedes prev under the rule
// [documentStore.change] applies before committing an edit: a version of
// zero on either side means that side is not tracked against, in which case
// there is nothing to compare and last-write-wins is all that is on offer;
// otherwise the newer one must be strictly greater.
func changeVersionWins(version, prev int) bool {
	return version <= 0 || prev <= 0 || version > prev
}

// enqueue returns the channel uri's next document notification must wait on
// before its handler may touch the store, and the function that operation
// calls once its handler returns — which both releases whatever is chained
// behind it and retires uri's entry when nothing is.
//
// wait is nil for the first notification a URI has queued, since there is
// nothing to wait on; a caller checks for that rather than receiving from a
// nil channel, which would block forever.
//
// Called from [FlowfileServer.announceInbound] on the connection's read loop,
// before a document notification's handler is dispatched onto its own
// goroutine — the one place per-URI arrival order still exists once
// jsonrpc2.AsyncHandler takes over dispatch. Each call chains behind whatever
// the previous call for the same URI returned, so the wait channels form a
// queue in wire arrival order, and a goroutine that waits on one before
// working is serialized behind its predecessor no matter which of the two
// the scheduler would otherwise have run first.
func (s *documentStore) enqueue(uri lsp.DocumentURI) (wait <-chan struct{}, done func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	wait = s.tail[uri]
	mine := make(chan struct{})
	if s.tail == nil {
		s.tail = make(map[lsp.DocumentURI]chan struct{})
	}
	s.tail[uri] = mine

	return wait, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		close(mine)
		// Only retire the entry if nothing has queued behind this one since —
		// a later enqueue already replaced it, and that call owns retiring it.
		if s.tail[uri] == mine {
			delete(s.tail, uri)
		}
	}
}

// setParseGate installs the test hook described on
// [documentStore.parseGate]. Locked because it is set from a different
// goroutine than the one that will read it inside [documentStore.change].
func (s *documentStore) setParseGate(f func(uri lsp.DocumentURI)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.parseGate = f
}

// setAwaitTrace installs the test hook described on [documentStore.awaitTrace].
// Locked because it is set from a different goroutine than the one that will
// read it inside [documentStore.await].
func (s *documentStore) setAwaitTrace(f func(boundExpired bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.awaitTrace = f
}

// setCloseGate installs the test hook described on [documentStore.closeGate].
// Locked because it is set from a different goroutine than the one that will
// read it inside [documentStore.close].
func (s *documentStore) setCloseGate(f func(uri lsp.DocumentURI)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeGate = f
}

// beginBuild records that a document notification for uri is being handled.
//
// Called before the parse rather than after it, because the parse is the long
// part: registering afterwards would leave the window this whole mechanism
// exists to close.
func (s *documentStore) beginBuild(uri lsp.DocumentURI) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.building == nil {
		s.building = make(map[lsp.DocumentURI]int)
	}
	s.building[uri]++
	s.wakeLocked()
}

// endBuild records that one has finished, landed or discarded as stale.
func (s *documentStore) endBuild(uri lsp.DocumentURI) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.building[uri] <= 1 {
		delete(s.building, uri)
	} else {
		s.building[uri]--
	}
	s.wakeLocked()
}

// wakeLocked releases every waiter so each can re-read the store. Callers hold
// s.mu.
func (s *documentStore) wakeLocked() {
	if s.settled != nil {
		close(s.settled)
		s.settled = nil
	}
}

// settledLocked returns the channel a waiter blocks on until the next change.
// Callers hold s.mu.
func (s *documentStore) settledLocked() chan struct{} {
	if s.settled == nil {
		s.settled = make(chan struct{})
	}
	return s.settled
}

// await returns the document open under uri, waiting for a build that has
// arrived and not yet landed.
//
// This is what every position request reads through, and it differs from
// [documentStore.get] in one way: an absent document is not immediately an
// answer. A request may have been started before the notification that builds
// the document it asks about, so an absent document is a question rather than an
// answer: is one coming? The store can say, because a build announces itself
// before it parses.
//
// The wait ends on whichever comes first: the document being current, the
// request being cancelled, the connection dropping, or a bound expiring. It
// never ends on nothing. A language server that blocks forever because a build
// it was promised never arrived is not failing closed, it is hanging, and an
// editor with a hover spinner that never resolves is worse off than one told
// null.
//
// When a bound expires the current document is returned anyway, absent or not,
// so a request that outlives a slow build still answers from the previous
// version rather than from nothing.
func (s *documentStore) await(ctx context.Context, disconnected <-chan struct{}, uri lsp.DocumentURI) (*document, bool) {
	now := time.Now()
	graceDeadline := now.Add(documentSettleGrace)
	buildDeadline := now.Add(documentBuildTimeout)

	s.mu.Lock()
	trace := s.awaitTrace
	s.mu.Unlock()

	// Whether a build was ever seen registered decides which bound applies: the
	// short one is for finding out that nothing is coming, the long one is for
	// waiting on something that is.
	sawBuild := false

	for {
		s.mu.Lock()
		doc, ok := s.docs[uri]
		inFlight := s.building[uri]
		settled := s.settledLocked()
		s.mu.Unlock()

		if inFlight > 0 {
			sawBuild = true
		}
		// A document with nothing in flight behind it is the current one. With a
		// build in flight it is not: during a burst of keystrokes the store holds
		// a version the client has already replaced, and answering from it would
		// describe text the author is no longer looking at.
		if ok && inFlight == 0 {
			if trace != nil {
				trace(false)
			}
			return doc, true
		}

		deadline := graceDeadline
		if sawBuild {
			deadline = buildDeadline
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if trace != nil {
				trace(true)
			}
			return doc, ok
		}

		timer := time.NewTimer(remaining)
		select {
		case <-settled:
			timer.Stop()
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, false
		case <-disconnected:
			timer.Stop()
			return nil, false
		}
	}
}

// open records a newly opened document and returns it, or the document already
// stored when this open would move it backwards.
//
// The version guard is [documentStore.change]'s, for the same reason: a caller
// that mutates a URI's document without going through the per-URI queue
// [documentStore.enqueue] serializes — every didOpen, didChange and didClose
// the connection actually serves is one such caller, but this package's own
// tests drive the store directly and are not — has no ordering guarantee of
// its own, and a stale open must not silently revert a newer edit. Without the
// guard, an open applied out of turn would replace the edited document with
// the opened text at version 1 -- an editor that takes the first keystrokes
// and then silently reverts them, answering hover and diagnostics from
// pre-edit text until the next change lands.
//
// A document still in the store means this open was overtaken. The protocol
// requires didClose before a document is opened again, [documentStore.close]
// removes it, and the connection's serialization guarantees a same-URI
// reopen's handler is not even dispatched until that close has finished
// (#1986) — so a real reopen never finds an incumbent there. What is left to
// find one is a client re-opening without closing, or a caller bypassing the
// serialization the connection provides; neither is a reason to move the
// document backwards.
func (s *documentStore) open(uri lsp.DocumentURI, version int, text string, tasks *v1.Registry) *document {
	// Announced before the parse and retired after the result is stored, so a
	// request that arrives in between waits for this rather than reading past it.
	// Deferred so a panic in the parse cannot leave a gate that never opens.
	s.beginBuild(uri)
	defer s.endBuild(uri)

	doc := newDocument(uri, version, text, tasks)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.docs == nil {
		s.docs = make(map[lsp.DocumentURI]*document)
	}
	if s.localByPath == nil {
		s.localByPath = make(map[string]lsp.DocumentURI)
	}
	if prev, ok := s.docs[uri]; ok {
		// Ordered against what the store holds, not against this open's own
		// version. Zero is a legal document version, not a sentinel: a client
		// that opens at zero and immediately edits to one is compliant, and
		// comparing only opens above zero would let that open revert the edit —
		// the very reversion this guard exists to stop. What zero means is that
		// the *stored* document cannot be ordered against, which is the client
		// that does not track versions at all; there last-write-wins is all
		// that is on offer, so the open applies.
		if prev.version > 0 && version <= prev.version {
			doc = prev
		}
	}
	s.docs[uri] = doc
	if path, ok := doc.filesystemPath(); ok {
		s.localByPath[filepath.Clean(path)] = uri
	}
	// Registered and woken on both paths: a request blocked on the build gate is
	// waiting for this call, and an overtaken open still has to release it.
	s.wakeLocked()
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
//
// s.mu is released between reading the base text and storing the result: the
// splice and [newDocument]'s parse both run unlocked. [documentStore.enqueue]
// and [documentStore.beginBuild] take s.mu too, and both are called from the
// connection's read loop before a notification's handler even starts (see
// [FlowfileServer.announceInbound]) — holding s.mu through a parse, the slow
// part and unbounded up to maxDocumentBytes, would stall that read loop for
// every URI on the connection, not just this one, until this one finished
// (#2071).
//
// Nothing serializes two calls for the same URI while this is unlocked: the
// connection's own per-URI queue keeps its own callers from overlapping, but
// this method does not enforce that itself, and both this package's tests
// and a server wired with bare jsonrpc2.AsyncHandler (see [NewHandler]'s doc
// comment) call it directly and can race. So the commit below re-applies
// [changeVersionWins] against whatever s.docs[uri] holds right now, under
// the second lock, rather than trusting the base text read above still to be
// current — without that re-check, two concurrent calls could both parse
// against the same base text and then commit in either order, letting a
// slower, lower-versioned parse overwrite a faster, higher-versioned one
// that had already landed (#2071 review, F3).
//
// Two more checks at commit time cover what a version comparison alone does
// not, both about the document this call actually read rather than the one
// now current:
//
//   - If a document existed when the base text was read but none exists by
//     commit time — closed while the splice and parse ran unlocked — the
//     result is dropped rather than resurrecting what the close removed
//     (#2089 review, O1).
//   - If a document existed both times but is not the *same* one — a
//     close-then-reopen, or another change landing in between, either of
//     which installs a new [*document] rather than mutating the old one in
//     place — an incremental change's splice is dropped too: it was computed
//     against text that is no longer this URI's history, and applying it
//     would corrupt whatever the replacement established. A full-sync
//     change is exempt from this one check, because it never depended on
//     that text in the first place; the version comparison above is all it
//     needs (#2089 review, second round, item a).
func (s *documentStore) change(uri lsp.DocumentURI, version int, changes []lsp.TextDocumentContentChangeEvent, tasks *v1.Registry) *document {
	// See [documentStore.open]. Registered before s.mu is taken and retired after
	// it is released: deferred calls run last-in-first-out, so the unlock below
	// happens first and endBuild is free to take the lock itself.
	s.beginBuild(uri)
	defer s.endBuild(uri)

	// Computed once, unconditionally: whether prev exists decides if this is
	// needed to proceed at all (below), and the commit re-check needs it
	// regardless of which branch ran, to tell an incremental change — whose
	// splice depends on a specific predecessor — from a full-sync one, which
	// does not.
	//
	// At least one entry carrying no range replaces everything and makes
	// whatever preceded it irrelevant, which is the sync kind this server
	// advertises. The test is "does a full replacement arrive", not "does a
	// range arrive": an empty set carries no range and still establishes
	// nothing, and a set ending in a full replacement carries one and
	// establishes everything.
	establishesText := false
	for _, c := range changes {
		if c.Range == nil {
			establishesText = true
			break
		}
	}

	s.mu.Lock()
	text := ""
	var prevDoc *document
	if prev, ok := s.docs[uri]; ok {
		prevDoc = prev
		if !changeVersionWins(version, prev.version) {
			s.mu.Unlock()
			return nil
		}
		text = prev.text
	} else {
		// No document to splice into, so this change set is applied only if
		// it establishes the whole text by itself.
		//
		// Anything else would fabricate a document out of nothing — a range
		// spliced into an empty string leaves the replacement alone, and an
		// empty change set leaves the empty string — stored at the edit's
		// version, which then looks newer than the didOpen still on its way.
		// The guard in open would keep it, and the buffer would stay wrong for
		// good. Ignoring the change instead leaves the open to land the real
		// text, and the client's next edit applies to that.
		if !establishesText {
			s.mu.Unlock()
			return nil
		}
	}
	gate := s.parseGate
	s.mu.Unlock()

	if gate != nil {
		gate(uri)
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

	s.mu.Lock()
	defer s.mu.Unlock()
	// Re-checked against whatever is current now, not against the base text
	// read above: see the doc comment above this method (#2071 review, F3;
	// #2089 review, O1 and second round item a).
	cur, ok := s.docs[uri]
	switch {
	case prevDoc != nil && !ok:
		// The document this call read a base text from was closed while the
		// splice and parse ran unlocked. Writing the parsed result now would
		// resurrect a document the client believes is closed.
		return nil
	case ok && !changeVersionWins(version, cur.version):
		return nil
	case prevDoc != nil && cur != prevDoc && !establishesText:
		// A different document now occupies this URI than the one this
		// incremental change's splice was computed against — a
		// close-then-reopen, or another change, landed while this one's
		// parse ran unlocked. The version comparison above cannot catch
		// this: the replacement may carry a lower version this change's own
		// version still numerically wins against, but the splice's offsets
		// were never computed against the replacement's text.
		return nil
	}
	if s.docs == nil {
		s.docs = make(map[lsp.DocumentURI]*document)
	}
	if s.localByPath == nil {
		s.localByPath = make(map[string]lsp.DocumentURI)
	}
	s.docs[uri] = doc
	if path, ok := doc.filesystemPath(); ok {
		s.localByPath[filepath.Clean(path)] = uri
	}
	s.wakeLocked()
	return doc
}

// get returns the document open under uri, if any, without waiting for a build
// that has not landed.
//
// Position requests use [documentStore.await] instead. This is for the callers
// with nothing to wait for: a notification that re-publishes what is already
// there, and the tests that drive the store directly.
func (s *documentStore) get(uri lsp.DocumentURI) (*document, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, ok := s.docs[uri]
	return doc, ok
}

// getByFilesystemPath finds an open local document independent of whether the
// client spelled its URI with an empty or localhost authority.
func (s *documentStore) getByFilesystemPath(path string) (*document, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	uri, ok := s.localByPath[filepath.Clean(path)]
	if !ok {
		return nil, false
	}
	doc, ok := s.docs[uri]
	return doc, ok
}

// close forgets a document.
//
// It used to be the one mutation here with no ordering of its own:
// [lsp.DidCloseTextDocumentParams] carries only a TextDocumentIdentifier, not
// a version the way open and change have, so a close reordered behind the
// didOpen that reopened the same URI deleted the document the reopen had just
// established (#1986). That gap is closed one layer up, not here — every
// didOpen, didChange and didClose the connection serves is queued per URI by
// [documentStore.enqueue] before this is ever called, so by the time a
// same-URI reopen's handler is dispatched, the close that precedes it in
// arrival order has already run to completion. What was once this function's
// own problem is now simply never true of it: close always applies to
// whatever the store currently holds for uri, because nothing queued behind
// it could have landed first.
func (s *documentStore) close(uri lsp.DocumentURI) {
	s.mu.Lock()
	gate := s.closeGate
	s.mu.Unlock()
	if gate != nil {
		gate(uri)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if doc := s.docs[uri]; doc != nil {
		if path, ok := doc.filesystemPath(); ok && s.localByPath[filepath.Clean(path)] == uri {
			delete(s.localByPath, filepath.Clean(path))
		}
	}
	delete(s.docs, uri)
	// Woken because the store changed, the same as every other mutation here: a
	// waiter re-reads and decides for itself. It is the one mutation that can
	// take a document away, and a waiter must not be left holding a channel that
	// nothing will ever close.
	s.wakeLocked()
}
