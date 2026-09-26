package lsp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
)

// requestWithParams builds a jsonrpc2 request the way the codec would deliver
// it, so announceInbound is tested against the same shape it sees in serving.
func requestWithParams(t *testing.T, method string, params any) *jsonrpc2.Request {
	t.Helper()
	req := &jsonrpc2.Request{Method: method}
	if params == nil {
		return req
	}
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	msg := json.RawMessage(raw)
	req.Params = &msg
	return req
}

// TestAnnounceInboundOrdersTheBuild is the deterministic half of the #317
// fix: a document notification's build is registered by announceInbound
// itself, which [NewHandler] calls on the read loop before dispatch, so the
// guarantee does not depend on how the handler goroutines are scheduled.
//
// The over-the-wire behavior is covered by the probabilistic test in
// requestrace_test.go; this one pins the mechanism those odds rest on.
func TestAnnounceInboundOrdersTheBuild(t *testing.T) {
	t.Parallel()

	s := &FlowfileServer{Logger: discardLogger()}
	uri := lsp.DocumentURI("file:///ordered.yaml")

	_, release, coalesced := s.announceInbound(requestWithParams(t, "textDocument/didOpen", lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{URI: uri, Version: 1, Text: "edition: v2026.3\n"},
	}))
	if coalesced {
		t.Fatal("a didOpen must never be coalesced")
	}

	if got := func() int {
		s.docs.mu.Lock()
		defer s.docs.mu.Unlock()
		return s.docs.building[uri]
	}(); got != 1 {
		t.Fatalf("after announceInbound: building[%s] = %d, want 1", uri, got)
	}

	// A request awaiting the document now blocks on the announced build rather
	// than answering absent, however long dispatch of the didOpen goroutine
	// takes. Simulate that dispatch landing after the request began.
	type result struct {
		doc *document
		ok  bool
	}
	got := make(chan result, 1)
	go func() {
		doc, ok := s.docs.await(context.Background(), nil, uri)
		got <- result{doc, ok}
	}()

	select {
	case r := <-got:
		t.Fatalf("await returned before the build landed: doc=%v ok=%v", r.doc, r.ok)
	case <-time.After(20 * time.Millisecond):
	}

	s.docs.open(uri, 1, "edition: v2026.3\n", nil)
	release()

	select {
	case r := <-got:
		if !r.ok || r.doc == nil {
			t.Fatalf("await after build landed: doc=%v ok=%v, want the document", r.doc, r.ok)
		}
		if r.doc.version != 1 {
			t.Fatalf("await returned version %d, want 1", r.doc.version)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("await did not return after the build landed and was released")
	}
}

// TestAnnounceInboundQueuesSameURINotificationsInAnnouncedOrder is the
// deterministic half of the #1986 fix, in the same spirit
// [TestAnnounceInboundOrdersTheBuild] pins for #317: it drives
// announceInbound and the store directly, in a single goroutine, so the
// property under test does not depend on how a scheduler happens to run
// anything.
//
// It reproduces the shape an independent review of an earlier version of
// this fix found still broken — a same-URI didClose immediately followed by
// a didOpen reopening it at version 1, all three of this test's requests
// announced (as [asyncHandler.Handle] announces them on the read loop) before
// any of their handlers has run at all. That is not a reordering: it is what
// jsonrpc2.AsyncHandler's goroutine-per-message dispatch does whenever an
// editor sends notifications faster than they are handled, which is the
// ordinary case for a close-then-reopen an editor issues in one breath. The
// earlier fix compared a close's own ticket against a counter that had
// already advanced for the reopen the instant it was announced — before the
// close's handler had run at all — and so treated it as superseded, leaving
// the incumbent in the store for [documentStore.open]'s own version guard to
// read the v1 reopen as stale against. This asserts the replacement
// mechanism's actual contract: each wait channel [documentStore.enqueue]
// hands out closes only once its predecessor's release has run, so a
// handler that respects it — as every real one does — cannot apply out of
// the order its request was announced in, whatever that predecessor's
// request was announced relative to this one's own dispatch.
func TestAnnounceInboundQueuesSameURINotificationsInAnnouncedOrder(t *testing.T) {
	t.Parallel()

	s := &FlowfileServer{Logger: discardLogger()}
	uri := lsp.DocumentURI("file:///queue.yaml")

	openReq := requestWithParams(t, "textDocument/didOpen", lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{URI: uri, Version: 1, Text: "name: original\n"},
	})
	closeReq := requestWithParams(t, "textDocument/didClose", lsp.DidCloseTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: uri},
	})
	reopenReq := requestWithParams(t, "textDocument/didOpen", lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{URI: uri, Version: 1, Text: "name: reopened\n"},
	})

	// All three announced before any handler below has run, exactly as the
	// read loop would announce a burst the goroutines behind it have not
	// caught up with yet.
	openWait, openRelease, _ := s.announceInbound(openReq)
	closeWait, closeRelease, _ := s.announceInbound(closeReq)
	reopenWait, reopenRelease, _ := s.announceInbound(reopenReq)

	if openWait != nil {
		t.Fatal("the first notification queued for a URI has nothing to wait on")
	}
	s.docs.open(uri, 1, "name: original\n", nil)
	openRelease()

	select {
	case <-closeWait:
	default:
		t.Fatal("the close's wait channel did not close once the open ahead of it released")
	}
	s.docs.close(uri)
	closeRelease()

	select {
	case <-reopenWait:
	default:
		t.Fatal("the reopen's wait channel did not close once the close ahead of it released")
	}
	got := s.docs.open(uri, 1, "name: reopened\n", nil)
	reopenRelease()

	if got.text != "name: reopened\n" {
		t.Fatalf("an in-order close-then-reopen(v1), applied in the order enqueue serializes it, kept %q", got.text)
	}
}

// changeReq builds a full-sync didChange request: the single-entry,
// no-range shape this server advertises and the one
// [documentStore.enqueueChange] coalesces.
func changeReq(t *testing.T, uri lsp.DocumentURI, version int, text string) *jsonrpc2.Request {
	t.Helper()
	return requestWithParams(t, "textDocument/didChange", lsp.DidChangeTextDocumentParams{
		TextDocument:   lsp.VersionedTextDocumentIdentifier{TextDocumentIdentifier: lsp.TextDocumentIdentifier{URI: uri}, Version: version},
		ContentChanges: []lsp.TextDocumentContentChangeEvent{{Text: text}},
	})
}

// TestAnnounceInboundCoalescesFullSyncDidChangeBurst is the deterministic
// half of the #2071 fix, in the spirit [TestAnnounceInboundOrdersTheBuild]
// and [TestAnnounceInboundQueuesSameURINotificationsInAnnouncedOrder] pin for
// #317 and #1986: it drives announceInbound directly, in a single goroutine,
// so the bound under test does not depend on how a scheduler happens to run
// anything.
//
// A burst of full-sync didChanges for one URI, every one of them announced —
// as [asyncHandler.Handle] announces them on the read loop — before any
// handler has run at all, is exactly what a burst of keystrokes produces
// under jsonrpc2.AsyncHandler's goroutine-per-message dispatch. Before the
// fix, every one of them queued: each got a wait channel and a release
// [asyncHandler.Handle] would hand to a goroutine of its own, so N
// notifications meant N goroutines parked holding N sets of params, however
// large N was. This shows the queue holds at most one of them, whatever N
// is: [asyncHandler.Handle] spawns a goroutine only for the one
// announceInbound reports as queued (coalesced == false), never for one it
// reports as coalesced — see its own doc comment — so the count this test
// makes of the former is the count of goroutines the fix leaves parked.
func TestAnnounceInboundCoalescesFullSyncDidChangeBurst(t *testing.T) {
	t.Parallel()

	s := &FlowfileServer{Logger: discardLogger()}
	uri := lsp.DocumentURI("file:///storm.yaml")

	const burst = 200
	var queued, coalescedCount int
	var wait <-chan struct{}
	var release func()
	for i := 1; i <= burst; i++ {
		w, r, coalesced := s.announceInbound(changeReq(t, uri, i, strings.Repeat("#", i)+"\n"))
		if coalesced {
			coalescedCount++
			if w != nil || r != nil {
				t.Fatalf("message %d: a coalesced didChange returned a non-nil wait or release", i)
			}
			continue
		}
		queued++
		wait, release = w, r
	}

	if queued != 1 {
		t.Fatalf("a burst of %d full-sync didChanges for one URI queued %d, want 1", burst, queued)
	}
	if coalescedCount != burst-1 {
		t.Fatalf("a burst of %d full-sync didChanges for one URI coalesced %d, want %d", burst, coalescedCount, burst-1)
	}
	if wait != nil {
		t.Fatal("the first notification queued for a URI has nothing to wait on")
	}

	// Exactly one build in flight and one queue entry for the whole burst,
	// not one per message — the same building count [documentStore.await]
	// relies on to know a build is still coming rather than never arriving.
	s.docs.mu.Lock()
	building := s.docs.building[uri]
	tail := len(s.docs.tail)
	s.docs.mu.Unlock()
	if building != 1 {
		t.Fatalf("building[%s] = %d after the burst, want 1", uri, building)
	}
	if tail != 1 {
		t.Fatalf("tail has %d entries after the burst, want 1", tail)
	}

	// The one queued message, once its handler claims its slot, reads the
	// newest version and text — the ones the burst's last message carried —
	// not its own: versions 1..(burst-1) were folded away rather than each
	// being separately applied and discarded.
	version, text := s.docs.claimChange(uri, 1, strings.Repeat("#", 1)+"\n")
	wantText := strings.Repeat("#", burst) + "\n"
	if version != burst || text != wantText {
		t.Fatalf("claimChange returned version=%d text=%q, want version=%d text=%q", version, text, burst, wantText)
	}

	release()

	s.docs.mu.Lock()
	_, stillCoalesced := s.docs.coalesced[uri]
	n := len(s.docs.building)
	q := len(s.docs.tail)
	s.docs.mu.Unlock()
	if stillCoalesced {
		t.Fatal("the coalesced slot survived its owner's release")
	}
	if n != 0 {
		t.Fatalf("building has %d entries after release, want 0", n)
	}
	if q != 0 {
		t.Fatalf("tail has %d entries after release, want 0", q)
	}
}

// TestAnnounceInboundIgnoresWhatBuildsNothing pins the negative space: only a
// document notification with a usable URI registers a build, and everything
// else must be a no-op, because a registration nothing will ever retire would
// hold every await for that URI to its full build timeout.
func TestAnnounceInboundIgnoresWhatBuildsNothing(t *testing.T) {
	t.Parallel()

	s := &FlowfileServer{Logger: discardLogger()}

	cases := []struct {
		name string
		req  *jsonrpc2.Request
	}{
		{"request method", requestWithParams(t, "textDocument/hover", lsp.TextDocumentPositionParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: "file:///a.yaml"},
		})},
		{"no params", requestWithParams(t, "textDocument/didOpen", nil)},
		{"malformed params", &jsonrpc2.Request{Method: "textDocument/didOpen", Params: func() *json.RawMessage {
			m := json.RawMessage(`{"textDocument":`)
			return &m
		}()}},
		{"empty uri", requestWithParams(t, "textDocument/didChange", lsp.DidChangeTextDocumentParams{})},
	}
	for _, tc := range cases {
		wait, release, coalesced := s.announceInbound(tc.req)
		if wait != nil {
			t.Fatalf("%s: announceInbound returned a wait channel for a message that queues nothing", tc.name)
		}
		if coalesced {
			t.Fatalf("%s: announceInbound reported a message that builds nothing as coalesced", tc.name)
		}
		s.docs.mu.Lock()
		n := len(s.docs.building)
		q := len(s.docs.tail)
		s.docs.mu.Unlock()
		if n != 0 {
			t.Fatalf("%s: announceInbound registered a build; building has %d entries, want 0", tc.name, n)
		}
		if q != 0 {
			t.Fatalf("%s: announceInbound registered a queue entry; tail has %d entries, want 0", tc.name, q)
		}
		release()
	}
}
