package lsp

import (
	"context"
	"encoding/json"
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

	release := s.announceInbound(requestWithParams(t, "textDocument/didOpen", lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{URI: uri, Version: 1, Text: "edition: v2026.2\n"},
	}))

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

	s.docs.open(uri, 1, "edition: v2026.2\n", nil)
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
		release := s.announceInbound(tc.req)
		s.docs.mu.Lock()
		n := len(s.docs.building)
		s.docs.mu.Unlock()
		if n != 0 {
			t.Fatalf("%s: announceInbound registered a build; building has %d entries, want 0", tc.name, n)
		}
		release()
	}
}
