package lsp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
	"github.com/stretchr/testify/require"
)

// The tests drive the real server over a real JSON-RPC connection rather than
// calling its internals. What an editor experiences is the protocol surface — that
// a notification gets no reply, that diagnostics arrive as a notification with the
// right ranges, that a capability the server claims is one it answers — and none
// of that is exercised by calling handler functions directly.

// A client is a test-side LSP client connected to a server over an in-memory pipe.
type client struct {
	t    *testing.T
	conn *jsonrpc2.Conn

	mu sync.Mutex
	// published records every publishDiagnostics notification, in order.
	published []lsp.PublishDiagnosticsParams
	// notified counts the notifications the client received, by method.
	notified map[string]int

	waiters []chan struct{}
}

// discardLogger returns a logger that throws everything away, which is the default
// for tests that are not asserting on log output.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// newClient starts a server on one end of a pipe and returns a client on the other.
func newClient(t *testing.T) *client {
	t.Helper()
	return newClientFor(t, &FlowfileServer{Logger: discardLogger()})
}

// newClientFor connects a client to a specific server, for tests that need to
// inspect or preload it.
func newClientFor(t *testing.T, server *FlowfileServer) *client {
	t.Helper()

	serverSide, clientSide := net.Pipe()
	c := &client{t: t, notified: map[string]int{}}

	// The server is wrapped in AsyncHandler exactly as the command does, so the
	// tests run against the same concurrency the real server sees.
	serverConn := jsonrpc2.NewConn(
		context.Background(),
		jsonrpc2.NewBufferedStream(serverSide, jsonrpc2.VSCodeObjectCodec{}),
		jsonrpc2.AsyncHandler(server),
	)

	c.conn = jsonrpc2.NewConn(
		context.Background(),
		jsonrpc2.NewBufferedStream(clientSide, jsonrpc2.VSCodeObjectCodec{}),
		jsonrpc2.HandlerWithError(c.handle).SuppressErrClosed(),
	)

	t.Cleanup(func() {
		_ = c.conn.Close()
		_ = serverConn.Close()
	})
	return c
}

// handle receives the server's notifications and requests.
func (c *client) handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.notified[req.Method]++
	if req.Method == "textDocument/publishDiagnostics" {
		var params lsp.PublishDiagnosticsParams
		if req.Params != nil {
			require.NoError(c.t, json.Unmarshal(*req.Params, &params))
		}
		c.published = append(c.published, params)
		for _, w := range c.waiters {
			close(w)
		}
		c.waiters = nil
	}
	return nil, nil
}

// initialize performs the handshake and returns the advertised capabilities.
func (c *client) initialize() lsp.ServerCapabilities {
	c.t.Helper()
	var result lsp.InitializeResult
	require.NoError(c.t, c.conn.Call(c.t.Context(), "initialize", lsp.InitializeParams{}, &result))
	require.NoError(c.t, c.conn.Notify(c.t.Context(), "initialized", struct{}{}))
	return result.Capabilities
}

// open sends didOpen and waits for the diagnostics it triggers.
func (c *client) open(uri, text string) lsp.PublishDiagnosticsParams {
	c.t.Helper()
	wait := c.expectPublish()
	require.NoError(c.t, c.conn.Notify(c.t.Context(), "textDocument/didOpen", lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{
			URI:        lsp.DocumentURI(uri),
			LanguageID: "flowfile",
			Version:    1,
			Text:       text,
		},
	}))
	return c.await(wait)
}

// change sends a full-text didChange and waits for the diagnostics it triggers.
func (c *client) change(uri, text string, version int) lsp.PublishDiagnosticsParams {
	c.t.Helper()
	wait := c.expectPublish()
	require.NoError(c.t, c.conn.Notify(c.t.Context(), "textDocument/didChange", lsp.DidChangeTextDocumentParams{
		TextDocument:   lsp.VersionedTextDocumentIdentifier{TextDocumentIdentifier: lsp.TextDocumentIdentifier{URI: lsp.DocumentURI(uri)}, Version: version},
		ContentChanges: []lsp.TextDocumentContentChangeEvent{{Text: text}},
	}))
	return c.await(wait)
}

// changeRange sends an incremental didChange, which the server honors even though
// it advertises full sync.
func (c *client) changeRange(uri string, version int, rng lsp.Range, text string) lsp.PublishDiagnosticsParams {
	c.t.Helper()
	wait := c.expectPublish()
	require.NoError(c.t, c.conn.Notify(c.t.Context(), "textDocument/didChange", lsp.DidChangeTextDocumentParams{
		TextDocument:   lsp.VersionedTextDocumentIdentifier{TextDocumentIdentifier: lsp.TextDocumentIdentifier{URI: lsp.DocumentURI(uri)}, Version: version},
		ContentChanges: []lsp.TextDocumentContentChangeEvent{{Range: &rng, Text: text}},
	}))
	return c.await(wait)
}

// expectPublish registers interest in the next publishDiagnostics notification
// before the request that causes it is sent, so the notification cannot be missed.
func (c *client) expectPublish() chan struct{} {
	ch := make(chan struct{})
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waiters = append(c.waiters, ch)
	return ch
}

// await blocks until diagnostics arrive and returns the most recent set.
func (c *client) await(wait chan struct{}) lsp.PublishDiagnosticsParams {
	c.t.Helper()
	select {
	case <-wait:
	case <-time.After(10 * time.Second):
		c.t.Fatal("timed out waiting for diagnostics")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.published[len(c.published)-1]
}

// hover requests hover documentation at a position.
func (c *client) hover(uri string, line, char int) *lsp.Hover {
	c.t.Helper()
	var result *lsp.Hover
	require.NoError(c.t, c.conn.Call(c.t.Context(), "textDocument/hover", lsp.TextDocumentPositionParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: lsp.DocumentURI(uri)},
		Position:     lsp.Position{Line: line, Character: char},
	}, &result))
	return result
}

// complete requests completion candidates at a position.
func (c *client) complete(uri string, line, char int) lsp.CompletionList {
	c.t.Helper()
	var result lsp.CompletionList
	require.NoError(c.t, c.conn.Call(c.t.Context(), "textDocument/completion", lsp.CompletionParams{
		TextDocumentPositionParams: lsp.TextDocumentPositionParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: lsp.DocumentURI(uri)},
			Position:     lsp.Position{Line: line, Character: char},
		},
	}, &result))
	return result
}

// definition requests the definition of whatever is at a position.
func (c *client) definition(uri string, line, char int) []lsp.Location {
	c.t.Helper()
	var result []lsp.Location
	require.NoError(c.t, c.conn.Call(c.t.Context(), "textDocument/definition", lsp.TextDocumentPositionParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: lsp.DocumentURI(uri)},
		Position:     lsp.Position{Line: line, Character: char},
	}, &result))
	return result
}

// format requests a full-document formatting edit.
func (c *client) format(uri string) []lsp.TextEdit {
	c.t.Helper()
	var result []lsp.TextEdit
	require.NoError(c.t, c.conn.Call(c.t.Context(), "textDocument/formatting", lsp.DocumentFormattingParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: lsp.DocumentURI(uri)},
	}, &result))
	return result
}

// symbols requests the document outline.
func (c *client) symbols(uri string) []lsp.SymbolInformation {
	c.t.Helper()
	var result []lsp.SymbolInformation
	require.NoError(c.t, c.conn.Call(c.t.Context(), "textDocument/documentSymbol", lsp.DocumentSymbolParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: lsp.DocumentURI(uri)},
	}, &result))
	return result
}

// publishCount returns how many diagnostic notifications have arrived.
func (c *client) publishCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.published)
}

// labels returns a completion list's labels, for readable assertions.
func labels(items []lsp.CompletionItem) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.Label)
	}
	return out
}

// hoverText returns a hover's rendered content.
func hoverText(h *lsp.Hover) string {
	if h == nil {
		return ""
	}
	var b []byte
	for _, c := range h.Contents {
		b = append(b, c.Value...)
		b = append(b, '\n')
	}
	return string(b)
}

// messages returns the diagnostic messages, for readable assertions.
func messages(ds []lsp.Diagnostic) []string {
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		out = append(out, d.Message)
	}
	return out
}

// A rawPeer speaks the wire protocol directly, with no client-side jsonrpc2
// machinery in between.
//
// It exists to test the things a well-behaved client library hides: that a
// notification draws no response at all, and that a malformed request draws an
// error rather than silence. Both are protocol rules an editor depends on and
// neither is observable through a Conn, which quietly discards a response it did
// not ask for.
type rawPeer struct {
	t      *testing.T
	stream jsonrpc2.ObjectStream
}

func newRawPeer(t *testing.T) *rawPeer {
	t.Helper()
	return newRawPeerFor(t, &FlowfileServer{Logger: discardLogger()})
}

// newRawPeerFor connects a raw peer to a specific server.
func newRawPeerFor(t *testing.T, server *FlowfileServer) *rawPeer {
	t.Helper()

	serverSide, clientSide := net.Pipe()
	serverConn := jsonrpc2.NewConn(
		context.Background(),
		jsonrpc2.NewBufferedStream(serverSide, jsonrpc2.VSCodeObjectCodec{}),
		jsonrpc2.AsyncHandler(server),
	)

	p := &rawPeer{
		t:      t,
		stream: jsonrpc2.NewBufferedStream(clientSide, jsonrpc2.VSCodeObjectCodec{}),
	}
	t.Cleanup(func() {
		_ = p.stream.Close()
		_ = serverConn.Close()
	})
	return p
}

// send writes a raw JSON-RPC object.
func (p *rawPeer) send(obj map[string]any) {
	p.t.Helper()
	obj["jsonrpc"] = "2.0"
	require.NoError(p.t, p.stream.WriteObject(obj))
}

// receive reads the next object the server writes, failing the test if none
// arrives within the timeout.
func (p *rawPeer) receive() map[string]any {
	p.t.Helper()
	type result struct {
		obj map[string]any
		err error
	}
	done := make(chan result, 1)
	go func() {
		var obj map[string]any
		err := p.stream.ReadObject(&obj)
		done <- result{obj, err}
	}()
	select {
	case r := <-done:
		require.NoError(p.t, r.err)
		return r.obj
	case <-time.After(10 * time.Second):
		p.t.Fatal("timed out waiting for a message from the server")
		return nil
	}
}

// silentFor reports whether the server writes nothing for the given duration.
func (p *rawPeer) silentFor(d time.Duration) bool {
	p.t.Helper()
	done := make(chan error, 1)
	go func() {
		var obj map[string]any
		done <- p.stream.ReadObject(&obj)
	}()
	select {
	case err := <-done:
		if errors.Is(err, io.EOF) {
			return true
		}
		return false
	case <-time.After(d):
		return true
	}
}
