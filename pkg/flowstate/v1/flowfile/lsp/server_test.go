package lsp

import (
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInitializeAdvertisesOnlyWhatIsImplemented pins the capability set. A server
// that claims a capability it does not answer makes the feature look broken rather
// than absent, which is worse than not claiming it.
func TestInitializeAdvertisesOnlyWhatIsImplemented(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	got := c.initialize()

	require.NotNil(t, got.TextDocumentSync)
	require.NotNil(t, got.TextDocumentSync.Options,
		"the options form is required to request save notifications")
	assert.True(t, got.TextDocumentSync.Options.OpenClose)
	assert.Equal(t, lsp.TDSKFull, got.TextDocumentSync.Options.Change)
	require.NotNil(t, got.TextDocumentSync.Options.Save)
	assert.True(t, got.TextDocumentSync.Options.Save.IncludeText)

	assert.True(t, got.HoverProvider)
	assert.True(t, got.DefinitionProvider)
	assert.True(t, got.DocumentSymbolProvider)
	require.NotNil(t, got.CompletionProvider)
	assert.Contains(t, got.CompletionProvider.TriggerCharacters, ".")
	assert.Contains(t, got.CompletionProvider.TriggerCharacters, ":")
	assert.Contains(t, got.CompletionProvider.TriggerCharacters, "{")

	// Everything not implemented must stay unadvertised.
	assert.False(t, got.ReferencesProvider)
	assert.False(t, got.RenameProvider)
	assert.False(t, got.CodeActionProvider)
	assert.False(t, got.DocumentFormattingProvider)
	assert.False(t, got.WorkspaceSymbolProvider)
	assert.Nil(t, got.SignatureHelpProvider)
	assert.Nil(t, got.CodeLensProvider)
	assert.Nil(t, got.ExecuteCommandProvider)
}

// TestNotificationsGetNoReply checks the rule the previous server broke: a
// notification carries no id, and replying to one is a protocol violation.
//
// This is asserted at the wire level, because a client library discards a response
// it did not ask for and so cannot see the mistake.
func TestNotificationsGetNoReply(t *testing.T) {
	t.Parallel()

	p := newRawPeer(t)

	p.send(map[string]any{"id": 1, "method": "initialize", "params": map[string]any{}})
	initResponse := p.receive()
	require.Equal(t, float64(1), initResponse["id"], "a request must be answered")
	require.Contains(t, initResponse, "result")

	p.send(map[string]any{"method": "initialized", "params": map[string]any{}})
	p.send(map[string]any{
		"method": "textDocument/didOpen",
		"params": map[string]any{
			"textDocument": map[string]any{
				"uri":        "file:///notif.yaml",
				"languageId": "flowfile",
				"version":    1,
				"text":       "name: x\nsteps: []\n",
			},
		},
	})

	// The only thing the server may write in response to those two notifications
	// is the diagnostics notification, which itself carries no id.
	published := p.receive()
	assert.Equal(t, "textDocument/publishDiagnostics", published["method"])
	assert.NotContains(t, published, "id")
	assert.NotContains(t, published, "result")

	assert.True(t, p.silentFor(250*time.Millisecond),
		"the server replied to a notification")
}

// TestMalformedParamsGetAnError checks that garbage in a request's parameters draws
// an error response rather than a panic or silence.
func TestMalformedParamsGetAnError(t *testing.T) {
	t.Parallel()

	p := newRawPeer(t)
	p.send(map[string]any{"id": 1, "method": "initialize", "params": map[string]any{}})
	p.receive()

	p.send(map[string]any{
		"id":     2,
		"method": "textDocument/hover",
		// position must be an object, not a string.
		"params": map[string]any{"textDocument": map[string]any{"uri": "file:///x"}, "position": "nope"},
	})

	resp := p.receive()
	require.Equal(t, float64(2), resp["id"])
	rpcErr, ok := resp["error"].(map[string]any)
	require.True(t, ok, "expected an error response, got %v", resp)
	assert.Equal(t, float64(jsonrpc2.CodeInvalidParams), rpcErr["code"])
	assert.Contains(t, rpcErr["message"], "invalid params")
}

// TestUnknownMethodIsRejected checks that an unsupported request gets
// MethodNotFound, and that an unknown notification is ignored instead.
func TestUnknownMethodIsRejected(t *testing.T) {
	t.Parallel()

	p := newRawPeer(t)
	p.send(map[string]any{"id": 1, "method": "initialize", "params": map[string]any{}})
	p.receive()

	p.send(map[string]any{"id": 2, "method": "textDocument/rename", "params": map[string]any{}})
	resp := p.receive()
	rpcErr, ok := resp["error"].(map[string]any)
	require.True(t, ok, "expected an error response, got %v", resp)
	assert.Equal(t, float64(jsonrpc2.CodeMethodNotFound), rpcErr["code"])

	p.send(map[string]any{"method": "telemetry/somethingNew", "params": map[string]any{}})
	assert.True(t, p.silentFor(250*time.Millisecond),
		"an unknown notification must be ignored, not answered")
}

// TestLifecycleIsEnforced checks the ordering rules: nothing before initialize, and
// nothing but exit after shutdown.
func TestLifecycleIsEnforced(t *testing.T) {
	t.Parallel()

	t.Run("request before initialize", func(t *testing.T) {
		p := newRawPeer(t)
		p.send(map[string]any{"id": 1, "method": "textDocument/documentSymbol", "params": map[string]any{
			"textDocument": map[string]any{"uri": "file:///x"},
		}})
		resp := p.receive()
		rpcErr, ok := resp["error"].(map[string]any)
		require.True(t, ok, "expected an error response, got %v", resp)
		assert.Equal(t, float64(codeServerNotInitialized), rpcErr["code"])
	})

	t.Run("request after shutdown", func(t *testing.T) {
		p := newRawPeer(t)
		p.send(map[string]any{"id": 1, "method": "initialize", "params": map[string]any{}})
		p.receive()

		p.send(map[string]any{"id": 2, "method": "shutdown"})
		shutdownResponse := p.receive()
		require.Equal(t, float64(2), shutdownResponse["id"])
		assert.Nil(t, shutdownResponse["result"], "shutdown must reply with null")

		p.send(map[string]any{"id": 3, "method": "textDocument/documentSymbol", "params": map[string]any{
			"textDocument": map[string]any{"uri": "file:///x"},
		}})
		resp := p.receive()
		rpcErr, ok := resp["error"].(map[string]any)
		require.True(t, ok, "expected an error response, got %v", resp)
		assert.Equal(t, float64(jsonrpc2.CodeInvalidRequest), rpcErr["code"])
	})
}

// TestRequestsForUnknownDocumentAreEmpty checks that asking about a document the
// server was never told about yields an empty answer rather than an error, since an
// editor can legitimately race a close against a request.
func TestRequestsForUnknownDocumentAreEmpty(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	assert.Nil(t, c.hover("file:///never-opened.yaml", 0, 0))
	assert.Empty(t, c.complete("file:///never-opened.yaml", 0, 0).Items)
	assert.Empty(t, c.definition("file:///never-opened.yaml", 0, 0))
	assert.Empty(t, c.symbols("file:///never-opened.yaml"))
}

// TestConcurrentRequests exercises the store and the analysis under the concurrency
// the connection's AsyncHandler actually produces. Run with -race, this is what
// proves a document snapshot is safe to read while a newer one is being built.
func TestConcurrentRequests(t *testing.T) {
	t.Parallel()

	const src = `name: concurrent
steps:
  - id: web
    task:
      name: http
      inputs:
        url: https://example.com
  - id: out
    task:
      name: echo
      inputs:
        message: ${web.body}
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///concurrent.yaml"
	c.open(uri, src)

	var wg sync.WaitGroup
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch i % 5 {
			case 0:
				// Edits race against the reads below, which is the point.
				_ = c.conn.Notify(t.Context(), "textDocument/didChange", lsp.DidChangeTextDocumentParams{
					TextDocument: lsp.VersionedTextDocumentIdentifier{
						TextDocumentIdentifier: lsp.TextDocumentIdentifier{URI: uri},
						Version:                i + 2,
					},
					ContentChanges: []lsp.TextDocumentContentChangeEvent{{Text: src}},
				})
			case 1:
				c.hover(uri, 11, 20)
			case 2:
				c.complete(uri, 11, 20)
			case 3:
				c.definition(uri, 11, 20)
			case 4:
				c.symbols(uri)
			}
		}()
	}
	wg.Wait()
}

// TestPanicBecomesAnError proves the recovery path answers the client instead of
// leaving it waiting, using a handler forced to panic.
func TestPanicBecomesAnError(t *testing.T) {
	t.Parallel()

	// A document store entry whose index is nil makes any position conversion
	// dereference nil. It is not reachable through the protocol — which is the
	// point of the recover — so it is constructed directly.
	s := &FlowfileServer{Logger: discardLogger()}
	s.docs.docs = map[lsp.DocumentURI]*document{
		"file:///poison.yaml": {uri: "file:///poison.yaml"},
	}

	p := newRawPeerFor(t, s)
	p.send(map[string]any{"id": 1, "method": "initialize", "params": map[string]any{}})
	p.receive()

	p.send(map[string]any{"id": 2, "method": "textDocument/completion", "params": map[string]any{
		"textDocument": map[string]any{"uri": "file:///poison.yaml"},
		"position":     map[string]any{"line": 0, "character": 0},
	}})

	resp := p.receive()
	require.Equal(t, float64(2), resp["id"])
	rpcErr, ok := resp["error"].(map[string]any)
	require.True(t, ok, "expected an error response, got %v", resp)
	assert.Equal(t, float64(jsonrpc2.CodeInternalError), rpcErr["code"])
	assert.Contains(t, rpcErr["message"], "internal error handling")

	// The connection must still work afterwards: one bad document cannot take
	// down every other file the editor has open.
	p.send(map[string]any{"id": 3, "method": "textDocument/documentSymbol", "params": map[string]any{
		"textDocument": map[string]any{"uri": "file:///other.yaml"},
	}})
	after := p.receive()
	assert.Equal(t, float64(3), after["id"])
	assert.NotContains(t, after, "error")
}

// TestExitClosesTheConnection checks that exit terminates the session, which is how
// the process knows to stop.
func TestExitClosesTheConnection(t *testing.T) {
	t.Parallel()

	p := newRawPeer(t)
	p.send(map[string]any{"id": 1, "method": "initialize", "params": map[string]any{}})
	p.receive()
	p.send(map[string]any{"id": 2, "method": "shutdown"})
	p.receive()
	p.send(map[string]any{"method": "exit"})

	// Nothing more arrives; the stream ends.
	assert.True(t, p.silentFor(500*time.Millisecond))
}

// TestDocumentContentsAreNeverLogged guards the rule that a Flowfile input may hold
// a credential, so nothing from a document may reach the log.
func TestDocumentContentsAreNeverLogged(t *testing.T) {
	t.Parallel()

	const secret = "sk-do-not-log-this-value"
	src := `name: secrets
steps:
  - id: a
    task:
      name: http
      inputs:
        url: https://example.com
        headers:
          Authorization: Bearer ` + secret + `
        mesage: typo-to-force-a-diagnostic
`

	log, logged := recordingLogger()
	s := &FlowfileServer{Logger: log}
	c := newClientFor(t, s)
	c.initialize()
	c.open("file:///secrets.yaml", src)
	c.hover("file:///secrets.yaml", 8, 30)
	c.complete("file:///secrets.yaml", 8, 30)

	assert.NotContains(t, logged(), secret)
	assert.NotContains(t, logged(), "Authorization")
}

// recordingLogger returns a logger that captures everything written to it at every
// level, so the test sees debug output too.
func recordingLogger() (*slog.Logger, func() string) {
	sink := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(sink, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return logger, sink.String
}

// syncBuffer is a mutex-guarded sink, since the server logs from several goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
