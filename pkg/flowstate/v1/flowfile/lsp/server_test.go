package lsp

import (
	"log/slog"
	"reflect"
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
	assert.True(t, got.DocumentFormattingProvider)
	require.NotNil(t, got.CompletionProvider)
	assert.Contains(t, got.CompletionProvider.TriggerCharacters, ".")
	assert.Contains(t, got.CompletionProvider.TriggerCharacters, ":")
	assert.Contains(t, got.CompletionProvider.TriggerCharacters, "{")

	// Everything not implemented must stay unadvertised.
	assert.False(t, got.ReferencesProvider)
	assert.False(t, got.RenameProvider)
	assert.False(t, got.CodeActionProvider)
	assert.False(t, got.WorkspaceSymbolProvider)
	assert.Nil(t, got.SignatureHelpProvider)
	assert.Nil(t, got.CodeLensProvider)
	assert.Nil(t, got.ExecuteCommandProvider)
}

// implementedCapabilities are the fields of lsp.ServerCapabilities this server is
// allowed to set, each paired with the request it answers.
//
// The list is of what *is* implemented, which is the direction that stays small: it
// changes when this server gains a feature, and a feature is exactly when somebody
// is already editing this file.
var implementedCapabilities = map[string]string{
	"TextDocumentSync":           "textDocument/didOpen",
	"HoverProvider":              "textDocument/hover",
	"CompletionProvider":         "textDocument/completion",
	"DefinitionProvider":         "textDocument/definition",
	"DocumentSymbolProvider":     "textDocument/documentSymbol",
	"DocumentFormattingProvider": "textDocument/formatting",
}

// TestNoCapabilityIsAdvertisedWithoutAHandler is the check the list above cannot be
// written by hand.
//
// The test beside it names eight providers that must stay off, which is a list of
// *negatives* — and a list of negatives over somebody else's struct is unbounded and
// silently incomplete. `lsp.ServerCapabilities` has more fields than that today, and
// gains more as the protocol does; a capability set on one nobody thought to name
// would be advertised and unchecked.
//
// So this asks the struct instead. Every field that is set has to be one this server
// implements, and the reverse: an entry here with nothing set means the list has
// outlived the capability. Neither direction can be satisfied by forgetting.
//
// It is the same shape as the catalog's function listing, and for the same reason —
// advertising something and not providing it reads to a user as broken rather than
// absent, and that is a worse answer than saying nothing.
func TestNoCapabilityIsAdvertisedWithoutAHandler(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	got := c.initialize()

	value := reflect.ValueOf(got)
	fields := value.Type()
	require.Positive(t, fields.NumField(), "ServerCapabilities has no fields; this checks nothing")

	set := map[string]bool{}
	for i := range fields.NumField() {
		if value.Field(i).IsZero() {
			continue
		}
		name := fields.Field(i).Name
		set[name] = true

		assert.Contains(t, implementedCapabilities, name,
			"the server advertises %q and nothing here says which request it answers; "+
				"an editor will route one and get an empty answer, which reads as broken "+
				"rather than absent", name)
	}

	for name, request := range implementedCapabilities {
		assert.Contains(t, set, name,
			"%q is listed as implemented (it answers %s) and is not advertised, so an editor "+
				"will never send one", name, request)
	}
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
	assert.Empty(t, c.format("file:///never-opened.yaml"))
}

// TestConcurrentRequests exercises the store and the analysis under the concurrency
// the connection's AsyncHandler actually produces. Run with -race, this is what
// proves a document snapshot is safe to read while a newer one is being built.
func TestConcurrentRequests(t *testing.T) {
	t.Parallel()

	const src = `name: concurrent
steps:
  - id: web
    http:
      url: https://example.com
  - id: out
    log:
      message: ${steps.web.body}
edition: v2026.2
`
	// The reads below all probe the `web` segment of `${steps.web.body}`, where
	// hover, completion and definition each have real work to do — a position with
	// nothing under it would have them all return early and race nothing.
	//
	// Both coordinates are found in the source rather than written down. They were
	// counted by hand and commented with the arithmetic, which is the form that
	// goes wrong silently: rooting moved the column six places, and a probe landing
	// on `steps` instead of `web` still resolves, so nothing here would have failed
	// while the test stopped exercising what it says it does.
	probeLine, probeChar := -1, -1
	for i, text := range strings.Split(src, "\n") {
		if at := strings.Index(text, "steps.web.body"); at >= 0 {
			probeLine, probeChar = i, at+len("steps.")+1
			break
		}
	}
	require.GreaterOrEqual(t, probeLine, 0, "the fixture no longer contains the reference this test probes")

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
				c.hover(uri, probeLine, probeChar)
			case 2:
				c.complete(uri, probeLine, probeChar)
			case 3:
				c.definition(uri, probeLine, probeChar)
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
    http:
      url: https://example.com
      headers:
        Authorization: Bearer 
edition: v2026.2
` + secret + `
      mesage: typo-to-force-a-diagnostic
`

	// Line 6 is `        Authorization: Bearer sk-...`, whose value begins at
	// character 23; character 28 is the last character of `Bearer`, immediately
	// before the credential. The misspelled `mesage:` forces a diagnostic, so the
	// document travels the reporting path as well as the request paths.
	const probeLine, probeChar = 6, 28

	log, logged := recordingLogger()
	s := &FlowfileServer{Logger: log}
	c := newClientFor(t, s)
	c.initialize()
	c.open("file:///secrets.yaml", src)
	c.hover("file:///secrets.yaml", probeLine, probeChar)
	c.complete("file:///secrets.yaml", probeLine, probeChar)

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
