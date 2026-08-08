package plugin

import (
	"context"
	"encoding/json"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	lsp "github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	lspserver "github.com/picatz/flowstate/pkg/flowstate/v1/flowfile/lsp"
)

// The editor half of the same gap reachable_test.go closes.
//
// A worker that launches a plugin runs its tasks correctly while the author's
// editor underlines every one of them, because the process serving the editor
// launched nothing and cannot know what it did not launch. `flow lsp
// --plugin-dir` is the opt-in that closes it, and this is the join it rests on:
// a real plugin binary, discovered and launched, its reconstructed TaskDefs
// registered into a registry, and that registry handed to a language server that
// then answers about a task this build has never compiled.
//
// It is here rather than in the lsp package because the plugin is the expensive
// half — the example is built with the toolchain and launched as a process — and
// this package already knows how to do that. The lsp package asserts the same
// seam with a stand-in task, which is the cheap half and the one that runs on a
// machine with no compiler.
//
// Registration is into a registry made for this test rather than the default
// one, deliberately. Only one test in this binary may take the global one-way
// door, and reachable_test.go is it.

// lspPluginWorkflow names the example plugin's task the way an author would.
const lspPluginWorkflow = `name: plugin-aware
steps:
  - id: greet
    example.greet:
      name: world
      greeting: Hello
edition: v2026.2
`

func TestAPluginsTasksReachTheEditor(t *testing.T) {
	t.Parallel()

	host := exampleHost(t)

	// The built-ins alone, which is what a `flow lsp` started with no
	// --plugin-dir answers from. Filtered by [flowstatev1.IsBuiltinTask] rather
	// than copied from the default registry wholesale, because another test in
	// this binary registers a plugin's tasks into that one and this comparison
	// must not depend on which ran first.
	builtins := flowstatev1.NewRegistry()
	for _, def := range flowstatev1.DefaultRegistry().All() {
		if flowstatev1.IsBuiltinTask(def.Name) {
			require.NoError(t, builtins.Register(def))
		}
	}

	_, known := builtins.Lookup("example.greet")
	require.False(t, known,
		"the premise: a build's own task set has no plugin task in it, and this one does")

	withPlugin := flowstatev1.NewRegistry()
	for _, def := range builtins.All() {
		require.NoError(t, withPlugin.Register(def))
	}

	// The seam, one call, exactly as `startPlugins` makes it.
	require.NoError(t, host.Register(withPlugin, nil))

	t.Run("without the flag the task is not offered", func(t *testing.T) {
		offered := stepKeyCompletions(t, builtins)
		assert.NotContains(t, offered, "example.greet",
			"a server that launched no plugins offered a plugin's task, so the editor "+
				"is promising something the file's worker may not have")
	})

	t.Run("with the flag it is", func(t *testing.T) {
		offered := stepKeyCompletions(t, withPlugin)
		assert.Contains(t, offered, "example.greet",
			"the plugin launched, its tasks registered, and the language server still "+
				"does not offer one — which is the state this whole path exists to end")
	})

	t.Run("hover reads the plugin's own descriptors", func(t *testing.T) {
		// Not a name in a list: the summary comes from the manifest the plugin
		// served over its socket, so this is the descriptor round trip arriving
		// in an editor's hover box.
		def, ok := withPlugin.Lookup("example.greet")
		require.True(t, ok)
		require.NotEmpty(t, def.Summary, "the example plugin's task declares no summary to look for")

		c := newLSPClient(t, withPlugin)
		c.open("file:///plugin-hover.yaml", lspPluginWorkflow)

		// Line 3, the `example.greet:` key.
		hover := c.hover("file:///plugin-hover.yaml", 3, 6)
		require.NotNil(t, hover, "hovering a plugin's task produced nothing")

		var content string
		for _, part := range hover.Contents {
			content += part.Value
		}
		assert.Contains(t, content, def.Summary,
			"hover did not describe the task from the descriptors the plugin shipped")
	})
}

// stepKeyCompletions asks a server built over one registry what may be written
// where a step's work goes.
func stepKeyCompletions(t *testing.T, tasks *flowstatev1.Registry) []string {
	t.Helper()

	const uri = "file:///plugin-completion.yaml"

	c := newLSPClient(t, tasks)
	c.open(uri, "name: c\nsteps:\n  - \n")

	var labels []string
	for _, item := range c.complete(uri, 2, 4).Items {
		labels = append(labels, item.Label)
	}

	return slices.Clip(labels)
}

// An lspClient is the minimum of an editor: enough of the protocol to open a
// document and ask a question about it.
//
// Deliberately over a real connection rather than by calling the server's
// internals, which are unexported anyway — what an editor gets is what travels
// over the pipe.
type lspClient struct {
	t    *testing.T
	conn *jsonrpc2.Conn

	mu      sync.Mutex
	waiters []chan struct{}
}

// newLSPClient starts a language server over the given registry and connects to
// it.
func newLSPClient(t *testing.T, tasks *flowstatev1.Registry) *lspClient {
	t.Helper()

	serverSide, clientSide := net.Pipe()
	c := &lspClient{t: t}

	serverConn := jsonrpc2.NewConn(
		context.Background(),
		jsonrpc2.NewBufferedStream(serverSide, jsonrpc2.VSCodeObjectCodec{}),
		lspserver.NewHandler(&lspserver.FlowfileServer{Tasks: tasks}),
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

	var result json.RawMessage
	require.NoError(t, c.conn.Call(t.Context(), "initialize", lsp.InitializeParams{}, &result))
	require.NoError(t, c.conn.Notify(t.Context(), "initialized", struct{}{}))

	return c
}

// handle counts the server's notifications, waking anything waiting on the
// diagnostics an edit produces.
func (c *lspClient) handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (any, error) {
	if req.Method != "textDocument/publishDiagnostics" {
		return nil, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, w := range c.waiters {
		close(w)
	}
	c.waiters = nil

	return nil, nil
}

// open sends didOpen and waits for the diagnostics it triggers.
//
// The wait is the point: didOpen is a notification and the connection wraps the
// handler in AsyncHandler, so without it a completion request can be answered
// before the document exists.
func (c *lspClient) open(uri, text string) {
	c.t.Helper()

	wait := make(chan struct{})
	c.mu.Lock()
	c.waiters = append(c.waiters, wait)
	c.mu.Unlock()

	require.NoError(c.t, c.conn.Notify(c.t.Context(), "textDocument/didOpen", lsp.DidOpenTextDocumentParams{
		TextDocument: lsp.TextDocumentItem{
			URI:        lsp.DocumentURI(uri),
			LanguageID: "flowfile",
			Version:    1,
			Text:       text,
		},
	}))

	select {
	case <-wait:
	case <-time.After(10 * time.Second):
		c.t.Fatal("timed out waiting for the diagnostics an open publishes")
	}
}

// complete asks for the candidates at a position.
func (c *lspClient) complete(uri string, line, char int) lsp.CompletionList {
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

// hover asks for the documentation at a position.
func (c *lspClient) hover(uri string, line, char int) *lsp.Hover {
	c.t.Helper()

	var result *lsp.Hover
	require.NoError(c.t, c.conn.Call(c.t.Context(), "textDocument/hover", lsp.TextDocumentPositionParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: lsp.DocumentURI(uri)},
		Position:     lsp.Position{Line: line, Character: char},
	}, &result))

	return result
}
