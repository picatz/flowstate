package lsp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"

	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
)

// A FlowfileServer answers Language Server Protocol requests about Flowfiles.
//
// The zero value is ready to use and safe for concurrent use, which matters
// because the connection wraps this handler in jsonrpc2.AsyncHandler: an editor
// will have a hover in flight while a keystroke is being processed.
type FlowfileServer struct {
	// Logger receives protocol-level messages. When nil, [slog.Default] is used,
	// which the command routes to standard error — standard output carries the
	// JSON-RPC stream and must not be written to by anything else.
	//
	// Document contents are never logged. A Flowfile input can hold a bearer
	// token or a webhook URL, and a language server's log is not a place those
	// should end up. Positions, URIs, and counts are logged instead.
	Logger *slog.Logger

	docs documentStore

	// initialized and shuttingDown track the protocol lifecycle. The spec
	// requires rejecting requests before initialize and after shutdown, and an
	// editor that reuses a connection depends on that being enforced.
	initialized  atomic.Bool
	shuttingDown atomic.Bool
}

// Handle implements [jsonrpc2.Handler].
//
// It replies to requests, never to notifications — a notification carries no id,
// and replying to one is a protocol violation that some clients treat as a fatal
// error — and it converts a panic into a JSON-RPC error so that one malformed
// document cannot take the server down with it.
func (s *FlowfileServer) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		// The stack is logged rather than returned: it names internal symbols
		// that are no use to an editor, and the request must still be answered
		// so the client is not left waiting for a reply that never comes.
		s.logger().Error("recovered from panic handling request",
			"method", req.Method, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		if !req.Notif {
			_ = conn.ReplyWithError(ctx, req.ID, &jsonrpc2.Error{
				Code:    jsonrpc2.CodeInternalError,
				Message: fmt.Sprintf("internal error handling %s", req.Method),
			})
		}
	}()

	result, err := s.dispatch(ctx, conn, req)
	if req.Notif {
		if err != nil {
			s.logger().Warn("notification failed", "method", req.Method, "error", err)
		}
		return
	}
	if err != nil {
		_ = conn.ReplyWithError(ctx, req.ID, asRPCError(err))
		return
	}
	if err := conn.Reply(ctx, req.ID, result); err != nil {
		s.logger().Warn("reply failed", "method", req.Method, "error", err)
	}
}

// codeServerNotInitialized is the LSP-specific JSON-RPC error code for a request
// that arrives before initialize. It is not in the JSON-RPC spec, so jsonrpc2 does
// not define it.
const codeServerNotInitialized int64 = -32002

// dispatch routes one message and returns the result to reply with, if any.
func (s *FlowfileServer) dispatch(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (any, error) {
	// The lifecycle is enforced rather than assumed. An editor that reuses a
	// connection — or a misconfigured client that starts asking for completions
	// before handshaking — should get a clear protocol error instead of results
	// computed from a state the server was never told about.
	switch req.Method {
	case "initialize", "exit":
	default:
		switch {
		case !s.initialized.Load():
			if req.Notif {
				return nil, nil
			}
			return nil, &jsonrpc2.Error{
				Code:    codeServerNotInitialized,
				Message: "server has not been initialized",
			}
		case s.shuttingDown.Load() && req.Method != "shutdown":
			if req.Notif {
				return nil, nil
			}
			return nil, &jsonrpc2.Error{
				Code:    jsonrpc2.CodeInvalidRequest,
				Message: "server is shutting down",
			}
		}
	}

	switch req.Method {
	case "initialize":
		s.initialized.Store(true)
		return &lsp.InitializeResult{Capabilities: capabilities()}, nil

	case "initialized":
		return nil, nil

	case "shutdown":
		// The spec is explicit: shutdown stops work but does not exit, and the
		// reply must be null.
		s.shuttingDown.Store(true)
		return nil, nil

	case "exit":
		if err := conn.Close(); err != nil && !errors.Is(err, jsonrpc2.ErrClosed) {
			s.logger().Warn("closing connection on exit", "error", err)
		}
		return nil, nil

	case "textDocument/didOpen":
		var params lsp.DidOpenTextDocumentParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc := s.docs.open(params.TextDocument.URI, params.TextDocument.Version, params.TextDocument.Text)
		s.publish(ctx, conn, doc)
		return nil, nil

	case "textDocument/didChange":
		var params lsp.DidChangeTextDocumentParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc := s.docs.change(params.TextDocument.URI, params.TextDocument.Version, params.ContentChanges)
		if doc == nil {
			// A stale edit, already superseded. Publishing diagnostics computed
			// from it would replace the newer document's report with an older one.
			s.logger().Debug("ignored stale change",
				"uri", params.TextDocument.URI, "version", params.TextDocument.Version)
			return nil, nil
		}
		s.publish(ctx, conn, doc)
		return nil, nil

	case "textDocument/didSave":
		// A save carries the text only when the client honors the advertised
		// includeText; either way the open document is the authority, so a save
		// simply re-publishes.
		var params lsp.DidSaveTextDocumentParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		if doc, ok := s.docs.get(params.TextDocument.URI); ok {
			s.publish(ctx, conn, doc)
		}
		return nil, nil

	case "textDocument/didClose":
		var params lsp.DidCloseTextDocumentParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		s.docs.close(params.TextDocument.URI)
		// Diagnostics belong to the editor's problem list until the server
		// clears them, and a closed document's problems are no longer actionable.
		s.notify(ctx, conn, lsp.PublishDiagnosticsParams{
			URI:         params.TextDocument.URI,
			Diagnostics: []lsp.Diagnostic{},
		})
		return nil, nil

	case "textDocument/hover":
		var params lsp.TextDocumentPositionParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.docs.get(params.TextDocument.URI)
		if !ok {
			return nil, nil
		}
		return hoverAt(doc, params.Position), nil

	case "textDocument/completion":
		var params lsp.CompletionParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.docs.get(params.TextDocument.URI)
		if !ok {
			return &lsp.CompletionList{Items: []lsp.CompletionItem{}}, nil
		}
		return completeAt(doc, params.Position), nil

	case "textDocument/definition":
		var params lsp.TextDocumentPositionParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.docs.get(params.TextDocument.URI)
		if !ok {
			return []lsp.Location{}, nil
		}
		locations := definitionAt(doc, params.Position)
		if locations == nil {
			locations = []lsp.Location{}
		}
		return locations, nil

	case "textDocument/documentSymbol":
		var params lsp.DocumentSymbolParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.docs.get(params.TextDocument.URI)
		if !ok {
			return []lsp.SymbolInformation{}, nil
		}
		return documentSymbols(doc), nil

	case "textDocument/formatting":
		var params lsp.DocumentFormattingParams
		if err := decode(req, &params); err != nil {
			return nil, err
		}
		doc, ok := s.docs.get(params.TextDocument.URI)
		if !ok {
			return []lsp.TextEdit{}, nil
		}
		edits := formatEdits(doc)
		if edits == nil {
			// A document that does not compile draws no edits — never a partial
			// or guessed one — so this is the empty list rather than nil: the
			// same "nothing to do" an already-formatted document returns, which
			// is the honest answer either way.
			edits = []lsp.TextEdit{}
		}
		return edits, nil

	case "$/cancelRequest", "$/setTrace", "$/logTrace", "workspace/didChangeConfiguration",
		"workspace/didChangeWatchedFiles":
		// Accepted and ignored. Cancellation is not honored because every
		// request here is bounded by the document size and completes in well
		// under the time an editor would wait.
		return nil, nil
	}

	if req.Notif {
		// An unknown notification is not an error: the protocol grows, and a
		// server that complains about a message it does not need is noise.
		s.logger().Debug("ignoring unknown notification", "method", req.Method)
		return nil, nil
	}
	return nil, &jsonrpc2.Error{
		Code:    jsonrpc2.CodeMethodNotFound,
		Message: fmt.Sprintf("method not supported: %s", req.Method),
	}
}

// capabilities describes exactly what this server implements.
//
// Advertising anything more makes an editor route requests here that get an empty
// answer, which reads to a user as the feature being broken rather than absent.
func capabilities() lsp.ServerCapabilities {
	return lsp.ServerCapabilities{
		TextDocumentSync: &lsp.TextDocumentSyncOptionsOrKind{
			// Only Options is set: the marshaler prefers Kind when both are
			// present, and the kind form cannot express that the server wants
			// save notifications.
			Options: &lsp.TextDocumentSyncOptions{
				OpenClose: true,
				Change:    lsp.TDSKFull,
				Save:      &lsp.SaveOptions{IncludeText: true},
			},
		},
		HoverProvider: true,
		CompletionProvider: &lsp.CompletionOptions{
			// Enough to open completion at each place a Flowfile has something
			// to offer: after a key's colon, inside ${...}, after a step id's
			// dot, and within a libs list.
			TriggerCharacters: []string{":", " ", ".", "{", "[", ",", "-"},
		},
		DefinitionProvider:         true,
		DocumentSymbolProvider:     true,
		DocumentFormattingProvider: true,
	}
}

// publish sends the diagnostics for a document, including when there are none.
//
// An empty array is meaningful: it is how a language server retracts problems it
// reported earlier. Skipping the notification when a document becomes clean leaves
// the editor showing errors the author has already fixed.
func (s *FlowfileServer) publish(ctx context.Context, conn *jsonrpc2.Conn, doc *document) {
	diagnostics := diagnose(doc)
	s.logger().Debug("published diagnostics",
		"uri", doc.uri, "version", doc.version, "count", len(diagnostics))
	s.notify(ctx, conn, lsp.PublishDiagnosticsParams{
		URI:         doc.uri,
		Diagnostics: diagnostics,
	})
}

// notify sends a notification, logging a failure rather than propagating it: there
// is no caller to return an error to.
func (s *FlowfileServer) notify(ctx context.Context, conn *jsonrpc2.Conn, params lsp.PublishDiagnosticsParams) {
	if err := conn.Notify(ctx, "textDocument/publishDiagnostics", params); err != nil {
		s.logger().Warn("publishing diagnostics failed", "uri", params.URI, "error", err)
	}
}

// logger returns the configured logger, or the default one.
func (s *FlowfileServer) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// decode unmarshals a request's parameters.
//
// Absent parameters are not an error: several notifications carry none, and a
// client that omits an empty object is within the spec.
func decode(req *jsonrpc2.Request, into any) error {
	if req.Params == nil {
		return nil
	}
	if err := json.Unmarshal(*req.Params, into); err != nil {
		return &jsonrpc2.Error{
			Code:    jsonrpc2.CodeInvalidParams,
			Message: fmt.Sprintf("invalid params for %s: %v", req.Method, err),
		}
	}
	return nil
}

// asRPCError converts an error into the JSON-RPC form, preserving a code that was
// chosen deliberately.
func asRPCError(err error) *jsonrpc2.Error {
	var rpcErr *jsonrpc2.Error
	if errors.As(err, &rpcErr) {
		return rpcErr
	}
	return &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: err.Error()}
}
