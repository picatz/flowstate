package lsp

import (
	"context"
	"encoding/json"

	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
)

// NewHandler wraps s the way a connection should serve it: each message is
// handled in its own goroutine, as jsonrpc2.AsyncHandler would arrange, after
// the one piece of work that must happen in arrival order has happened.
//
// That work is announcing a document build. The read loop is the only place
// arrival order still exists — an async handler starts a goroutine per message
// and from then on the scheduler decides — so a build announced from inside the
// spawned didOpen goroutine can be announced after a request goroutine spawned
// behind it has already looked. [documentStore.await] papers over that with a
// grace period, which covers the ordinary case and not a stalled scheduler: a
// handler goroutine delayed past the grace leaves the request answering null
// for a document the editor did open. Announcing the build here, before
// dispatch, makes the guarantee ordering rather than timing: any request the
// client sent after a document notification finds that build registered, no
// matter how the goroutines behind them are scheduled.
//
// Wrapping s in jsonrpc2.AsyncHandler directly still works and still answers
// correctly; it just falls back to the grace period for the window this
// closes.
func NewHandler(s *FlowfileServer) jsonrpc2.Handler {
	return asyncHandler{server: s}
}

type asyncHandler struct {
	server *FlowfileServer
}

// Handle implements [jsonrpc2.Handler]. It runs on the connection's read loop,
// so everything before the `go` below happens in the order messages arrived.
func (h asyncHandler) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	release := h.server.announceInbound(req)
	go func() {
		defer release()
		h.server.Handle(ctx, conn, req)
	}()
}

// announceInbound records, before asynchronous dispatch, that req will build a
// document, and returns the function that retires the record after the message
// has been handled. For any message that builds nothing it records nothing and
// returns a no-op.
//
// The retire runs after [FlowfileServer.Handle] returns rather than inside the
// store's own open/change bookkeeping, because those also announce and retire
// around themselves — the counts nest, and this outer one is what holds the
// gate closed across the window between dispatch and the handler reaching the
// store.
//
// A malformed or empty params is not an error here: the handler will reject it
// with a proper protocol answer, and there is no document to wait for.
func (s *FlowfileServer) announceInbound(req *jsonrpc2.Request) func() {
	switch req.Method {
	case "textDocument/didOpen", "textDocument/didChange":
	default:
		return func() {}
	}
	if req.Params == nil {
		return func() {}
	}
	var params struct {
		TextDocument struct {
			URI lsp.DocumentURI `json:"uri"`
		} `json:"textDocument"`
	}
	if err := json.Unmarshal(*req.Params, &params); err != nil || params.TextDocument.URI == "" {
		return func() {}
	}
	uri := params.TextDocument.URI
	s.docs.beginBuild(uri)
	return func() { s.docs.endBuild(uri) }
}
