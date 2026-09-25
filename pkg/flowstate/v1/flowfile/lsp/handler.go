package lsp

import (
	"context"
	"encoding/json"

	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
)

// NewHandler wraps s the way a connection should serve it: each message is
// handled in its own goroutine, as jsonrpc2.AsyncHandler would arrange, after
// the work that must happen in arrival order has happened.
//
// That work is announcing a document build, and — for didClose, which has no
// build of its own — claiming the arrival-order ticket [documentStore.close]
// needs to tell a superseded close from a current one. The read loop is the
// only place arrival order still exists — an async handler starts a goroutine
// per message and from then on the scheduler decides — so a build announced
// from inside the spawned didOpen goroutine can be registered after a request
// goroutine spawned behind it has already looked. [documentStore.await] papers
// over that with a grace period, which covers the ordinary case and not a
// stalled scheduler: a handler goroutine delayed past the grace leaves the
// request answering null for a document the editor did open. Announcing the
// build here, before dispatch, makes the guarantee ordering rather than
// timing: any request the client sent after a document notification finds
// that build registered, no matter how the goroutines behind them are
// scheduled. The same is true of a didClose's ticket: assigned here, it
// reflects wire order even though the close it belongs to may run late.
//
// Wrapping s in jsonrpc2.AsyncHandler directly still works and still answers
// correctly for open, change and hover; it just falls back to the grace period
// for the window this closes, and a close loses its ordering entirely — see
// [documentStore.close].
func NewHandler(s *FlowfileServer) jsonrpc2.Handler {
	return asyncHandler{server: s}
}

type asyncHandler struct {
	server *FlowfileServer
}

// Handle implements [jsonrpc2.Handler]. It runs on the connection's read loop,
// so everything before the `go` below happens in the order messages arrived.
func (h asyncHandler) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	ctx, release := h.server.announceInbound(ctx, req)
	go func() {
		defer release()
		h.server.Handle(ctx, conn, req)
	}()
}

// closeTicketKey is the context key [FlowfileServer.announceInbound] uses to
// carry a didClose's arrival-order ticket to the goroutine that runs its
// handler. A context is what makes the trip: the ticket is only knowable on
// the read loop, before jsonrpc2.AsyncHandler's `go` hands the message to a
// goroutine the scheduler is free to run whenever it likes.
type closeTicketKey struct{}

// closeTicket reads the ticket [FlowfileServer.announceInbound] stored for a
// didClose, or zero when ctx carries none — a request reached some other way
// than through [NewHandler], for which [documentStore.close] always applies.
func closeTicket(ctx context.Context) int64 {
	ticket, _ := ctx.Value(closeTicketKey{}).(int64)
	return ticket
}

// announceInbound records, before asynchronous dispatch, the arrival-order
// work a document notification needs done while the read loop still has it:
// a didOpen or didChange registers that it will build a document, and every
// one of the three — didOpen, didChange and didClose — claims the next
// per-URI ticket [documentStore.nextTicket] hands out, which travels onward in
// the returned context for didClose to read back with [closeTicket]. It
// returns the function that retires the build record after the message has
// been handled. For any message that builds and claims nothing it returns ctx
// unchanged and a no-op.
//
// The retire runs after [FlowfileServer.Handle] returns rather than inside the
// store's own open/change bookkeeping, because those also announce and retire
// around themselves — the counts nest, and this outer one is what holds the
// gate closed across the window between dispatch and the handler reaching the
// store.
//
// A malformed or empty params is not an error here: the handler will reject it
// with a proper protocol answer, and there is no document to wait for or
// ticket to claim.
func (s *FlowfileServer) announceInbound(ctx context.Context, req *jsonrpc2.Request) (context.Context, func()) {
	switch req.Method {
	case "textDocument/didOpen", "textDocument/didChange", "textDocument/didClose":
	default:
		return ctx, func() {}
	}
	if req.Params == nil {
		return ctx, func() {}
	}
	var params struct {
		TextDocument struct {
			URI lsp.DocumentURI `json:"uri"`
		} `json:"textDocument"`
	}
	if err := json.Unmarshal(*req.Params, &params); err != nil || params.TextDocument.URI == "" {
		return ctx, func() {}
	}
	uri := params.TextDocument.URI
	ticket := s.docs.nextTicket(uri)

	if req.Method == "textDocument/didClose" {
		// A close builds nothing [documentStore.await] would wait for, so it
		// does not join beginBuild/endBuild; its ticket rides in ctx instead,
		// for [documentStore.close] to compare against the counter's value
		// once its own goroutine gets to run.
		return context.WithValue(ctx, closeTicketKey{}, ticket), func() {}
	}

	s.docs.beginBuild(uri)
	return ctx, func() { s.docs.endBuild(uri) }
}
