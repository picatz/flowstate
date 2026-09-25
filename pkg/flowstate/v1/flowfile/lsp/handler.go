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
// That work is announcing a document build, and — for every one of didOpen,
// didChange and didClose — queuing the message behind whatever a same-URI
// document notification before it has not yet finished handling. The read
// loop is the only place arrival order still exists — an async handler starts
// a goroutine per message and from then on the scheduler decides — so a build
// announced from inside the spawned didOpen goroutine can be registered after
// a request goroutine spawned behind it has already looked, and two
// same-URI notifications can run in the opposite order from the one they
// arrived in. [documentStore.await] papers over the first with a grace
// period, which covers the ordinary case and not a stalled scheduler: a
// handler goroutine delayed past the grace leaves the request answering null
// for a document the editor did open. Announcing the build here, before
// dispatch, makes that guarantee ordering rather than timing: any request the
// client sent after a document notification finds that build registered, no
// matter how the goroutines behind them are scheduled. The queue answers the
// second the same way: assigned here, in arrival order, it is what a
// same-URI didClose reordered behind the didOpen that reopened it cannot get
// past (#1986) — open, change and close each still decide for themselves
// whether the result is worth applying, the queue only decides what order
// they get to ask in.
//
// Wrapping s in jsonrpc2.AsyncHandler directly still works and still answers
// correctly for open, change and hover; it just falls back to the grace
// period for the window this closes, and the three document notifications
// lose the ordering this file gives them — see [documentStore.enqueue].
func NewHandler(s *FlowfileServer) jsonrpc2.Handler {
	return asyncHandler{server: s}
}

type asyncHandler struct {
	server *FlowfileServer
}

// Handle implements [jsonrpc2.Handler]. It runs on the connection's read loop,
// so everything before the `go` below happens in the order messages arrived.
func (h asyncHandler) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	wait, release := h.server.announceInbound(req)
	go func() {
		defer release()
		if wait != nil {
			// Blocks this goroutine, not the read loop: the wait was claimed
			// above, synchronously, before this goroutine even existed, so
			// which one of two same-URI notifications reaches this point
			// first no longer decides which one's handler runs first.
			<-wait
		}
		h.server.Handle(ctx, conn, req)
	}()
}

// announceInbound records, before asynchronous dispatch, the arrival-order
// work a document notification needs done while the read loop still has it:
// a didOpen or didChange registers that it will build a document (see
// [documentStore.beginBuild]), and every one of the three — didOpen,
// didChange and didClose — claims its place in [documentStore.enqueue]'s
// per-URI queue, so the goroutine about to run its handler waits its turn
// there before [FlowfileServer.Handle] is even called. It returns that wait
// channel and the function that retires this notification's queue entry and
// build record once the message has been handled. For any message that
// queues and builds nothing it returns a nil wait and a no-op release.
//
// The retire runs after [FlowfileServer.Handle] returns rather than inside
// the store's own open/change/close bookkeeping, because those also announce
// and retire around themselves — the counts and the queue both nest, and this
// outer one is what holds the gate closed across the window between dispatch
// and the handler reaching the store, panic or not: [asyncHandler.Handle]
// defers it, so a handler that panics still releases whatever is queued
// behind it rather than stalling that URI forever.
//
// A malformed or empty params is not an error here: the handler will reject
// it with a proper protocol answer, and there is no document to wait for or
// queue entry to claim.
func (s *FlowfileServer) announceInbound(req *jsonrpc2.Request) (wait <-chan struct{}, release func()) {
	switch req.Method {
	case "textDocument/didOpen", "textDocument/didChange", "textDocument/didClose":
	default:
		return nil, func() {}
	}
	if req.Params == nil {
		return nil, func() {}
	}
	var params struct {
		TextDocument struct {
			URI lsp.DocumentURI `json:"uri"`
		} `json:"textDocument"`
	}
	if err := json.Unmarshal(*req.Params, &params); err != nil || params.TextDocument.URI == "" {
		return nil, func() {}
	}
	uri := params.TextDocument.URI

	wait, done := s.docs.enqueue(uri)

	if req.Method == "textDocument/didClose" {
		// A close builds nothing [documentStore.await] would wait for, so it
		// does not join beginBuild/endBuild; it still joins the queue above,
		// which is the whole of what it needs.
		return wait, done
	}

	s.docs.beginBuild(uri)
	return wait, func() {
		s.docs.endBuild(uri)
		done()
	}
}
