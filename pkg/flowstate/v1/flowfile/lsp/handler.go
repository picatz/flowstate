package lsp

import (
	"context"
	"encoding/json"

	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
)

// maxInFlightPerConnection bounds how many notifications and requests one
// connection may have dispatched to their own goroutine and not yet
// finished, at once, across every URI and method.
//
// This is the general shape #2071 asked for: work a client can queue is
// bounded where it is spent (CLAUDE.md invariant 5). An earlier attempt
// bounded only a same-URI full-sync didChange burst by coalescing it, and a
// second round of review found the coalescing mechanism itself introduced
// ordering defects twice over — see the history on #2071 and #2089. This
// bound instead covers every shape at the read loop's own granularity: a
// burst of incremental changes, a burst spread across many URIs, a pile of
// slow requests, and anything else all draw from the same budget.
//
// 64 matches this repository's other per-connection concurrency bound, the
// webhook receiver's DefaultWebhookConcurrency: an ordinary editor never has
// this many keystrokes, opens, or requests genuinely in flight at once, and
// a connection that does is either a pathological client or one already
// backed up on something slower than this server — either way, blocking its
// read loop rather than growing this connection's outstanding goroutines
// without bound is the correct answer.
const maxInFlightPerConnection = 64

// NewHandler wraps s the way a connection should serve it: each message is
// handled in its own goroutine, as jsonrpc2.AsyncHandler would arrange, after
// the work that must happen in arrival order has happened, and behind a
// connection-wide bound on how many may be in flight at once
// ([maxInFlightPerConnection]).
//
// The arrival-order work is announcing a document build, and — for every one
// of didOpen, didChange and didClose — queuing the message behind whatever a
// same-URI document notification before it has not yet finished handling.
// The read loop is the only place arrival order still exists — an async
// handler starts a goroutine per message and from then on the scheduler
// decides — so a build announced from inside the spawned didOpen goroutine
// can be registered after a request goroutine spawned behind it has already
// looked, and two same-URI notifications can run in the opposite order from
// the one they arrived in. [documentStore.await] papers over the first with a
// grace period, which covers the ordinary case and not a stalled scheduler: a
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
// lose the ordering this file gives them — see [documentStore.enqueue]. It
// also loses the in-flight bound below.
func NewHandler(s *FlowfileServer) jsonrpc2.Handler {
	return newHandlerWithLimit(s, maxInFlightPerConnection, nil)
}

// newHandlerWithLimit is [NewHandler] with the connection-wide limit and the
// trace hook both exposed, for a test that needs a limit small enough to
// fill deterministically and a way to count what is in flight without
// depending on wall-clock timing to catch a peak. NewHandler always passes
// [maxInFlightPerConnection] and a nil trace.
func newHandlerWithLimit(s *FlowfileServer, limit int, trace func(bounded, entering bool)) asyncHandler {
	return asyncHandler{server: s, inFlight: make(chan struct{}, limit), dispatchTrace: trace}
}

type asyncHandler struct {
	server *FlowfileServer

	// inFlight is the connection's concurrency bound, as a buffered channel:
	// a token per message dispatched to its own goroutine and not yet
	// finished. Unlike a shed-not-queue limiter (this repository's webhook
	// receiver, for one), the acquire below blocks rather than refuses: an
	// LSP connection has no retry semantics to shed onto, so a full window
	// makes the read loop itself apply backpressure, which is what leaves a
	// slow or malicious client's own transport buffer holding the backlog
	// instead of this process's heap.
	inFlight chan struct{}

	// dispatchTrace, when set, is called by the goroutine [asyncHandler.Handle]
	// spawns for every message, bounded or exempt: once with entering = true
	// immediately before it calls [FlowfileServer.Handle], and once with
	// entering = false immediately after that call returns. bounded reports
	// whether the message held one of [asyncHandler.inFlight]'s tokens. It
	// exists for a test that needs to count, deterministically, how many
	// bounded goroutines are between those two points at once — the peak
	// [maxInFlightPerConnection] bounds — and to confirm an exempt message's
	// goroutine ran at all, rather than inferring either from wall-clock
	// timing. Nil in production, where the call costs nothing.
	dispatchTrace func(bounded, entering bool)
}

// bypassesInFlightLimit reports whether method should be dispatched without
// waiting for, or holding, one of the connection's [asyncHandler.inFlight]
// tokens.
//
// $/cancelRequest, shutdown and exit are the connection's own escape
// hatches — cancelling a request or stopping the server — and must still be
// answered promptly when the window is full of everything else, or a client
// trying to get out of a stalled connection would have to wait behind
// exactly the backlog it is trying to escape. Nothing else is exempt: every
// other method's own cost is what the bound exists to cover.
//
// This exemption's guarantee is about the message itself, not about
// anything already ahead of it: the read loop dispatches strictly in wire
// order, so a $/cancelRequest sent once an earlier, non-exempt message has
// already blocked the read loop acquiring its own token is queued behind
// that message on the wire like anything else, and cannot be read — let
// alone dispatched — until that earlier acquire succeeds. What this
// exemption guarantees is that a $/cancelRequest never becomes the message
// blocking the read loop, so sending one before the window's next
// non-exempt message is what reaches the server always gets through
// immediately, however full the window already is.
func bypassesInFlightLimit(method string) bool {
	switch method {
	case "$/cancelRequest", "shutdown", "exit":
		return true
	default:
		return false
	}
}

// Handle implements [jsonrpc2.Handler]. It runs on the connection's read loop,
// so everything before the `go` below happens in the order messages arrived.
//
// # Why the acquire below cannot deadlock
//
// A token holder never waits on anything that itself needs a token it does
// not have and cannot get:
//
//   - A request's only indefinite-seeming wait is [FlowfileServer.awaitDoc]'s
//     wait for a build, and that is bounded twice over — [documentBuildTimeout]
//     and the connection dropping — so it always releases its token in
//     bounded time rather than holding it forever.
//   - [bypassesInFlightLimit] exempts $/cancelRequest, shutdown and exit from
//     the acquire entirely, so none of the three ever becomes the message a
//     full window leaves the read loop stuck on — see that function's own
//     doc comment for the one thing this does not reach: a message already
//     queued behind an earlier, non-exempt one that got stuck first.
//   - The per-URI queue's wait channel (below) is always a predecessor that
//     already holds, or already released, its own token: [documentStore.enqueue]
//     is called from inside this same acquire-then-announce sequence, in wire
//     order, on the read loop, so a message cannot be announced — and so
//     cannot become something a later message's wait channel points at —
//     until its own acquire above has already returned. A wait channel here
//     therefore never points at a message still stuck trying to acquire.
func (h asyncHandler) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	if bypassesInFlightLimit(req.Method) {
		go func() {
			if h.dispatchTrace != nil {
				h.dispatchTrace(false, true)
				defer h.dispatchTrace(false, false)
			}
			h.server.Handle(ctx, conn, req)
		}()
		return
	}

	// Acquired here, on the read loop, before this message's goroutine even
	// exists, and blocking: seeing #2071's rationale in
	// [maxInFlightPerConnection]. A message already queued behind another
	// same-URI notification has not reached this point yet when that
	// notification is dispatched — the read loop dispatches in wire order,
	// one message at a time, so whatever a same-URI wait channel below
	// chains behind has already acquired its own token by the time this
	// call could be waiting on it. No token holder ever waits on a message
	// that has not been able to acquire one.
	h.inFlight <- struct{}{}

	wait, release := h.server.announceInbound(req)
	go func() {
		defer func() { <-h.inFlight }()
		defer release()
		if wait != nil {
			// Blocks this goroutine, not the read loop: the wait was claimed
			// above, synchronously, before this goroutine even existed, so
			// which one of two same-URI notifications reaches this point
			// first no longer decides which one's handler runs first.
			<-wait
		}
		if h.dispatchTrace != nil {
			h.dispatchTrace(true, true)
			defer h.dispatchTrace(true, false)
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
