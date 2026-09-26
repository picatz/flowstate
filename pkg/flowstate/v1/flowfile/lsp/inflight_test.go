package lsp

import (
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/require"
)

// TestAsyncHandlerBoundsInFlightMessagesPerConnection is the regression test
// for #2071's chosen mechanism after two rounds of independent review found
// coalescing a same-URI full-sync didChange burst kept reintroducing
// ordering defects (see the history on #2071 and #2089): a per-connection
// cap on in-flight messages, covering every method and every URI rather than
// one narrow shape.
//
// A burst of far more than [maxInFlightPerConnection]'s worth of full-sync
// didChanges for as many distinct URIs — the shape coalescing never bounded
// at all — never lets more than the limit's worth of them be dispatched to
// [FlowfileServer.Handle] at once, counted through
// [asyncHandler.dispatchTrace] rather than inferred from timing. Once the
// window is exactly full, a $/cancelRequest sent next is still dispatched
// promptly ([bypassesInFlightLimit]'s whole purpose — a client that sent it
// only after enough more of the burst to itself get stuck behind a
// non-exempt message the read loop cannot get past would find no exemption
// helps a message queued behind one already stuck; the exemption's job is
// to keep a message from becoming that itself), and once the burst is
// released every one of its documents has still landed correctly.
func TestAsyncHandlerBoundsInFlightMessagesPerConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const limit = 4
		const burst = 40

		server := &FlowfileServer{Logger: discardLogger()}

		// Holds every dispatched didChange's parse open — for every URI,
		// unconditionally — until released, so the burst is guaranteed to
		// still be piled up behind the limit when the peak is read, rather
		// than merely likely to be: without it, a fast parse could finish
		// before the rest of the burst is even sent.
		release := make(chan struct{})
		server.docs.setParseGate(func(lsp.DocumentURI) { <-release })

		var current, peak, bypassed atomic.Int64
		h := newHandlerWithLimit(server, limit, func(bounded, entering bool) {
			if !bounded {
				if entering {
					bypassed.Add(1)
				}
				return
			}
			var n int64
			if entering {
				n = current.Add(1)
			} else {
				n = current.Add(-1)
			}
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
		})

		c := newClientWithHandler(t, server, h)
		c.initialize()

		uris := make([]string, burst)
		for i := range burst {
			uris[i] = fmt.Sprintf("file:///inflight-%02d.yaml", i)
		}

		// Exactly the limit's worth first, sent sequentially from this
		// goroutine: each one's acquire succeeds immediately (a slot is
		// free), so none of these sends block, and the read loop is not yet
		// stuck on anything.
		for i := range limit {
			c.changeNoWait(uris[i], "name: n\n", 1)
		}
		synctest.Wait()

		if got := current.Load(); got != limit {
			t.Fatalf("in-flight bounded goroutines = %d once the first %d messages settled, want the limit (%d)", got, limit, limit)
		}
		if got := peak.Load(); got != limit {
			t.Fatalf("peak in-flight = %d, want %d", got, limit)
		}

		// The window is exactly full, and the read loop is idle — nothing
		// has asked it to acquire a token it cannot get yet. A cancel sent
		// now must still be dispatched rather than waiting behind anything.
		require.NoError(t, c.conn.Notify(t.Context(), "$/cancelRequest", struct{}{}))
		synctest.Wait()
		if got := bypassed.Load(); got != 1 {
			t.Fatalf("a $/cancelRequest sent while the window was exactly full was not dispatched (count = %d, want 1): "+
				"it must not need one of the exhausted tokens to run", got)
		}

		// The rest of the burst, sent from its own goroutine: the read loop
		// will read message limit+1 and then get stuck acquiring a token
		// for it, so this loop's remaining sends block behind that — a
		// synctest-trackable channel operation (net.Pipe is channel-based) —
		// rather than needing a goroutine per send.
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := limit; i < burst; i++ {
				c.changeNoWait(uris[i], "name: n\n", 1)
			}
		}()
		synctest.Wait()

		if got := current.Load(); got != limit {
			t.Fatalf("in-flight bounded goroutines = %d once the rest of the burst piled up, want the limit (%d) unchanged", got, limit)
		}
		if got := peak.Load(); got != limit {
			t.Fatalf("peak in-flight = %d, want %d: the rest of the burst exceeded the connection-wide bound", got, limit)
		}
		// The bound must be the read loop actually blocking (transport
		// backpressure), not merely a cap on how many goroutines run
		// [FlowfileServer.Handle] concurrently: a mutant that acquires the
		// token from inside the spawned goroutine, after it has already
		// been created, would still leave dispatchTrace's counts at the
		// limit above while spawning one goroutine per message with no
		// bound at all — the exact growth this fix exists to stop. This is
		// what tells the two apart: the sender is still blocked mid-burst,
		// not finished, because the read loop itself has not read past the
		// message that filled the window.
		select {
		case <-done:
			t.Fatal("the burst's sender finished sending while the window was full: the read loop did not apply backpressure")
		default:
		}

		close(release)
		synctest.Wait()

		select {
		case <-done:
		default:
			t.Fatal("the burst's sender never finished sending every message after the gate was released")
		}

		if got := current.Load(); got != 0 {
			t.Fatalf("in-flight bounded goroutines = %d after the burst drained, want 0", got)
		}
		if got := peak.Load(); got != limit {
			t.Fatalf("peak in-flight = %d after the burst drained, want %d (recorded once, unchanged by draining)", got, limit)
		}

		// The server still answers correctly once the window has drained:
		// every one of the burst's documents landed, not just the ones that
		// happened to be first admitted.
		for _, uri := range uris {
			doc, ok := c.server.docs.await(t.Context(), make(chan struct{}), lsp.DocumentURI(uri))
			if !ok || doc.text != "name: n\n" {
				t.Fatalf("uri %s did not land correctly after the burst drained: ok=%v text=%q", uri, ok, doc.text)
			}
		}
	})
}

// TestAsyncHandlerBoundsInFlightMessagesPerConnectionForOneURI is
// [TestAsyncHandlerBoundsInFlightMessagesPerConnection]'s single-URI
// variant, matching #2071's own acceptance text: N full-sync didChanges for
// one URI. Sent to one URI, only the first of them ever reaches
// [FlowfileServer.Handle] at a time — the rest queue behind it on
// [documentStore.enqueue]'s per-URI chain, the same way they always have —
// so [asyncHandler.dispatchTrace] would show at most one "entering"
// regardless of whether the connection-wide bound is wired correctly at
// all; that is not what is under test here. This reads
// [asyncHandler.inFlight]'s length directly instead, which reports every
// token currently held whether its holder is running [FlowfileServer.Handle]
// or itself still queued behind a same-URI predecessor, and applies the
// same backpressure check as the multi-URI test: the sender must still be
// blocked, not finished, while the window is full.
func TestAsyncHandlerBoundsInFlightMessagesPerConnectionForOneURI(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const limit = 4
		const burst = 40
		const uri = "file:///inflight-one-uri.yaml"

		server := &FlowfileServer{Logger: discardLogger()}
		release := make(chan struct{})
		server.docs.setParseGate(func(lsp.DocumentURI) { <-release })

		h := newHandlerWithLimit(server, limit, nil)
		c := newClientWithHandler(t, server, h)
		c.initialize()

		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 1; i <= burst; i++ {
				c.changeNoWait(uri, fmt.Sprintf("name: v%d\n", i), i)
			}
		}()
		synctest.Wait()

		if got := len(h.inFlight); got != limit {
			t.Fatalf("tokens held = %d once the same-URI burst settled, want the limit (%d)", got, limit)
		}

		// Backpressure, not merely a per-goroutine cap — see the identical
		// assertion's comment in
		// [TestAsyncHandlerBoundsInFlightMessagesPerConnection]. A mutant
		// that acquires from inside the spawned goroutine would let this
		// sender finish immediately, having handed every one of the 40
		// messages to a goroutine of its own with no bound on how many
		// existed at once.
		select {
		case <-done:
			t.Fatal("the same-URI burst's sender finished sending while the window was full: the read loop did not apply backpressure")
		default:
		}

		close(release)
		synctest.Wait()

		select {
		case <-done:
		default:
			t.Fatal("the same-URI burst's sender never finished sending every message after the gate was released")
		}

		if got := len(h.inFlight); got != 0 {
			t.Fatalf("tokens held = %d after the burst drained, want 0", got)
		}

		doc, ok := c.server.docs.await(t.Context(), make(chan struct{}), uri)
		if !ok || doc.version != burst {
			t.Fatalf("the document did not settle on the burst's last version: ok=%v version=%d, want %d", ok, doc.version, burst)
		}
	})
}
