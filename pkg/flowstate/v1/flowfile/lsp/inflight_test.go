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
