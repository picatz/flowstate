package lsp

import (
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests drive the server the way an editor actually does, which is not the
// way the rest of this package's tests do.
//
// Every other test opens a document through [client.open], which sends didOpen and
// then blocks until the diagnostics it triggers arrive. That wait is politeness no
// editor owes the server: VS Code sends didOpen and a hover for the cursor's
// position in the same breath, and publishDiagnostics is a notification it is free
// to process whenever. The wait also hides a defect, because it gives the didOpen
// handler a whole round trip of grace to finish building the document in.
//
// The connection wraps the server in jsonrpc2.AsyncHandler, which starts a
// goroutine per message (async.go:16) from the read loop (conn.go:224). Messages
// are therefore *started* in arrival order and make progress in whatever order the
// scheduler picks — so a hover that arrives after didOpen can reach the store
// before didOpen has put anything in it. The answer was a null hover, which is
// indistinguishable to a client from "nothing to say about this position": the
// silent-wrong shape, reported by an external audit as a shipped feature being
// broken.

// raceSource is a small loop workflow: enough grammar that a hover has a real
// answer to give, small enough that the parse is quick.
const raceSource = `name: race
steps:
  - id: fan
    for_each:
      items: ${[1, 2, 3]}
      as: n
      steps:
        - id: shout
          log:
            message: ${n}
edition: v2026.3
`

// TestHoverAnsweredWhenItArrivesWithDidOpen is the regression test for #317.
//
// The client is deliberately impolite: it sends didOpen and immediately calls
// hover, with no wait for diagnostics in between and no sleep. That is the whole
// point — a position request may reach the server before the document it asks
// about has been built, and it must still be answered from that document rather
// than from the store's ignorance of it.
//
// The elapsed bound matters as much as the content. Answering correctly after
// waiting out a build that never lands is a hang wearing a pass's clothes, so the
// call has to come back promptly as well as correctly.
func TestHoverAnsweredWhenItArrivesWithDidOpen(t *testing.T) {
	// Not parallel: this is a scheduling claim, and it is sharper when the test
	// is not competing with the rest of the package for cores.

	// A fresh connection each round, because the race is at the front of a
	// document's life and only happens once per document. Each round is its
	// own bubble, so the goroutines one round leaves behind cannot be counted
	// as the next round's.
	for round := range 5 {
		synctest.Test(t, func(t *testing.T) { hoverRacesDidOpen(t, round) })
	}
}

// hoverRacesDidOpen is one round of [TestHoverAnsweredWhenItArrivesWithDidOpen].
func hoverRacesDidOpen(t *testing.T, round int) {
	c := newClient(t)
	c.initialize()

	// require.NotNilf below is not a substitute: [documentStore.await]'s
	// build deadline is 2s, and a build that lands just under that ceiling
	// still answers non-nil and correct — a wall-clock threshold loose
	// enough to tolerate scheduling jitter on a contended box is also loose
	// enough for that failure mode to pass under it, which is the bug this
	// test exists to catch wearing a passing assertion's clothes. So this
	// asks the store directly rather than timing the round trip: hoverTrace
	// records whether the wait that answered this hover ever gave up
	// because [documentBuildTimeout] expired, as opposed to finding the
	// document already landed. A hover racing an in-flight build honestly
	// is allowed to wait — that is the mechanism #317 needs — but it must
	// never be the deadline that ends the wait for a document that arrived
	// in the same breath as its didOpen. Inside the bubble that deadline is
	// virtual and passes only once every goroutine is blocked, so it firing
	// here means the document never landed at all, not that a build was slow.
	var boundExpired atomic.Bool
	c.server.docs.setAwaitTrace(func(expired bool) { boundExpired.Store(expired) })

	uri := "file:///race-open.yaml"
	c.openNoWait(uri, raceSource)

	at := positionOf(t, raceSource, "for_each:", 0)
	got := c.hover(uri, at.Line, at.Character)

	require.NotNilf(t, got, "round %d: hover answered null for a document the client had already opened", round)
	assert.Containsf(t, hoverText(got), "for_each", "round %d", round)
	assert.Falsef(t, boundExpired.Load(), "round %d: hover's wait ended because documentBuildTimeout expired rather than because the document was found built, which the elapsed-time check this replaced could not tell apart from a fast answer", round)
}

// TestHoverOnNeverOpenedDocumentAnswersNull is the other direction, and the one
// that keeps the fix from being a hang.
//
// Waiting for a document that is on its way is correct. Waiting for one that is
// never coming is not: a client asking about a URI it never opened gets null, and
// gets it without the connection stalling. Fail closed here means answer, not
// block.
//
// The bound on the wait is virtual: inside the bubble the five seconds pass the
// instant every goroutine is blocked, so a hover that hangs is reported at once
// rather than after the grace the store gives a document on its way.
func TestHoverOnNeverOpenedDocumentAnswersNull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newClient(t)
		c.initialize()

		done := make(chan *lsp.Hover, 1)
		go func() {
			done <- c.hover("file:///never-opened.yaml", 3, 5)
		}()

		select {
		case got := <-done:
			assert.Nil(t, got, "a document the client never opened has no hover")
		case <-time.After(5 * time.Second):
			t.Fatal("hover on a never-opened document did not return: the wait has no bound")
		}
	})
}

// TestHoverThroughAChangeStormAnswersFromTheLatestVersion covers the join of the
// fix and the coalescing the server already did.
//
// Typing produces a burst of didChange notifications and no waiting of any kind.
// Three separate claims live here, and they are separate because the fix
// delivers them to different strengths.
//
//   - A hover fired into the middle of the burst is never null and never
//     describes text the client did not send. Both of those are the silent-wrong
//     shape and both are absolute.
//   - The burst coalesces the way it did before the fix: the store settles on the
//     last change's text, and the out-of-order arrivals AsyncHandler produces are
//     still rejected as stale rather than reverting the document.
//   - The latest version is what hover answers from, once the burst has landed.
//     An editor asks again; this asserts the answer converges on the newest text
//     rather than sticking on one from the middle of the burst.
//
// What is deliberately *not* asserted is that a hover sent in the same breath as
// the last change of a burst describes that change. It cannot be: the read loop
// starts a goroutine per message and a request has no way to learn how many
// notifications were started ahead of it, so a hover racing an unlanded didChange
// answers from the previous version. That is a stale answer rather than a null
// one, which is the difference between an editor showing something a keystroke
// out of date and an editor showing nothing at all. Closing it as well would take
// an ordered hook ahead of the goroutine, which lives in the connection's
// construction rather than in this package.
func TestHoverThroughAChangeStormAnswersFromTheLatestVersion(t *testing.T) {
	synctest.Test(t, testHoverThroughAChangeStorm)
}

func testHoverThroughAChangeStorm(t *testing.T) {
	c := newClient(t)
	c.initialize()

	uri := "file:///race-storm.yaml"
	c.openNoWait(uri, raceSource)

	// Each change renames the loop's inner step; only the last introduces
	// `final_marker`, so what a hover names says which version answered it.
	const versions = 8
	names := make([]string, 0, versions)
	var latest string
	for i := 1; i <= versions; i++ {
		name := "step_" + string(rune('a'+i-1))
		if i == versions {
			name = "final_marker"
		}
		names = append(names, name)
		latest = strings.Replace(raceSource, "id: shout", "id: "+name, 1)
		c.changeNoWait(uri, latest, i+1)
	}

	// Whether the answer came from in-memory state or from riding out a build,
	// asked of the wait itself rather than of a stopwatch — the same instrument
	// the test above uses, for the same reason. A wall-clock bound cannot
	// separate the two: [documentBuildTimeout] is two seconds, so a hover that
	// waited out the entire build still returns well inside any threshold loose
	// enough to survive a contended box.
	var boundExpired atomic.Bool
	c.server.docs.setAwaitTrace(func(expired bool) { boundExpired.Store(expired) })

	// Into the middle of the burst, with no wait of any kind. The position is the
	// inner step's id, which every version of the document has in the same place.
	at := positionOf(t, latest, "id: final_marker", 4)
	got := c.hover(uri, at.Line, at.Character)

	require.NotNil(t, got, "hover answered null for a document in the middle of a change storm")
	assert.False(t, boundExpired.Load(), "hover's wait ended because a build bound expired rather than because the document was found built, which is the blocking this test exists to exclude")
	text := hoverText(got)
	named := slices.ContainsFunc(append(names, "shout"), func(name string) bool {
		return strings.Contains(text, "step `"+name+"`")
	})
	assert.Truef(t, named, "hover described a version the client never sent: %q", text)

	// The burst coalesces onto the last change rather than onto whichever
	// goroutine happened to finish last. Asked through the store's own
	// settle signal, [documentStore.await] — the same one every position
	// request already waits behind — rather than synctest.Wait() plus an
	// unguarded [documentStore.get] read: that pair is supposed to be
	// equivalent once the bubble is idle, but #1980 saw the read come back
	// with the document's pre-burst text once under full-tree load, and
	// nothing traced from the connection's read loop down to
	// [documentStore.change] found a goroutine or a real blocking call
	// synctest would not have tracked. synctest.Wait() stays, so every
	// goroutine the burst started — not just this URI's build count — is
	// idle before the bubble ends; the settle-signal read is the assertion
	// that no longer trusts idleness alone to mean "landed."
	synctest.Wait()
	doc, ok := c.server.docs.await(c.t.Context(), make(chan struct{}), lsp.DocumentURI(uri))
	require.True(t, ok, "the document is gone")
	require.Equal(t, latest, doc.text, "the document did not settle on the newest text")

	// And asking again answers from it.
	require.Contains(t, hoverText(c.hover(uri, at.Line, at.Character)), "final_marker", "hover kept answering from a version older than the last change")
}

// TestDidOpenWaitsForAnInFlightDidCloseOnTheSameURI is the wire-level
// regression test for #1986 itself: a same-URI didClose and didOpen sent back
// to back must have their handlers run in that order, because the connection
// wraps the server in jsonrpc2.AsyncHandler, which starts a goroutine per
// message and gives up the arrival order the protocol otherwise implies. A
// close whose handler goroutine the scheduler happens to run behind the
// reopen's own goroutine used to delete the document the reopen had just
// established — the client believes the buffer is open, the server holds
// nothing.
//
// The reorder this test proves against is forced through
// [documentStore.closeGate] rather than raced: the close's handler is held
// open, mid-dispatch, well past the point a scheduler would ordinarily have
// let it finish, and the reopen is sent and given every chance to run ahead
// of it. The final assertion is the one that catches the regression: without
// the queue, the late close deletes the document the reopen established, so
// nothing survives. The check made while the close is still gated shows the
// reopen has not run ahead of it; on its own it cannot tell a queue from
// [documentStore.open]'s version guard, which also keeps the incumbent for a
// reopen at version 1, so it describes the queued state rather than proving it.
func TestDidOpenWaitsForAnInFlightDidCloseOnTheSameURI(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &FlowfileServer{Logger: discardLogger()}
		uri := lsp.DocumentURI("file:///reorder.yaml")

		proceed := make(chan struct{})
		server.docs.setCloseGate(func(u lsp.DocumentURI) {
			if u == uri {
				<-proceed
			}
		})

		c := newClientFor(t, server)
		c.initialize()

		c.openNoWait(string(uri), "name: original\n")
		synctest.Wait()
		if _, ok := c.rawServerDoc(string(uri)); !ok {
			t.Fatal("the initial open never landed")
		}

		// The close and the reopen an editor sends right behind it, exactly as
		// they go out on the wire: didClose then didOpen for the same URI, with
		// nothing waiting in between. synctest.Wait below returns only once
		// every goroutine in the bubble is durably blocked, which the close's
		// handler now is: parked in the gate above, mid-dispatch.
		c.closeNoWait(string(uri))
		synctest.Wait()

		// The reopen at version 1 — the version a real reopen actually carries
		// — sent while the close is still gated open.
		c.openVersionNoWait(string(uri), "name: reopened\n", 1)
		synctest.Wait()

		// The reopen's handler must not have run at all yet: it is queued
		// behind the still-gated close, so the store still shows the
		// *original* document untouched, not the reopen and not nothing.
		// [client.rawServerDoc] on purpose, not [documentStore.await]: the
		// reopen's own build is in flight (its beginBuild already ran when it
		// was announced), so await would wait out its full build timeout
		// rather than answer this instant — this assertion is about the
		// state *before* anything settles.
		doc, ok := c.rawServerDoc(string(uri))
		require.True(t, ok, "the original document was removed before the gated close was ever released")
		require.Equal(t, "name: original\n", doc.text,
			"the reopen's handler ran ahead of the still-gated close it should be queued behind")

		// Releasing the gate lets the close finish, which unblocks the
		// reopen's handler in turn.
		close(proceed)
		synctest.Wait()

		doc, ok = c.server.docs.await(c.t.Context(), make(chan struct{}), uri)
		require.True(t, ok, "a close that finally ran left no document for the reopen queued behind it")
		assert.Equal(t, "name: reopened\n", doc.text,
			"the reopened document's text did not survive the close it was queued behind")
	})
}

// TestPerURIQueueStateIsBoundedByWhatIsInFlight is the boundedness half of
// #1986's fix: [documentStore.enqueue] keeps one entry per URI with a
// document notification still queued or in flight, and CLAUDE.md's "bound
// work where it is spent" applies to that map the same as to any other one a
// long-running connection accumulates. A `flow lsp` process serving an editor
// for a whole session opens and closes many files, and an entry that
// survived its own notification finishing would make this map grow with
// every URI the editor had ever touched rather than with how many it has
// open right now.
func TestPerURIQueueStateIsBoundedByWhatIsInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newClient(t)
		c.initialize()
		uri := "file:///bounded.yaml"

		c.openNoWait(uri, "name: one\n")
		synctest.Wait()
		c.closeNoWait(uri)
		synctest.Wait()

		c.server.docs.mu.Lock()
		n := len(c.server.docs.tail)
		c.server.docs.mu.Unlock()
		assert.Zero(t, n, "a URI with nothing queued or in flight still has a queue entry")
	})
}
