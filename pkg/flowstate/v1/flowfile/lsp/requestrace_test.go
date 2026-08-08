package lsp

import (
	"slices"
	"strings"
	"testing"
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
edition: v2026.2
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
	// document's life and only happens once per document.
	for round := range 5 {
		c := newClient(t)
		c.initialize()

		uri := "file:///race-open.yaml"
		c.openNoWait(uri, raceSource)

		at := positionOf(t, raceSource, "for_each:", 0)
		start := time.Now()
		got := c.hover(uri, at.Line, at.Character)
		elapsed := time.Since(start)

		require.NotNilf(t, got, "round %d: hover answered null for a document the client had already opened", round)
		assert.Containsf(t, hoverText(got), "for_each", "round %d", round)
		assert.Lessf(t, elapsed, time.Second, "round %d: hover took %s, which is a wait that is not ending on the build", round, elapsed)
	}
}

// TestHoverOnNeverOpenedDocumentAnswersNull is the other direction, and the one
// that keeps the fix from being a hang.
//
// Waiting for a document that is on its way is correct. Waiting for one that is
// never coming is not: a client asking about a URI it never opened gets null, and
// gets it without the connection stalling. Fail closed here means answer, not
// block.
func TestHoverOnNeverOpenedDocumentAnswersNull(t *testing.T) {
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

	// Into the middle of the burst, with no wait of any kind. The position is the
	// inner step's id, which every version of the document has in the same place.
	at := positionOf(t, latest, "id: final_marker", 4)
	start := time.Now()
	got := c.hover(uri, at.Line, at.Character)
	elapsed := time.Since(start)

	require.NotNil(t, got, "hover answered null for a document in the middle of a change storm")
	assert.Less(t, elapsed, time.Second, "hover took %s, which is a wait that is not ending on a build", elapsed)
	text := hoverText(got)
	named := slices.ContainsFunc(append(names, "shout"), func(name string) bool {
		return strings.Contains(text, "step `"+name+"`")
	})
	assert.Truef(t, named, "hover described a version the client never sent: %q", text)

	// The burst coalesces onto the last change rather than onto whichever
	// goroutine happened to finish last.
	require.Eventually(t, func() bool {
		doc, ok := c.serverDoc(uri)
		return ok && doc.text == latest
	}, 5*time.Second, 10*time.Millisecond, "the document did not settle on the newest text")

	// And asking again answers from it.
	require.Eventually(t, func() bool {
		return strings.Contains(hoverText(c.hover(uri, at.Line, at.Character)), "final_marker")
	}, 5*time.Second, 10*time.Millisecond, "hover kept answering from a version older than the last change")
}
