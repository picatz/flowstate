package lsp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The two fixtures of #1555: a callee whose default breaks its own `must:`
// (line 6, column 14), and one that also calls itself (line 10, column 11).
const (
	calleeHead = "edition: v2026.4\nname: callee\ninputs:\n  shards:\n    type: int\n    default: 0\n    must: this >= 1\nsteps:\n"

	selfCallingCallee = calleeHead + "  - id: loop\n    call: ./callee.yaml\n"
	plainCallee       = calleeHead + "  - id: work\n    log:\n      message: hi\n"
	fixedCallee       = "edition: v2026.4\nname: callee\ninputs:\n  shards:\n    type: int\n    default: 1\n    must: this >= 1\nsteps:\n  - id: work\n    log:\n      message: hi\n"

	calleeCaller = "edition: v2026.4\nname: caller\nsteps:\n  - id: place\n    call: ./lib/callee.yaml\n"
)

// calleeFixture is a caller and a callee written to a temporary directory.
type calleeFixture struct {
	c          *client
	dir        string
	callerPath string
	calleePath string
	callerURI  lsp.DocumentURI
	calleeURI  lsp.DocumentURI
}

func newCalleeFixture(t *testing.T, callee string) *calleeFixture {
	t.Helper()
	dir := t.TempDir()
	f := &calleeFixture{
		c:          newClient(t),
		dir:        dir,
		callerPath: filepath.Join(dir, "workflow.yaml"),
		calleePath: filepath.Join(dir, "lib", "callee.yaml"),
	}
	f.callerURI, f.calleeURI = fileURI(f.callerPath), fileURI(f.calleePath)
	require.NoError(t, os.MkdirAll(filepath.Dir(f.calleePath), 0o755))
	f.writeCallee(t, callee)
	f.c.initialize()

	return f
}

func (f *calleeFixture) writeCallee(t *testing.T, text string) {
	t.Helper()
	require.NoError(t, os.WriteFile(f.calleePath, []byte(text), 0o644))
}

// lastFor is the most recent notification for uri, relatedInformation kept.
func (f *calleeFixture) lastFor(uri lsp.DocumentURI) (publishDiagnosticsParams, bool) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	for i := len(f.c.withRelated) - 1; i >= 0; i-- {
		if f.c.withRelated[i].URI == uri {
			return f.c.withRelated[i], true
		}
	}

	return publishDiagnosticsParams{}, false
}

// settled lets the server finish everything it has been sent, then requires the
// last notification for uri to satisfy ok: what an editor would be showing.
// Inside a [synctest.Test] bubble.
func (f *calleeFixture) settled(t *testing.T, uri lsp.DocumentURI, what string, ok func(publishDiagnosticsParams) bool) publishDiagnosticsParams {
	t.Helper()
	synctest.Wait()
	last, found := f.lastFor(uri)
	require.True(t, found, "no notification for %s, wanted %s", uri, what)
	require.True(t, ok(last), "wanted %s on %s; last: %+v", what, uri, last)

	return last
}

func hasDiagnostics(p publishDiagnosticsParams) bool { return len(p.Diagnostics) > 0 }
func isEmpty(p publishDiagnosticsParams) bool        { return p.Diagnostics != nil && len(p.Diagnostics) == 0 }

func TestPublishesCalleeDiagnosticsOnCalleeURI(t *testing.T) {
	t.Parallel()

	t.Run("a default that breaks its own must is published at that default", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			f := newCalleeFixture(t, plainCallee)
			f.c.openNoWait(string(f.callerURI), calleeCaller)

			got := f.settled(t, f.calleeURI, "the callee's diagnostics", hasDiagnostics)
			require.Len(t, got.Diagnostics, 1, "%+v", got.Diagnostics)
			d := got.Diagnostics[0]
			assert.Equal(t, lsp.Position{Line: 5, Character: 13}, d.Range.Start, "6:14 is the default, in zero-based positions")
			assert.Greater(t, d.Range.End.Character, d.Range.Start.Character)
			assert.Equal(t, lsp.Error, d.Severity)
			assert.Contains(t, d.Message, "`this >= 1`")
			assert.NotContains(t, d.Message, `workflow "callee":`)
			assert.NotEmpty(t, d.Code)
			require.Len(t, d.RelatedInformation, 1)
			rel := d.RelatedInformation[0]
			assert.Equal(t, f.callerURI, rel.Location.URI, "the reason it was compiled is the caller's call:")
			assert.Equal(t, lsp.Position{Line: 4, Character: 10}, rel.Location.Range.Start)
			assert.Contains(t, rel.Message, `step "place"`)

			caller := f.settled(t, f.callerURI, "the caller's diagnostics", hasDiagnostics)
			require.Len(t, caller.Diagnostics, 1, "only its own callee-failed, no copy of the callee's text: %+v", caller.Diagnostics)
			assert.Equal(t, "callee-failed", caller.Diagnostics[0].Code)
			assert.Equal(t, 4, caller.Diagnostics[0].Range.Start.Line)
			assert.NotContains(t, caller.Diagnostics[0].Message, "this >= 1")
			require.Len(t, caller.Diagnostics[0].RelatedInformation, 1)
			first := caller.Diagnostics[0].RelatedInformation[0].Location
			assert.Equal(t, f.calleeURI, first.URI, "the caller's diagnostic points at the callee's")
			assert.Equal(t, d.Range.Start, first.Range.Start)
		})
	})

	t.Run("a self-call is published at the call", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			f := newCalleeFixture(t, selfCallingCallee)
			f.c.openNoWait(string(f.callerURI), calleeCaller)

			got := f.settled(t, f.calleeURI, "the callee's diagnostics", hasDiagnostics)
			require.Len(t, got.Diagnostics, 1, "%+v", got.Diagnostics)
			d := got.Diagnostics[0]
			assert.Equal(t, lsp.Position{Line: 9, Character: 10}, d.Range.Start, "10:11")
			assert.Equal(t, "call-cycle", d.Code)
			assert.Contains(t, d.Message, "calls itself")
			require.Len(t, d.RelatedInformation, 1)
			assert.Equal(t, f.callerURI, d.RelatedInformation[0].Location.URI)
			assert.Equal(t, lsp.Position{Line: 4, Character: 10}, d.RelatedInformation[0].Location.Range.Start)

			caller := f.settled(t, f.callerURI, "the caller's diagnostics", hasDiagnostics)
			require.Len(t, caller.Diagnostics, 1)
			assert.Equal(t, "callee-failed", caller.Diagnostics[0].Code)
		})
	})
}

func TestCalleeDiagnosticsClearWhenTheCalleeIsFixed(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newCalleeFixture(t, plainCallee)
		f.c.openNoWait(string(f.callerURI), calleeCaller)
		f.settled(t, f.calleeURI, "the callee's diagnostics", hasDiagnostics)

		// The editor never opened the callee: it is fixed on disk, and the next
		// analysis of the caller is what has to retract the squiggle.
		f.writeCallee(t, fixedCallee)
		f.c.changeNoWait(string(f.callerURI), calleeCaller+"# touched\n", 2)

		f.settled(t, f.calleeURI, "an empty list for the fixed callee", isEmpty)
		f.settled(t, f.callerURI, "a clean caller", isEmpty)
	})
}

func TestCalleeDiagnosticsClearWhenTheCallIsRemoved(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newCalleeFixture(t, plainCallee)
		f.c.openNoWait(string(f.callerURI), calleeCaller)
		f.settled(t, f.calleeURI, "the callee's diagnostics", hasDiagnostics)

		f.c.changeNoWait(string(f.callerURI), "edition: v2026.4\nname: caller\nsteps:\n  - id: place\n    log:\n      message: hi\n", 2)

		f.settled(t, f.calleeURI, "an empty list once nothing calls it", isEmpty)
	})
}

func TestCalleeDiagnosticsClearWhenTheCallerCloses(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newCalleeFixture(t, plainCallee)
		f.c.openNoWait(string(f.callerURI), calleeCaller)
		f.settled(t, f.calleeURI, "the callee's diagnostics", hasDiagnostics)

		f.c.closeNoWait(string(f.callerURI))
		synctest.Wait()

		f.settled(t, f.calleeURI, "an empty list for the callee", isEmpty)
		f.settled(t, f.callerURI, "an empty list for the caller", isEmpty)
		assert.Zero(t, f.c.sourceCountFor(f.calleeURI), "nothing keeps contributing to the callee")
	})
}

func TestAnOpenCalleeWithholdsTheCallersView(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newCalleeFixture(t, plainCallee)
		f.c.openNoWait(string(f.callerURI), calleeCaller)
		f.settled(t, f.calleeURI, "the callee's diagnostics", hasDiagnostics)

		// The buffer is already fixed; the disk is not. The buffer's analysis wins.
		f.c.openNoWait(string(f.calleeURI), fixedCallee)
		f.settled(t, f.calleeURI, "the buffer's own, clean analysis", isEmpty)

		f.c.changeNoWait(string(f.callerURI), calleeCaller+"# again\n", 2)
		f.settled(t, f.callerURI, "the caller republished", func(p publishDiagnosticsParams) bool {
			return len(p.Diagnostics) == 1 && p.Diagnostics[0].Code == "callee-failed"
		})
		assert.Equal(t, 2, f.c.sourceCountFor(f.calleeURI), "the caller's contribution is kept, only withheld")
		last, _ := f.lastFor(f.calleeURI)
		assert.Empty(t, last.Diagnostics, "the caller's view of the disk must not overwrite the open buffer's")

		// Closing the buffer returns the callee to what the disk says.
		f.c.closeNoWait(string(f.calleeURI))
		f.settled(t, f.calleeURI, "the caller's view again", hasDiagnostics)
	})
}

func TestSavingACalleeRepublishesItsOpenCallers(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newCalleeFixture(t, plainCallee)
		f.c.openNoWait(string(f.callerURI), calleeCaller)
		f.settled(t, f.calleeURI, "the callee's diagnostics", hasDiagnostics)
		f.c.openNoWait(string(f.calleeURI), fixedCallee)
		f.settled(t, f.calleeURI, "the buffer's own, clean analysis", isEmpty)

		// Saving writes the buffer to disk; the caller's stored view is from before.
		f.writeCallee(t, fixedCallee)
		require.NoError(t, f.c.conn.Notify(t.Context(), "textDocument/didSave", lsp.DidSaveTextDocumentParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: f.calleeURI},
		}))
		f.settled(t, f.callerURI, "a clean caller", isEmpty)

		// Had the save not republished the caller, closing the callee would bring its
		// stale problem back.
		f.c.closeNoWait(string(f.calleeURI))
		synctest.Wait()
		assert.Zero(t, f.c.sourceCountFor(f.calleeURI))
		f.settled(t, f.calleeURI, "the callee closed", isEmpty)
	})
}

func TestCalleeTargetsPerCallerAreBounded(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newCalleeFixture(t, plainCallee)
		const calls = maxCalleeTargets + 6
		var caller strings.Builder
		caller.WriteString("edition: v2026.4\nname: caller\nsteps:\n")
		for i := range calls {
			name := fmt.Sprintf("c%02d.yaml", i)
			require.NoError(t, os.WriteFile(filepath.Join(f.dir, "lib", name), []byte(plainCallee), 0o644))
			fmt.Fprintf(&caller, "  - id: s%02d\n    call: ./lib/%s\n", i, name)
		}
		f.c.openNoWait(string(f.callerURI), caller.String())

		got := f.settled(t, f.callerURI, "the caller's diagnostics", hasDiagnostics)
		assert.Len(t, got.Diagnostics, calls, "every call site still says its callee failed")
		f.c.server.diagnosticsMu.Lock()
		defer f.c.server.diagnosticsMu.Unlock()
		assert.Len(t, f.c.server.diagnosticsBySource[f.callerURI], maxCalleeTargets+1, "the caller's own URI and at most %d callee files", maxCalleeTargets)
	})
}

// A callee's diagnostic can carry a repair measured against the callee's text.
// Offered on the caller's buffer it would rewrite a line that has nothing to do
// with it.
func TestNoCodeActionOffersACalleeEditOnTheCallerBuffer(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const ternary = "edition: v2026.4\nname: callee\nsteps:\n  - id: a\n    value: ${true ? \"yes\" : \"no\"}\n"
		f := newCalleeFixture(t, ternary)
		f.c.openNoWait(string(f.callerURI), calleeCaller)

		got := f.settled(t, f.calleeURI, "the callee's syntax error", hasDiagnostics)
		require.Len(t, got.Diagnostics, 1, "%+v", got.Diagnostics)
		f.settled(t, f.callerURI, "the caller's callee-failed", hasDiagnostics)

		actions := f.c.codeAction(string(f.callerURI), wholeOf(calleeCaller), nil, nil)
		for _, a := range actions {
			assert.NotContains(t, a.Title, "quote the expression", "the callee's repair belongs to the callee's buffer")
			if a.Edit != nil {
				for uri := range a.Edit.Changes {
					assert.Equal(t, string(f.callerURI), uri)
				}
			}
		}

		doc, ok := f.c.server.docs.get(f.callerURI)
		require.True(t, ok)
		own, callees, _ := diagnoseAll(doc)
		for _, c := range own {
			assert.Empty(t, c.source.File, "a callee's diagnostic is never carried as this buffer's")
		}
		require.NotEmpty(t, callees)
		require.NotEmpty(t, callees[0].GetDiagnostics())
		assert.NotEmpty(t, callees[0].GetDiagnostics()[0].GetEdits(), "the fixture must carry a repair for the test to mean anything")
	})
}

// Two open workflows that call the same broken file are two reasons for one
// diagnostic on it, and the published copy keeps both call sites.
func TestSharedCalleeDiagnosticKeepsEveryCallSite(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newCalleeFixture(t, plainCallee)
		otherPath := filepath.Join(f.dir, "other.yaml")
		otherURI := fileURI(otherPath)
		f.c.openNoWait(string(f.callerURI), calleeCaller)
		f.c.openNoWait(string(otherURI), strings.Replace(calleeCaller, "name: caller", "name: other", 1))

		got := f.settled(t, f.calleeURI, "one diagnostic with both call sites", func(p publishDiagnosticsParams) bool {
			return len(p.Diagnostics) == 1 && len(p.Diagnostics[0].RelatedInformation) == 2
		})
		var uris []lsp.DocumentURI
		for _, r := range got.Diagnostics[0].RelatedInformation {
			uris = append(uris, r.Location.URI)
		}
		assert.Equal(t, []lsp.DocumentURI{otherURI, f.callerURI}, uris, "sorted, one per caller")

		// The stored contributions are what each source said, not the merge.
		f.c.server.diagnosticsMu.Lock()
		defer f.c.server.diagnosticsMu.Unlock()
		for _, source := range []lsp.DocumentURI{f.callerURI, otherURI} {
			stored := f.c.server.diagnosticsBySource[source][f.calleeURI]
			require.Len(t, stored, 1)
			assert.Len(t, stored[0].RelatedInformation, 1, "%s's own contribution is untouched", source)
		}
	})
}

func TestMergeRelatedDedupesSortsAndBounds(t *testing.T) {
	t.Parallel()
	at := func(uri string, line int) relatedInformation {
		p := lsp.Position{Line: line}
		return relatedInformation{Location: lsp.Location{URI: lsp.DocumentURI(uri), Range: lsp.Range{Start: p, End: p}}, Message: "m"}
	}
	var a, b []relatedInformation
	for i := range maxRelatedPerDiagnostic {
		a = append(a, at("file:///a", i))
		b = append(b, at("file:///b", i))
	}
	a = append(a, at("file:///a", 0))
	snapshot := append([]relatedInformation(nil), a...)

	got := mergeRelated(b, a)
	require.Len(t, got, maxRelatedPerDiagnostic, "bounded")
	assert.Equal(t, at("file:///a", 0), got[0], "sorted by location, so the bound drops the same tail every time")
	assert.Equal(t, at("file:///a", maxRelatedPerDiagnostic-1), got[len(got)-1], "duplicates collapsed before the cap")
	assert.Equal(t, snapshot, a, "inputs are not mutated")
}

// A callee that is clean when its caller is published has no diagnostics for
// the caller to be recorded against, but saving it broken must still reach the
// caller.
func TestSavingACleanCalleeBrokenRepublishesItsOpenCaller(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newCalleeFixture(t, fixedCallee)
		f.c.openNoWait(string(f.callerURI), calleeCaller)
		f.settled(t, f.callerURI, "a clean caller", isEmpty)
		f.c.openNoWait(string(f.calleeURI), plainCallee)

		f.writeCallee(t, plainCallee)
		require.NoError(t, f.c.conn.Notify(t.Context(), "textDocument/didSave", lsp.DidSaveTextDocumentParams{
			TextDocument: lsp.TextDocumentIdentifier{URI: f.calleeURI},
		}))
		caller := f.settled(t, f.callerURI, "callee-failed", func(p publishDiagnosticsParams) bool {
			return len(p.Diagnostics) == 1 && p.Diagnostics[0].Code == "callee-failed"
		})
		require.Len(t, caller.Diagnostics, 1)

		// With the callee's buffer gone, the caller's view of it is what shows.
		f.c.closeNoWait(string(f.calleeURI))
		f.settled(t, f.calleeURI, "the caller's view of the callee", hasDiagnostics)
	})
}

const (
	// middleCallee calls the fixture's callee, as the middle link of A -> B -> C.
	middleCallee = "edition: v2026.4\nname: middle\nsteps:\n  - id: leaf\n    call: ./callee.yaml\n"
	// outerCaller calls middleCallee, and so reaches the fixture's callee only through it.
	outerCaller = "edition: v2026.4\nname: outer\nsteps:\n  - id: hop\n    call: ./lib/middle.yaml\n"
	// loggingOuter is outerCaller after its call is removed.
	loggingOuter = "edition: v2026.4\nname: outer\nsteps:\n  - id: hop\n    log:\n      message: hi\n"
)

func (f *calleeFixture) saveCallee(t *testing.T, text string) {
	t.Helper()
	f.writeCallee(t, text)
	require.NoError(t, f.c.conn.Notify(t.Context(), "textDocument/didSave", lsp.DidSaveTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: f.calleeURI},
	}))
}

// publicationsFor counts the notifications sent for uri so far.
func (f *calleeFixture) publicationsFor(uri lsp.DocumentURI) int {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	n := 0
	for _, p := range f.c.withRelated {
		if p.URI == uri {
			n++
		}
	}

	return n
}

// A workflow that reaches the saved file through another callee neither calls it
// nor holds a diagnostic against it while everything is clean, so only what its
// compile read can bring it back (#2714).
func TestSavingACalleeRepublishesWorkflowsReachingItThroughAnotherCallee(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newCalleeFixture(t, fixedCallee)
		middlePath := filepath.Join(f.dir, "lib", "middle.yaml")
		require.NoError(t, os.WriteFile(middlePath, []byte(middleCallee), 0o644))
		outerURI, middleURI := f.callerURI, fileURI(middlePath)

		f.c.openNoWait(string(outerURI), outerCaller)
		f.c.openNoWait(string(middleURI), middleCallee)
		f.c.openNoWait(string(f.calleeURI), fixedCallee)
		f.settled(t, outerURI, "a clean outer workflow", isEmpty)
		f.settled(t, middleURI, "a clean middle workflow", isEmpty)

		f.saveCallee(t, plainCallee)

		f.settled(t, outerURI, "callee-failed", func(p publishDiagnosticsParams) bool {
			return len(p.Diagnostics) == 1 && p.Diagnostics[0].Code == "callee-failed"
		})
	})
}

// What a workflow read is replaced when it is published again, so one edited to
// stop reaching the saved file is not republished by its save, and it is dropped
// when the workflow closes.
func TestCalleeReadsAreReplacedOnRepublishAndDroppedOnClose(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newCalleeFixture(t, fixedCallee)
		middlePath := filepath.Join(f.dir, "lib", "middle.yaml")
		require.NoError(t, os.WriteFile(middlePath, []byte(middleCallee), 0o644))
		outerURI := f.callerURI

		f.c.openNoWait(string(outerURI), outerCaller)
		f.c.openNoWait(string(f.calleeURI), fixedCallee)
		f.settled(t, outerURI, "a clean outer workflow", isEmpty)
		f.saveCallee(t, plainCallee)
		f.settled(t, outerURI, "callee-failed", hasDiagnostics)

		// The chain is removed: the workflow no longer reads the callee at all.
		f.c.changeNoWait(string(outerURI), loggingOuter, 2)
		f.settled(t, outerURI, "a clean outer workflow once the call is gone", isEmpty)
		before := f.publicationsFor(outerURI)

		f.saveCallee(t, fixedCallee)
		synctest.Wait()
		assert.Equal(t, before, f.publicationsFor(outerURI), "a workflow that no longer reaches the file is not republished by its save")

		f.c.changeNoWait(string(outerURI), outerCaller, 3)
		f.settled(t, outerURI, "a clean outer workflow again", isEmpty)
		f.c.closeNoWait(string(outerURI))
		synctest.Wait()
		f.c.server.diagnosticsMu.Lock()
		defer f.c.server.diagnosticsMu.Unlock()
		assert.Empty(t, f.c.server.calleeReadsBySource, "a closed workflow keeps no record of what it read")
	})
}

func (f *calleeFixture) notifySaved(t *testing.T, uri lsp.DocumentURI) {
	t.Helper()
	require.NoError(t, f.c.conn.Notify(t.Context(), "textDocument/didSave", lsp.DidSaveTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: uri},
	}))
}

// A callee that could not be read has no compiled frame, but creating it must
// still reach the workflow that calls it.
func TestSavingAMissingCalleeRepublishesItsOpenCaller(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newCalleeFixture(t, fixedCallee)
		require.NoError(t, os.Remove(f.calleePath))
		f.c.openNoWait(string(f.callerURI), calleeCaller)
		f.settled(t, f.callerURI, "call-unreadable", hasDiagnostics)
		f.c.openNoWait(string(f.calleeURI), fixedCallee)

		f.writeCallee(t, fixedCallee)
		f.notifySaved(t, f.calleeURI)

		f.settled(t, f.callerURI, "a clean caller", isEmpty)
	})
}

// A pin the callee no longer matches stops its compile before a frame exists;
// repairing the callee must still reach the workflow.
func TestSavingACalleeThatFailedItsPinRepublishesItsOpenCaller(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		f := newCalleeFixture(t, plainCallee)
		pinned := calleeCaller + "    digest: " + v1.ContentDigest([]byte(fixedCallee)) + "\n"
		f.c.openNoWait(string(f.callerURI), pinned)
		f.settled(t, f.callerURI, "a pin failure", hasDiagnostics)
		f.c.openNoWait(string(f.calleeURI), fixedCallee)

		f.writeCallee(t, fixedCallee)
		f.notifySaved(t, f.calleeURI)

		f.settled(t, f.callerURI, "a clean caller", isEmpty)
	})
}

// A workflow that read more callee files than are remembered cannot be matched
// against a save, so any callee save republishes it.
func TestSavingAnyCalleeRepublishesAWorkflowWhoseReadsOverflowed(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		server := &FlowfileServer{Logger: discardLogger(), calleeReadLimit: 1}
		c := newClientFor(t, server)
		f := &calleeFixture{c: c, dir: dir}
		c.initialize()

		head := "edition: v2026.4\nname: n\nsteps:\n"
		// Names sort a before b, so b is the callee omitted from the one kept.
		for _, name := range []string{"a.yaml", "b.yaml"} {
			require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(head+"  - id: s\n    log:\n      message: hi\n"), 0o644))
		}
		callerURI := fileURI(filepath.Join(dir, "caller.yaml"))
		bURI := fileURI(filepath.Join(dir, "b.yaml"))
		c.openNoWait(string(callerURI), head+"  - id: p\n    call: ./a.yaml\n  - id: q\n    call: ./b.yaml\n")
		f.settled(t, callerURI, "a clean caller", isEmpty)
		c.openNoWait(string(bURI), head+"  - id: s\n    log:\n      message: hi\n")

		broken := calleeHead + "  - id: work\n    log:\n      message: hi\n"
		require.NoError(t, os.WriteFile(filepath.Join(dir, "b.yaml"), []byte(broken), 0o644))
		f.notifySaved(t, bURI)

		f.settled(t, callerURI, "callee-failed", hasDiagnostics)
	})
}

// A callee that does not exist yet, under a directory reached through a symlink,
// is keyed by the path it will have once created, so saving it by its real path
// reaches the workflow.
func TestSavingACalleeCreatedUnderASymlinkedDirectoryRepublishesItsCaller(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		realDir := filepath.Join(root, "real")
		// Not a sibling of the real directory, so a name built from one relative
		// to the other would climb out of the link's own spelling.
		linkDir := filepath.Join(root, "ws", "deep", "link")
		require.NoError(t, os.Mkdir(realDir, 0o755))
		require.NoError(t, os.MkdirAll(filepath.Dir(linkDir), 0o755))
		require.NoError(t, os.Symlink(realDir, linkDir))
		c := newClient(t)
		c.initialize()
		f := &calleeFixture{c: c}

		callerURI := fileURI(filepath.Join(linkDir, "workflow.yaml"))
		c.openNoWait(string(callerURI), calleeCaller)
		f.settled(t, callerURI, "call-unreadable", hasDiagnostics)

		require.NoError(t, os.MkdirAll(filepath.Join(realDir, "lib"), 0o755))
		calleePath := filepath.Join(realDir, "lib", "callee.yaml")
		require.NoError(t, os.WriteFile(calleePath, []byte(fixedCallee), 0o644))
		calleeURI := fileURI(calleePath)
		c.openNoWait(string(calleeURI), fixedCallee)
		f.notifySaved(t, calleeURI)

		f.settled(t, callerURI, "a clean caller", isEmpty)
	})
}
