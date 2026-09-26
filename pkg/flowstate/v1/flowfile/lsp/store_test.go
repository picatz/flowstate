package lsp

import (
	"testing"
	"time"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFilesystemPath covers the URI shapes a real editor sends, not only the
// `file:///plain/path.yaml` one a prefix trim happens to get right.
//
// A `file://` URI is percent-encoded and may carry an authority, and trimming
// the scheme off the front of the string — the shape this used to take —
// leaves a space as `%20`, a `#` as `%23`, and a non-ASCII name as its UTF-8
// escapes, none of which is a path that exists on disk. This is the table for
// the decode this package's `call:` resolution depends on.
func TestFilesystemPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		uri  lsp.DocumentURI
		want string
		ok   bool
	}{
		{
			name: "plain absolute path",
			uri:  "file:///home/user/workflow.yaml",
			want: "/home/user/workflow.yaml",
			ok:   true,
		},
		{
			name: "a space, percent-encoded",
			uri:  "file:///home/user/my%20workflows/workflow.yaml",
			want: "/home/user/my workflows/workflow.yaml",
			ok:   true,
		},
		{
			name: "a hash, percent-encoded",
			uri:  "file:///home/user/issue%23123/workflow.yaml",
			want: "/home/user/issue#123/workflow.yaml",
			ok:   true,
		},
		{
			name: "a non-ASCII name, percent-encoded UTF-8",
			uri:  "file:///home/user/caf%C3%A9/workflow.yaml",
			want: "/home/user/café/workflow.yaml",
			ok:   true,
		},
		{
			name: "an explicit localhost authority",
			uri:  "file://localhost/home/user/workflow.yaml",
			want: "/home/user/workflow.yaml",
			ok:   true,
		},
		{
			name: "a genuine remote authority is refused",
			uri:  "file://otherhost/home/user/workflow.yaml",
			want: "",
			ok:   false,
		},
		{
			name: "windows drive with the empty-authority form",
			uri:  "file:///C:/Users/dev/workflow.yaml",
			want: "C:/Users/dev/workflow.yaml",
			ok:   true,
		},
		{
			name: "windows drive parsed as the authority",
			uri:  "file://C:/Users/dev/workflow.yaml",
			want: "C:/Users/dev/workflow.yaml",
			ok:   true,
		},
		{
			name: "an untitled buffer has no path",
			uri:  "untitled:Untitled-1",
			want: "",
			ok:   false,
		},
		{
			name: "a synthesized scheme has no path",
			uri:  "vscode-notebook-cell:/home/user/workflow.yaml",
			want: "",
			ok:   false,
		},
		{
			name: "an empty URI has no path",
			uri:  "",
			want: "",
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			doc := &document{uri: tt.uri}
			got, ok := doc.filesystemPath()

			assert.Equal(t, tt.ok, ok, "filesystemPath(%q) ok", tt.uri)
			assert.Equal(t, tt.want, got, "filesystemPath(%q) path", tt.uri)
		})
	}
}

func TestChangeCanInitializeTheLocalPathIndexBeforeOpen(t *testing.T) {
	t.Parallel()
	var store documentStore
	changedURI := lsp.DocumentURI("file:///tmp/changed.test.yaml")
	changed := store.change(changedURI, 1, []lsp.TextDocumentContentChangeEvent{{Text: validSuite}}, nil)
	require.NotNil(t, changed)

	indexed, ok := store.getByFilesystemPath("/tmp/changed.test.yaml")
	require.True(t, ok)
	assert.Same(t, changed, indexed)

	openedURI := lsp.DocumentURI("file:///tmp/opened.test.yaml")
	opened := store.open(openedURI, 1, validSuite, nil)
	indexed, ok = store.getByFilesystemPath("/tmp/opened.test.yaml")
	require.True(t, ok)
	assert.Same(t, opened, indexed)
}

// TestConcurrentChangesSettleOnTheHigherVersionRegardlessOfParseOrder is the
// regression test for the TOCTOU an independent review of #2089 found in the
// mutex-release fix for #2071: [documentStore.change] read the version guard
// under its first lock and committed under its second with no re-check, so
// two concurrent calls for the same URI — which [NewHandler]'s own doc
// comment says a bare jsonrpc2.AsyncHandler, or this package's own tests,
// can produce without the connection's per-URI queue in the way — could both
// pass the guard against the same base version and then commit in whichever
// order their unlocked parses happened to finish, letting a slower,
// lower-versioned parse overwrite a faster, higher-versioned one that had
// already landed.
//
// [documentStore.parseGate] forces the order this test needs rather than
// hoping a slow parse reliably outruns a fast one across many iterations:
// the lower-versioned change is held at the gate until the higher-versioned
// one has already committed, so releasing it exercises exactly the window
// the commit-time re-check exists for, every run, deterministically (#2089
// review, second round, item b).
func TestConcurrentChangesSettleOnTheHigherVersionRegardlessOfParseOrder(t *testing.T) {
	t.Parallel()

	var store documentStore
	uri := lsp.DocumentURI("file:///concurrent-change-toctou.yaml")
	store.open(uri, 1, raceSource, nil)

	entered := make(chan struct{})
	proceed := make(chan struct{})
	store.setParseGate(func(u lsp.DocumentURI) {
		if u == uri {
			close(entered)
			<-proceed
		}
	})

	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		store.change(uri, 2, []lsp.TextDocumentContentChangeEvent{{Text: raceSource + "#slow\n"}}, nil)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the lower-versioned change never reached the unlocked parse")
	}

	// The higher-versioned change commits next, unimpeded: clearing the
	// gate only affects a call that reads s.parseGate from here on, not the
	// lower-versioned goroutine already parked inside the closure it read
	// earlier.
	store.setParseGate(nil)
	got := store.change(uri, 3, []lsp.TextDocumentContentChangeEvent{{Text: raceSource + "#fast\n"}}, nil)
	require.NotNil(t, got, "the higher-versioned change was itself dropped")
	require.Equal(t, 3, got.version)

	// Release the lower-versioned change now that the higher-versioned one
	// has committed; the commit-time re-check must drop it rather than
	// overwrite what already landed.
	close(proceed)
	select {
	case <-slowDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the lower-versioned change never finished after its gate was released")
	}

	d, ok := store.get(uri)
	require.True(t, ok, "the document is gone")
	require.Equal(t, 3, d.version,
		"a lower-versioned change released after a higher-versioned one had already committed overwrote it")
	require.Equal(t, raceSource+"#fast\n", d.text)
}

// TestChangeDropsAStaleResultWhenTheDocumentWasClosedDuringItsParse is the
// regression test for O1 of the independent review of #2089: the commit-time
// re-check [documentStore.change] added for the TOCTOU above only compared
// versions when a document was still present, so a document closed while an
// in-flight change's splice and parse ran unlocked — the exact window that
// re-check exists for — was silently resurrected by that change's stale
// result once it finally committed, rather than staying closed.
func TestChangeDropsAStaleResultWhenTheDocumentWasClosedDuringItsParse(t *testing.T) {
	t.Parallel()

	var store documentStore
	uri := lsp.DocumentURI("file:///closed-during-parse.yaml")
	store.open(uri, 1, "name: one\n", nil)

	entered := make(chan struct{})
	proceed := make(chan struct{})
	store.setParseGate(func(u lsp.DocumentURI) {
		if u == uri {
			close(entered)
			<-proceed
		}
	})

	changeDone := make(chan struct{})
	go func() {
		defer close(changeDone)
		store.change(uri, 2, []lsp.TextDocumentContentChangeEvent{{Text: "name: two\n"}}, nil)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the gated change never reached the unlocked splice and parse")
	}

	// The document is closed while the change above is parked, unlocked, at
	// the parse gate — exactly the window the commit-time re-check covers.
	store.close(uri)

	close(proceed)
	select {
	case <-changeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the gated change never finished after its gate was released")
	}

	if _, ok := store.get(uri); ok {
		t.Fatal("a document closed during a change's unlocked parse was resurrected by that change's stale result")
	}
}

// TestChangeDropsAnIncrementalEditWhoseBaseDocumentWasReplacedDuringItsParse
// is the regression test for item (a) of the second round of independent
// review of #2089: the commit-time re-check compared versions but never the
// document's identity, so an incremental change whose splice was computed
// against one *document could still land after a close-then-reopen (or any
// other change) installed a different one in its place, corrupting the
// replacement with offsets computed against text that was no longer
// current — for a caller outside the connection's per-URI queue, the same
// caller the TOCTOU test above already covers for versions.
//
// A full-sync change in the same position is exempt, and settles on its own
// text rather than being dropped: it never depended on the replaced
// document's text to begin with, so there is nothing stale in it to guard
// against.
func TestChangeDropsAnIncrementalEditWhoseBaseDocumentWasReplacedDuringItsParse(t *testing.T) {
	t.Parallel()

	uri := lsp.DocumentURI("file:///replaced-during-parse.yaml")

	// gatedIncrementalChange starts apply's store.change on its own
	// goroutine with the store's parse gate held, and returns once that
	// goroutine has reached the gate — before releasing it. t must be the
	// calling subtest's own *testing.T: this runs from inside a parallel
	// t.Run, and by the time that runs, this function's own enclosing test
	// body has already returned, so a t.Fatal here against that outer,
	// already-returned t (rather than the subtest's) would be invalid.
	gatedIncrementalChange := func(t *testing.T, store *documentStore, apply func(store *documentStore)) (proceed, done chan struct{}) {
		t.Helper()

		entered := make(chan struct{})
		proceed = make(chan struct{})
		store.setParseGate(func(u lsp.DocumentURI) {
			if u == uri {
				close(entered)
				<-proceed
			}
		})
		done = make(chan struct{})
		go func() {
			defer close(done)
			apply(store)
		}()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the gated change never reached the unlocked splice and parse")
		}
		return proceed, done
	}

	t.Run("incremental change is dropped", func(t *testing.T) {
		t.Parallel()

		var store documentStore
		store.open(uri, 1, "name: one\n", nil)

		proceed, done := gatedIncrementalChange(t, &store, func(store *documentStore) {
			// An incremental edit computed against "name: one\n": replaces
			// "one" (columns 6-9) with "two".
			store.change(uri, 5, []lsp.TextDocumentContentChangeEvent{{
				Range: &lsp.Range{
					Start: lsp.Position{Line: 0, Character: 6},
					End:   lsp.Position{Line: 0, Character: 9},
				},
				Text: "two",
			}}, nil)
		})

		// Closed and reopened with different content — a new *document —
		// while the incremental change above is parked, unlocked, at the
		// parse gate.
		store.close(uri)
		reopened := store.open(uri, 1, "name: reopened\n", nil)

		close(proceed)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the gated incremental change never finished after its gate was released")
		}

		current, ok := store.get(uri)
		require.True(t, ok, "the reopened document is gone")
		assert.Same(t, reopened, current,
			"an incremental change computed against a since-replaced document overwrote the reopened one")
		assert.Equal(t, "name: reopened\n", current.text)
	})

	t.Run("full-sync change is not dropped", func(t *testing.T) {
		t.Parallel()

		var store documentStore
		store.open(uri, 1, "name: one\n", nil)

		proceed, done := gatedIncrementalChange(t, &store, func(store *documentStore) {
			store.change(uri, 5, []lsp.TextDocumentContentChangeEvent{{Text: "name: full-sync\n"}}, nil)
		})

		store.close(uri)
		store.open(uri, 1, "name: reopened\n", nil)

		close(proceed)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the gated full-sync change never finished after its gate was released")
		}

		current, ok := store.get(uri)
		require.True(t, ok, "the document is gone")
		assert.Equal(t, "name: full-sync\n", current.text,
			"a full-sync change was dropped even though it never depended on the document it was computed alongside")
		assert.Equal(t, 5, current.version)
	})
}
