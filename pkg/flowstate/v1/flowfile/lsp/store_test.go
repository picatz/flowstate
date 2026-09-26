package lsp

import (
	"strings"
	"sync"
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
// One change's text is large enough that its parse reliably takes longer
// than the other's, so the two are not a coin flip: the failure mode is a
// scheduling race, not a 50/50 one, and repeating it drives the odds of
// never observing it on a fixed build to negligible while keeping a broken
// one's failure rate visible well within a short run.
func TestConcurrentChangesSettleOnTheHigherVersionRegardlessOfParseOrder(t *testing.T) {
	t.Parallel()

	slow := raceSource + strings.Repeat("#padding\n", 15000)
	uri := lsp.DocumentURI("file:///concurrent-change-toctou.yaml")

	bad := 0
	for range 200 {
		var s documentStore
		s.open(uri, 1, raceSource, nil)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.change(uri, 2, []lsp.TextDocumentContentChangeEvent{{Text: slow}}, nil)
		}()
		go func() {
			defer wg.Done()
			s.change(uri, 3, []lsp.TextDocumentContentChangeEvent{{Text: raceSource}}, nil)
		}()
		wg.Wait()

		if d, ok := s.get(uri); !ok || d.version != 3 {
			bad++
		}
	}
	require.Zero(t, bad,
		"a slower, lower-versioned parse overwrote a faster, higher-versioned one that had already committed")
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
