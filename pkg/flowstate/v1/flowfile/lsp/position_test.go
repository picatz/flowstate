package lsp

import (
	"testing"
	"unicode/utf8"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests exist because the bug they prevent is invisible in ASCII. Every
// position the server sends or receives is in UTF-16 code units, while the YAML
// parser reports code points and Go indexes bytes; a file with one accented
// character silently shifts every position after it on that line.

func TestUTF16Len(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want int
	}{
		{"empty", "", 0},
		{"ascii", "hello", 5},
		{"latin1 supplement is one unit", "ö", 1},
		{"two byte sequence", "héllo", 5},
		{"three byte sequence", "日本語", 3},
		{"astral plane needs a surrogate pair", "🙂", 2},
		{"mixed", "a🙂ö", 4},
		{"combining marks count separately", "é", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, utf16Len(tt.in))
		})
	}
}

func TestLineIndexConversions(t *testing.T) {
	t.Parallel()

	// Each line mixes byte widths so that byte, code point, and UTF-16 columns
	// disagree in different ways.
	const text = "ascii line\n" +
		"héllo wörld\n" +
		"emoji 🙂 here\n" +
		"日本語\n"
	ix := newLineIndex(text)

	t.Run("line count includes the empty final line", func(t *testing.T) {
		// A document ending in a newline has a last, empty line; an editor can
		// legitimately place the cursor there.
		assert.Equal(t, 5, ix.lineCount())
		assert.Equal(t, "", ix.line(4))
	})

	t.Run("out of range lines are empty rather than panicking", func(t *testing.T) {
		assert.Equal(t, "", ix.line(-1))
		assert.Equal(t, "", ix.line(99))
	})

	tests := []struct {
		name  string
		line  int
		bytes int
		// runes and utf16 are the same column expressed in the other two units.
		runes int
		utf16 int
	}{
		{name: "ascii start", line: 0, bytes: 0, runes: 0, utf16: 0},
		{name: "ascii middle", line: 0, bytes: 6, runes: 6, utf16: 6},
		{name: "after a two byte rune", line: 1, bytes: 3, runes: 2, utf16: 2},
		{name: "after two two byte runes", line: 1, bytes: 10, runes: 8, utf16: 8},
		{name: "before an emoji", line: 2, bytes: 6, runes: 6, utf16: 6},
		{name: "after an emoji", line: 2, bytes: 10, runes: 7, utf16: 8},
		{name: "after three three byte runes", line: 3, bytes: 9, runes: 3, utf16: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.utf16, ix.utf16OfByte(tt.line, tt.bytes), "byte to UTF-16")
			assert.Equal(t, tt.bytes, ix.byteOfUTF16(tt.line, tt.utf16), "UTF-16 to byte")
			assert.Equal(t, tt.bytes, ix.byteOfRune(tt.line, tt.runes), "code point to byte")
		})
	}
}

func TestPositionRoundTrip(t *testing.T) {
	t.Parallel()

	const text = "name: ünïcödé\nsteps:\n  - id: 🙂first\n    log: échö\n"
	ix := newLineIndex(text)

	// Every byte offset that begins a rune must survive the trip to an LSP
	// position and back. A mid-rune offset has no position to correspond to.
	for off := range len(text) + 1 {
		if off < len(text) && !utf8.RuneStart(text[off]) {
			continue
		}
		pos := ix.positionOfOffset(off)
		assert.Equal(t, off, ix.offsetOfPosition(pos), "offset %d via %v", off, pos)
	}
}

// TestUTF16OfByteInsideARuneIsTheRunesStart is the named regression for the
// defect [FuzzLSPDocumentEdits] found in CI: a diagnostic whose range ran
// backwards by one, `5:24` to `5:23`, on a line holding an emoji.
//
// The producer had handed [lineIndex.rangeOfOffsets] a forward pair of byte
// offsets with the start three bytes into the emoji. Counting that prefix as
// written made each cut byte a replacement character — three units — while
// the whole emoji, one byte on, is a surrogate pair — two. A larger byte column
// giving a smaller UTF-16 column is what let a forward pair of offsets come out
// as a backwards range, so the property pinned here is monotonicity over every
// byte column of the line, and the specific claim is that a column inside a
// rune counts as that rune's start.
func TestUTF16OfByteInsideARuneIsTheRunesStart(t *testing.T) {
	t.Parallel()

	ix := newLineIndex("a🙂é!")

	// Byte columns 1 through 4 are the emoji; 5 and 6 the two-byte é.
	for byteCol, want := range []int{0, 1, 1, 1, 1, 3, 3, 4, 5} {
		assert.Equal(t, want, ix.utf16OfByte(0, byteCol), "byte column %d", byteCol)
	}

	// The same property over a line the reader cannot decode whole: a lone
	// continuation byte and a truncated sequence. There is no right column for
	// a byte inside those, only the requirement that walking forward through
	// the bytes never walks the column backwards.
	ix = newLineIndex("x\x80y\xf0\x9f\x98z🙂")
	prev := 0
	for byteCol := range len(ix.line(0)) + 1 {
		got := ix.utf16OfByte(0, byteCol)
		assert.GreaterOrEqual(t, got, prev, "byte column %d", byteCol)
		prev = got
	}
}

// TestRangeOfOffsetsIsForwardForAnyForwardPair is the property the regression
// above serves: whatever byte offsets a producer found, in or between runes,
// the range handed to the editor runs forwards.
func TestRangeOfOffsetsIsForwardForAnyForwardPair(t *testing.T) {
	t.Parallel()

	const text = "name: ünïcödé 🙂\nlog: \xffé🙂!\n"
	ix := newLineIndex(text)
	for start := range len(text) + 1 {
		for end := start; end <= len(text); end++ {
			r := ix.rangeOfOffsets(start, end)
			forward := r.Start.Line < r.End.Line ||
				(r.Start.Line == r.End.Line && r.Start.Character <= r.End.Character)
			require.True(t, forward, "offsets %d..%d gave %+v", start, end, r)
		}
	}
}

func TestOffsetOfYAMLMatchesParserColumns(t *testing.T) {
	t.Parallel()

	// The parser reports 1-based lines and 1-based code point columns.
	const text = "name: x\n  héllo: wörld\n"
	ix := newLineIndex(text)

	// Line 2, code point column 5, is the second `l` of héllo — two spaces, then
	// h, é, l — which is one byte further along than the column suggests.
	assert.Equal(t, len("name: x\n  hé"), ix.offsetOfYAML(2, 5))

	// Clamping rather than panicking, since a position can outlive its document.
	assert.Equal(t, len(text), ix.offsetOfYAML(99, 1))
	assert.Equal(t, len("name: x\n"), ix.offsetOfYAML(2, 0))
}

func TestOffsetInExpr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		line int
		col  int
		want int
	}{
		{name: "start", src: "a + b", line: 1, col: 0, want: 0},
		{name: "middle", src: "a + b", line: 1, col: 4, want: 4},
		{name: "past the end clamps", src: "a + b", line: 1, col: 99, want: 5},
		{name: "code point column after non-ascii", src: "'ü' + x", line: 1, col: 5, want: 6},
		{name: "second line", src: "a +\nb", line: 2, col: 0, want: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, offsetInExpr(tt.src, tt.line, tt.col))
		})
	}
}

func TestRangeHelpers(t *testing.T) {
	t.Parallel()

	const text = "name: x\n    indented: value\n"
	ix := newLineIndex(text)

	t.Run("whole line", func(t *testing.T) {
		assert.Equal(t, lsp.Range{
			Start: lsp.Position{Line: 0, Character: 0},
			End:   lsp.Position{Line: 0, Character: 7},
		}, ix.rangeOfLine(0))
	})

	t.Run("line content skips indentation", func(t *testing.T) {
		got := ix.rangeOfLineContent(1)
		assert.Equal(t, 4, got.Start.Character)
		assert.Equal(t, "indented: value", textInRange(text, got))
	})

	t.Run("a blank line falls back to the whole line", func(t *testing.T) {
		blank := newLineIndex("   \n")
		assert.Equal(t, blank.rangeOfLine(0), blank.rangeOfLineContent(0))
	})
}

func TestContains(t *testing.T) {
	t.Parallel()

	rng := lsp.Range{
		Start: lsp.Position{Line: 2, Character: 4},
		End:   lsp.Position{Line: 2, Character: 9},
	}
	tests := []struct {
		name string
		pos  lsp.Position
		want bool
	}{
		{"before the start", lsp.Position{Line: 2, Character: 3}, false},
		{"at the start", lsp.Position{Line: 2, Character: 4}, true},
		{"inside", lsp.Position{Line: 2, Character: 6}, true},
		// The end is inclusive so that pointing at a token's last character still
		// resolves to it, which is what an editor sends when the cursor sits
		// after the final character of a word.
		{"at the end", lsp.Position{Line: 2, Character: 9}, true},
		{"past the end", lsp.Position{Line: 2, Character: 10}, false},
		{"earlier line", lsp.Position{Line: 1, Character: 6}, false},
		{"later line", lsp.Position{Line: 3, Character: 6}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, contains(rng, tt.pos))
		})
	}
}

// TestStoreAppliesIncrementalEdits checks the splice arithmetic directly, including
// across a non-ASCII line where the byte and UTF-16 offsets differ.
func TestStoreAppliesIncrementalEdits(t *testing.T) {
	t.Parallel()

	var store documentStore
	store.open("file:///edit.yaml", 1, "name: ünïcödé\nsteps: []\n", nil)

	// Replace "ünïcödé" with "plain". The name starts at UTF-16 column 6.
	doc := store.change("file:///edit.yaml", 2, []lsp.TextDocumentContentChangeEvent{{
		Range: &lsp.Range{
			Start: lsp.Position{Line: 0, Character: 6},
			End:   lsp.Position{Line: 0, Character: 13},
		},
		Text: "plain",
	}}, nil)
	assert.Equal(t, "name: plain\nsteps: []\n", doc.text)

	// A change with no range replaces everything.
	doc = store.change("file:///edit.yaml", 3, []lsp.TextDocumentContentChangeEvent{{Text: "name: other\n"}}, nil)
	assert.Equal(t, "name: other\n", doc.text)

	// An inverted range is tolerated rather than panicking.
	doc = store.change("file:///edit.yaml", 4, []lsp.TextDocumentContentChangeEvent{{
		Range: &lsp.Range{
			Start: lsp.Position{Line: 0, Character: 8},
			End:   lsp.Position{Line: 0, Character: 2},
		},
		Text: "",
	}}, nil)
	require.NotNil(t, doc)
	assert.NotPanics(t, func() { _ = doc.text })

	store.close("file:///edit.yaml")
	_, ok := store.get("file:///edit.yaml")
	assert.False(t, ok)
}

// TestStoreRejectsStaleEdits covers the ordering guard. The connection's
// AsyncHandler starts a goroutine per message, so two edits can be applied out of
// order; the older one must not win.
func TestStoreRejectsStaleEdits(t *testing.T) {
	t.Parallel()

	var store documentStore
	store.open("file:///stale.yaml", 1, "name: one\n", nil)

	newer := store.change("file:///stale.yaml", 5, []lsp.TextDocumentContentChangeEvent{{Text: "name: five\n"}}, nil)
	require.NotNil(t, newer)
	assert.Equal(t, "name: five\n", newer.text)

	// An edit that arrives late is dropped rather than reverting the document.
	assert.Nil(t, store.change("file:///stale.yaml", 3, []lsp.TextDocumentContentChangeEvent{{Text: "name: three\n"}}, nil))
	// The same version twice is also stale: it has already been applied.
	assert.Nil(t, store.change("file:///stale.yaml", 5, []lsp.TextDocumentContentChangeEvent{{Text: "name: other\n"}}, nil))

	current, ok := store.get("file:///stale.yaml")
	require.True(t, ok)
	assert.Equal(t, "name: five\n", current.text)

	// A client that does not track versions still gets last-write-wins, because
	// there is nothing to order by.
	var untracked documentStore
	untracked.open("file:///untracked.yaml", 0, "name: one\n", nil)
	got := untracked.change("file:///untracked.yaml", 0, []lsp.TextDocumentContentChangeEvent{{Text: "name: two\n"}}, nil)
	require.NotNil(t, got)
	assert.Equal(t, "name: two\n", got.text)

	// Reopening resets the version, since an editor may close and reopen a file
	// — and closing is what makes it a reopen. The protocol requires didClose
	// before a document is opened again, and close removes it from the store, so
	// this open has no incumbent to be ordered against.
	store.close("file:///stale.yaml")
	reopened := store.open("file:///stale.yaml", 1, "name: reopened\n", nil)
	assert.Equal(t, "name: reopened\n", reopened.text)
}

// TestStoreIgnoresARangeEditWithNothingToSpliceInto covers the case that makes
// ordering an open against the stored version safe for an incremental client.
//
// A range is computed against text the client believes the server holds.
// Applying one to an empty string does not reproduce that text — the clamps
// leave the replacement alone — and storing that at the edit's version would
// outrank the didOpen still on its way, so the guard in open would then keep a
// truncated buffer for good rather than letting the open repair it. Ignoring
// the edit leaves the open to land the real text.
func TestStoreIgnoresARangeEditWithNothingToSpliceInto(t *testing.T) {
	t.Parallel()

	full := "steps:\n  - id: one\n"
	var store documentStore
	ignored := store.change("file:///splice.yaml", 2, []lsp.TextDocumentContentChangeEvent{{
		Range: &lsp.Range{
			Start: lsp.Position{Line: 1, Character: 8},
			End:   lsp.Position{Line: 1, Character: 11},
		},
		Text: "two",
	}}, nil)
	assert.Nil(t, ignored, "a range edit was spliced into a document the store does not have")
	if _, ok := store.get("file:///splice.yaml"); ok {
		t.Fatal("a range edit with no base created a document")
	}

	// The open that carries the real text still lands it.
	opened := store.open("file:///splice.yaml", 1, full, nil)
	require.NotNil(t, opened)
	assert.Equal(t, full, opened.text, "the open did not recover the document")

	// And an edit after it applies to that text rather than to nothing.
	edited := store.change("file:///splice.yaml", 2, []lsp.TextDocumentContentChangeEvent{{
		Range: &lsp.Range{
			Start: lsp.Position{Line: 1, Character: 8},
			End:   lsp.Position{Line: 1, Character: 11},
		},
		Text: "two",
	}}, nil)
	require.NotNil(t, edited)
	assert.Equal(t, "steps:\n  - id: two\n", edited.text)

	// A change with no range needs no base, and is the sync kind this server
	// advertises, so it still applies.
	var whole documentStore
	replaced := whole.change("file:///whole.yaml", 1,
		[]lsp.TextDocumentContentChangeEvent{{Text: full}}, nil)
	require.NotNil(t, replaced, "a full-text change with no base was ignored")
	assert.Equal(t, full, replaced.text)

	// The test is whether a full replacement arrives, not whether a range
	// does. An empty change set carries no range and still establishes
	// nothing: applying it would store an empty document at the edit's
	// version, which outranks the didOpen still on its way and leaves the
	// buffer empty for good — the same permanent failure as the ranged case,
	// reached through a different door.
	for _, empty := range [][]lsp.TextDocumentContentChangeEvent{{}, nil} {
		var store documentStore
		assert.Nil(t, store.change("file:///empty.yaml", 5, empty, nil),
			"a change set establishing no text created a document")
		if _, ok := store.get("file:///empty.yaml"); ok {
			t.Fatal("a change set establishing no text created a document")
		}
		opened := store.open("file:///empty.yaml", 1, full, nil)
		require.NotNil(t, opened)
		assert.Equal(t, full, opened.text, "the open did not recover the document")
	}

	// And the converse: a set whose trailing entry replaces everything does
	// establish the text, however it begins, because that replacement makes
	// whatever preceded it irrelevant.
	var mixed documentStore
	both := mixed.change("file:///mixed.yaml", 3, []lsp.TextDocumentContentChangeEvent{
		{
			Range: &lsp.Range{
				Start: lsp.Position{Line: 0, Character: 0},
				End:   lsp.Position{Line: 0, Character: 3},
			},
			Text: "ignored",
		},
		{Text: full},
	}, nil)
	require.NotNil(t, both, "a change set ending in a full replacement was ignored")
	assert.Equal(t, full, both.text)
}

// TestStoreRejectsAnOvertakenOpen covers the same ordering guard from the other
// side. AsyncHandler starts a goroutine per message, so a didOpen can be
// scheduled behind a didChange for the same document; the open must not revert
// the edit. Before the guard, this left an editor that opened a file, took the
// first keystrokes, and then silently answered every read from the pre-edit
// text until the next keystroke landed.
func TestStoreRejectsAnOvertakenOpen(t *testing.T) {
	t.Parallel()

	// The change arriving first is the ordering under test: change tolerates it,
	// starting from empty text and taking the full-text replacement, so the edit
	// is applied and would then be discarded by the open.
	var store documentStore
	edited := store.change("file:///overtaken.yaml", 4, []lsp.TextDocumentContentChangeEvent{{Text: "name: edited\n"}}, nil)
	require.NotNil(t, edited)

	overtaken := store.open("file:///overtaken.yaml", 1, "name: opened\n", nil)
	require.NotNil(t, overtaken)
	assert.Equal(t, "name: edited\n", overtaken.text, "an open behind a change reverted the document")
	assert.Equal(t, 4, overtaken.version, "an open behind a change reverted the version")

	// The store agrees with what the open returned, so a caller publishing
	// diagnostics from the return value and a later request reading the store
	// cannot disagree about the text.
	current, ok := store.get("file:///overtaken.yaml")
	require.True(t, ok)
	assert.Same(t, overtaken, current)

	// The path index is registered on this path too: a request blocked on the
	// build gate is waiting for this call, and an overtaken open still releases
	// it rather than leaving it waiting.
	indexed, ok := store.getByFilesystemPath("/overtaken.yaml")
	require.True(t, ok)
	assert.Same(t, current, indexed)

	// An open that is not overtaken still opens, so the guard does not strand a
	// client on text the store happens to hold.
	fresh := store.open("file:///fresh.yaml", 1, "name: fresh\n", nil)
	require.NotNil(t, fresh)
	assert.Equal(t, "name: fresh\n", fresh.text)

	// Zero is a legal document version, not a sentinel. A compliant client may
	// open at zero and edit to one, and that open must not revert the edit
	// either — what cannot be ordered against is a *stored* version of zero.
	var zeroOpen documentStore
	zeroOpen.change("file:///zero.yaml", 1, []lsp.TextDocumentContentChangeEvent{{Text: "name: edited\n"}}, nil)
	late := zeroOpen.open("file:///zero.yaml", 0, "name: opened\n", nil)
	require.NotNil(t, late)
	assert.Equal(t, "name: edited\n", late.text, "an open at version zero reverted a later edit")

	// A client that does not track versions keeps last-write-wins, the same
	// tolerance change has, because the stored document carries nothing to
	// order by.
	var untracked documentStore
	untracked.change("file:///untracked.yaml", 0, []lsp.TextDocumentContentChangeEvent{{Text: "name: one\n"}}, nil)
	reopened := untracked.open("file:///untracked.yaml", 0, "name: two\n", nil)
	require.NotNil(t, reopened)
	assert.Equal(t, "name: two\n", reopened.text)
}

func TestNewLineIndexHandlesNoTrailingNewline(t *testing.T) {
	t.Parallel()

	ix := newLineIndex("one\ntwo")
	require.Equal(t, 2, ix.lineCount())
	assert.Equal(t, "two", ix.line(1))
	assert.Equal(t, 7, ix.offsetOfPosition(lsp.Position{Line: 1, Character: 3}))
}
