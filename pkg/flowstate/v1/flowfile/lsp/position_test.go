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

	const text = "name: ünïcödé\nsteps:\n  - id: 🙂first\n    echo: échö\n"
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
	store.open("file:///edit.yaml", 1, "name: ünïcödé\nsteps: []\n")

	// Replace "ünïcödé" with "plain". The name starts at UTF-16 column 6.
	doc := store.change("file:///edit.yaml", 2, []lsp.TextDocumentContentChangeEvent{{
		Range: &lsp.Range{
			Start: lsp.Position{Line: 0, Character: 6},
			End:   lsp.Position{Line: 0, Character: 13},
		},
		Text: "plain",
	}})
	assert.Equal(t, "name: plain\nsteps: []\n", doc.text)

	// A change with no range replaces everything.
	doc = store.change("file:///edit.yaml", 3, []lsp.TextDocumentContentChangeEvent{{Text: "name: other\n"}})
	assert.Equal(t, "name: other\n", doc.text)

	// An inverted range is tolerated rather than panicking.
	doc = store.change("file:///edit.yaml", 4, []lsp.TextDocumentContentChangeEvent{{
		Range: &lsp.Range{
			Start: lsp.Position{Line: 0, Character: 8},
			End:   lsp.Position{Line: 0, Character: 2},
		},
		Text: "",
	}})
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
	store.open("file:///stale.yaml", 1, "name: one\n")

	newer := store.change("file:///stale.yaml", 5, []lsp.TextDocumentContentChangeEvent{{Text: "name: five\n"}})
	require.NotNil(t, newer)
	assert.Equal(t, "name: five\n", newer.text)

	// An edit that arrives late is dropped rather than reverting the document.
	assert.Nil(t, store.change("file:///stale.yaml", 3, []lsp.TextDocumentContentChangeEvent{{Text: "name: three\n"}}))
	// The same version twice is also stale: it has already been applied.
	assert.Nil(t, store.change("file:///stale.yaml", 5, []lsp.TextDocumentContentChangeEvent{{Text: "name: other\n"}}))

	current, ok := store.get("file:///stale.yaml")
	require.True(t, ok)
	assert.Equal(t, "name: five\n", current.text)

	// A client that does not track versions still gets last-write-wins, because
	// there is nothing to order by.
	var untracked documentStore
	untracked.open("file:///untracked.yaml", 0, "name: one\n")
	got := untracked.change("file:///untracked.yaml", 0, []lsp.TextDocumentContentChangeEvent{{Text: "name: two\n"}})
	require.NotNil(t, got)
	assert.Equal(t, "name: two\n", got.text)

	// Reopening resets the version, since an editor may close and reopen a file.
	reopened := store.open("file:///stale.yaml", 1, "name: reopened\n")
	assert.Equal(t, "name: reopened\n", reopened.text)
}

func TestNewLineIndexHandlesNoTrailingNewline(t *testing.T) {
	t.Parallel()

	ix := newLineIndex("one\ntwo")
	require.Equal(t, 2, ix.lineCount())
	assert.Equal(t, "two", ix.line(1))
	assert.Equal(t, 7, ix.offsetOfPosition(lsp.Position{Line: 1, Character: 3}))
}
