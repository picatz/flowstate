package lsp

import (
	"sort"

	"github.com/sourcegraph/go-lsp"
)

// Four coordinate systems meet in this package and no two of them agree:
//
//   - LSP counts a line's characters in UTF-16 code units, 0-based.
//   - The YAML parser reports 1-based lines and 1-based columns counted in
//     Unicode code points.
//   - CEL reports 1-based lines and 0-based columns, also in code points.
//   - Go indexes strings by byte.
//
// For ASCII all four coincide, which is exactly why confusing them goes
// unnoticed: a single non-ASCII character in a string value silently shifts
// every position reported for the rest of that line, and an editor then
// underlines the wrong text or refuses the edit outright. All conversion happens
// in this file, so no other file has to remember which unit it is holding.

// A lineIndex splits a document into lines once, so positions can be converted
// between byte, code point, and UTF-16 offsets without rescanning the text.
type lineIndex struct {
	text string

	// lines holds each line's text excluding its terminator, and starts holds
	// the byte offset in text where each line begins. Both always have at
	// least one element, so an empty document still has line 0.
	lines  []string
	starts []int
}

// newLineIndex indexes the lines of text.
func newLineIndex(text string) *lineIndex {
	ix := &lineIndex{text: text}
	start := 0
	for i := range len(text) {
		if text[i] == '\n' {
			ix.lines = append(ix.lines, text[start:i])
			ix.starts = append(ix.starts, start)
			start = i + 1
		}
	}
	ix.lines = append(ix.lines, text[start:])
	ix.starts = append(ix.starts, start)
	return ix
}

// lineCount returns the number of lines, which is always at least one.
func (ix *lineIndex) lineCount() int { return len(ix.lines) }

// line returns the text of a 0-based line without its terminator.
//
// An out-of-range line yields the empty string rather than panicking, because
// positions can arrive from an editor that has already applied an edit the
// server has not seen yet.
func (ix *lineIndex) line(n int) string {
	if n < 0 || n >= len(ix.lines) {
		return ""
	}
	return ix.lines[n]
}

// utf16Len returns the number of UTF-16 code units s encodes, which is the unit
// an LSP character offset counts.
func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		n++
		if r > 0xFFFF {
			// Outside the basic multilingual plane a rune needs a surrogate
			// pair, so it occupies two UTF-16 code units.
			n++
		}
	}
	return n
}

// byteOfRune returns the byte column of a 0-based code point column on a line,
// clamped to the end of the line.
func (ix *lineIndex) byteOfRune(line0, runeCol int) int {
	l := ix.line(line0)
	if runeCol <= 0 {
		return 0
	}
	n := 0
	for i := range l {
		if n == runeCol {
			return i
		}
		n++
	}
	return len(l)
}

// byteOfUTF16 returns the byte column of a 0-based UTF-16 column on a line,
// clamped to the end of the line.
func (ix *lineIndex) byteOfUTF16(line0, utf16Col int) int {
	l := ix.line(line0)
	if utf16Col <= 0 {
		return 0
	}
	units := 0
	for i, r := range l {
		if units >= utf16Col {
			return i
		}
		units++
		if r > 0xFFFF {
			units++
		}
	}
	return len(l)
}

// utf16OfByte returns the UTF-16 column of a byte column on a line.
func (ix *lineIndex) utf16OfByte(line0, byteCol int) int {
	l := ix.line(line0)
	byteCol = min(max(byteCol, 0), len(l))
	return utf16Len(l[:byteCol])
}

// offsetOfYAML returns the absolute byte offset of a position as the YAML parser
// reports it: a 1-based line and a 1-based code point column.
func (ix *lineIndex) offsetOfYAML(line1, col1 int) int {
	line0 := max(line1-1, 0)
	if line0 >= len(ix.starts) {
		return len(ix.text)
	}
	return ix.starts[line0] + ix.byteOfRune(line0, max(col1-1, 0))
}

// offsetInExpr returns the byte offset within src of a location as CEL reports
// it: a 1-based line and a 0-based code point column.
//
// The walk is over the expression source rather than the document, because an
// expression's second line and the document's second line are not the same
// thing — the expression may start part-way into a document line, and mapping
// through the document would misplace every column on its first line.
func offsetInExpr(src string, line1, col0 int) int {
	line, col := 1, 0
	for i, r := range src {
		if line == line1 && col == col0 {
			return i
		}
		if r == '\n' {
			line++
			col = 0
			continue
		}
		col++
	}
	return len(src)
}

// positionOfOffset converts an absolute byte offset into an LSP position.
func (ix *lineIndex) positionOfOffset(off int) lsp.Position {
	off = min(max(off, 0), len(ix.text))
	// The line containing off is the last one starting at or before it.
	line0 := sort.Search(len(ix.starts), func(i int) bool { return ix.starts[i] > off }) - 1
	line0 = max(line0, 0)
	return lsp.Position{Line: line0, Character: ix.utf16OfByte(line0, off-ix.starts[line0])}
}

// offsetOfPosition converts an LSP position into an absolute byte offset.
func (ix *lineIndex) offsetOfPosition(p lsp.Position) int {
	if p.Line < 0 {
		return 0
	}
	if p.Line >= len(ix.starts) {
		return len(ix.text)
	}
	return ix.starts[p.Line] + ix.byteOfUTF16(p.Line, p.Character)
}

// rangeOfOffsets converts a byte offset span into an LSP range.
func (ix *lineIndex) rangeOfOffsets(start, end int) lsp.Range {
	if end < start {
		end = start
	}
	return lsp.Range{Start: ix.positionOfOffset(start), End: ix.positionOfOffset(end)}
}

// rangeOfLine returns the range covering a whole 0-based line, which is the best
// available position for a problem known only by line number.
func (ix *lineIndex) rangeOfLine(line0 int) lsp.Range {
	line0 = min(max(line0, 0), len(ix.lines)-1)
	return lsp.Range{
		Start: lsp.Position{Line: line0, Character: 0},
		End:   lsp.Position{Line: line0, Character: utf16Len(ix.line(line0))},
	}
}

// rangeOfLineContent returns the range covering a line's content, excluding its
// leading indentation, so a whole-line diagnostic does not underline blank space.
func (ix *lineIndex) rangeOfLineContent(line0 int) lsp.Range {
	line0 = min(max(line0, 0), len(ix.lines)-1)
	l := ix.line(line0)
	indent := 0
	for indent < len(l) && (l[indent] == ' ' || l[indent] == '\t') {
		indent++
	}
	if indent == len(l) {
		return ix.rangeOfLine(line0)
	}
	return lsp.Range{
		Start: lsp.Position{Line: line0, Character: ix.utf16OfByte(line0, indent)},
		End:   lsp.Position{Line: line0, Character: utf16Len(l)},
	}
}

// documentStart is the range every editor accepts for a problem with no position
// of its own, such as a document that is too large to analyze.
var documentStart = lsp.Range{
	Start: lsp.Position{Line: 0, Character: 0},
	End:   lsp.Position{Line: 0, Character: 0},
}

// contains reports whether pos falls within r, treating the end as inclusive so
// that hovering the last character of a token still resolves to it.
func contains(r lsp.Range, pos lsp.Position) bool {
	if pos.Line < r.Start.Line || pos.Line > r.End.Line {
		return false
	}
	if pos.Line == r.Start.Line && pos.Character < r.Start.Character {
		return false
	}
	if pos.Line == r.End.Line && pos.Character > r.End.Character {
		return false
	}
	return true
}
