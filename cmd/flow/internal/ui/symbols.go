package ui

// Symbols are typographic marks, never emoji.
//
// Emoji are excluded on purpose and the reasons are practical rather than
// aesthetic. They render at inconsistent widths, so a column that contains one is
// a column that no longer lines up; support varies enough that a fallback glyph in
// the middle of a status line is a real outcome; screen readers announce them
// unpredictably and at length; and they carry tone into text whose whole job is to
// report a fact. A workload that failed at four in the morning is not improved by
// a picture of a fire.
//
// What is here instead is a small set of restrained marks, each with an ASCII
// fallback chosen so the line still parses at a glance without it. A mark is
// always decoration for a label and never a replacement for one: the status is the
// word `RUNNING`, and the mark beside it only helps the eye find the row.

// SymbolSet is the marks one stream may use.
//
// A value rather than package state, because two streams of one process can have
// different answers — a piped stdout and a terminal stderr — and a package-level
// set would force one of them to be wrong.
type SymbolSet struct {
	// Outcome marks, for a line reporting how something went.
	Success string
	Failure string
	Warning string
	Waiting string
	Running string
	Skipped string

	// Structure marks, for relating one line to another.
	Bullet   string
	Arrow    string
	Ellipsis string

	// Divider fills a horizontal rule.
	Divider string
}

// unicodeSymbols is the preferred set: geometric marks that share a visual weight
// and, importantly, are all single-width, so a column of them stays a column.
var unicodeSymbols = SymbolSet{
	Success:  "✓", // ✓ check mark
	Failure:  "✗", // ✗ ballot x
	Warning:  "△", // △ white up-pointing triangle
	Waiting:  "○", // ○ white circle
	Running:  "▶", // ▶ black right-pointing triangle
	Skipped:  "—", // — em dash
	Bullet:   "•", // • bullet
	Arrow:    "→", // → rightwards arrow
	Ellipsis: "…", // … horizontal ellipsis
	Divider:  "─", // ─ box drawings light horizontal
}

// asciiSymbols is what a pipe, a CI log, and a terminal that cannot be measured
// receive.
//
// Every mark is one column wide here too, so switching sets never changes a
// layout — a table rendered with one set and read with the other still lines up,
// which matters because golden output in a test is compared against a terminal's.
var asciiSymbols = SymbolSet{
	Success:  "+",
	Failure:  "x",
	Warning:  "!",
	Running:  ">",
	Waiting:  "o",
	Skipped:  "-",
	Bullet:   "*",
	Arrow:    ">",
	Ellipsis: "...",
	Divider:  "-",
}

// Symbols returns the set this stream may use.
func (c Capabilities) Symbols() SymbolSet {
	if c.Unicode {
		return unicodeSymbols
	}
	return asciiSymbols
}

// Mark returns the symbol standing for an outcome tone.
//
// Neutral has no mark of its own: a fact with no valence should not be decorated
// as though it had one, and a bullet would imply a list that is not there.
func (s SymbolSet) Mark(tone Tone) string {
	switch tone {
	case ToneSuccess:
		return s.Success
	case ToneDanger:
		return s.Failure
	case ToneWarning:
		return s.Warning
	case ToneInfo:
		return s.Running
	default:
		return s.Bullet
	}
}
