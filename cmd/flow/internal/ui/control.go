package ui

import (
	"strconv"
	"strings"
	"unicode"
)

// EscapeControl renders control characters as their escaped spelling, for text
// a terminal is about to be handed and this process did not write.
//
// A workload's failure message, a signal's name, a task's error — these are
// chosen by whoever ran the workload, and a line printed from one is a line
// somebody else composed. Passed through bare, a newline in it fabricates rows
// that look like the command's own output, a tab breaks the column alignment a
// reader is scanning down, and an ANSI escape restyles or clears the terminal.
// A `flow timeline` row promising one event per line, or a `flow get` line
// promising one retrying step, is a promise the text can otherwise break.
//
// Text only. A machine-readable answer (`-o json`) carries the value as it is,
// because a consumer parsing JSON is not a terminal interpreting bytes, and
// escaping there would hand back something that is not what the run produced.
//
// # Why this is written twice
//
// `flowtest`'s `escapeControlRunes` is the same rule at the same threat, added
// for the same reason (Codex, #1052), and this is not a second opinion about
// it. It is a second *copy*, because there is no import direction that would
// let one serve both: `pkg/flowstate/v1/flowtest` is a library and cannot reach
// a package internal to `cmd/flow`, and a CLI terminal-rendering helper does
// not belong in the public API of the test harness. Both are exercised against
// the shapes that matter — a newline, a tab, an escape — so a change to one
// that the other does not follow shows up as a test that disagrees with its
// sibling rather than as a surface that quietly stopped escaping.
func EscapeControl(s string) string {
	if !strings.ContainsFunc(s, unicode.IsControl) {
		return s
	}

	var b strings.Builder
	b.Grow(len(s))

	for _, r := range s {
		if unicode.IsControl(r) {
			// Through strconv so the spelling is Go's own (\n, \t, \x1b)
			// rather than a table this has to keep, with the surrounding
			// quotes trimmed off because the rune is going inside a line.
			quoted := strconv.QuoteRune(r)
			b.WriteString(quoted[1 : len(quoted)-1])

			continue
		}
		b.WriteRune(r)
	}

	return b.String()
}
