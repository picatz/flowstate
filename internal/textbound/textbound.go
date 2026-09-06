// Package textbound bounds text that another party chose before it reaches an
// error message, a log line, or a response.
//
// A plugin's stderr, a handshake line, a token claim, a relying party's error
// body, a task's own answer, a review thread: each is text some other process
// wrote, and an error naming what was wrong with it must not be able to carry
// a megabyte of that process's choosing. The same rule was written in ten
// places in this repository under three cut rules; this package is the one
// copy.
//
// Every result is valid UTF-8. A byte cut through a multi-byte sequence
// produces invalid UTF-8, which a proto3 string field will not hold and
// protojson refuses to encode at all — so one overlong value would fail a
// whole response's marshalling rather than shorten its own sentence, and a
// broken rune in a log line is a line some consumer will refuse to parse. Text
// another process chose is exactly where a sequence straddling the cut is
// likely rather than hypothetical.
package textbound

import (
	"strings"
	"unicode/utf8"
)

// Cut bounds s to at most limit bytes without splitting a rune and drops any
// bytes that are not valid UTF-8, so the result is always valid. It carries no
// marker: a caller that states the cut in its own words appends its own.
//
// Only the bounded prefix is examined, so the work is proportional to limit
// rather than to len(s). A limit below zero is treated as zero.
func Cut(s string, limit int) string {
	if limit < 0 {
		limit = 0
	}
	if len(s) > limit {
		start := limit
		for start > 0 && !utf8.RuneStart(s[start]) {
			start--
		}
		// Only a rune that begins before the limit and ends past it straddles
		// the cut. Continuation bytes that do not complete one are already
		// invalid, and the sanitizing pass below drops them either way.
		if _, size := utf8.DecodeRuneInString(s[start:]); start+size > limit {
			limit = start
		}
		s = s[:limit]
	}
	return strings.ToValidUTF8(s, "")
}

// Truncate is [Cut] with "..." appended when s was longer than limit. A string
// within the limit is returned unchanged apart from invalid bytes, which are
// dropped.
func Truncate(s string, limit int) string {
	cut := Cut(s, limit)
	if len(s) <= max(limit, 0) {
		return cut
	}
	return cut + "..."
}
