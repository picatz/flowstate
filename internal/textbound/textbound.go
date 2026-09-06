// Package textbound bounds text that another party chose before it reaches an
// error message or a log line.
//
// A plugin's stderr, a handshake line, a token claim, a relying party's error
// body, a review thread: each is text some other process wrote, and an error
// naming what was wrong with it must not be able to carry a megabyte of that
// process's choosing. The same rule was written five times in this repository
// with three different cut rules; this package is the one copy.
package textbound

import "unicode/utf8"

// Truncate bounds s to at most limit bytes of its own text, marking a cut with
// "...". A string within the limit is returned unchanged.
//
// It cuts on a rune boundary. Everything this bounds was chosen by another
// process, so cutting mid-rune is not hypothetical, and a broken rune in a log
// line is a log line some consumer will refuse to parse. The cut therefore
// walks back to the start of the rune straddling the limit rather than
// emitting its leading bytes.
//
// A limit below zero is treated as zero: the result is then the marker alone.
func Truncate(s string, limit int) string {
	if limit < 0 {
		limit = 0
	}
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	return s[:limit] + "..."
}
