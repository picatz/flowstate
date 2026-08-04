package main

import "strconv"

// itoaLen renders a count for a summary string. A tiny wrapper so
// exec.go's event summaries do not import strconv on their own for one call
// site each.
func itoaLen(n int) string {
	return strconv.Itoa(n)
}

// truncateRunes bounds a string to at most n bytes, cut on a rune boundary,
// for text going into an error message or a log line. It never reports
// whether it truncated - callers that need that use truncateBytes.
func truncateRunes(s string, n int) string {
	out, _ := truncateBytes(s, n)
	return out
}

// truncateBytes bounds a string to n bytes on a rune boundary, reporting
// whether it cut anything off - the same shape and reasoning as
// plugins/vcs/diff.go's own truncateBytes, copied rather than imported
// because plugins/vcs and plugins/codex are separate modules by design (see
// this plugin's go.mod).
func truncateBytes(s string, n int) (string, bool) {
	if len(s) <= n {
		return s, false
	}
	for n > 0 && !isRuneStart(s[n]) {
		n--
	}
	return s[:n], true
}

// isRuneStart reports whether b is not a UTF-8 continuation byte.
func isRuneStart(b byte) bool {
	return b&0xC0 != 0x80
}
