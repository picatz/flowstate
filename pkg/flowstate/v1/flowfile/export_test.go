package flowfile

// MergeRefusalsForTest exposes mergeRefusals so the union finish applies can be
// pinned directly: no rewrite today produces a refusable shape from a clean
// one, so the merge cannot yet be reached through Fix with a real document.
func MergeRefusalsForTest(first, later []Diagnostic) []Diagnostic {
	return mergeRefusals(first, later)
}

// CELLastTokenForTest exposes celLastToken so the line-and-column walk can be
// pinned on inputs a Flowfile makes awkward to reach: a line the source does not
// have, and a multi-byte character before the reported position. Both are
// positions cel-go can report and neither is worth a fixture file.
func CELLastTokenForTest(src string, line, column int) string {
	return celLastToken(src, line, column)
}
