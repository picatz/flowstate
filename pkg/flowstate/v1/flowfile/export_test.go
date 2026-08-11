package flowfile

// MergeRefusalsForTest exposes mergeRefusals so the union finish applies can be
// pinned directly: no rewrite today produces a refusable shape from a clean
// one, so the merge cannot yet be reached through Fix with a real document.
func MergeRefusalsForTest(first, later []Diagnostic) []Diagnostic {
	return mergeRefusals(first, later)
}
