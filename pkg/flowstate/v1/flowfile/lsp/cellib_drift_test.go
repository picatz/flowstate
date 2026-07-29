package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestEveryCELLibraryHasASummary covers the hand-written half of this file.
//
// The set of libraries is not at risk and should not be tested as if it were:
// `celLibraries` iterates `v1.ExtensionLibraries()`, and the functions each one
// provides are computed by diffing environments. Both are derived, which is why
// the editor has never offered a library the evaluator would refuse.
//
// `librarySummaries` is the exception — one sentence per library, written by
// hand, and the only part of a hover a schema cannot produce. A library added to
// the evaluator therefore arrives in the editor already: completion offers it,
// hover names it and lists the functions it provides. What it lacks is the line
// saying what it is *for*, and a blank where an explanation belongs reads as a
// broken editor rather than a missing sentence.
//
// So this is a small test about a small gap, which is the honest description of
// it. The README and the schema comment kept lists that had actually drifted —
// nine names and eight against the evaluator's eleven, both missing `json`, which
// a shipped example uses. This file kept none, and that is the design working.
func TestEveryCELLibraryHasASummary(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, librarySummaries, "no summaries; this test is checking nothing")

	accepted := v1.ExtensionLibraries()
	require.NotEmpty(t, accepted, "the evaluator offers no libraries; this test is checking nothing")

	for _, name := range accepted {
		summary, described := librarySummaries[name]
		assert.True(t, described,
			"CEL library %q has no summary\n"+
				"  completion already offers it and hover already names it, so what an author sees is a\n"+
				"  library with a blank where the explanation goes; add a line to librarySummaries", name)
		assert.NotEmpty(t, summary, "CEL library %q has an empty summary", name)
	}

	// The other direction. A summary for a library the evaluator does not accept
	// is a sentence nobody can reach, and it outlives the removal that made it
	// dead — so it is the copy most likely to still be there years later.
	for name := range librarySummaries {
		assert.Contains(t, accepted, name,
			"there is a summary for CEL library %q, which the evaluator does not accept", name)
	}
}
