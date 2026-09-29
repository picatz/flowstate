package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// TestDebugGetSaysAMissedUntilOnce: `flow debug get` prints the snapshot and
// then lists its observations. The missed-`until` notice is one the snapshot
// already prints, so it is not listed again (exact-head review, #2204), while
// every other observation still is.
func TestDebugGetSaysAMissedUntilOnce(t *testing.T) {
	t.Parallel()

	notice := flowdebug.MissedUntilNotice("settle")
	out := formatDebugGet(&v1.DebugSnapshot{
		State: v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED,
		Observations: []*v1.DebugObservation{
			{Kind: v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED, Text: "first finished"},
			{Kind: v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_NOTICE, Text: notice},
		},
	})

	assert.Equal(t, 1, strings.Count(out, notice), "the notice was printed more than once, or not at all: %q", out)
	assert.Contains(t, out, "  · first finished\n", "an ordinary observation was dropped from the list")
}
