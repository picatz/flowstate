package flowdap

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// TestAPauseRefusalSaysHowTheRunEnded: a completed run refuses a pause in the
// words both drivers use for it, and anything else says what ended instead
// (Codex, #2220).
func TestAPauseRefusalSaysHowTheRunEnded(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		state v1.DebugRunState
		exit  int
		want  string
	}{
		{v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, 0, flowdebug.MissedPauseNotice},
		{v1.DebugRunState_DEBUG_RUN_STATE_UNSPECIFIED, 0, flowdebug.MissedPauseNotice},
		{v1.DebugRunState_DEBUG_RUN_STATE_UNSPECIFIED, 1, "the run ended before it reached a step boundary to pause at"},
		{v1.DebugRunState_DEBUG_RUN_STATE_FAILED, 1, "the run ended before it reached a step boundary to pause at"},
		{v1.DebugRunState_DEBUG_RUN_STATE_DETACHED, 0, "the debug session ended before the run reached a step boundary to pause at"},
		{v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED, 1, "the debug session ended before the run reached a step boundary to pause at"},
	} {
		assert.Equal(t, test.want, pauseRefusal(test.state, test.exit), "%s, exit %d", test.state, test.exit)
	}
}
