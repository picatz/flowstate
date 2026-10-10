package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestTheBacklogSummaryParserIsPinnedToTheWriter(t *testing.T) {
	t.Parallel()

	lease := &v1.DebugSession{SessionId: "run-1/debug/0"}
	session, ok := ParseDebugBacklogSummary(debugBacklogSummary(lease))
	require.True(t, ok)
	assert.Equal(t, "run-1/debug/0", session)

	for _, label := range []string{debugLeaseSummary(lease), "debug lease  pacing a backlog of asks", "debug lease a b pacing a backlog of asks", "x"} {
		_, ok := ParseDebugBacklogSummary(label)
		assert.False(t, ok, "%q", label)
	}

	_, _, ok = ParseDebugLeaseSummary("debug lease s\x00 held by x expires")
	assert.False(t, ok, "a control character cannot be part of a session token")
}
