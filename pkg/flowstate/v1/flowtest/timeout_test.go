package flowtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestRunCaseBoundsAnUnscriptedSignalWait is the availability regression: an
// untimed wait whose signal is absent must become a case error instead of
// holding flow test (and its caller's CI job) forever.
func TestRunCaseBoundsAnUnscriptedSignalWait(t *testing.T) {
	const source = `
edition: v2026.3
name: missing-signal
steps:
  - id: approval
    wait_for_signal:
      name: approve
`

	load := func() (*v1.Workflow, error) {
		return flowfile.Unmarshal([]byte(source))
	}
	limit := 50 * time.Millisecond
	started := time.Now()
	result, _, _ := runCaseWithin(&Test{Name: "missing signal"}, "", load, limit)

	require.False(t, result.GetPassed())
	require.Contains(t, result.GetError(), "wall-clock limit")
	require.Contains(t, result.GetError(), limit.String())
	require.Less(t, time.Since(started), time.Second)
}
