package flowtest

import (
	"context"
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
  - id: before
    sleep: 1s
  - id: approval
    wait_for_signal:
      name: approve
`

	load := func() (*v1.Workflow, error) {
		return flowfile.Unmarshal([]byte(source))
	}
	limit := 50 * time.Millisecond
	started := time.Now()
	result, _, transcript, _, _ := runCaseWithin(t.Context(), &Test{Name: "missing signal"}, "", load, false, fileVars{}, limit)

	require.False(t, result.GetPassed())
	require.Contains(t, result.GetError(), "wall-clock limit")
	require.Contains(t, result.GetError(), limit.String())
	require.Contains(t, transcript.GetStepValues(), "before",
		"the completed work before the blocked signal wait must remain available for coverage")
	require.Less(t, time.Since(started), time.Second)
}

func TestRunCaseDoesNotMisreportAnotherDeadlineAsItsWallLimit(t *testing.T) {
	const source = `
edition: v2026.3
name: caller-deadline
steps:
  - id: approval
    wait_for_signal:
      name: approve
`
	load := func() (*v1.Workflow, error) {
		return flowfile.Unmarshal([]byte(source))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	failed := true

	result, _, _, _, _ := runCaseWithin(ctx, &Test{
		Name:   "caller deadline",
		Expect: Expectation{Failed: &failed},
	}, "", load, false, fileVars{}, time.Second)

	require.True(t, result.GetPassed(),
		"a deadline other than the harness backstop remains an ordinary run failure the case can expect")
	require.Empty(t, result.GetError())
}
