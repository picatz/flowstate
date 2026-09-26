package flowtest

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestRunCaseBoundsAnUnscriptedSignalWait is the availability regression: an
// untimed wait whose signal is absent must become a case error instead of
// holding flow test (and its caller's CI job) forever.
//
// It runs inside a [synctest] bubble rather than against the real wall clock
// (#2109). The 50ms backstop [caseContextWithin] installs starts counting
// before the Flowfile is even parsed, and everything from there to the
// `approval` step's park — parsing, compiling, running `before`'s virtual
// `sleep: 1s` — is real CPU work with no bound of its own. Outside a bubble
// that work competes with the wall clock the deadline is measured against, so
// a busy machine can spend the whole 50ms budget before `approval` is
// reached, cutting `before` out of the transcript though nothing was wrong.
// Inside the bubble, [synctest]'s fake clock only advances once every
// goroutine the bubble knows about is durably blocked, so that same CPU work
// costs the bubble nothing — the deadline can only fire once `runCase` is
// truly parked on the unscripted wait, deterministically, on a loaded runner
// or an idle one alike.
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

	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		ctx, cancel := caseContextWithin(t.Context(), limit)
		defer cancel()
		result, _, transcript, _, _ := runCase(ctx, &Test{Name: "missing signal"}, "", load, false, fileVars{})

		require.False(t, result.GetPassed())
		require.Contains(t, result.GetError(), "wall-clock limit")
		require.Contains(t, result.GetError(), limit.String())
		require.Contains(t, transcript.GetStepValues(), "before",
			"the completed work before the blocked signal wait must remain available for coverage")
		// Exact, not a headroom check: nothing else in this case's bubble ever
		// parks on a timer, so the bubble's clock advances straight from zero
		// to the deadline the instant `runCase` blocks on `approval` — real
		// scheduler jitter never enters into it.
		require.Equal(t, limit, time.Since(started),
			"the case's own backstop, not real scheduling delay, must be what ends this run")
	})
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
	caseCtx, cancelCase := caseContextWithin(ctx, time.Second)
	defer cancelCase()
	failed := true

	result, _, _, _, _ := runCase(caseCtx, &Test{
		Name:   "caller deadline",
		Expect: Expectation{Failed: &failed},
	}, "", load, false, fileVars{})

	require.True(t, result.GetPassed(),
		"a deadline other than the harness backstop remains an ordinary run failure the case can expect")
	require.Empty(t, result.GetError())
}

func TestCaseWallLimitIsSharedBySeededSchedules(t *testing.T) {
	ctx, cancel := caseContextWithin(t.Context(), 20*time.Millisecond)
	defer cancel()
	accumulator := newScheduleAccumulator(dst.Budget{Schedules: 100, Seed0: 1})
	runs := 0

	accumulator.run(ctx, func(ctx context.Context) (*v1.TestCase, *v1.Workflow, *v1.Workflow_StepOutputs, []TranscriptLine, error) {
		runs++
		<-ctx.Done()
		return &v1.TestCase{Name: "blocked"}, nil, nil, nil, ctx.Err()
	})

	require.Equal(t, 1, runs,
		"an expired case budget must stop exploration instead of granting every seed a fresh timeout")
}
