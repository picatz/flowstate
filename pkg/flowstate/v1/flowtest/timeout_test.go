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
func TestRunCaseEndsAnUnscriptedSignalWaitAsStuck(t *testing.T) {
	const source = `
edition: v2026.4
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
		result, _, transcript, _, _, _ := runCase(ctx, &Test{Name: "missing signal"}, "", load, false, fileVars{})

		require.False(t, result.GetPassed())
		require.Contains(t, result.GetError(), "stuck")
		require.Contains(t, result.GetError(), `"approve"`)
		require.Contains(t, transcript.GetStepValues(), "before",
			"the completed work before the blocked signal wait must remain available for coverage")
		// Exact, not a headroom check: nothing else in this case's bubble ever
		// parks on a timer, so the bubble's clock advances straight to the
		// liveness settle the instant `runCase` blocks on `approval`, well short
		// of the case's own limit.
		require.Equal(t, livenessSettle, time.Since(started),
			"the liveness check, not the wall-clock backstop, must be what ends this run")
	})
}

// TestRunCaseDoesNotMisreportAnotherDeadlineAsItsWallLimit runs in a [synctest]
// bubble for the reason the sibling above does: the caller's 20ms deadline has to
// fire while the run is parked on the wait, and against the real clock it raced
// the parse and compile that precede it, failing under `-race -cpu=1 -count=20`
// when they took longer than the deadline.
func TestRunCaseDoesNotMisreportAnotherDeadlineAsItsWallLimit(t *testing.T) {
	const source = `
edition: v2026.4
name: caller-deadline
steps:
  - id: approval
    wait_for_signal:
      name: approve
`
	load := func() (*v1.Workflow, error) {
		return flowfile.Unmarshal([]byte(source))
	}

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		caseCtx, cancelCase := caseContextWithin(ctx, time.Second)
		defer cancelCase()
		failed := true

		result, _, _, _, _, _ := runCase(caseCtx, &Test{
			Name:   "caller deadline",
			Expect: Expectation{Failed: &FailedClaim{Want: failed}},
		}, "", load, false, fileVars{})

		require.True(t, result.GetPassed(),
			"a deadline other than the harness backstop remains an ordinary run failure the case can expect")
		require.Empty(t, result.GetError())
	})
}

func TestCaseWallLimitIsSharedBySeededSchedules(t *testing.T) {
	ctx, cancel := caseContextWithin(t.Context(), 20*time.Millisecond)
	defer cancel()
	accumulator := newScheduleAccumulator(dst.Budget{Schedules: 100, Seed0: 1})
	runs := 0

	accumulator.run(ctx, func(ctx context.Context) (*v1.TestCase, *v1.Workflow, *v1.Workflow_StepOutputs, []TranscriptLine, caseShown, error) {
		runs++
		<-ctx.Done()
		return &v1.TestCase{Name: "blocked"}, nil, nil, nil, caseShown{}, ctx.Err()
	})

	require.Equal(t, 1, runs,
		"an expired case budget must stop exploration instead of granting every seed a fresh timeout")
}
