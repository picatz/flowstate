package engine

import (
	"fmt"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

// TestLoopResultsSizeErrorTextMatchesAcrossDrivers pins the sentence a run
// records under ${steps.<id>.error} when a loop's `results` cross
// [v1.MaxLoopResultsBytes] — invariant 3's exact shape, the same one
// steperror.go exists for: a value an author's expression compares must read
// the same wherever the run happened.
//
// Both drivers position the failure identically — "iteration %d: " — at the
// identical call site (the durable driver's executor.runLoop, right after
// [v1.AccumulateLoopResult]; the local driver's runLoop in eval.go, at the
// matching point). This test does not drive either loop far enough to
// actually cross the bound — that is TestAccumulateLoopResult's job, and
// TestRunWorkflowLoopResultsAcrossCAN's — it isolates the one thing that
// silently drifted before this test existed: whether the *position wrap*
// composes into the identical string on both drivers.
func TestLoopResultsSizeErrorTextMatchesAcrossDrivers(t *testing.T) {
	sizeErr := v1.LoopResultsSizeError(600000, v1.MaxLoopResultsBytes)

	// The local driver's own composition, verbatim from eval.go's runLoop:
	// `fmt.Errorf("iteration %d: %w", i, sizeErr)`.
	localText := fmt.Errorf("iteration %d: %w", 3, sizeErr).Error()

	// The durable driver's own composition, verbatim from executor.runLoop:
	// `stepFailed(sizeErr, "iteration %d", i)`.
	durableErr := stepFailed(sizeErr, "iteration %d", 3)
	var runFailed *ErrRunFailed
	require.ErrorAs(t, durableErr, &runFailed,
		"stepFailed must classify a plain error as an ErrRunFailed, the same shape recordedStepError expects")

	require.Equal(t, localText, runFailed.Message,
		"a loop that crosses the results byte bound must record the identical sentence "+
			"under ${steps.<id>.error} whether it ran locally or durably")
}

// TestForEachResultsSizeErrorTextMatchesAcrossDrivers is the `for_each` sibling
// of the check above: a for_each that crosses [v1.MaxLoopResultsBytes] must
// record the identical sentence whichever driver ran it. The local driver's
// runForEach in eval.go wraps with `fmt.Errorf("iteration %d: %w", ...)`; the
// durable driver's executor.runForEach — both its sequential per-iteration check
// and its concurrent join — wraps with `stepFailed(sizeErr, "iteration %d", ...)`.
// Those compositions must land on one string, the same invariant-3 shape
// steperror.go exists for.
func TestForEachResultsSizeErrorTextMatchesAcrossDrivers(t *testing.T) {
	sizeErr := v1.ForEachResultsSizeError(600000, v1.MaxLoopResultsBytes)

	localText := fmt.Errorf("iteration %d: %w", 3, sizeErr).Error()

	durableErr := stepFailed(sizeErr, "iteration %d", 3)
	var runFailed *ErrRunFailed
	require.ErrorAs(t, durableErr, &runFailed,
		"stepFailed must classify a plain error as an ErrRunFailed, the same shape recordedStepError expects")

	require.Equal(t, localText, runFailed.Message,
		"a for_each that crosses the results byte bound must record the identical sentence "+
			"under ${steps.<id>.error} whether it ran locally or durably")
}
