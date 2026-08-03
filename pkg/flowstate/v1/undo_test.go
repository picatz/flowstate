package flowstatev1_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The budget a cancelled run compensates under, tested where it is decided.
//
// Both drivers reach [v1.RunUndoLogWithin], and neither can be made to exhaust a
// two-minute budget in a test that anybody would run: locally the clock is real,
// and durably the budget is measured against `workflow.Now`, which the test
// environment advances by timers rather than by how long an activity took. Testing
// it through a driver would therefore mean either waiting two minutes or making
// [v1.UndoBudget] settable, and a bound that tests weaken is a bound that stops
// meaning anything.
//
// So the clock is the parameter it already is. What these pin is the rule itself,
// and the rule is shared — which is the same reason the constant lives in this
// package rather than twice in the two drivers.

// registered builds a log of compensations, oldest first, as a run would.
func registered(steps ...string) *v1.UndoLog {
	log := v1.NewUndoLog(nil)
	for _, step := range steps {
		log.Register(&v1.PendingUndo{StepId: step})
	}

	return log
}

// TestUndoBudgetIsReachedAndTheRestAreReported is the direction CLAUDE.md asks for
// where a bound exists: that it was reached, and not merely that it was not
// exceeded.
//
// A budget nothing spends is satisfied by an implementation that never enforces it
// at all, so the assertion has to be that the entries past the budget were *not
// attempted* — which is checked against what `run` actually saw, rather than
// against the summary, because a summary can name a step no call was made for.
func TestUndoBudgetIsReachedAndTheRestAreReported(t *testing.T) {
	t.Parallel()

	// Enough for the first compensation only. Registration order is first, second,
	// third; compensation runs third, second, first.
	left := []time.Duration{time.Minute, 0, 0}

	var attempted []string
	results := v1.RunUndoLogWithin(registered("first", "second", "third"),
		func() time.Duration {
			remaining := left[0]
			left = left[1:]

			return remaining
		},
		func(entry *v1.PendingUndo, _ time.Duration) error {
			attempted = append(attempted, entry.GetStepId())

			return nil
		})

	require.Equal(t, []string{"third"}, attempted,
		"a compensation was attempted after the budget for a cancelled run was spent")

	require.Equal(t, []v1.UndoResult{
		{Step: "third"},
		{Step: "second", Err: v1.ErrUndoBudget.Error()},
		{Step: "first", Err: v1.ErrUndoBudget.Error()},
	}, results,
		"the compensations the budget left no room for are not reported as unattempted")
}

// TestUndoSummaryDistinguishesUnattemptedFromFailed pins the sentence an operator
// acts on.
//
// The two outcomes need different words. "Could not undo X: <reason>" says the
// engine tried and the world may be in either state; the budget message says
// nothing was attempted, so what X held is certainly still held. An operator
// deciding what to clean up by hand is reading precisely this distinction, and
// collapsing the two into one phrasing would cost them the answer.
func TestUndoSummaryDistinguishesUnattemptedFromFailed(t *testing.T) {
	t.Parallel()

	summary := v1.UndoSummary([]v1.UndoResult{
		{Step: "third"},
		{Step: "second", Err: "boom"},
		{Step: "first", Err: v1.ErrUndoBudget.Error()},
	})

	require.Equal(t,
		`; compensation ran in reverse order: undid "third", `+
			`could not undo "second": boom, `+
			`could not undo "first": `+v1.ErrUndoBudget.Error(),
		summary)
	require.Contains(t, summary, "not attempted",
		"the summary does not say that a compensation was never tried")
}

// TestUndoWithoutABudgetAttemptsEverything is the other direction, and the one a
// suite of budget tests would let go wrong.
//
// A failure-triggered compensation has no budget: the run is already failing,
// nothing is waiting on it, and each entry is bounded by the same per-step timeouts
// every other task gets. Passing a nil clock must therefore attempt every entry —
// an implementation that treated "no budget" as "no time left" would take back
// nothing on the path the feature originally shipped for.
func TestUndoWithoutABudgetAttemptsEverything(t *testing.T) {
	t.Parallel()

	var attempted []string
	results := v1.RunUndoLogWithin(registered("first", "second", "third"), nil,
		func(entry *v1.PendingUndo, within time.Duration) error {
			attempted = append(attempted, entry.GetStepId())
			require.Zero(t, within,
				"a compensation with no budget was given one anyway")

			return nil
		})

	require.Equal(t, []string{"third", "second", "first"}, attempted)
	require.Len(t, results, 3)
	for _, result := range results {
		require.Empty(t, result.Err)
	}
}

// TestUndoBudgetErrorIsOneValue guards the thing that made the retry defaults a bug
// for months: a sentence written down twice.
//
// Both drivers report this outcome, and an operator comparing a local rehearsal
// with a production run has to read the same words. The check is that the value is
// reachable and stable rather than rebuilt per driver — if a driver ever spells its
// own, this is where the divergence starts.
func TestUndoBudgetErrorIsOneValue(t *testing.T) {
	t.Parallel()

	require.True(t, errors.Is(v1.ErrUndoBudget, v1.ErrUndoBudget))
	require.Equal(t, 2*time.Minute, v1.UndoBudget,
		"the budget changed; both drivers read this constant, and DSL.md quotes it")
}
