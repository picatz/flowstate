package flowstatev1_test

import (
	"errors"
	"strings"
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

// TestEveryRefusalOffersARemedyThatValidates is the guard for issue #253, where
// [v1.CheckUndoPlacement]'s loop refusal told an author to "move the compensated
// step into a called workflow and undo it there" — a remedy
// [v1.UndoScope.IntoCall] refuses, since a loop's placement composes *through* a
// call rather than being laundered into [v1.UndoScopeCall] by it. An author who
// followed the instruction got the same refusal back, one file later, and had no
// reason to suspect the diagnostic rather than themselves.
//
// CLAUDE.md's rule is that a diagnostic names what to do instead. A remedy the
// validator itself rejects is worse than no remedy: it is a false diagnostic, the
// class the same document calls worse than a missing one.
//
// So this pins the relationship rather than the wording. A scope may only be told
// to move into a `call:` if moving into a call actually changes its answer — which
// is exactly `IntoCall()` returning something other than itself. Open the loop
// boundary later and this test goes quiet on its own; keep it closed and no
// refusal can drift back into recommending the escape hatch that is not one.
func TestEveryRefusalOffersARemedyThatValidates(t *testing.T) {
	t.Parallel()

	for _, scope := range []v1.UndoScope{
		v1.UndoScopeTopLevel,
		v1.UndoScopeCall,
		v1.UndoScopeConcurrent,
		v1.UndoScopeLoop,
	} {
		node := &v1.Node{
			Id:   "compensated",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
			Undo: &v1.Compensation{Task: &v1.Task{Name: "log"}},
		}

		err := v1.CheckUndoPlacement(node, scope)
		if err == nil {
			// Placements that accept a compensation have no remedy to check.
			continue
		}

		if scope.IntoCall() != scope {
			// A call *does* change this scope's answer, so recommending one is
			// honest. Nothing here refuses in that shape today; the branch
			// exists so opening the boundary does not silently skip the check.
			continue
		}

		// Every refusal opens by stating the rule — "only supported on a
		// top-level step or a step inside a `call:`" — and that clause names a
		// `call:` legitimately, as one of the two placements that accept a
		// compensation at all. What must not name one is everything after it:
		// the explanation and the remedy, which is where #253's false
		// instruction lived. The rule statement ends at the first semicolon.
		_, remedy, found := strings.Cut(err.Error(), ";")
		require.True(t, found,
			"the refusal for scope %v does not state the rule before explaining itself, so "+
				"this test cannot tell its remedy from its preamble: %s", scope, err)

		require.NotContains(t, remedy, "call",
			"the refusal for scope %v recommends a `call:`, but IntoCall leaves the placement "+
				"unchanged — an author following that remedy is refused again, one file later", scope)
	}
}
