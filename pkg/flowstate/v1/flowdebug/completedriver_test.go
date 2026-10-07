package flowdebug_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

func candidateTexts(answer flowdebug.Completion) []string {
	texts := make([]string, 0, len(answer.Candidates))
	for _, c := range answer.Candidates {
		texts = append(texts, c.Text)
	}

	return texts
}

// heldAtDone is a driver over a real local run held at its last step, where
// `price` has produced a value and `each` has run.
func heldAtDone(t *testing.T) (*flowdebug.Driver, *debugRun) {
	t.Helper()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	waitHeld(t, run.session, 0)
	driver := flowdebug.NewDriver(run.session)
	_, err := driver.Do(t.Context(), "until done")
	require.NoError(t, err)

	return driver, run
}

// TestADriverCompletesOverATarget: a front with no session of its own offers the
// names the run actually holds, asking the target, as the prompt does from its
// scope. A driver and a prompt over the same run agree on every name a driver
// offers.
func TestADriverCompletesOverATarget(t *testing.T) {
	t.Parallel()

	driver, run := heldAtDone(t)

	roots, err := driver.Complete(t.Context(), "inspect ste")
	require.NoError(t, err)
	assert.Equal(t, "ste", roots.Prefix)
	assert.Contains(t, candidateTexts(roots), "steps.", "a scope root is written with the dot that continues it")

	steps, err := driver.Complete(t.Context(), "inspect steps.pr")
	require.NoError(t, err)
	assert.Equal(t, []string{"price"}, candidateTexts(steps), "a step that produced outputs")

	outputs, err := driver.Complete(t.Context(), "inspect steps.price.")
	require.NoError(t, err)
	assert.Equal(t, []string{"value"}, candidateTexts(outputs), "the names the step's outputs have")

	// What a step's output holds is the run's data, not a name an author wrote,
	// so no keys are offered under it — the rule the prompt keeps.
	data, err := driver.Complete(t.Context(), "inspect steps.price.value.")
	require.NoError(t, err)
	assert.Empty(t, candidateTexts(data), "a key inside a produced value is the datum")

	// The prompt over the same held run offers every one of these names.
	for _, line := range []string{"inspect steps.", "inspect steps.pr", "inspect steps.price.", "inspect steps.price.value."} {
		driverAnswer, err := driver.Complete(t.Context(), line)
		require.NoError(t, err)
		promptAnswer := run.session.Complete(line, len(line))
		assert.Subset(t, candidateTexts(promptAnswer), candidateTexts(driverAnswer), "line %q: a name the driver offers the prompt would not", line)
	}
}

// TestADriverCompletesVerbsAndStepsFromWhatTheTargetSays: the first word is the
// driver's own vocabulary, and a step argument is what the snapshot names.
func TestADriverCompletesVerbsAndStepsFromWhatTheTargetSays(t *testing.T) {
	t.Parallel()

	driver, _ := heldAtDone(t)
	_, err := driver.Do(t.Context(), "break price")
	require.NoError(t, err)

	verbs, err := driver.Complete(t.Context(), "ba")
	require.NoError(t, err)
	assert.Equal(t, []string{"back", "backtrace"}, candidateTexts(verbs))

	driverOnly, err := driver.Complete(t.Context(), "pa")
	require.NoError(t, err)
	assert.Equal(t, []string{"pause"}, candidateTexts(driverOnly), "a driver-only verb is offered on a driver")

	promptOnly, err := driver.Complete(t.Context(), "qu")
	require.NoError(t, err)
	assert.Empty(t, candidateTexts(promptOnly), "`quit` is the prompt's, and a driver does not offer a verb it refuses")

	deleting, err := driver.Complete(t.Context(), "delete ")
	require.NoError(t, err)
	assert.Equal(t, []string{"price"}, candidateTexts(deleting), "`delete` takes a breakpoint this session holds")

	until, err := driver.Complete(t.Context(), "until do")
	require.NoError(t, err)
	assert.Equal(t, []string{"done"}, candidateTexts(until), "the step the run is held at")

	// Past an `if` the argument is an expression, so a step id is not offered.
	condition, err := driver.Complete(t.Context(), "break price if ste")
	require.NoError(t, err)
	assert.Equal(t, []string{"steps."}, candidateTexts(condition))
}

// TestADriverWithoutInspectCompletesVerbsAndStepsOnly: a target that refuses an
// inspection — a caller without the durable inspect action — still completes
// everything that does not need one, rather than failing the keystroke.
func TestADriverWithoutInspectCompletesVerbsAndStepsOnly(t *testing.T) {
	t.Parallel()

	_, run := heldAtDone(t)
	driver := flowdebug.NewDriver(refusesInspect{run.session})

	names, err := driver.Complete(t.Context(), "inspect steps.")
	require.NoError(t, err)
	assert.Empty(t, candidateTexts(names), "no inspection, so no names")

	roots, err := driver.Complete(t.Context(), "inspect ste")
	require.NoError(t, err)
	assert.Equal(t, []string{"steps."}, candidateTexts(roots), "the root is the language's, and needs no inspection")

	verbs, err := driver.Complete(t.Context(), "inspe")
	require.NoError(t, err)
	assert.Equal(t, []string{"inspect "}, candidateTexts(verbs))
}

type refusesInspect struct{ flowdebug.Target }

func (refusesInspect) Inspect(context.Context, *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	return nil, flowdebug.ErrNotPaused
}

// TestADriverCompletionIsNotTheOldStop: after the run moves, the names are the
// new stop's, not a list remembered from the one it left.
func TestADriverCompletionIsNotTheOldStop(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	waitHeld(t, run.session, 0)
	driver := flowdebug.NewDriver(run.session)

	before, err := driver.Complete(t.Context(), "inspect steps.")
	require.NoError(t, err)
	assert.NotContains(t, candidateTexts(before), "price", "nothing has run at the start")

	atStart, err := driver.Complete(t.Context(), "inspect ste")
	require.NoError(t, err)
	assert.Equal(t, []string{"steps."}, candidateTexts(atStart), "the root is taught before any step has produced outputs")

	_, err = driver.Do(t.Context(), "until done")
	require.NoError(t, err)

	after, err := driver.Complete(t.Context(), "inspect steps.")
	require.NoError(t, err)
	assert.Contains(t, candidateTexts(after), "price", "a stop the run has reached since")
}
