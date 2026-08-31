package flowdebug_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// A step id the workflow does not declare, typed at the prompt.
//
// `flow debug replay` has refused this since it learned to check a script
// against the workflow it replays — with a position, the declared ids, and a
// did-you-mean. The prompt armed it instead: `break nosuchstep` answered
// "breakpoint at nosuchstep" and listed a breakpoint that could never fire,
// while `until nosuchstep` printed nothing at all and ran the workflow to its
// end, taking every command queued behind it with it. One mistyped character
// forfeited the session, and the two fronts disagreed about the same word.

// TestBreakRefusesAStepTheWorkflowDoesNotDeclare pins the arm-nothing half:
// the id is refused, named, and no breakpoint is left behind to look armed.
func TestBreakRefusesAStepTheWorkflowDoesNotDeclare(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:    strings.NewReader("break nosuchstep\nbreakpoints\ncontinue\n"),
		Out:   &console,
		Steps: []flowdebug.Step{{ID: "build"}, {ID: "deploy"}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	require.NoError(t, <-walked(t, session, "build", "deploy"))

	out := console.String()
	assert.Contains(t, out, `no step named "nosuchstep"`,
		"the prompt armed a step the workflow does not declare:\n%s", out)
	assert.Contains(t, out, `"build"`,
		"the refusal does not name what the workflow does declare:\n%s", out)
	assert.NotContains(t, out, "breakpoint at nosuchstep",
		"a breakpoint that can never fire was still reported as set:\n%s", out)
	assert.Contains(t, out, "no breakpoints",
		"the refused id was left in the breakpoint set:\n%s", out)
}

// TestUntilRefusesAStepTheWorkflowDoesNotDeclare pins the sharper half: the
// session stays at its prompt, so the commands queued behind the typo are
// still answered rather than being swallowed by a run let go to its end.
func TestUntilRefusesAStepTheWorkflowDoesNotDeclare(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:    strings.NewReader("until nosuchstep\ninspect 1 + 1\ncontinue\n"),
		Out:   &console,
		Steps: []flowdebug.Step{{ID: "build"}, {ID: "deploy"}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	require.NoError(t, <-walked(t, session, "build", "deploy"))

	out := console.String()
	assert.Contains(t, out, `no step named "nosuchstep"`,
		"until ran the workflow to its end on a name nothing declares:\n%s", out)
	assert.Contains(t, out, "2",
		"the command queued behind the refused one was never answered, "+
			"which is the session being forfeited by a typo:\n%s", out)
}

// TestBreakSuggestsTheNearestDeclaredStep is the replay path's own courtesy,
// now at the prompt: one keystroke off gets the word rather than the list.
func TestBreakSuggestsTheNearestDeclaredStep(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:    strings.NewReader("break biuld\ncontinue\n"),
		Out:   &console,
		Steps: []flowdebug.Step{{ID: "build"}, {ID: "deploy"}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	require.NoError(t, <-walked(t, session, "build", "deploy"))

	assert.Contains(t, console.String(), `did you mean "build"?`,
		"a one-keystroke typo was answered with the list rather than the word:\n%s", console.String())
}

// TestBreakStillArmsADeclaredStep is the other direction, and the one a check
// like this can most easily break: a name the workflow does declare arms
// exactly as before, including a callee's own step, which is a real stop.
func TestBreakStillArmsADeclaredStep(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("break deploy\nbreakpoints\ncontinue\ncontinue\n"),
		Out: &console,
		Steps: []flowdebug.Step{
			{Workflow: "outer", ID: "build"},
			{Workflow: "inner", Declaration: 1, Via: "build", ID: "deploy"},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	require.NoError(t, <-walked(t, session, "build", "deploy"))

	out := console.String()
	assert.Contains(t, out, "breakpoint at deploy",
		"a step declared by a callee was refused, though the run stops there:\n%s", out)
	assert.NotContains(t, out, "no step named",
		"a declared id was refused:\n%s", out)
}

// TestAnEmptyInventoryRefusesNothing keeps the fail-open [checkStepArgument]
// already takes: a caller that supplied no steps has said nothing about what
// exists, and absence of evidence must not become evidence of absence. An
// embedder driving the session with no inventory keeps arming whatever it
// names.
func TestAnEmptyInventoryRefusesNothing(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("break anything\ncontinue\ncontinue\n"),
		Out: &console,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	require.NoError(t, <-walked(t, session, "build", "anything"))

	out := console.String()
	assert.Contains(t, out, "breakpoint at anything",
		"a session told nothing about its steps refused one anyway:\n%s", out)
	assert.NotContains(t, out, "no step named",
		"an empty inventory was read as proof the step does not exist:\n%s", out)
}
