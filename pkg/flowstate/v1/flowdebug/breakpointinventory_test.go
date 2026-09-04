package flowdebug_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// A breakpoint on a step the workflow does not declare is refused at every entry
// point, rather than reported as armed and never taken.
//
// #1347 taught the prompt to refuse `break nosuchstep`. The two programmatic
// doors kept accepting one: a caller that supplied Options.Steps and a misspelled
// id got a session that listed the breakpoint, started in modeRun, and could
// reach the end of the workflow without ever presenting a prompt — silent in
// exactly the way the prompt's was, which is #1367.

// TestOptionsBreakpointOnAnUndeclaredStepIsRefused covers the constructor.
func TestOptionsBreakpointOnAnUndeclaredStepIsRefused(t *testing.T) {
	t.Parallel()

	_, err := flowdebug.New(flowdebug.Options{
		Controlled:  true,
		Out:         io.Discard,
		Steps:       []flowdebug.Step{{ID: "build"}, {ID: "deploy"}},
		Breakpoints: []string{"deploi"},
	})
	require.Error(t, err, "a breakpoint naming no declared step was armed rather than refused")

	// The refusal carries the prompt's own words, suggestion included, because a
	// caller meets this instead of the prompt and deserves the same help.
	require.Contains(t, err.Error(), `no step named "deploi"`)
	require.Contains(t, err.Error(), `deploy`, "the near miss should be suggested")
}

// TestOptionsBreakpointOnADeclaredStepIsArmed is the direction that must keep
// working: the check refuses a name the inventory does not have, not every name.
func TestOptionsBreakpointOnADeclaredStepIsArmed(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{
		Controlled:  true,
		Out:         io.Discard,
		Steps:       []flowdebug.Step{{ID: "build"}, {ID: "deploy"}},
		Breakpoints: []string{"deploy"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
}

// TestOptionsBreakpointWithNoInventoryIsArmed pins the documented fail-open.
//
// A caller that supplied no steps has said nothing about what exists, so absence
// of evidence is not evidence the step is missing — the same rule
// Session.unknownStepNotice already applies at the prompt.
func TestOptionsBreakpointWithNoInventoryIsArmed(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{
		Controlled:  true,
		Out:         io.Discard,
		Breakpoints: []string{"anything at all"},
	})
	require.NoError(t, err, "an empty inventory must refuse nothing")
	t.Cleanup(func() { _ = session.Close() })
}

// TestSetBreakpointsOnAnUndeclaredStepIsRefused covers the second door, and
// checks the set it had is what it keeps: a refusal that half-replaced the set
// would leave a client's markers and the session's breakpoints disagreeing.
func TestSetBreakpointsOnAnUndeclaredStepIsRefused(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{
		Controlled:  true,
		Out:         io.Discard,
		Steps:       []flowdebug.Step{{ID: "build"}, {ID: "deploy"}},
		Breakpoints: []string{"build"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	err = session.SetBreakpoints([]string{"deploy", "deploi"})
	require.Error(t, err, "a breakpoint naming no declared step was armed rather than refused")
	require.Contains(t, err.Error(), `no step named "deploi"`)

	// And the session still works afterwards: the refusal is a rejected request,
	// not a session left in a state later calls trip over. The set it keeps is
	// checked in breakpointinventory_internal_test.go, where it is reachable.
	require.NoError(t, session.SetBreakpoints([]string{"deploy"}))
}

// TestUnknownStepAnswersPerIDForAFrontEnd is the seam the DAP adapter uses to
// answer a client per breakpoint rather than losing a whole set to one typo.
func TestUnknownStepAnswersPerIDForAFrontEnd(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{
		Controlled: true,
		Out:        io.Discard,
		Steps:      []flowdebug.Step{{ID: "build"}, {ID: "deploy"}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	notice, unknown := session.UnknownStep("deploi")
	require.True(t, unknown)
	require.Contains(t, notice, `no step named "deploi"`)

	_, unknown = session.UnknownStep("deploy")
	require.False(t, unknown, "a declared step must not be reported unknown")

	// Surrounding space is not a different step id *here*, which is what lets the
	// DAP adapter hand over a name straight from a client. It is not the rule
	// everywhere: SetBreakpoints refuses whitespace outright through
	// oneArgument, because the prompt it composes a line for cannot quote one.
	_, unknown = session.UnknownStep("  deploy  ")
	require.False(t, unknown)
}
