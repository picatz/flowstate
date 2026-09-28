package flowdebug_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// TestAKnownCommandAtTheAutopsyIsNotCalledUnknown: `backtrace` at the autopsy
// is a real command with nothing left to act on, and says so, rather than
// sending the author looking for a misspelling. A misspelling is still one.
func TestAKnownCommandAtTheAutopsyIsNotCalledUnknown(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{In: strings.NewReader("backtrace\nbacktrcae\nquit\n"), Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	session.Autopsy(t.Context(), &v1.Scope{}, nil, []string{"expect.ran: expected a step"})

	printed := out.String()
	assert.Contains(t, printed, "`backtrace` has nothing to act on at the autopsy")
	assert.NotContains(t, printed, `unknown command "backtrace"`)
	assert.Contains(t, printed, `unknown command "backtrcae"`, "a misspelling at the autopsy was not called one")
}

// TestABreakpointsEchoCarriesItsHitCount: the echo names everything that
// decides when the breakpoint stops, the hit count included, so it does not
// read broader than it is.
func TestABreakpointsEchoCarriesItsHitCount(t *testing.T) {
	t.Parallel()

	out, _ := loopingRun(t, 5, "break body hit 2\nbreak each if true\ncontinue\ncontinue\ncontinue\n")

	assert.Contains(t, out, "breakpoint at body hit 2\n")
	assert.Contains(t, out, "breakpoint at each if true\n", "a breakpoint with no hit count echoed one")
}

// TestAnUntilTheRunNeverReachesIsSaid: an `until` naming an arrival the run
// never makes, an iteration past the last, leaves the run to complete, and the
// session says the stop never came rather than ending as if it had. One the
// run reaches says nothing of the kind.
func TestAnUntilTheRunNeverReachesIsSaid(t *testing.T) {
	t.Parallel()

	finished := func(script string) string {
		t.Helper()

		var out strings.Builder
		session, err := flowdebug.New(flowdebug.Options{In: strings.NewReader(script), Out: &out})
		require.NoError(t, err)
		t.Cleanup(func() { _ = session.Close() })

		ctx := v1.NewContextWithDebugger(t.Context(), session)
		_, runErr := v1.Run(ctx, &v1.Workflow{Name: "looping", Steps: []*v1.Node{{
			Id: "each",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    v1.NewLiteralList(0, 1, 2),
				Iterator: "n",
				Body:     []*v1.Node{{Id: "body", Kind: &v1.Node_Value{Value: v1.NewExpr("n")}}},
			}},
		}}})
		// What every driver does when its run returns.
		session.Finished(runErr)
		require.NoError(t, runErr)

		return out.String()
	}

	missed := finished("until each[9]/body\n")
	assert.Contains(t, missed, "the run completed without stopping at `until each[9]/body`")

	reached := finished("until each[1]/body\ncontinue\n")
	assert.Contains(t, reached, "break at")
	assert.NotContains(t, reached, "without stopping at", "an until the run reached was reported missed")
}

// TestQuitSaysWhatEndedTheRunOnce: the `quit` error is the session-ended
// sentinel, worded once rather than restated.
func TestQuitSaysWhatEndedTheRunOnce(t *testing.T) {
	t.Parallel()

	_, _, runErr := runDebugged(t, "quit\n", flowdebug.Options{})
	require.Error(t, runErr)
	assert.True(t, errors.Is(runErr, v1.ErrDebugSessionEnded))
	assert.Equal(t, 1, strings.Count(runErr.Error(), "ended"), "the quit error says the session ended twice: %v", runErr)
	assert.Contains(t, runErr.Error(), "`quit`")
}

// TestAScriptsCommentsDrawNoPromptOfTheirOwn: a comment line consumes no
// prompt, so a script with comments in front of its first command draws one
// `debug> ` for that command rather than one per line read.
func TestAScriptsCommentsDrawNoPromptOfTheirOwn(t *testing.T) {
	t.Parallel()

	out, _, err := runDebugged(t, "# one\n# two\n# three\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(out, flowdebug.Prompt), "comments drew prompts of their own: %q", out)
}
