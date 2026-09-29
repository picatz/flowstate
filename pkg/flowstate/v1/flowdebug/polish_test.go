package flowdebug_test

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

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

	// Reached, but the condition never held: said as it was asked, not as
	// a bare target the run did reach.
	declined := finished("until body if n == 99\n")
	assert.Contains(t, declined, "the run completed without stopping at `until body if n == 99`")

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

// TestTheDriversBreakpointEchoCarriesWhatDecidesTheStop: the typed Driver —
// retained MCP sessions, embed, durable `flow debug` — echoes a breakpoint
// with its hit count and condition, as the prompt does, through one renderer.
func TestTheDriversBreakpointEchoCarriesWhatDecidesTheStop(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	waitHeld(t, run.session, 0)
	driver := flowdebug.NewDriver(run.session)

	counted, err := driver.Do(t.Context(), "break touch hit 2")
	require.NoError(t, err)
	assert.Contains(t, counted.Text, "breakpoint at touch hit 2\n")

	both, err := driver.Do(t.Context(), "break each/touch hit 2 if item == 2")
	require.NoError(t, err)
	assert.Contains(t, both.Text, "breakpoint at each/touch hit 2 if item == 2\n")

	_, err = driver.Do(t.Context(), "detach")
	require.NoError(t, err)
	require.NoError(t, <-run.done)
}

// TestARunsReturnReportedTwiceSaysTheMissedUntilOnce: the stateful MCP session
// hears its run return twice — from flowtest, then through [flowdebug.Session.Finished]
// with the case's verdict — and says a missed `until` once, in the transcript
// and in the observations, while the verdict it reports is still its own.
func TestARunsReturnReportedTwiceSaysTheMissedUntilOnce(t *testing.T) {
	t.Parallel()

	for _, verdict := range []error{nil, errors.New("the case did not pass")} {
		var out strings.Builder
		session, err := flowdebug.New(flowdebug.Options{In: strings.NewReader("until each[9]/body\n"), Out: &out})
		require.NoError(t, err)
		t.Cleanup(func() { _ = session.Close() })

		ctx := v1.NewContextWithDebugger(t.Context(), session)
		_, runErr := v1.Run(ctx, &v1.Workflow{Name: "looping", Steps: []*v1.Node{{
			Id: "each",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    v1.NewLiteralList(0, 1),
				Iterator: "n",
				Body:     []*v1.Node{{Id: "body", Kind: &v1.Node_Value{Value: v1.NewExpr("n")}}},
			}},
		}}})
		require.NoError(t, runErr)
		session.RunReturned(runErr)
		session.Finished(verdict)

		assert.Equal(t, 1, strings.Count(out.String(), "without stopping at"), "verdict %v: %q", verdict, out.String())
		snapshot, err := session.Snapshot(t.Context())
		require.NoError(t, err)
		notices := 0
		for _, observation := range snapshot.GetObservations() {
			if strings.Contains(observation.GetText(), "without stopping at") {
				notices++
			}
		}
		assert.Equal(t, 1, notices, "verdict %v: the notice was recorded %d times", verdict, notices)
		want := v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED
		if verdict != nil {
			want = v1.DebugRunState_DEBUG_RUN_STATE_FAILED
		}
		assert.Equal(t, want, snapshot.GetState(), "reporting the run's return changed the verdict")
	}
}

// A case that expects its run to fail passes, and its verdict — nil — reaches
// [flowdebug.Session.Finished] after the run's own error reached
// RunReturned. The first report is the run's, so a failed run is never said
// to have completed past its `until`.
func TestAFailedRunWhoseCasePassedIsNotCalledCompleted(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{In: strings.NewReader("until each[9]/body\n"), Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ctx := v1.NewContextWithDebugger(t.Context(), session)
	_, runErr := v1.Run(ctx, &v1.Workflow{Name: "failing", Steps: []*v1.Node{
		{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items:    v1.NewLiteralList(0, 1),
			Iterator: "n",
			Body:     []*v1.Node{{Id: "body", Kind: &v1.Node_Value{Value: v1.NewExpr("n")}}},
		}}},
		{Id: "boom", Kind: &v1.Node_Value{Value: v1.NewExpr("1 / 0")}},
	}})
	require.Error(t, runErr)
	session.RunReturned(runErr)
	session.Finished(nil)

	assert.NotContains(t, out.String(), "without stopping at", "a failed run was called completed")
	snapshot, err := session.Snapshot(t.Context())
	require.NoError(t, err)
	for _, observation := range snapshot.GetObservations() {
		assert.NotContains(t, observation.GetText(), "without stopping at", "a failed run was recorded as completed")
	}
}

// TestARenderedSnapshotSaysAMissedUntil: the Driver's fronts render a
// completed run through [flowdebug.FormatSnapshot], so the missed-`until`
// notice a durable run records (#2201) or a local session records is printed
// there. Only that notice, only on a completed run.
func TestARenderedSnapshotSaysAMissedUntil(t *testing.T) {
	t.Parallel()

	missed := &v1.DebugObservation{
		Kind: v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_NOTICE,
		Text: flowdebug.MissedUntilNotice("each[9]/body"),
	}
	other := &v1.DebugObservation{Kind: v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_NOTICE, Text: "body: the condition could not be evaluated"}
	logged := &v1.DebugObservation{Kind: v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_LOG, Text: flowdebug.MissedUntilNotice("a log line")}

	completed := flowdebug.FormatSnapshot(&v1.DebugSnapshot{
		State: v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, Observations: []*v1.DebugObservation{other, missed, logged},
	})
	assert.Contains(t, completed, "\n  the run completed without stopping at `until each[9]/body`\n")
	assert.NotContains(t, completed, "could not be evaluated", "another notice was rendered")
	assert.NotContains(t, completed, "a log line", "a log observation was rendered as the notice")

	failed := flowdebug.FormatSnapshot(&v1.DebugSnapshot{
		State: v1.DebugRunState_DEBUG_RUN_STATE_FAILED, Observations: []*v1.DebugObservation{missed},
	})
	assert.NotContains(t, failed, "without stopping at", "a failed run was called completed")
}

// TestTheDriverSaysALocalRunsMissedUntil drives a real local run through the
// Driver the MCP session tools and embed use: the session records the notice,
// and the Driver's answer carries it once.
func TestTheDriverSaysALocalRunsMissedUntil(t *testing.T) {
	t.Parallel()

	run := startDebugRun(t, "main.yaml", map[string]string{"main.yaml": journeyFlowfile, "child.yaml": childFlowfile}, nil)
	waitHeld(t, run.session, 0)

	result, err := flowdebug.NewDriver(run.session).Do(t.Context(), "until each[9]/touch")
	require.NoError(t, err)
	require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, result.Snapshot.GetState())
	assert.Equal(t, 1, strings.Count(result.Text, "the run completed without stopping at `until each[9]/touch`"), result.Text)
}

// TestAMissedUntilNoticeIsBoundedAndWellFormed: a target may be 4 KiB, and
// each driver caps an observation, the durable one at 512 runes. The notice
// cuts the target itself, so it closes its quote and reads the same whichever
// driver records it.
func TestAMissedUntilNoticeIsBoundedAndWellFormed(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("segment/", 512) + "step"
	notice := flowdebug.MissedUntilNotice(long)
	assert.LessOrEqual(t, utf8.RuneCountInString(notice), 512, "the notice would be clipped by the durable driver's cap")
	assert.True(t, strings.HasSuffix(notice, "…`"), "the cut notice does not say it was cut, or does not close its quote: %q", notice[len(notice)-20:])
	assert.Equal(t, "the run completed without stopping at `until each[9]/body`", flowdebug.MissedUntilNotice("each[9]/body"),
		"an ordinary target was changed")
}
