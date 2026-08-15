package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource"
)

// observed is the moment a poll result is folded in, for the tests that state the
// clock rather than read it.
//
// absorb takes the time as a parameter precisely so that the outage allowance — the
// one bound here measured in seconds — can be asserted in seconds without a test
// spending them.
var observed = time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC)

// pollAnswer is one prepared reply.
type pollAnswer struct {
	response *v1.GetResponse
	err      error
}

// scriptedPoller answers a prepared sequence and then repeats its last answer.
//
// Repeating rather than running out, because "the run is still RUNNING and stays
// that way" is a real case — it is what a long workload looks like — and a poller
// that panicked at the end of its script could not describe it.
type scriptedPoller struct {
	answers []pollAnswer
	calls   int

	// holdFrom, when set, makes the poller wait on release before answering the
	// call at that index and every one after it.
	//
	// It exists so a test can assert a *progression* without betting on frame
	// timing. bubbletea coalesces redraws, so a script that finishes a run in four
	// quick answers can legitimately reach the terminal state inside one frame,
	// and an assertion that one string was drawn before another then fails on a
	// loaded machine while passing on a quiet one — a test measuring the
	// scheduler. Holding the answer until the test has seen what it is waiting for
	// makes the order causal: the run cannot finish before the frame that proves
	// it was watched while running.
	holdFrom int
	release  chan struct{}
}

func (p *scriptedPoller) Poll(ctx context.Context) (*v1.GetResponse, error) {
	call := p.calls
	p.calls++

	if p.release != nil && call >= p.holdFrom {
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	answer := p.answers[min(call, len(p.answers)-1)]

	return answer.response, answer.err
}

// runningPoll and finishedPoll are the two answers most of these tests need.
func runningPoll(steps ...string) pollAnswer {
	return pollAnswer{response: response(v1.RunResponse_STATUS_RUNNING, steps...)}
}

func finishedPoll(steps ...string) pollAnswer {
	return pollAnswer{response: response(v1.RunResponse_STATUS_COMPLETED, steps...)}
}

// response builds a GetResponse with the given status and completed steps.
func response(status v1.RunResponse_Status, steps ...string) *v1.GetResponse {
	msg := &v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		RunId:      "0198f1e2-0000-7000-8000-000000000000",
		Status:     status,
	}

	if len(steps) > 0 {
		values := make(map[string]*v1.Node_Outputs, len(steps))
		for _, id := range steps {
			values[id] = &v1.Node_Outputs{
				NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("ok")},
			}
		}
		msg.Kind = &v1.GetResponse_Outputs{Outputs: &v1.Workflow_StepOutputs{StepValues: values}}
	}

	return msg
}

// runningAt is a running run reporting where it has got to, the way the server
// answers one: a top-level step, and the path into it where there is one.
//
// Built as a whole GetResponse rather than as a position alone, because what is
// under test is a poll — a position arrives beside a status and a run id, and a
// change detector that only sees one of them is the bug these tests exist for.
func runningAt(stepID string, path ...string) pollAnswer {
	answer := runningPoll()
	answer.response.Progress = &v1.RunProgress{StepId: stepID, Path: path}

	return answer
}

// retryingAt is a running run whose current activity keeps failing.
//
// The attempt count and the message are what Temporal reports and what the server
// projects; a next-attempt time is added by [scheduledIn] where a test is about the
// countdown rather than about the retry.
func retryingAt(stepID string, attempt int32, failure string) pollAnswer {
	answer := runningAt(stepID)
	answer.response.PendingActivities = []*v1.PendingActivity{{
		Attempt:     attempt,
		LastFailure: failure,
	}}

	return answer
}

// scheduledIn sets when the pending activity's next attempt is due.
func scheduledIn(answer pollAnswer, wait time.Duration) pollAnswer {
	for _, pending := range answer.response.GetPendingActivities() {
		pending.NextAttemptScheduledTime = timestamppb.New(observed.Add(wait))
	}

	return answer
}

// failedResponse builds a run that ended badly, carrying its message.
func failedResponse(status v1.RunResponse_Status, message string) *v1.GetResponse {
	msg := response(status)
	msg.Kind = &v1.GetResponse_Error{Error: &v1.RunResponse_Error{Message: message}}

	return msg
}

// transientRefusal and permanentRefusal build the two classes, through the same
// classification clientPoller applies — so a test cannot accidentally describe a
// refusal as transient that the real poller would call permanent.
func transientRefusal() error {
	return classifyPollError("flowstate-workflow-3f7c", serverFlags{},
		connect.NewError(connect.CodeUnavailable, errors.New("connection refused")))
}

func permanentRefusal() error {
	return classifyPollError("flowstate-workflow-3f7c", serverFlags{},
		connect.NewError(connect.CodeNotFound, errors.New("no such run")))
}

// plainSurface is a watch surface writing into buffers, which is the non-terminal
// shape.
func plainSurface() (*ui.UI, *strings.Builder, *strings.Builder) {
	var out, errOut strings.Builder

	return ui.Plain(&out, &errOut), &out, &errOut
}

// watchCommandForTest builds the command runWatch expects, with its flags reset.
//
// The flags are package-level because cobra binds them there, so they are restored
// rather than left set for whichever test runs next. `--output` is not among them any
// more: it is declared on the command and read back off it, so a test asks for a
// format the way a caller does.
func watchCommandForTest(t *testing.T) (*cobra.Command, *strings.Builder, *strings.Builder) {
	t.Helper()

	var out, errOut strings.Builder
	cmd := &cobra.Command{}
	addFollowFlags(cmd)
	addOutputFlag(cmd)
	cmd.Flags().String("run-id", "", "")
	addServerFlags(cmd)
	require.NoError(t, cmd.Flags().Set("interval", minWatchInterval.String()))
	cmd.SetContext(t.Context())
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)

	return cmd, &out, &errOut
}

// TestWatchReportsOnlyChanges is the whole difference from a `flow get` loop.
//
// A run that sits on one step for four minutes should produce four minutes of
// silence, not 240 identical lines. Asserted by counting the lines a stable run
// produces rather than by inspecting one of them.
func TestWatchReportsOnlyChanges(t *testing.T) {
	poller := &scriptedPoller{answers: []pollAnswer{
		runningPoll(),
		runningPoll(),
		runningPoll(),
		runningPoll("checkout"),
		runningPoll("checkout"),
		runningPoll("checkout"),
		finishedPoll("checkout", "build"),
	}}
	surface, _, errOut := plainSurface()

	require.NoError(t, followPlainly(t.Context(), surface, renderingOf(FormatText), poller, time.Millisecond,
		"flowstate-workflow-3f7c", nil))

	lines := reportedLines(errOut.String())
	require.Len(t, lines, 3,
		"one line per change is the point; got one per poll, or missed a change:\n%s", errOut.String())

	require.Contains(t, lines[0], "RUNNING")
	require.NotContains(t, lines[0], "checkout", "a step reported before it had produced anything")
	require.Contains(t, lines[1], "checkout")
	require.Contains(t, lines[2], "COMPLETED")
	require.Contains(t, lines[2], "build")
}

// reportedLines splits the progress account into lines, dropping the trailing empty
// one.
func reportedLines(s string) []string {
	trimmed := strings.TrimSuffix(s, "\n")
	if trimmed == "" {
		return nil
	}

	return strings.Split(trimmed, "\n")
}

// TestWatchReportsTheFirstAnswerAsAChange holds honest the reasoning that lets absorb
// do without a flag saying which answer is the first.
//
// The claim is that the first answer to get past the UNSPECIFIED guard necessarily
// differs from the zero state, because the zero value of status *is* UNSPECIFIED. So
// both halves are asserted: that the premise is still true of the generated enum, and
// that an answer carrying nothing but a status — no run id, no outputs, everything
// else at the zero value — is reported as a change on the strength of the status
// alone.
//
// A mutation removing the status comparison passes every other test here, because
// every response they build carries a run id too.
func TestWatchReportsTheFirstAnswerAsAChange(t *testing.T) {
	require.Equal(t, v1.RunResponse_STATUS_UNSPECIFIED, v1.RunResponse_Status(0),
		"the zero status is no longer UNSPECIFIED, so a first answer can now match the zero state "+
			"and absorb needs to know which answer is the first after all")

	state := newWatchState("flowstate-workflow-3f7c", nil)

	bare := &v1.GetResponse{Status: v1.RunResponse_STATUS_RUNNING}
	require.True(t, state.absorb(observed, bare, nil).Changed,
		"the first answer went unreported, so a reader was told nothing until a step finished")

	// And the same answer again is not news.
	require.False(t, state.absorb(observed, bare, nil).Changed)
}

// TestWatchCountsAPositionChangeAsAChange is the regression direction for the whole
// feature.
//
// A run moving from one step to the next keeps its status, its run id and — until it
// finishes — its outputs, so a change detector built from those three sees nothing at
// all. The "nothing has changed yet" rule then suppresses exactly the news a live view
// exists to deliver: the plain shape prints one line at the start of a run and nothing
// again until it ends, and the view repaints an identical screen.
func TestWatchCountsAPositionChangeAsAChange(t *testing.T) {
	state := newWatchState("flowstate-workflow-3f7c", nil)

	require.True(t, state.absorb(observed, runningAt("checkout").response, nil).Changed,
		"the first answer went unreported")

	// The same answer again is not news, which is what makes the assertion below a
	// claim about the position rather than about every poll.
	require.False(t, state.absorb(observed, runningAt("checkout").response, nil).Changed)

	require.True(t, state.absorb(observed, runningAt("build").response, nil).Changed,
		"a run that moved to another step was reported as unchanged, so a live view "+
			"showed the step it had left")
	require.Equal(t, "build", state.position)

	// And into a step, which is where a run spends the interesting part of a loop.
	require.True(t, state.absorb(observed, runningAt("deploy", "each", "upload").response, nil).Changed)
	require.Equal(t, "deploy > each > upload", state.position,
		"the path into the step was dropped, so every iteration of a loop reads the same")
}

// TestWatchCountsARetryAsAChangeButNotItsCountdown is the join between "report every
// change" and "report only changes".
//
// A climbing attempt count is news — it is the difference between a slow step and a
// stuck one. The countdown to the next attempt is not: it falls by the poll interval
// on every answer, so keying on it makes every poll a change, which is a line per
// second in a CI log and the `flow get` loop this command replaces.
func TestWatchCountsARetryAsAChangeButNotItsCountdown(t *testing.T) {
	state := newWatchState("flowstate-workflow-3f7c", nil)

	state.absorb(observed, runningAt("deploy").response, nil)

	require.True(t, state.absorb(observed, retryingAt("deploy", 2, "connection refused").response, nil).Changed,
		"a step that started failing was reported as unchanged")
	require.True(t, state.absorb(observed, retryingAt("deploy", 3, "connection refused").response, nil).Changed,
		"an attempt count climbing under an unchanging status went unreported, which is "+
			"the signature of a stuck run")
	require.True(t, state.absorb(observed, retryingAt("deploy", 3, "no route to host").response, nil).Changed,
		"the failure changed and the reader was not told")

	// The countdown alone, twice, at two different values.
	same := scheduledIn(retryingAt("deploy", 3, "no route to host"), 30*time.Second)
	require.False(t, state.absorb(observed, same.response, nil).Changed,
		"a countdown ticking was reported as the run having changed")

	sooner := scheduledIn(retryingAt("deploy", 3, "no route to host"), 5*time.Second)
	require.False(t, state.absorb(observed, sooner.response, nil).Changed,
		"a countdown ticking was reported as the run having changed")

	// It is still rendered, measured against the moment the answer was observed
	// rather than whenever this happens to be drawn.
	require.Equal(t, []string{"retrying, attempt 3: no route to host (next attempt in 5s)"},
		state.pending)
}

// TestWatchPlainLinesSayWhereTheRunIsAndWhatFailed is requirement three's half of the
// feature: a script, a CI job and a screen reader get the same account, one line per
// change, on stderr.
//
// The position and the retry are both on the line, because this shape's discipline is
// one line per change and a step that is failing is one change rather than two.
func TestWatchPlainLinesSayWhereTheRunIsAndWhatFailed(t *testing.T) {
	poller := &scriptedPoller{answers: []pollAnswer{
		runningAt("checkout"),
		runningAt("checkout"),
		runningAt("build"),
		retryingAt("deploy", 4, "connection refused"),
		retryingAt("deploy", 4, "connection refused"),
		finishedPoll("checkout", "build", "deploy"),
	}}
	surface, out, errOut := plainSurface()

	require.NoError(t, followPlainly(t.Context(), surface, renderingOf(FormatText), poller, time.Millisecond,
		"flowstate-workflow-3f7c", nil))

	lines := reportedLines(errOut.String())
	require.Len(t, lines, 4,
		"one line per change, and a change is a move as well as a status:\n%s", errOut.String())

	require.Contains(t, lines[0], "on checkout")
	require.Contains(t, lines[1], "on build",
		"a run moved between steps and the account said nothing")
	require.Contains(t, lines[2], "on deploy")
	require.Contains(t, lines[2], "attempt 4",
		"an attempt count climbing was left off, so a stuck run reads as a slow one")
	require.Contains(t, lines[2], "connection refused",
		"the reason the step keeps failing was left off")
	require.Contains(t, lines[3], "COMPLETED")

	require.NotContains(t, out.String(), "on build",
		"the account was written to stdout, which corrupts anything piping the outputs")
}

// TestWatchStopsOnEveryTerminalStatusAndKeepsGoingOtherwise covers the table rather
// than one row of it.
//
// A status added to the schema and not added to terminalStatus makes a watch hang
// forever on a run that has finished, which reads as a slow workload rather than a
// bug. Enumerating the generated enum means a new status fails this test rather than
// shipping.
func TestWatchStopsOnEveryTerminalStatusAndKeepsGoingOtherwise(t *testing.T) {
	terminal := map[v1.RunResponse_Status]bool{
		v1.RunResponse_STATUS_COMPLETED:  true,
		v1.RunResponse_STATUS_FAILED:     true,
		v1.RunResponse_STATUS_CANCELED:   true,
		v1.RunResponse_STATUS_TERMINATED: true,
		v1.RunResponse_STATUS_TIMED_OUT:  true,
	}

	names := v1.RunResponse_Status(0).Descriptor().Values()
	for i := range names.Len() {
		status := v1.RunResponse_Status(names.Get(i).Number())

		t.Run(statusLabel(status), func(t *testing.T) {
			state := newWatchState("flowstate-workflow-3f7c", nil)
			progress := state.absorb(observed, response(status), nil)

			switch {
			case status == v1.RunResponse_STATUS_UNSPECIFIED:
				// Not "still running": a status the schema forbids is a server that
				// has not answered the question, and waiting on it waits forever.
				require.True(t, progress.Done, "a watch waited on a status the schema forbids")
				require.Error(t, progress.Err)
				require.True(t, state.gaveUp)

			case terminal[status]:
				require.True(t, progress.Done, "a watch kept polling a run that had finished")
				require.NoError(t, progress.Err)

			default:
				require.False(t, progress.Done, "a watch stopped on a run that was still going")
			}
		})
	}
}

// TestWatchOutcomeIsTheRunsOutcome is what makes `flow watch x && ./promote.sh`
// safe.
func TestWatchOutcomeIsTheRunsOutcome(t *testing.T) {
	for _, tc := range []struct {
		status v1.RunResponse_Status
		fails  bool
		word   string
	}{
		{status: v1.RunResponse_STATUS_COMPLETED, fails: false},
		{status: v1.RunResponse_STATUS_FAILED, fails: true, word: "failed"},
		{status: v1.RunResponse_STATUS_CANCELED, fails: true, word: "canceled"},
		{status: v1.RunResponse_STATUS_TERMINATED, fails: true, word: "terminated"},
		{status: v1.RunResponse_STATUS_TIMED_OUT, fails: true, word: "timed out"},
		// Watching stopped before the run did, so there is no outcome to report.
		{status: v1.RunResponse_STATUS_RUNNING, fails: false},
	} {
		t.Run(statusLabel(tc.status), func(t *testing.T) {
			err := outcomeError(tc.status, "flowstate-workflow-3f7c", "")
			if !tc.fails {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			// The word rather than "failed" for everything: "terminated" and "timed
			// out" are different things to go and look at.
			require.ErrorContains(t, err, tc.word)
			require.ErrorContains(t, err, "flowstate-workflow-3f7c")
		})
	}
}

// TestWatchOutcomeCarriesTheFailureMessage checks that the reason travels with the
// exit code, so a CI log has it without a second command.
func TestWatchOutcomeCarriesTheFailureMessage(t *testing.T) {
	poller := &scriptedPoller{answers: []pollAnswer{
		{response: failedResponse(v1.RunResponse_STATUS_FAILED, `step "deploy" could not reach the registry`)},
	}}
	surface, out, _ := plainSurface()

	err := followPlainly(t.Context(), surface, renderingOf(FormatText), poller, time.Millisecond,
		"flowstate-workflow-3f7c", nil)

	require.ErrorContains(t, err, "failed")
	require.ErrorContains(t, err, "could not reach the registry")
	require.Empty(t, out.String(), "a failed run wrote outputs it does not have")
}

// TestWatchSurvivesAnOutageAndSaysSo is why a watch is worth having over a shell
// loop: it lasts as long as the run, and over an hour a dropped connection is close
// to certain.
func TestWatchSurvivesAnOutageAndSaysSo(t *testing.T) {
	poller := &scriptedPoller{answers: []pollAnswer{
		runningPoll(),
		{err: transientRefusal()},
		{err: transientRefusal()},
		finishedPoll("checkout"),
	}}
	surface, _, errOut := plainSurface()

	require.NoError(t, followPlainly(t.Context(), surface, renderingOf(FormatText), poller, time.Millisecond,
		"flowstate-workflow-3f7c", nil),
		"a watch died on a transient refusal it should have survived")

	account := errOut.String()
	require.Contains(t, account, "UNREACHABLE",
		"the server went quiet and the reader was left watching a still screen")
	require.Contains(t, account, "connection refused",
		"the outage was announced without saying what went wrong")
	require.Contains(t, account, "COMPLETED", "the recovery was not reported")

	lines := reportedLines(account)
	require.Len(t, lines, 3,
		"an outage that persists is not news each second:\n%s", account)
}

// TestWatchGivesUpAfterTheOutageAllowance is the other half: tolerating a blip must
// not mean tolerating an outage forever.
//
// Asserted as *reached* as well as not exceeded. "gave up no later than the allowance"
// is also satisfied by a watch that gave up on the first refusal, which is the bug the
// tolerance exists to prevent.
func TestWatchGivesUpAfterTheOutageAllowance(t *testing.T) {
	state := newWatchState("flowstate-workflow-3f7c", nil)

	// A refusal every second, driven by a stated clock rather than a real one, so the
	// assertion is about the allowance and not about how fast the test machine is.
	var progress watchProgress
	var elapsed time.Duration
	for step := time.Duration(0); !progress.Done; step += time.Second {
		progress = state.absorb(observed.Add(step), nil, transientRefusal())
		elapsed = step

		require.LessOrEqual(t, step, outageAllowance,
			"a watch never gave up on a server that had been unreachable for %s", step)
	}

	require.Equal(t, outageAllowance, elapsed,
		"a watch gave up after %s of an allowance that promises %s", elapsed, outageAllowance)
	require.ErrorContains(t, progress.Err, "gave up")
	require.ErrorContains(t, progress.Err, outageAllowance.String(),
		"the elapsed time reported is not the time actually spent")
	require.True(t, state.gaveUp)
}

// TestWatchAllowanceIsTheSameSpanAtEveryInterval is the regression test for a bound
// stated in one unit and enforced in another.
//
// The allowance used to be `outageAllowance / interval` failures. At a ten-second
// interval that is three, and with the first poll happening immediately the failures
// land at 0, 10 and 20 seconds — so a watch gave up after twenty while reporting
// thirty, and an outage that recovered at twenty-five was killed by a promise that
// said it would not be. Nothing about the number of attempts is asserted here, because
// the number of attempts is not the promise.
func TestWatchAllowanceIsTheSameSpanAtEveryInterval(t *testing.T) {
	for _, interval := range []time.Duration{
		minWatchInterval, time.Second, 10 * time.Second, time.Hour,
	} {
		t.Run(interval.String(), func(t *testing.T) {
			state := newWatchState("w", nil)

			// One failure short of the allowance must not end the watch, whatever the
			// interval and however few attempts that took.
			for step := time.Duration(0); step < outageAllowance; step += interval {
				require.False(t, state.absorb(observed.Add(step), nil, transientRefusal()).Done,
					"gave up %s into a %s allowance", step, outageAllowance)
			}

			require.True(t, state.absorb(observed.Add(outageAllowance), nil, transientRefusal()).Done,
				"did not give up after the whole allowance had passed")
		})
	}
}

// TestWatchReportsTheTimeItActuallySpent checks that the elapsed span in the message is
// measured rather than recited.
//
// Every other test here uses an interval that divides the allowance, so the measured
// span lands exactly on the constant and an assertion against it passes whether the
// number was computed or copied. Seven seconds does not divide thirty, so the two
// differ — and the message has to say the one that happened.
func TestWatchReportsTheTimeItActuallySpent(t *testing.T) {
	state := newWatchState("w", nil)

	for _, second := range []int{0, 7, 14, 21, 28} {
		require.False(t,
			state.absorb(observed.Add(time.Duration(second)*time.Second), nil, transientRefusal()).Done,
			"gave up %ds into a %s allowance", second, outageAllowance)
	}

	progress := state.absorb(observed.Add(35*time.Second), nil, transientRefusal())
	require.True(t, progress.Done)
	require.ErrorContains(t, progress.Err, "35s",
		"the message did not report the span actually spent unreachable")
	require.NotContains(t, progress.Err.Error(), outageAllowance.String(),
		"the message recited the allowance instead of the measurement")
}

// TestWatchNeverGivesUpOnOneFailure checks the property that used to need a floor on
// attempts to state, and now falls out of measuring time.
//
// One refusal is not evidence of an outage at any interval: a watch that treats it as
// one is the shell loop people were already using, with fewer features.
func TestWatchNeverGivesUpOnOneFailure(t *testing.T) {
	state := newWatchState("w", nil)

	progress := state.absorb(observed, nil, transientRefusal())
	require.False(t, progress.Done, "a single transient refusal ended a watch")
	require.True(t, progress.Changed, "the reader was not told the server had gone quiet")
	require.False(t, state.gaveUp)
}

// TestWatchDoesNotRetryAPermanentRefusal writes the negative direction of the
// tolerance above.
//
// Asserting only that a transient refusal is survived is a functionality test in a
// robustness test's clothes: a poller that retried *everything* would pass it. What
// matters is that a mistyped id is refused at once rather than after thirty seconds
// of a watch saying nothing useful.
func TestWatchDoesNotRetryAPermanentRefusal(t *testing.T) {
	poller := &scriptedPoller{answers: []pollAnswer{{err: permanentRefusal()}}}
	surface, _, _ := plainSurface()

	err := followPlainly(t.Context(), surface, renderingOf(FormatText), poller, time.Millisecond,
		"flowstate-workflow-3f7c", nil)

	require.ErrorContains(t, err, "check the id")
	require.Equal(t, 1, poller.calls,
		"a refusal that will not become acceptable was asked about %d times", poller.calls)
}

// TestWatchOutageClockRestartsOnRecovery checks that a second blip an hour later gets
// its own allowance rather than inheriting a spent one.
func TestWatchOutageClockRestartsOnRecovery(t *testing.T) {
	state := newWatchState("w", nil)

	// Most of an allowance spent, then an answer.
	require.False(t, state.absorb(observed, nil, transientRefusal()).Done)
	require.False(t, state.absorb(observed.Add(outageAllowance-time.Second), nil, transientRefusal()).Done)

	recovery := state.absorb(observed.Add(outageAllowance), response(v1.RunResponse_STATUS_RUNNING), nil)
	require.True(t, recovery.Changed, "the recovery was not reported")
	require.Zero(t, state.outageSince, "the outage clock kept running after the server answered")

	// A later failure starts over, so it does not immediately exceed an allowance
	// measured from an outage that ended.
	later := observed.Add(time.Hour)
	require.False(t, state.absorb(later, nil, transientRefusal()).Done,
		"a fresh outage inherited a spent allowance")
	require.False(t, state.absorb(later.Add(time.Second), nil, transientRefusal()).Done)
}

// TestWatchSeparatesOutputsFromProgress is the property that makes
// `flow watch x | jq` work, and it is the same one `flow get` holds.
func TestWatchSeparatesOutputsFromProgress(t *testing.T) {
	poller := &scriptedPoller{answers: []pollAnswer{runningPoll(), finishedPoll("greet")}}
	surface, out, errOut := plainSurface()

	require.NoError(t, followPlainly(t.Context(), surface, renderingOf(FormatText), poller, time.Millisecond,
		"flowstate-workflow-3f7c", nil))

	// Everything on stdout has to parse, or a pipe into jq breaks.
	var outputs map[string]any
	require.NoError(t, json.Unmarshal([]byte(out.String()), &outputs),
		"stdout was not one JSON document: %q", out.String())
	require.Contains(t, outputs, "steps")

	require.NotContains(t, out.String(), "COMPLETED",
		"the progress account was written to stdout, which corrupts anything piping the outputs")
	require.Contains(t, errOut.String(), "COMPLETED")
}

// TestWatchJSONLIsOneDocumentPerChange checks the event-stream shape a program or an
// agent reads as it arrives.
func TestWatchJSONLIsOneDocumentPerChange(t *testing.T) {
	poller := &scriptedPoller{answers: []pollAnswer{
		runningPoll(),
		runningPoll(),
		runningPoll("checkout"),
		finishedPoll("checkout", "build"),
	}}
	surface, out, errOut := plainSurface()

	require.NoError(t, followPlainly(t.Context(), surface, renderingOf(FormatJSONL), poller, time.Millisecond,
		"flowstate-workflow-3f7c", nil))

	lines := reportedLines(out.String())
	require.Len(t, lines, 3, "one document per change:\n%s", out.String())

	for _, line := range lines {
		var document map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &document), "not a document: %q", line)
		// The server's own field names, so a reader indexes a documented schema
		// rather than a shape invented for the occasion.
		require.Contains(t, document, "workflowId")
		require.Contains(t, document, "status")
	}

	require.Empty(t, errOut.String(),
		"prose was written alongside a machine format, which a reader would have to parse")
}

// TestWatchJSONIsOneDocumentAtTheEnd checks that the single-document form writes
// nothing until the last change is known.
func TestWatchJSONIsOneDocumentAtTheEnd(t *testing.T) {
	poller := &scriptedPoller{answers: []pollAnswer{
		runningPoll(),
		runningPoll("checkout"),
		finishedPoll("checkout", "build"),
	}}
	surface, out, _ := plainSurface()

	require.NoError(t, followPlainly(t.Context(), surface, renderingOf(FormatJSON), poller, time.Millisecond,
		"flowstate-workflow-3f7c", nil))

	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(out.String()), &document),
		"stdout was not one document: %q", out.String())
	require.Equal(t, "STATUS_COMPLETED", document["status"])
}

// TestWatchWritesNothingFinalWhenItGaveUp checks that a watch which lost the server
// does not present the last state it happened to see as the answer.
//
// A program reading --output json would otherwise conclude from a document saying
// STATUS_RUNNING that the run was still going, when what actually happened is that
// nobody knows.
func TestWatchWritesNothingFinalWhenItGaveUp(t *testing.T) {
	// One attempt's worth of allowance is impossible to ask for, so the state is
	// driven directly and the shape's ending is what is under test.
	state := newWatchState("flowstate-workflow-3f7c", nil)
	state.absorb(observed, response(v1.RunResponse_STATUS_RUNNING), nil)
	state.stop(errors.New("the server stopped answering"))

	for _, format := range []OutputFormat{FormatText, FormatJSON, FormatJSONL} {
		t.Run(string(format), func(t *testing.T) {
			surface, out, _ := plainSurface()

			require.ErrorContains(t, finishWatch(surface, renderingOf(format), state), "stopped answering")
			require.Empty(t, out.String(),
				"a watch that lost the server wrote %q as though it were the answer", out.String())
		})
	}
}

// interruptedPoller stops the watch from inside a poll, which is what ctrl+c does to
// one waiting on a run that is still going.
//
// Two shapes of that, because they take different paths through the loop: cancelled
// between polls, and cancelled *during* one — where the request fails with a
// cancelled context and connect reports it as a refusal like any other.
type interruptedPoller struct {
	cancel context.CancelFunc

	// refuse makes the interrupted poll fail, rather than answering and leaving the
	// cancellation to be noticed afterwards.
	refuse bool

	calls int
}

func (p *interruptedPoller) Poll(ctx context.Context) (*v1.GetResponse, error) {
	p.calls++
	if p.calls < 2 {
		return response(v1.RunResponse_STATUS_RUNNING), nil
	}

	p.cancel()

	if p.refuse {
		// What connect returns for a request whose context went away underneath it.
		return nil, classifyPollError("flowstate-workflow-3f7c", serverFlags{},
			connect.NewError(connect.CodeCanceled, ctx.Err()))
	}

	return response(v1.RunResponse_STATUS_RUNNING), nil
}

// TestWatchInterruptedStillNamesTheRunForAMachine is why an interrupted follow is not
// simply silent.
//
// `flow run -o json` starts a durable workload and then follows it. Interrupted before
// the run finishes, it used to write nothing at all — leaving a caller holding no
// machine-readable name for something that is still running, and therefore unable to
// watch, cancel, or terminate it without a human reading stderr. The document it gets
// now is the last state known, which is at worst the run as it was started.
func TestWatchInterruptedStillNamesTheRunForAMachine(t *testing.T) {
	t.Run("interrupted before any poll succeeded", func(t *testing.T) {
		surface, out, _ := plainSurface()

		// Cancelled before the first poll returns, so the seed is the only thing the
		// follow ever knows — the case a caller most needs covered and the one least
		// likely to be exercised by accident.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		started := response(v1.RunResponse_STATUS_RUNNING)
		require.NoError(t, followPlainly(ctx, surface, renderingOf(FormatJSON),
			&scriptedPoller{answers: []pollAnswer{{err: transientRefusal()}}},
			time.Millisecond, "flowstate-workflow-3f7c", started))

		var document map[string]any
		require.NoError(t, json.Unmarshal([]byte(out.String()), &document),
			"an interrupted machine-readable follow wrote %q, which names no run", out.String())
		require.Equal(t, "flowstate-workflow-3f7c", document["workflowId"])
		require.Equal(t, "STATUS_RUNNING", document["status"])
	})

	t.Run("the text shapes have already said it", func(t *testing.T) {
		// Nothing on stdout, because a person was told on stderr as it happened and
		// stdout carries the outputs — which a run still going has not produced.
		surface, out, _ := plainSurface()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		require.NoError(t, followPlainly(ctx, surface, renderingOf(FormatText),
			&scriptedPoller{answers: []pollAnswer{runningPoll()}},
			time.Millisecond, "flowstate-workflow-3f7c", response(v1.RunResponse_STATUS_RUNNING)))

		require.Empty(t, out.String(), "a run still going wrote outputs it does not have")
	})
}

// TestRunNamesTheStartedRunToAMachine is the same property through the command that
// makes it matter: `flow run` is the one that creates something whose identity a caller
// cannot recover if it is lost.
func TestRunNamesTheStartedRunToAMachine(t *testing.T) {
	fake := &fakeWorkflowService{
		runResponse: &v1.RunResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_RUNNING,
		},
		// Never answers, so following learns nothing beyond the seed.
		getErr: connect.NewError(connect.CodeUnavailable, errors.New("connection refused")),
	}
	serveFake(t, fake)

	cmd, out, _ := watchCommandForTest(t)
	require.NoError(t, cmd.Flags().Set("interval", "1ms"))
	require.NoError(t, cmd.Flags().Set("output", string(FormatJSON)))

	ctx, cancel := context.WithCancel(t.Context())
	cmd.SetContext(ctx)

	// Interrupted between starting the run and learning anything about it, which is
	// the window that leaves a caller holding nothing. Cancelled from inside the first
	// Get rather than on a timer, so the ordering is stated rather than raced for —
	// cancelling before the call would have failed `Run` itself and tested nothing.
	fake.onGet = cancel

	require.NoError(t, runWorkflow(cmd, []string{"../../examples/hello-world/workflow.yaml"}))

	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(out.String()), &document),
		"a durable workload was started and its id was never written where a program could read it: %q",
		out.String())
	require.Equal(t, "flowstate-workflow-3f7c", document["workflowId"])
	require.Equal(t, "0198f1e2-0000-7000-8000-000000000000", document["runId"])
}

// TestWatchDoesNotRestateTheStatusAsAReason checks that a failure message which only
// names the status again is left off.
//
// The server answers a terminal run's failure with the status name itself, so appending
// it unguarded reads `run "x" failed: STATUS_FAILED` — a sentence restating its own
// subject, which looks like a reason was retrieved when none was.
func TestWatchDoesNotRestateTheStatusAsAReason(t *testing.T) {
	for _, failure := range []string{"STATUS_FAILED", "FAILED", "failed", "  failed  "} {
		err := outcomeError(v1.RunResponse_STATUS_FAILED, "flowstate-workflow-3f7c", failure)

		require.EqualError(t, err, `run "flowstate-workflow-3f7c" failed`,
			"a message that only restates the status was appended as though it explained something")
	}

	// A real reason is still carried, which is the direction that matters once the
	// server has one to give.
	require.ErrorContains(t,
		outcomeError(v1.RunResponse_STATUS_FAILED, "flowstate-workflow-3f7c", "the registry refused the push"),
		"the registry refused the push")
}

// TestWatchStopsWhenTheWatcherDoesRatherThanWhenTheRunDoes checks that ctrl+c on a
// run that is still going is not reported as the run's outcome.
//
// A pipeline that treats an interrupted watch as a failed workload has a false
// negative it will act on — and the interrupted-mid-poll case is the one that gets
// there by accident, because CodeCanceled is a refusal and a refusal is how a watch
// learns the server has stopped answering.
func TestWatchStopsWhenTheWatcherDoesRatherThanWhenTheRunDoes(t *testing.T) {
	for name, refuse := range map[string]bool{
		"cancelled between polls": false,
		"cancelled during a poll": true,
	} {
		t.Run(name, func(t *testing.T) {
			surface, out, _ := plainSurface()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			poller := &interruptedPoller{cancel: cancel, refuse: refuse}

			require.NoError(t, followPlainly(ctx, surface, renderingOf(FormatText), poller, time.Millisecond,
				"flowstate-workflow-3f7c", nil),
				"an interrupted watch was reported as a failed run")
			require.Empty(t, out.String(), "a run still going wrote outputs it does not have")
		})
	}
}

// TestWatchDrawsNoViewWhenAFormatWasAskedFor checks the precedence between the flag
// and the terminal.
//
// A view drawn over somebody's requested JSON is this command guessing against a
// flag, which is the mistake --output exists to prevent. Asserted with the surface
// claiming a terminal, because that is the only configuration where the two answers
// differ.
func TestWatchDrawsNoViewWhenAFormatWasAskedFor(t *testing.T) {
	for _, format := range []OutputFormat{FormatJSON, FormatJSONL} {
		t.Run(string(format), func(t *testing.T) {
			poller := &scriptedPoller{answers: []pollAnswer{finishedPoll("greet")}}
			surface, out, _ := plainSurface()
			surface.ErrCaps.TTY = true

			require.NoError(t, watchRun(t.Context(), surface, renderingOf(format), poller, time.Millisecond, false,
				"flowstate-workflow-3f7c", nil))

			require.Contains(t, out.String(), `"steps"`)
			require.NotContains(t, out.String(), "\x1b",
				"a live view was drawn over a requested document")
		})
	}
}

// Run implements [flowstatev1connect.WorkflowServiceHandler].
//
// Defined on the same fake as every other verb, whose fields live in signals_test.go:
// one stand-in server covers the whole service.
func (f *fakeWorkflowService) Run(_ context.Context, req *connect.Request[v1.RunRequest]) (*connect.Response[v1.RunResponse], error) {
	f.gotRun = req.Msg
	if f.runErr != nil {
		return nil, f.runErr
	}

	return connect.NewResponse(f.runResponse), nil
}

// TestWatchPlainForcesLinesOnATerminal checks the escape hatch that makes detecting a
// terminal safe.
//
// Somebody who wants a scrollable transcript, is capturing under `script(1)`, or is
// reading with a screen reader a repainting view fights has to be able to ask. Without
// that, a terminal is a trap — and it can only be checked with the surface claiming
// one, because that is the configuration where the flag changes the answer.
func TestWatchPlainForcesLinesOnATerminal(t *testing.T) {
	poller := &scriptedPoller{answers: []pollAnswer{runningPoll(), finishedPoll("greet")}}
	surface, out, errOut := plainSurface()
	surface.ErrCaps.TTY = true

	require.NoError(t, watchRun(t.Context(), surface, renderingOf(FormatText), poller, time.Millisecond, true,
		"flowstate-workflow-3f7c", nil))

	require.Len(t, reportedLines(errOut.String()), 2,
		"--plain did not produce one line per change:\n%s", errOut.String())
	require.NotContains(t, errOut.String(), "q stops watching",
		"a live view was drawn despite --plain")
	require.Contains(t, out.String(), `"steps"`)
}

// TestRunFollowsToAnyTerminalStatus is a regression test for a loop that could not
// end.
//
// `flow run` used to poll for COMPLETED and FAILED and treat everything else as
// "still going", so a canceled, terminated, or timed-out run left it printing "still
// going" forever about a run that had stopped. It follows through the same code
// `flow watch` uses now, and this asserts the statuses that used to be invisible.
func TestRunFollowsToAnyTerminalStatus(t *testing.T) {
	for _, tc := range []struct {
		status v1.RunResponse_Status
		fails  bool
		word   string
	}{
		{status: v1.RunResponse_STATUS_COMPLETED, fails: false},
		{status: v1.RunResponse_STATUS_CANCELED, fails: true, word: "canceled"},
		{status: v1.RunResponse_STATUS_TERMINATED, fails: true, word: "terminated"},
		{status: v1.RunResponse_STATUS_TIMED_OUT, fails: true, word: "timed out"},
	} {
		t.Run(statusLabel(tc.status), func(t *testing.T) {
			fake := &fakeWorkflowService{
				runResponse: &v1.RunResponse{
					WorkflowId: "flowstate-workflow-3f7c",
					RunId:      "0198f1e2-0000-7000-8000-000000000000",
					Status:     v1.RunResponse_STATUS_RUNNING,
				},
				getResponse: response(tc.status, "hello"),
			}
			serveFake(t, fake)
			cmd, out, errOut := watchCommandForTest(t)
			require.NoError(t, cmd.Flags().Set("interval", "1ms"))

			err := runWorkflow(cmd, []string{"../../examples/hello-world/workflow.yaml"})

			require.NotNil(t, fake.gotRun, "the workload was never started")

			// Following is deliberately not pinned to the attempt just started: a
			// workload that continues as new gets a fresh run id, and a watch pinned
			// to the first would report a run that has already handed over.
			require.Nil(t, fake.gotGet.RunId,
				"the follow-up was pinned to the first attempt, which a continue-as-new leaves behind")

			// Said as soon as the run starts, because following is where somebody
			// might stop paying attention, and the id is how they come back.
			require.Contains(t, errOut.String(), "flow watch flowstate-workflow-3f7c")

			if !tc.fails {
				require.NoError(t, err)
				require.Contains(t, out.String(), "hello",
					"a completed run did not write its outputs to stdout")

				return
			}

			require.Error(t, err, "a run that stopped was followed forever")
			require.ErrorContains(t, err, tc.word)
		})
	}
}

// itself on every redraw.
//
// Protobuf maps have no iteration order and Go randomizes its own, so an unsorted
// list reads as though the run were going backwards. Repeated because a single call
// on an unsorted implementation is right by luck often enough to pass.
func TestCompletedStepsIsOrderedTheSameEveryTime(t *testing.T) {
	msg := response(v1.RunResponse_STATUS_RUNNING,
		"checkout", "build", "test", "package", "sign", "deploy", "verify")
	want := []string{"build", "checkout", "deploy", "package", "sign", "test", "verify"}

	for range 200 {
		require.Equal(t, want, completedSteps(msg))
	}
}

// TestClampWatchIntervalRaisesRatherThanRefuses checks that a smaller number is
// honoured up to the floor rather than rejected.
//
// Refusing `--interval 10ms` teaches nothing; asking a server forty times a second
// is the outcome that matters.
func TestClampWatchIntervalRaisesRatherThanRefuses(t *testing.T) {
	require.Equal(t, minWatchInterval, clampWatchInterval(10*time.Millisecond))
	require.Equal(t, minWatchInterval, clampWatchInterval(0))
	require.Equal(t, minWatchInterval, clampWatchInterval(-time.Second))
	require.Equal(t, 5*time.Second, clampWatchInterval(5*time.Second))
}

// TestWatchRefusesARunIDThatIsNotAUUIDBeforeWatching checks that a malformed flag is
// refused in the same breath `flow get` refuses it, rather than on the first poll
// several hundred milliseconds into a live view.
func TestWatchRefusesARunIDThatIsNotAUUIDBeforeWatching(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)
	cmd, _, _ := watchCommandForTest(t)

	require.NoError(t, cmd.Flags().Set("run-id", "the-latest-one"))

	require.Error(t, runWatch(cmd, []string{"flowstate-workflow-3f7c"}))
	require.Nil(t, fake.gotGet, "an invalid run id was sent anyway")
}

// TestClientPollerAsksWhatGetAsks is the one thing a fake poller cannot check.
//
// Everything above establishes that the state machine and both shapes behave; none
// of it establishes that `Poll` sends the request `flow get` sends, which is the
// only reason to believe the fake describes the real thing.
func TestClientPollerAsksWhatGetAsks(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: response(v1.RunResponse_STATUS_RUNNING, "checkout"),
	}
	address := serveFake(t, fake)

	runID := "0198f1e2-0000-7000-8000-000000000000"
	poller := clientPoller{
		workflowID: "flowstate-workflow-3f7c",
		runID:      runID,
		server:     serverFlags{address: address},
	}

	got, err := poller.Poll(t.Context())
	require.NoError(t, err)
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, got.GetStatus())
	require.Equal(t, []string{"checkout"}, completedSteps(got))

	require.Equal(t, "flowstate-workflow-3f7c", fake.gotGet.GetWorkflowId())
	require.Equal(t, runID, fake.gotGet.GetRunId())
}

// TestClientPollerLeavesAnUnsetRunIDAbsent checks that unset means "whichever
// attempt is current" rather than an empty string the schema refuses for not being a
// UUID.
func TestClientPollerLeavesAnUnsetRunIDAbsent(t *testing.T) {
	fake := &fakeWorkflowService{getResponse: response(v1.RunResponse_STATUS_RUNNING)}
	address := serveFake(t, fake)

	_, err := clientPoller{
		workflowID: "flowstate-workflow-3f7c",
		server:     serverFlags{address: address},
	}.Poll(t.Context())
	require.NoError(t, err)
	require.Nil(t, fake.gotGet.RunId, "an empty run id was sent instead of none at all")
}

// TestClientPollerFailsClosedWithNoSpec is `flow watch <id>`'s case: it asks about
// a run it did not start, on a separate invocation from whatever did, so it never
// holds the workflow specification that declared these outputs. CLAUDE.md's
// fail-closed rule says the safe answer to "cannot determine" is redact, and this
// checks the actual secret string is absent from the polled response, not merely
// that a marker is present.
func TestClientPollerFailsClosedWithNoSpec(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			Status: v1.RunResponse_STATUS_COMPLETED,
			RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
				"token": v1.NewLiteral("shh-do-not-print-me"),
			}},
		},
	}
	address := serveFake(t, fake)

	got, err := clientPoller{
		workflowID: "flowstate-workflow-3f7c",
		server:     serverFlags{address: address},
	}.Poll(t.Context())
	require.NoError(t, err)

	require.NotContains(t, got.GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue(),
		"shh-do-not-print-me")
	require.Equal(t, "[redacted: token]",
		got.GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue())
}

// TestClientPollerRevealsWithFlagAndPrecisionWithSpec checks both non-default
// paths: reveal defeats redaction, and a poller holding the specification `flow
// run` just submitted redacts only what it declared sensitive, leaving the rest —
// the non-regression direction — untouched.
func TestClientPollerRevealsWithFlagAndPrecisionWithSpec(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			Status: v1.RunResponse_STATUS_COMPLETED,
			RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
				"token": v1.NewLiteral("shh-do-not-print-me"),
				"url":   v1.NewLiteral("https://example.com/build/12"),
			}},
		},
	}
	address := serveFake(t, fake)

	spec := &v1.Workflow{
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "token", Sensitive: true},
			{Name: "url"},
		},
	}

	got, err := clientPoller{
		workflowID: "flowstate-workflow-3f7c",
		server:     serverFlags{address: address},
		spec:       spec,
	}.Poll(t.Context())
	require.NoError(t, err)

	require.Equal(t, "[redacted: token]",
		got.GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue())
	require.Equal(t, "https://example.com/build/12",
		got.GetRunOutputs().GetValues()["url"].GetLiteral().GetStringValue(),
		"a value the specification did not mark sensitive must render unchanged")

	revealed, err := clientPoller{
		workflowID: "flowstate-workflow-3f7c",
		server:     serverFlags{address: address},
		reveal:     true,
	}.Poll(t.Context())
	require.NoError(t, err)
	require.Equal(t, "shh-do-not-print-me",
		revealed.GetRunOutputs().GetValues()["token"].GetLiteral().GetStringValue(),
		"--reveal-sensitive must show the real value even with no specification in hand")
}

// TestClientPollerDoesNotWaitForeverOnAStalledServer is the regression test for a
// bound that could never start.
//
// A server that accepts the connection and then sends nothing produces no error, so a
// watch whose outage allowance advances only when a poll *returns* had nothing to
// count: the advertised thirty seconds never began and the command hung until somebody
// killed it. The fix is a deadline below the RPC layer, and what this asserts is that a
// stall becomes a failure the allowance can see — classified transient, so the watch
// tolerates a blip and gives up on an outage, rather than sitting there.
//
// The real timeout is far too long to wait for, so the test shortens it and restores
// it. What is under test is that the deadline exists and where its failure lands, not
// its value.
func TestClientPollerDoesNotWaitForeverOnAStalledServer(t *testing.T) {
	stalled := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		// Accepted, and then nothing: no status, no headers, no body.
		//
		// Bounded anyway, and generously, so that a handler still parked here cannot
		// wedge the package for the whole test timeout however the cleanups run. A
		// test whose failure mode is a three-hundred-second hang is a test that
		// stops people running the suite.
		select {
		case <-stalled:
		case <-time.After(30 * time.Second):
		}
	}))

	// Registered in this order because cleanups run last-registered-first, and
	// httptest's Close *waits* for outstanding requests. Releasing the handler has to
	// happen before the server is asked to shut down, or the two wait on each other —
	// which is exactly what the first version of this test did.
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(stalled) })

	previousTimeout := requestTimeout
	t.Cleanup(func() { requestTimeout = previousTimeout })
	requestTimeout = 200 * time.Millisecond

	// The address is carried on the poller now rather than read from a package
	// variable, so it is given here — which is also how a poller gets one in the
	// command, since it outlives the command that built it.
	start := time.Now()
	_, err := clientPoller{
		workflowID: "flowstate-workflow-3f7c",
		server:     serverFlags{address: server.URL},
	}.Poll(t.Context())
	elapsed := time.Since(start)

	require.Error(t, err, "a stalled server was waited on indefinitely")
	require.Less(t, elapsed, 10*time.Second, "the poll took %s, so no deadline applied", elapsed)

	// Transient, so the outage allowance counts it and eventually gives up — rather
	// than permanent, which would abandon a watch over one slow moment.
	var transient transientError
	require.True(t, errors.As(err, &transient),
		"a stalled server was treated as a permanent refusal, so one slow moment ends a watch")
}

// TestClientPollerClassifiesRefusals checks the classification against the real
// transport, because it is what decides whether a watch waits out its allowance or
// reports at once.
func TestClientPollerClassifiesRefusals(t *testing.T) {
	for _, tc := range []struct {
		code      connect.Code
		transient bool
	}{
		{code: connect.CodeUnavailable, transient: true},
		{code: connect.CodeInternal, transient: true},
		{code: connect.CodeNotFound, transient: false},
		{code: connect.CodeUnauthenticated, transient: false},
		{code: connect.CodePermissionDenied, transient: false},
		{code: connect.CodeInvalidArgument, transient: false},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			fake := &fakeWorkflowService{
				getErr: connect.NewError(tc.code, errors.New("refused")),
			}
			address := serveFake(t, fake)

			_, err := clientPoller{
				workflowID: "flowstate-workflow-3f7c",
				server:     serverFlags{address: address},
			}.Poll(t.Context())
			require.Error(t, err)

			var transient transientError
			require.Equal(t, tc.transient, errors.As(err, &transient),
				"%s was classified as transient=%v", tc.code, !tc.transient)
		})
	}
}

// TestWatchCountsAGateChangeAsAChange is the concurrent-work direction of the
// change detector. A gate opening or closing inside a parallel block or a
// concurrent loop moves neither the position (those workers deliberately carry
// none) nor the pending activities, so without a wait-set key the news that a
// run is now waiting on somebody, or has stopped, is exactly the change a poll
// swallows. The deadline countdown must not count: the same gate ten seconds
// closer to its bound is not news.
func TestWatchCountsAGateChangeAsAChange(t *testing.T) {
	state := newWatchState("flowstate-workflow-3f7c", nil)

	parked := func(deadline *timestamppb.Timestamp, names ...string) *v1.GetResponse {
		progress := &v1.RunProgress{StepId: "both"}
		for _, name := range names {
			progress.PendingWaits = append(progress.PendingWaits, &v1.PendingWait{
				StepId:     name + "_gate",
				SignalName: name,
				Deadline:   deadline,
			})
		}
		return &v1.GetResponse{Status: v1.RunResponse_STATUS_RUNNING, Progress: progress}
	}

	deadline := timestamppb.New(observed.Add(45 * time.Second))

	require.True(t, state.absorb(observed, parked(deadline, "left"), nil).Changed,
		"the first answer went unreported")
	require.False(t, state.absorb(observed.Add(10*time.Second), parked(deadline, "left"), nil).Changed,
		"the same gate closer to its deadline was reported as news, so a bounded wait makes every poll a change")

	require.True(t, state.absorb(observed, parked(deadline, "left", "right"), nil).Changed,
		"a second gate opened inside concurrent work and the poll swallowed it")
	require.True(t, state.absorb(observed, parked(deadline, "right"), nil).Changed,
		"a gate was released and the poll swallowed it, leaving the view naming a gate nobody holds")
	require.False(t, state.absorb(observed, parked(deadline, "right"), nil).Changed)
}

// stubGitHubActionsRunner stands up a fake runner OIDC endpoint, counting how many
// times it was asked to mint a token, and points ACTIONS_ID_TOKEN_REQUEST_URL /
// ACTIONS_ID_TOKEN_REQUEST_TOKEN at it for the duration of the test — the two env
// vars a job granted `id-token: write` finds set, and [credentialsource.NewGitHubActionsSource]
// reads on every mint.
//
// The minted token's exp claim is an hour out, so a correctly-caching source never
// has a reason to re-mint on its own; any request beyond the first is the source
// itself being rebuilt with an empty cache, which is exactly the bug this exists to
// catch.
func stubGitHubActionsRunner(t *testing.T) *atomic.Int64 {
	t.Helper()

	var mints atomic.Int64

	key := authtest.GenerateKey("gha-stub", jwa.RS256)
	token := key.Sign(
		map[string]any{"typ": "JWT", "alg": "RS256", "kid": key.ID()},
		map[string]any{
			"iss": "https://token.actions.githubusercontent.com",
			"sub": "repo:acme/infra:ref:refs/heads/main",
			"aud": "https://flowstate.example.com",
			"exp": time.Now().Add(time.Hour).Unix(),
			"iat": time.Now().Unix(),
		},
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mints.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"value": token})
	}))
	t.Cleanup(server.Close)

	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_URL", server.URL)
	t.Setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "runner-request-token")

	return &mints
}

// TestFollowMintsTheCredentialOnceAcrossManyPolls is the regression test for
// reusing the credential source across a follow's whole life, not just its first
// poll.
//
// clientPoller.Poll used to build a fresh client through newWorkflowServiceClient
// on every tick, which resolved --credential-source fresh each time too — a new,
// empty-cache [credentialsource.Source] every poll rather than one built once and
// consulted repeatedly. Against a github-actions source that mints from a runner's
// OIDC endpoint, that is a fresh mint on every single poll: at the default
// one-second interval, an hour of `flow watch` mints thirty-six hundred tokens
// instead of one. A test asserting a single poll works cannot see this, because a
// fresh client on the first-and-only tick is indistinguishable from a cached one —
// it has to watch across many polls and count what actually reached the runner.
func TestFollowMintsTheCredentialOnceAcrossManyPolls(t *testing.T) {
	mints := stubGitHubActionsRunner(t)

	const polls = 5

	fake := &fakeWorkflowService{
		getResponse: response(v1.RunResponse_STATUS_RUNNING, "checkout"),
	}
	serveFake(t, fake)

	cmd, _, _ := watchCommandForTest(t)
	require.NoError(t, cmd.Flags().Set("credential-source", "github-actions"))
	require.NoError(t, cmd.Flags().Set("audience", "https://flowstate.example.com"))
	require.NoError(t, cmd.Flags().Set("plain", "true"))

	ctx, cancel := context.WithCancel(t.Context())
	cmd.SetContext(ctx)

	var seen int
	fake.onGet = func() {
		seen++
		if seen >= polls {
			cancel()
		}
	}

	// Interrupted deliberately, once enough polls have happened to distinguish "built
	// once" from "built every tick" — the run itself never finishes, which is the
	// point: nothing here exercises whether a watch ends well, only how many times it
	// asked for a credential while going.
	require.NoError(t, runWatch(cmd, []string{"flowstate-workflow-3f7c"}))

	require.GreaterOrEqual(t, seen, polls, "the watch stopped before it polled enough times to tell")
	require.Equal(t, int64(1), mints.Load(),
		"the credential source was rebuilt on a poll after the first, minting again instead of reusing the cache")
}

// TestFollowRefusesAMisconfiguredCredentialSourceImmediately is the fail-fast
// direction of the same fix: a --credential-source that can never succeed must
// not be given thirty seconds of classifyPollError's outage allowance to fail in.
//
// github-actions with no --audience is refused by [credentialsource.Resolve] at
// construction, before a token is ever asked for. The transport used to carry
// that error forward and return it from the first RoundTrip, where connect-go
// wraps any non-connect error as CodeUnavailable — the same code a genuinely
// unreachable server produces, and one classifyPollError treats as worth asking
// again. So the assertion here is not merely that this errors, but that it does
// so having never entered the polling loop at all: the fake server records zero
// Get calls and the failure carries no worthAskingAgain-style RPC code.
func TestFollowRefusesAMisconfiguredCredentialSourceImmediately(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: response(v1.RunResponse_STATUS_RUNNING, "checkout"),
	}
	serveFake(t, fake)

	cmd, _, _ := watchCommandForTest(t)
	require.NoError(t, cmd.Flags().Set("credential-source", "github-actions"))
	// --audience deliberately left unset.

	err := runWatch(cmd, []string{"flowstate-workflow-3f7c"})
	require.Error(t, err, "github-actions with no audience must be refused")

	require.Nil(t, fake.gotGet,
		"the poll loop was entered at all for a credential source that can never succeed")

	var transient transientError
	require.False(t, errors.As(err, &transient),
		"a configuration error was classified as transient and would be retried for the outage allowance: %v", err)
	require.ErrorIs(t, err, credentialsource.ErrSourceUnusable)
}
