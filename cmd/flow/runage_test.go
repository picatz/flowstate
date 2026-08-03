package main

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `RUNNING` is the same word for a workload three seconds in and one wedged since
// Tuesday, so `flow get` says how long as well as what.
//
// The rendering is small and every branch of it is a decision that could reasonably
// have gone another way, which is what makes it worth pinning: an empty string where a
// server said nothing, elapsed rather than an instant, whole seconds, and a negative
// duration that is a fact about two clocks rather than about the run.

// TestRunAgeSaysHowLongARunHasBeenGoing covers each branch and the reason for it.
func TestRunAgeSaysHowLongARunHasBeenGoing(t *testing.T) {
	t.Parallel()

	// A fixed clock, because the running branch measures against the moment the
	// caller supplies. Measuring against a live time.Now() here raced the test
	// body: under a loaded -race run, more than half a second between this line
	// and the assertion rendered 1m31s and failed a test about arithmetic.
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	started := now.Add(-90 * time.Second)

	for _, test := range []struct {
		name string
		msg  *v1.GetResponse
		want string
		why  string
	}{
		{
			name: "a run still going",
			msg: &v1.GetResponse{
				Status:    v1.RunResponse_STATUS_RUNNING,
				StartTime: timestamppb.New(started),
			},
			want: " (running for 1m30s)",
			why:  "a running run does not say how long it has been running",
		},
		{
			name: "a run that finished",
			msg: &v1.GetResponse{
				Status:    v1.RunResponse_STATUS_COMPLETED,
				StartTime: timestamppb.New(started),
				CloseTime: timestamppb.New(started.Add(4 * time.Second)),
			},
			// Took, not "running for": the elapsed time of a finished run is a
			// property of the run rather than of when somebody asked about it, so it
			// must not be measured against the reader's clock.
			want: " (took 4s)",
			why:  "a finished run's age is measured against now instead of its own close time",
		},
		{
			name: "a server that said nothing",
			msg: &v1.GetResponse{
				Status: v1.RunResponse_STATUS_RUNNING,
			},
			// Empty rather than "0s". A server too old to send a start time told this
			// command nothing, and "0s" is a fact invented on its behalf.
			want: "",
			why:  "a run with no start time was given an age anyway",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, test.want, runAge(test.msg, now), test.why)
		})
	}
}

// TestRunAgeOfAFinishedRunDoesNotMoveWithTheClock is the property the "took" branch
// exists for, stated as something that could fail.
//
// A finished run's duration is fixed. Measuring it against `time.Now()` would make
// `flow get` on a run that finished last week report a week — plausible-looking, wrong,
// and getting worse the longer nobody checks.
func TestRunAgeOfAFinishedRunDoesNotMoveWithTheClock(t *testing.T) {
	t.Parallel()

	longAgo := time.Now().Add(-30 * 24 * time.Hour)
	msg := &v1.GetResponse{
		Status:    v1.RunResponse_STATUS_COMPLETED,
		StartTime: timestamppb.New(longAgo),
		CloseTime: timestamppb.New(longAgo.Add(7 * time.Second)),
	}

	assert.Equal(t, " (took 7s)", runAge(msg, time.Now()),
		"a run that finished a month ago is reported as having taken a month")
}

// TestRoundedDurationIsProseNotAMeasurement pins both of its jobs.
//
// Whole seconds, because a run's age is read rather than computed and `1m23.4917s` is a
// measurement of the instant this command happened to run at. And never negative: a
// start time in the future is two clocks disagreeing, which is a fact about the
// deployment rather than about the workload, and `-2s` in a status line reads as a bug
// in the run.
func TestRoundedDurationIsProseNotAMeasurement(t *testing.T) {
	t.Parallel()

	assert.Equal(t, time.Second, roundedDuration(1400*time.Millisecond),
		"a duration was reported with more precision than a reader wants")
	assert.Equal(t, 2*time.Second, roundedDuration(1600*time.Millisecond))

	assert.Equal(t, time.Duration(0), roundedDuration(-2*time.Second),
		"a clock skew between the server and this machine was reported as a negative age")
}

// A run reports RUNNING whether it is three seconds in or wedged since Tuesday, and
// the age says which. Where it has got to says *what*, which is the other half of the
// same question — and the rendering has one decision in it worth pinning.

// TestRunPositionIsEmptyWhenTheServerSaidNothing is that decision.
//
// No position is not the same as the beginning. A run whose worker did not answer,
// and one on an interpreter built before the query existed, both report nothing —
// facts about the deployment rather than about the workload. Rendering either as "on
// the first step" invents a fact on the run's behalf, and does it exactly when
// somebody is looking because something seems stuck.
func TestRunPositionIsEmptyWhenTheServerSaidNothing(t *testing.T) {
	t.Parallel()

	theme := ui.Plain(io.Discard, io.Discard).ErrTheme

	assert.Empty(t, runPosition(theme, nil),
		"a run with no reported position was given one anyway")
	assert.Empty(t, runPosition(theme, &v1.RunProgress{CompletedSteps: 3}),
		"a position with no step name still rendered something")
}

// TestRunPositionReadsAsAPathInward keeps the nested case on one line.
//
// Where a run actually spends its time is inside something — the body of a loop that
// is the second step — and the whole value of this beside a status is being readable
// at a glance. Three lines of indentation would say the same thing and be worse.
func TestRunPositionReadsAsAPathInward(t *testing.T) {
	t.Parallel()

	theme := ui.Plain(io.Discard, io.Discard).ErrTheme

	assert.Equal(t, " on deploy", runPosition(theme, &v1.RunProgress{StepId: "deploy"}),
		"a top-level position did not render as the step it is on")

	assert.Equal(t, " on each > upload",
		runPosition(theme, &v1.RunProgress{StepId: "each", Path: []string{"upload"}}),
		"a position inside a loop body did not say what it was inside")
}
