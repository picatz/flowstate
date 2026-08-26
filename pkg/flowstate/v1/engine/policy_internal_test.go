package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// The two timeouts and the rule that applies a step's own over them moved to v1,
// where the local driver can read them — it had neither, so a step declaring no
// `timeout:` was bounded per attempt and overall in production and by nothing at
// all in the run that exists to rehearse it.
//
// Moving a value is the one change that is supposed to be invisible, and the way
// to say so is to pin the whole options struct rather than the constants: what a
// step actually runs under is this, and a value read from a new home that produces
// a different option here would have moved the behavior with it.

// TestDefaultActivityOptionsAreUnchangedByTheMove pins the options a step with no
// policy at all runs under.
func TestDefaultActivityOptionsAreUnchangedByTheMove(t *testing.T) {
	t.Parallel()

	want := workflow.ActivityOptions{
		StartToCloseTimeout:    2 * time.Minute,
		ScheduleToCloseTimeout: 10 * time.Minute,

		// Not part of the move — later, deliberate changes, kept in this literal
		// because the point of pinning the whole struct is that every field a step
		// runs under is written down somewhere a diff has to touch.
		// [TestAStepWaitsForItsOwnCancellation] is where the reasoning lives, and
		// these two are one feature: the heartbeat is how a cancellation reaches a
		// running activity at all, so it is what makes waiting for one short.
		WaitForCancellation: true,
		HeartbeatTimeout:    30 * time.Second,

		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        30 * time.Second,
			MaximumAttempts:        5,
			NonRetryableErrorTypes: nonRetryableErrorTypes(),
		},
	}

	assert.Equal(t, want, activityOptionsFor(nil, ""),
		"the durations moved to v1 and the behavior moved with them")

	// Stated twice on purpose: the literals above are what the constants used to
	// be, and this is that the constants are what v1 now says.
	assert.Equal(t, v1.DefaultStartToCloseTimeout, want.StartToCloseTimeout)
	assert.Equal(t, v1.DefaultScheduleToCloseTimeout, want.ScheduleToCloseTimeout)
}

// TestDeclaredTimeoutPrecedenceIsUnchangedByTheMove pins the two directions a
// declared `timeout:` moves the options in, which is the part that now lives in
// [v1.StepTimeoutsFor].
func TestDeclaredTimeoutPrecedenceIsUnchangedByTheMove(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name                     string
		policy                   *v1.StepPolicy
		startToClose, scheduleTo time.Duration
	}{
		{
			name:         "a short timeout leaves the overall bound alone",
			policy:       &v1.StepPolicy{Timeout: durationpb.New(30 * time.Second)},
			startToClose: 30 * time.Second,
			scheduleTo:   10 * time.Minute,
		},
		{
			name:         "a long timeout widens the overall bound to fit its attempts",
			policy:       &v1.StepPolicy{Timeout: durationpb.New(5 * time.Minute)},
			startToClose: 5 * time.Minute,
			scheduleTo:   25 * time.Minute,
		},
		{
			name: "sized by the attempts the step declares, not by the default",
			policy: &v1.StepPolicy{
				Timeout: durationpb.New(5 * time.Minute),
				Retry:   &v1.RetryPolicy{MaxAttempts: 1},
			},
			startToClose: 5 * time.Minute,
			scheduleTo:   10 * time.Minute,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			opts := activityOptionsFor(test.policy, "")
			assert.Equal(t, test.startToClose, opts.StartToCloseTimeout)
			assert.Equal(t, test.scheduleTo, opts.ScheduleToCloseTimeout)
		})
	}
}

// TestAStepWaitsForItsOwnCancellation pins the option a saga's correctness rests on.
//
// Temporal's default for `WaitForCancellation` is false, which resolves an activity
// future when cancellation is *requested* rather than when the activity has stopped.
// The workflow then believes the step is over while the worker is still running it,
// and compensation for a cancelled run starts immediately — so the `delete` races
// the `create` it is undoing, can win, and the summary reports a resource taken
// back that is in fact still allocated. That is the one failure mode worse than not
// compensating at all, because it is the sentence that makes somebody stop looking.
//
// Pinned as configuration rather than as a race reproduced in a test, deliberately.
// The window is real and it is also small and scheduler-dependent, so a test that
// tried to hit it would either be slow or be the flaky kind that gets deleted — and
// a deleted test defends nothing. What is asserted instead is the property that
// closes the window, in both the shape a step gets by default and the shape a step
// that declares its own policy gets.
func TestAStepWaitsForItsOwnCancellation(t *testing.T) {
	t.Parallel()

	assert.True(t, defaultActivityOptions().WaitForCancellation,
		"a cancelled step reports finished before it has stopped, so a compensation "+
			"can undo work that is still on its way to succeeding")

	// Not overridable, for the reason NonRetryableErrorTypes is not: whether a
	// workload's effects have actually stopped when the run says they have is not
	// something a file gets an opinion about. Every knob a step can turn is turned
	// here, so this fails if a later edit ever routes one of them through this
	// field.
	declared := activityOptionsFor(&v1.StepPolicy{
		Timeout: durationpb.New(time.Second),
		Retry: &v1.RetryPolicy{
			MaxAttempts:        9,
			InitialInterval:    durationpb.New(time.Millisecond),
			BackoffCoefficient: 3,
			MaxInterval:        durationpb.New(time.Minute),
		},
	}, stepSummary("declares_a_policy"))

	assert.True(t, declared.WaitForCancellation,
		"a step that declares a retry or a timeout stopped waiting for its own "+
			"cancellation, so declaring an ordinary policy silently reopened the window")
}
