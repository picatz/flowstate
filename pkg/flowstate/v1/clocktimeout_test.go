package flowstatev1_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A step's `timeout:` and `total_timeout:` are measured on the run's clock, so
// under a virtual clock they fire when virtual time reaches them rather than
// never. These tests drive the local driver directly, with a task that spends
// virtual time the way a stub's scripted delay will.

var clockTimeoutStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// runUnderVirtualClock runs one step whose task spends `spend` of virtual time
// (or the whole of a time it never gets, when spend is long) under policy, and
// returns what the run reported and how much virtual time went by.
func runUnderVirtualClock(t *testing.T, spend time.Duration, fail error, policy *v1.StepPolicy) (*v1.Workflow_StepOutputs, time.Duration, error) {
	t.Helper()

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{
		Name:    "spend_virtual_time",
		Summary: "test fixture that spends virtual time, then answers or fails",
		Fn: func(ctx context.Context, _ map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			if spend > 0 {
				// The deadline is withdrawn on every way out: an abandoned
				// virtual timer would leave the clock believing this
				// goroutine still parked, and advance to it.
				clock := v1.ClockFromContext(ctx)
				timer := clock.After(spend)
				defer v1.DiscardTimer(clock, timer)
				select {
				case <-timer:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			if fail != nil {
				return nil, fail
			}

			return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"ok": v1.NewLiteral(true)}}, nil
		},
	}))

	clock := v1.NewVirtualClock(clockTimeoutStart)
	ctx := v1.NewContextWithRegistry(v1.NewContextWithClock(t.Context(), clock), registry)

	started := time.Now()
	out, err := v1.Run(ctx, &v1.Workflow{
		Name:    "clock-timeout",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:     "step",
			Kind:   &v1.Node_Task{Task: &v1.Task{Name: "spend_virtual_time"}},
			Policy: policy,
		}},
	})
	require.Less(t, time.Since(started), 10*time.Second, "the run spent real time; the clock was not driving it")

	return out, clock.Now().Sub(clockTimeoutStart), err
}

// TestAStepTimeoutFiresOnTheVirtualClock is the claim itself: a task that would
// wait an hour is ended at ten virtual seconds, classified as a timeout.
func TestAStepTimeoutFiresOnTheVirtualClock(t *testing.T) {
	t.Parallel()

	_, elapsed, err := runUnderVirtualClock(t, time.Hour, nil,
		&v1.StepPolicy{Timeout: durationpb.New(10 * time.Second), Retry: &v1.RetryPolicy{MaxAttempts: 1}})

	require.Error(t, err)
	require.Equal(t, v1.ErrorKindTimeout, v1.ClassifyError(err))
	require.Equal(t, 10*time.Second, elapsed, "the step ended at a moment other than its deadline")
}

// TestAStepUnderATimeoutThatAnswersInTimeSpendsOnlyItsOwnTime is the negative
// direction, and the failure #278 was: a deadline that pulled the clock forward
// on its own would end every step at its bound however fast it answered.
func TestAStepUnderATimeoutThatAnswersInTimeSpendsOnlyItsOwnTime(t *testing.T) {
	t.Parallel()

	for _, spend := range []time.Duration{0, 4 * time.Second, 9 * time.Second} {
		out, elapsed, err := runUnderVirtualClock(t, spend, nil,
			&v1.StepPolicy{Timeout: durationpb.New(10 * time.Second)})

		require.NoError(t, err, "spent %s under a 10s timeout", spend)
		require.True(t, out.GetStepValues()["step"].GetNamedValues()["ok"].GetLiteral().GetBoolValue())
		require.Equal(t, spend, elapsed, "a step answering after %s moved the clock by %s", spend, elapsed)
	}
}

// TestATotalTimeoutEndsTheRetryLoopOnTheVirtualClock is the schedule-to-close
// budget: the attempts fail at once, the backoff spends virtual time, and the
// budget lapses mid-backoff with attempts to spare.
func TestATotalTimeoutEndsTheRetryLoopOnTheVirtualClock(t *testing.T) {
	t.Parallel()

	_, elapsed, err := runUnderVirtualClock(t, 0, &v1.TaskError{Task: "spend_virtual_time", Kind: v1.ErrorKindUpstream, Err: errors.New("reset")},
		&v1.StepPolicy{
			TotalTimeout: durationpb.New(5 * time.Second),
			Retry: &v1.RetryPolicy{
				MaxAttempts:        20,
				InitialInterval:    durationpb.New(2 * time.Second),
				BackoffCoefficient: 1,
				MaxInterval:        durationpb.New(2 * time.Second),
			},
		})

	require.Error(t, err)
	require.Equal(t, v1.ErrorKindTimeout, v1.ClassifyError(err))
	require.Equal(t, 5*time.Second, elapsed, "the budget ended the step at a moment other than its deadline")
}

// TestAnAttemptTimeoutUnderATotalTimeoutStillReadsAsADeadline checks the nested
// case: the per-attempt context hangs off the budget's, and a lapsed budget
// must read as a deadline one level down, as it does on the wall clock.
func TestAnAttemptTimeoutUnderATotalTimeoutStillReadsAsADeadline(t *testing.T) {
	t.Parallel()

	_, elapsed, err := runUnderVirtualClock(t, time.Hour, nil, &v1.StepPolicy{
		Timeout:      durationpb.New(30 * time.Second),
		TotalTimeout: durationpb.New(10 * time.Second),
		Retry:        &v1.RetryPolicy{MaxAttempts: 1},
	})

	require.Error(t, err)
	require.Equal(t, v1.ErrorKindTimeout, v1.ClassifyError(err))
	require.Equal(t, 10*time.Second, elapsed, "the nearer bound did not win")
}

// TestAVirtualBoundKeepsTheParentsWallDeadline: only the virtual instant is
// hidden from a task. `flow test` installs a wall-clock case deadline above the
// virtual clock, and deadline-aware tasks must keep seeing it.
func TestAVirtualBoundKeepsTheParentsWallDeadline(t *testing.T) {
	t.Parallel()

	var sawDeadline bool
	var deadline time.Time
	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{
		Name:    "report_deadline",
		Summary: "test fixture that reports whether its context carries a deadline",
		Fn: func(ctx context.Context, _ map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			deadline, sawDeadline = ctx.Deadline()
			return &v1.Node_Outputs{}, nil
		},
	}))

	parent, cancel := context.WithTimeout(t.Context(), time.Hour)
	defer cancel()
	wall, _ := parent.Deadline()

	ctx := v1.NewContextWithRegistry(v1.NewContextWithClock(parent, v1.NewVirtualClock(clockTimeoutStart)), registry)
	_, err := v1.Run(ctx, &v1.Workflow{
		Name:    "clock-deadline",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:     "step",
			Kind:   &v1.Node_Task{Task: &v1.Task{Name: "report_deadline"}},
			Policy: &v1.StepPolicy{Timeout: durationpb.New(10 * time.Second)},
		}},
	})
	require.NoError(t, err)
	require.True(t, sawDeadline, "the parent's wall deadline was hidden from the task")
	require.True(t, wall.Equal(deadline), "the task saw %s, not the parent's %s", deadline, wall)
}
