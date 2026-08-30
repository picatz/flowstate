package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// retryBackoffTimeoutShape is the part of Temporal's activity failure visible
// to workflow code when ScheduleToClose expires between attempts. It is returned
// as a workflow result so the assertion below observes the real SDK/server
// boundary rather than reproducing the SDK's conversion in test code.
type retryBackoffTimeoutShape struct {
	RetryState        enumspb.RetryState
	HasTimeoutError   bool
	TimeoutType       enumspb.TimeoutType
	HasApplicationErr bool
	ApplicationType   string
	ApplicationText   string
	CauseText         string
}

type retryBackoffProbeInput struct {
	ScheduleToClose time.Duration
	StartToClose    time.Duration
	RetryInterval   time.Duration
	MaximumAttempts int32
	NonRetryable    bool
	AttemptTimeout  bool
}

func retryBackoffTimeoutProbe(ctx workflow.Context, input retryBackoffProbeInput) (retryBackoffTimeoutShape, error) {
	startToClose := input.StartToClose
	if startToClose == 0 {
		startToClose = 5 * time.Second
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    startToClose,
		ScheduleToCloseTimeout: input.ScheduleToClose,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    input.RetryInterval,
			BackoffCoefficient: 1,
			MaximumInterval:    input.RetryInterval,
			MaximumAttempts:    input.MaximumAttempts,
		},
	})

	err := workflow.ExecuteActivity(ctx, retryBackoffDependencyFailure, input.NonRetryable, input.AttemptTimeout).Get(ctx, nil)
	shape := retryBackoffTimeoutShape{}

	var activityErr *temporal.ActivityError
	if errors.As(err, &activityErr) {
		shape.RetryState = activityErr.RetryState()
	}

	var timeoutErr *temporal.TimeoutError
	shape.HasTimeoutError = errors.As(err, &timeoutErr)
	if shape.HasTimeoutError {
		shape.TimeoutType = timeoutErr.TimeoutType()
	}

	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		shape.HasApplicationErr = true
		shape.ApplicationType = applicationErr.Type()
		shape.ApplicationText = applicationErr.Message()
		if applicationErr.Unwrap() != nil {
			shape.CauseText = applicationErr.Unwrap().Error()
		}
	}

	return shape, nil
}

func retryBackoffDependencyFailure(ctx context.Context, nonRetryable, attemptTimeout bool) error {
	if attemptTimeout {
		<-ctx.Done()
		return ctx.Err()
	}

	if nonRetryable {
		return temporal.NewNonRetryableApplicationError(
			"the dependency rejected the request",
			v1.ErrorKindInvalidInput.String(),
			errors.New("bad input"),
		)
	}

	return temporal.NewApplicationErrorWithOptions(
		"the dependency returned 502",
		v1.ErrorKindUpstream.String(),
		temporal.ApplicationErrorOptions{Cause: errors.New("502 Bad Gateway")},
	)
}

// TestScheduleToCloseLapsingDuringBackoffPreservesTheLastFailure pins the
// substrate shape #1163 depends on against a real Temporal dev server. The
// first attempt fails immediately, while its three-second retry delay cannot
// fit inside the 750ms ScheduleToClose budget, so the budget necessarily lapses
// while no attempt is running.
func TestScheduleToCloseLapsingDuringBackoffPreservesTheLastFailure(t *testing.T) {
	t.Parallel()

	temporalClient := newTemporalNamespace(t)
	taskQueue := "retry-backoff-timeout-" + t.Name()
	w := worker.New(temporalClient, taskQueue, worker.Options{})
	w.RegisterWorkflow(retryBackoffTimeoutProbe)
	w.RegisterActivity(retryBackoffDependencyFailure)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	for _, test := range []struct {
		name           string
		input          retryBackoffProbeInput
		wantRetryState enumspb.RetryState
		wantTimeout    enumspb.TimeoutType
		wantType       v1.ErrorKind
		wantMessage    string
		wantCause      string
	}{
		{
			name: "schedule-to-close lapses during backoff",
			input: retryBackoffProbeInput{
				ScheduleToClose: 750 * time.Millisecond,
				RetryInterval:   3 * time.Second,
				MaximumAttempts: 10,
			},
			wantRetryState: enumspb.RETRY_STATE_TIMEOUT,
			wantType:       v1.ErrorKindUpstream,
			wantMessage:    "the dependency returned 502",
			wantCause:      "502 Bad Gateway",
		},
		{
			name: "schedule-to-close lapses after an attempt timeout",
			input: retryBackoffProbeInput{
				ScheduleToClose: time.Second,
				StartToClose:    250 * time.Millisecond,
				RetryInterval:   3 * time.Second,
				MaximumAttempts: 10,
				AttemptTimeout:  true,
			},
			wantRetryState: enumspb.RETRY_STATE_TIMEOUT,
			wantTimeout:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		},
		{
			name: "maximum attempts",
			input: retryBackoffProbeInput{
				ScheduleToClose: 5 * time.Second,
				RetryInterval:   time.Second,
				MaximumAttempts: 1,
			},
			wantRetryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			wantType:       v1.ErrorKindUpstream,
			wantMessage:    "the dependency returned 502",
			wantCause:      "502 Bad Gateway",
		},
		{
			name: "non-retryable application failure",
			input: retryBackoffProbeInput{
				ScheduleToClose: 5 * time.Second,
				RetryInterval:   time.Second,
				MaximumAttempts: 10,
				NonRetryable:    true,
			},
			wantRetryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
			wantType:       v1.ErrorKindInvalidInput,
			wantMessage:    "the dependency rejected the request",
			wantCause:      "bad input",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			run, err := temporalClient.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{
				ID:        "retry-backoff-" + test.name + "-" + t.Name(),
				TaskQueue: taskQueue,
			}, retryBackoffTimeoutProbe, test.input)
			require.NoError(t, err)

			var got retryBackoffTimeoutShape
			require.NoError(t, run.Get(t.Context(), &got))

			require.Equal(t, test.wantRetryState, got.RetryState)
			if test.wantTimeout != enumspb.TIMEOUT_TYPE_UNSPECIFIED {
				require.True(t, got.HasTimeoutError)
				require.Equal(t, test.wantTimeout, got.TimeoutType,
					"the server must report the overall budget rather than retain the prior attempt's timeout type")
				require.False(t, got.HasApplicationErr)
				return
			}

			require.False(t, got.HasTimeoutError)
			require.True(t, got.HasApplicationErr)
			require.Equal(t, test.wantType.String(), got.ApplicationType)
			require.Equal(t, test.wantMessage, got.ApplicationText)
			require.Equal(t, test.wantCause, got.CauseText,
				"the last attempt's own structured cause must survive the server round trip")
		})
	}
}
