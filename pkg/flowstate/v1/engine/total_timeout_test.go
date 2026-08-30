package engine_test

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTotalTimeoutEndsTheStepDurable is the second of the two driver callers
// [conformance.TotalTimeoutTaskDef] asks for: the durable driver turns a step's
// `total_timeout:` into the activity's ScheduleToCloseTimeout, which Temporal
// enforces server-side across every attempt and every backoff between them —
// the same promise TestTotalTimeoutEndsTheStepLocal makes of the local driver's
// own caused context budget, reached by an entirely different mechanism (#920).
//
// Registered on [v1.DefaultRegistry] rather than a context-scoped one, because
// the activity executing a step runs in a context Temporal hands it and cannot
// see a registry installed on the workflow's — see registerPluginIdentityTask
// in plugin_identity_test.go for the whole of that boundary.
func TestTotalTimeoutEndsTheStepDurable(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int64

	require.NoError(t, v1.DefaultRegistry().Register(conformance.TotalTimeoutTaskDef(&attempts)))

	temporalClient := newTemporalNamespace(t)
	startWorker(t, temporalClient)

	run, err := temporalClient.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{
		ID:        "total-timeout-tolerated-" + t.Name(),
		TaskQueue: engine.RunTaskQueueName,
	}, engine.Run, &v1.RunState{
		Workflow: conformance.TotalTimeoutWorkflow("total-timeout-durable", "poll"),
	})
	require.NoError(t, err)

	var out v1.Workflow_StepOutputs
	require.NoError(t, run.Get(t.Context(), &out),
		"the budget expiring is an ordinary step failure `continue_on_error:` tolerates, not a failure of the run")

	conformance.AssertTotalTimeoutEndedTheStep(t, "the durable driver", out.GetStepValues()["poll"], attempts.Load())
	conformance.AssertTotalTimeoutSuppressesWidening(t, "the durable driver")

	run, err = temporalClient.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{
		ID:        "total-timeout-failure-" + t.Name(),
		TaskQueue: engine.RunTaskQueueName,
	}, engine.Run, &v1.RunState{
		Workflow: conformance.TotalTimeoutFailureWorkflow("total-timeout-failure-durable", "poll"),
	})
	require.NoError(t, err)

	err = run.Get(t.Context(), nil)
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr), "the terminal failure must carry Flowstate's structured kind")
	kind, ok := v1.ParseErrorKind(appErr.Type())
	require.True(t, ok)
	conformance.AssertTotalTimeoutFailure(t, "the durable driver", kind, appErr.Message())

	var dependencyErr *temporal.ApplicationError
	for cause := appErr.Unwrap(); cause != nil; cause = errors.Unwrap(cause) {
		candidate, isApplicationError := cause.(*temporal.ApplicationError)
		if isApplicationError && candidate.Type() == v1.ErrorKindUpstream.String() {
			dependencyErr = candidate
			break
		}
	}
	require.NotNil(t, dependencyErr,
		"the last attempt's structured dependency failure must remain reachable beneath the overall timeout")
	require.ErrorContains(t, dependencyErr, conformance.TotalTimeoutFailure)

	run, err = temporalClient.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{
		ID:        "total-timeout-exhaustion-" + t.Name(),
		TaskQueue: engine.RunTaskQueueName,
	}, engine.Run, &v1.RunState{
		Workflow: conformance.TotalTimeoutExhaustionWorkflow("total-timeout-exhaustion-durable", "poll"),
	})
	require.NoError(t, err)

	err = run.Get(t.Context(), nil)
	require.Error(t, err)
	appErr = nil
	require.True(t, errors.As(err, &appErr))
	kind, ok = v1.ParseErrorKind(appErr.Type())
	require.True(t, ok)
	conformance.AssertTotalTimeoutLeavesAttemptExhaustionAlone(
		t, "the durable driver", kind, appErr.Message())
}
