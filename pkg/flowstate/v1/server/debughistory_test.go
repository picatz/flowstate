package server_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// DebugHistory against a real Temporal server and worker: who may read a run's
// past, what the answer carries, and what it refuses (#2248).

// finishedDebuggableRun starts the debuggable workflow, releases its gate and
// waits for it to close, and returns its ids.
func finishedDebuggableRun(t *testing.T, fixture *tenantFixture) (workflowID, runID string) {
	t.Helper()

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: debuggableWorkflow()}))
	require.NoError(t, err)
	workflowID = started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	_, err = fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID, Name: "deploy-approved",
		Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(false)}},
	}))
	require.NoError(t, err)

	run := fixture.temporal.GetWorkflow(t.Context(), workflowID, "")
	require.NoError(t, run.Get(t.Context(), nil))

	return workflowID, run.GetRunID()
}

func TestDebugHistoryReadsAClosedRunAtEachOfItsPoints(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	workflowID, runID := finishedDebuggableRun(t, fixture)
	sre := as(t.Context(), "sre-1@example.com")

	last, err := fixture.teamA.DebugHistory(sre, connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID}))
	require.NoError(t, err)
	assert.Equal(t, v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED, last.Msg.GetFidelity())
	require.GreaterOrEqual(t, len(last.Msg.GetBoundaries()), 3, "a run with a gate has several workflow tasks")
	assert.Equal(t, last.Msg.GetBoundaries()[len(last.Msg.GetBoundaries())-1], last.Msg.GetEventId(), "zero names the last point")
	require.NotNil(t, last.Msg.GetSnapshot())
	assert.Equal(t, v1.WorkflowIRDigest(debuggableWorkflow()), last.Msg.GetSnapshot().GetIrDigest(),
		"the program the run executes is the one the snapshot names")

	// Every point the answer lists can be read, and answers for itself.
	for _, point := range last.Msg.GetBoundaries() {
		got, err := fixture.teamA.DebugHistory(sre, connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID, EventId: point}))
		require.NoError(t, err, "event %d", point)
		assert.Equal(t, point, got.Msg.GetEventId())
	}

	// An earlier point is the run as it was: before the gate was released, its
	// progress had completed fewer steps than at the end.
	first, err := fixture.teamA.DebugHistory(sre, connect.NewRequest(&v1.DebugHistoryRequest{
		WorkflowId: workflowID, RunId: runID, EventId: last.Msg.GetBoundaries()[0],
	}))
	require.NoError(t, err)
	assert.Less(t, first.Msg.GetProgress().GetCompletedSteps(), last.Msg.GetProgress().GetCompletedSteps())
}

func TestDebugHistoryRefusesWhatItCannotRead(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	workflowID, runID := finishedDebuggableRun(t, fixture)
	sre := as(t.Context(), "sre-1@example.com")
	read := func(ctx context.Context, request *v1.DebugHistoryRequest) error {
		_, err := fixture.teamA.DebugHistory(ctx, connect.NewRequest(request))

		return err
	}

	t.Run("a caller the run's debug policy does not name", func(t *testing.T) {
		err := read(as(t.Context(), "intruder@example.com"), &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID})
		assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	})
	t.Run("a caller without the debug action", func(t *testing.T) {
		err := read(as(t.Context(), "sre-1@example.com", "workload.signal"), &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID})
		assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	})
	t.Run("another tenant", func(t *testing.T) {
		_, err := fixture.teamB.DebugHistory(sre, connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID}))
		require.Error(t, err)
		assert.NotEqual(t, connect.CodeUnknown, connect.CodeOf(err))
	})
	t.Run("a run id that names nothing", func(t *testing.T) {
		err := read(sre, &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: uuid.NewString()})
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})
	t.Run("a run id left out", func(t *testing.T) {
		err := read(sre, &v1.DebugHistoryRequest{WorkflowId: workflowID})
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
	t.Run("a point that is not a boundary", func(t *testing.T) {
		boundaries, err := fixture.teamA.DebugHistory(sre, connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID}))
		require.NoError(t, err)
		// The event after a boundary is inside its workflow task.
		inside := boundaries.Msg.GetBoundaries()[1] + 1
		require.NotContains(t, boundaries.Msg.GetBoundaries(), inside)
		err = read(sre, &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID, EventId: inside})
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		err = read(sre, &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID, EventId: 1 << 40})
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
	t.Run("a caller that has gone away", func(t *testing.T) {
		ctx, cancel := context.WithCancel(sre)
		cancel()
		err := read(ctx, &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID})
		require.Error(t, err)
	})
	t.Run("a run that declares no debug policy", func(t *testing.T) {
		started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: gatedWorkflow()}))
		require.NoError(t, err)
		waitUntilParkedAtTheGate(t, fixture.temporal, started.Msg.GetWorkflowId())
		run := fixture.temporal.GetWorkflow(t.Context(), started.Msg.GetWorkflowId(), "")
		err = read(sre, &v1.DebugHistoryRequest{WorkflowId: started.Msg.GetWorkflowId(), RunId: run.GetRunID()})
		assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	})
}
