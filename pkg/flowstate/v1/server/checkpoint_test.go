package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestGetCheckpointDescribesTheStartOfARealRun starts a real run and reads the
// checkpoint its first segment began from: the claim is that the description is
// about that run's carried state (its first step, the digest of the workflow it
// executes) and that another tenant cannot read it.
func TestGetCheckpointDescribesTheStartOfARealRun(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)
	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	resp, err := fixture.teamA.GetCheckpoint(t.Context(), connect.NewRequest(&v1.GetCheckpointRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)

	require.Empty(t, resp.Msg.GetUnavailableReason(), "a run that has not left its first segment starts from a legal position")
	info := resp.Msg.GetCheckpoint()
	require.NotNil(t, info)
	assert.Equal(t, workflowID, info.GetWorkflowId())
	assert.NotEmpty(t, info.GetRunId())
	assert.Zero(t, info.GetSegment())
	assert.Equal(t, gatedWorkflow().GetSteps()[0].GetId(), info.GetStep(),
		"a first segment starts before its first step")
	assert.NotEmpty(t, info.GetSpecHash())
	assert.Positive(t, info.GetSizeBytes())

	t.Run("another tenant cannot read it", func(t *testing.T) {
		t.Parallel()

		_, err := fixture.teamB.GetCheckpoint(t.Context(), connect.NewRequest(&v1.GetCheckpointRequest{
			WorkflowId: workflowID,
		}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err),
			"a run in another tenant must be indistinguishable from a missing one")
	})

	t.Run("a missing run is not found", func(t *testing.T) {
		t.Parallel()

		_, err := fixture.teamA.GetCheckpoint(t.Context(), connect.NewRequest(&v1.GetCheckpointRequest{
			WorkflowId: "no-such-run",
		}))
		require.Error(t, err)
	})
}
