package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// TestResumeRunStartsAnOrdinaryRunFromACheckpoint resumes a real run's start
// state, with and without a patch, and proves the refusals that keep a resume
// from becoming a way around the origin's rules.
func TestResumeRunStartsAnOrdinaryRunFromACheckpoint(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)
	origin := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, origin)

	t.Run("unpatched", func(t *testing.T) {
		t.Parallel()

		resumed, err := fixture.teamA.ResumeRun(t.Context(), connect.NewRequest(&v1.ResumeRunRequest{
			WorkflowId:   origin,
			ExpectedStep: "request",
			Reason:       "rehearse the gate again",
		}))
		require.NoError(t, err)
		assert.NotEqual(t, origin, resumed.Msg.GetWorkflowId(), "a resume is a run of its own")
		assert.NotEmpty(t, resumed.Msg.GetRunId())
		assert.Equal(t, "request", resumed.Msg.GetOrigin().GetStep())
		assert.Empty(t, resumed.Msg.GetPatchDigest())

		// An ordinary run: it reaches the same gate the origin is parked on.
		waitUntilParkedAtTheGate(t, fixture.temporal, resumed.Msg.GetWorkflowId())
	})

	t.Run("patched", func(t *testing.T) {
		t.Parallel()

		patch := proto.Clone(gatedWorkflow()).(*v1.Workflow)
		patch.Steps[2].GetTask().Inputs["message"] = v1.NewLiteral("deploying the fixed program")

		resumed, err := fixture.teamA.ResumeRun(t.Context(), connect.NewRequest(&v1.ResumeRunRequest{
			WorkflowId: origin,
			Patch:      patch,
		}))
		require.NoError(t, err)
		assert.NotEmpty(t, resumed.Msg.GetPatchDigest())
		assert.Equal(t, v1.CanonicalDigest(patch), resumed.Msg.GetPatchDigest())

		newID := resumed.Msg.GetWorkflowId()
		waitUntilParkedAtTheGate(t, fixture.temporal, newID)
		_, err = fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
			WorkflowId: newID,
			Name:       "deploy-approved",
			Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"approved": v1.NewLiteral(true),
			}},
		}))
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			got, gerr := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: newID}))
			return gerr == nil && got.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
		}, 60*time.Second, 200*time.Millisecond, "the patched run never completed")
	})

	t.Run("refusals", func(t *testing.T) {
		t.Parallel()

		resume := func(c *v1.ResumeRunRequest) error {
			_, err := fixture.teamA.ResumeRun(t.Context(), connect.NewRequest(c))
			return err
		}

		wrongName := proto.Clone(gatedWorkflow()).(*v1.Workflow)
		wrongName.Name = "renamed"
		err := resume(&v1.ResumeRunRequest{WorkflowId: origin, Patch: wrongName})
		require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err),
			"a patch may change only the steps, so a renamed workflow is refused: %v", err)

		err = resume(&v1.ResumeRunRequest{WorkflowId: origin, ExpectedStep: "deploy"})
		require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err),
			"a checkpoint standing before another step than the caller inspected is refused: %v", err)

		invalid := proto.Clone(gatedWorkflow()).(*v1.Workflow)
		invalid.Steps[2] = &v1.Node{Id: "deploy"}
		err = resume(&v1.ResumeRunRequest{WorkflowId: origin, Patch: invalid})
		require.Error(t, err, "a patch with a step that has no kind must not be admitted")

		err = resume(&v1.ResumeRunRequest{WorkflowId: "no-such-run"})
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("another tenant cannot resume it", func(t *testing.T) {
		t.Parallel()

		_, err := fixture.teamB.ResumeRun(t.Context(), connect.NewRequest(&v1.ResumeRunRequest{WorkflowId: origin}))
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err),
			"a run in another tenant must be indistinguishable from a missing one")
	})
}

// TestResumeRunCannotPatchADeploymentOwnedWorkflow proves a patch does not
// carry caller-written steps past the substitution [server.WithTrustedWorkflows]
// makes on Run: the trusted specification binds, so only an unpatched resume
// of it is allowed.
func TestResumeRunCannotPatchADeploymentOwnedWorkflow(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal, server.WithTrustedWorkflows("", gatedWorkflow()))

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: gatedWorkflow()}))
	require.NoError(t, err)
	origin := started.Msg.GetWorkflowId()

	patch := proto.Clone(gatedWorkflow()).(*v1.Workflow)
	patch.Steps[2].GetTask().Inputs["message"] = v1.NewLiteral("rewritten by the caller")

	_, err = flowstate.ResumeRun(t.Context(), connect.NewRequest(&v1.ResumeRunRequest{WorkflowId: origin, Patch: patch}))
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err), "%v", err)

	_, err = flowstate.ResumeRun(t.Context(), connect.NewRequest(&v1.ResumeRunRequest{WorkflowId: origin}))
	require.NoError(t, err, "the trusted specification itself may be resumed unpatched")
}
