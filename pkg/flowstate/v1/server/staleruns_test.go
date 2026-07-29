package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A run id goes stale, and saying so is the whole job.
//
// A run id is a position in a chain rather than a name for the workload: a
// workload that continued as new is several executions sharing one workflow id,
// and the id `flow list` prints is whichever segment was current when the listing
// ran. An operator who copies that id and acts on it a moment later is doing the
// obvious thing, and it stops working for a reason nothing tells them.
//
// It used to answer `internal` — a 500 for a request that is well-formed,
// authorized, and about a workload that plainly exists. Wrong twice over: it says
// the server broke when nothing did, and it leaves an operator unable to tell
// whether retrying would help.

// TestActingOnAFinishedRunSaysSoRatherThanFailing covers the three RPCs that take
// a run id and act on it.
//
// Driven through the real client against a real Temporal, because the thing under
// test is how Temporal's own refusal is classified — a fake would be asserting
// this repo's guess about that refusal rather than the refusal.
func TestActingOnAFinishedRunSaysSoRatherThanFailing(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	_, err = fixture.teamA.Cancel(t.Context(), connect.NewRequest(&v1.CancelRequest{WorkflowId: workflowID}))
	require.NoError(t, err)

	// Read the run id back the way an operator would, rather than from the start
	// response: what this is about is the id a *listing* hands out.
	var runID string
	require.Eventually(t, func() bool {
		resp, lerr := fixture.teamA.List(t.Context(), connect.NewRequest(&v1.ListRequest{}))
		if lerr != nil {
			return false
		}
		for _, run := range resp.Msg.GetRuns() {
			if run.GetWorkflowId() == workflowID && run.GetStatus() != v1.RunResponse_STATUS_RUNNING {
				runID = run.GetRunId()
				return runID != ""
			}
		}
		return false
	}, 60*time.Second, 200*time.Millisecond, "the run never appeared in a listing as finished")

	for name, act := range map[string]func() error{
		"terminate": func() error {
			_, err := fixture.teamA.Terminate(t.Context(), connect.NewRequest(&v1.TerminateRequest{
				WorkflowId: workflowID,
				RunId:      runID,
				Reason:     "acting on an id from a listing",
			}))
			return err
		},
		"signal": func() error {
			_, err := fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
				WorkflowId: workflowID,
				RunId:      runID,
				Name:       "approval",
			}))
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := act()
			require.Error(t, err, "%s on a finished run reported success", name)

			assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err),
				"%s on a finished run answered %s; a stale run id is the caller's situation, not a server fault",
				name, connect.CodeOf(err))

			// The message has to carry the remedy. The id was accurate when it was
			// printed, so an operator has no reason to suspect it without being
			// told, and no reason to guess that dropping it is what works.
			assert.Contains(t, err.Error(), "already finished")
			assert.Contains(t, err.Error(), "omit the run id",
				"the refusal does not say what to do instead")
		})
	}
}

// TestActingOnACurrentRunStillWorks checks that a run id is still usable.
//
// It is not the other direction of the classification, and saying so matters: a
// request that succeeds never reaches the classifier, so this stays green even if
// every error is answered FailedPrecondition. Verified by making that change and
// watching it pass. What guards the classification is
// TestActOnRunErrorLeavesEverythingElseInternal, next to the function itself.
//
// What this does cover is that the fix did not reach too far in the other sense —
// rejecting or ignoring a run id that names the segment the caller meant, which is
// the reason the field exists.
func TestActingOnACurrentRunStillWorks(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID, runID := started.Msg.GetWorkflowId(), started.Msg.GetRunId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	// The run id of a segment that is current is still a perfectly good argument,
	// and is the reason the field exists: it pins the request to the execution the
	// caller meant rather than to whatever is running under that id now.
	_, err = fixture.teamA.Terminate(t.Context(), connect.NewRequest(&v1.TerminateRequest{
		WorkflowId: workflowID,
		RunId:      runID,
		Reason:     "still current",
	}))
	require.NoError(t, err, "terminating the current segment by run id was refused")
}
