package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A run parked on an approval was RUNNING and nothing else through this RPC. An
// operator could see that it was waiting and not what for, so the signal name to
// send was recoverable only from the file the run was compiled from - and whether
// a `signals:` policy would refuse them was not recoverable at all.
//
// The waits ride on the position's own query, so nothing in the Get handler had
// to learn about them: what these tests add is that the answer survives the
// worker, the server, the wire and the schema, which is the path a capability
// has to reach before anyone can use it.

// gateWorkflow parks on a signal and then sleeps, so the run is still going
// after the gate has been opened and can be asked what it is parked on then.
func gateWorkflow(name, signal string, policy *v1.SignalPolicy) *v1.Workflow {
	wf := &v1.Workflow{
		Name: name,
		Steps: []*v1.Node{
			{
				Id: "gate",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: signal}},
					// Bounded so a failing test does not leave a worker holding
					// a run open, and long enough that the run is reliably still
					// parked when it is asked.
					Timeout: durationpb.New(10 * time.Minute),
				}},
			},
			{
				Id: "settle",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind: &v1.Wait_Duration{Duration: durationpb.New(90 * time.Second)},
				}},
			},
		},
	}
	if policy != nil {
		wf.Signals = map[string]*v1.SignalPolicy{signal: policy}
	}

	return wf
}

// TestAGetOnAParkedRunReportsTheGate is the feature, end to end through the RPC.
func TestAGetOnAParkedRunReportsTheGate(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gateWorkflow("parked-on-a-gate", "approve", &v1.SignalPolicy{
			Allow: []*v1.SignalPolicyRule{{Subject: "https://idp.example#release-manager"}},
		}),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	var progress *v1.RunProgress
	require.Eventually(t, func() bool {
		got, gerr := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		if gerr != nil {
			return false
		}
		progress = got.Msg.GetProgress()

		return len(progress.GetPendingWaits()) == 1
	}, 60*time.Second, 250*time.Millisecond,
		"a run parked on a gate never reported it; last answer was %v", progress)

	wait := progress.GetPendingWaits()[0]
	assert.Equal(t, "gate", wait.GetStepId())
	assert.Equal(t, "approve", wait.GetSignalName(),
		"the reported name is the one an operator passes to `flow signal`")
	assert.Empty(t, wait.GetPath(), "a top-level gate reported a path into itself")
	assert.True(t, wait.GetPoliced(),
		"a gate whose name the workflow's signals: declares reported itself unpoliced, which is "+
			"the difference between a delivery nobody sent and one the server refused")
	require.NotNil(t, wait.GetDeadline(), "a bounded gate reported no deadline")
	assert.True(t, wait.GetDeadline().AsTime().After(time.Now()),
		"a gate that has not lapsed reported a deadline in the past")
	assert.False(t, progress.GetPendingWaitsTruncated())

	// Cleaned up rather than left parked for its whole timeout, since the
	// fixture's worker is shared with every other test in this package.
	_, err = fixture.teamA.Terminate(t.Context(), connect.NewRequest(&v1.TerminateRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)
}

// TestAGateStopsBeingReportedOnceItIsOpened is the half that makes the field
// worth reading: a live answer that only ever grew would name a gate somebody
// had already opened, and an operator would send a second signal to a run that
// had moved on.
func TestAGateStopsBeingReportedOnceItIsOpened(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		// Unpoliced, so the fixture's own caller may deliver: what is under test
		// here is the reporting, and the policy path is pinned above.
		Workflow: gateWorkflow("opened-gate", "proceed", nil),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	require.Eventually(t, func() bool {
		got, gerr := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))

		return gerr == nil && len(got.Msg.GetProgress().GetPendingWaits()) == 1
	}, 60*time.Second, 250*time.Millisecond, "the run never reported parking on its gate")

	_, err = fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "proceed",
	}))
	require.NoError(t, err)

	// The run walks on to the sleep after the gate, so it is still RUNNING and
	// still answering when this asks: an empty set here is "parked on nothing",
	// not "finished".
	var progress *v1.RunProgress
	require.Eventually(t, func() bool {
		got, gerr := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		if gerr != nil || got.Msg.GetStatus() != v1.RunResponse_STATUS_RUNNING {
			return false
		}
		progress = got.Msg.GetProgress()

		return progress.GetStepId() == "settle"
	}, 60*time.Second, 250*time.Millisecond,
		"the run never moved past the gate it was signaled through; last answer was %v", progress)

	assert.Empty(t, progress.GetPendingWaits(),
		"the run kept reporting a gate that had already been opened")

	_, err = fixture.teamA.Terminate(t.Context(), connect.NewRequest(&v1.TerminateRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)
}

// TestAnOlderWorkersAnswerCarriesNoWaits is the old-writer, new-reader
// direction, and the reason there is no compatibility arm anywhere to read.
//
// A run pinned to an interpreter built before this existed answers the progress
// query with a message carrying only the three fields that existed then. Proto3
// writes nothing at all for a field that is unset, so the bytes below are
// byte-for-byte what such a worker emits, and the new reader must decode them
// into an answer that reports a position and no waits - never an error, never a
// default that reads like "this run is parked on nothing" being distinguishable
// from "this worker cannot say".
//
// The distinction the schema draws is one level up and is already pinned by
// [TestAGetStillWorksWhenNoWorkerCanAnswer]: a query that fails leaves progress
// itself unset, which is what "no worker answered" looks like.
func TestAnOlderWorkersAnswerCarriesNoWaits(t *testing.T) {
	t.Parallel()

	dataConverter := converter.GetDefaultDataConverter()

	payload, err := dataConverter.ToPayload(&v1.RunProgress{
		StepId:         "gate",
		Path:           []string{"each"},
		CompletedSteps: 2,
	})
	require.NoError(t, err)

	var got v1.RunProgress
	require.NoError(t, dataConverter.FromPayload(payload, &got),
		"an older worker's answer could not be decoded by a reader that knows the new fields")

	assert.Equal(t, "gate", got.GetStepId(), "the fields that did exist stopped arriving")
	assert.Equal(t, []string{"each"}, got.GetPath())
	assert.Equal(t, int32(2), got.GetCompletedSteps())

	assert.Nil(t, got.GetPendingWaits(),
		"an older worker's answer produced waits it never reported")
	assert.False(t, got.GetPendingWaitsTruncated(),
		"an older worker's answer called itself truncated, which would tell a reader "+
			"that waits were dropped rather than never sent")
}
