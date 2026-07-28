package server_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Stopping and listing are the same authorization question Get and Signal ask, so
// they are tested the same way: the negative direction first, because a check that
// refuses everyone passes a test that only tries what should work.
//
// Listing is the one that needs the negative direction most. Get and Signal are
// refused by naming a run the caller cannot reach; a listing is not asked about a
// run at all, so nothing about the request itself reveals a mistake. A List that
// forgot to filter would look completely healthy to every test that only checked
// that a tenant can see its own runs.

// TestAnotherTenantCannotStopARun checks that a run cannot be stopped by someone
// who cannot see it.
//
// Worth more than the read-side equivalent: reading another tenant's run leaks
// their data, and terminating it takes their workload away, which they discover
// as an outage.
func TestAnotherTenantCannotStopARun(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	require.NotEmpty(t, workflowID)

	t.Run("cannot cancel it", func(t *testing.T) {
		_, err := fixture.teamB.Cancel(t.Context(), connect.NewRequest(&v1.CancelRequest{
			WorkflowId: workflowID,
		}))
		require.Error(t, err, "another tenant cancelled a run it cannot see")

		// Not found rather than denied, for the same reason every other verb
		// answers that way: denied would confirm the run exists somewhere.
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("cannot terminate it", func(t *testing.T) {
		_, err := fixture.teamB.Terminate(t.Context(), connect.NewRequest(&v1.TerminateRequest{
			WorkflowId: workflowID,
			Reason:     "not mine to stop",
		}))
		require.Error(t, err, "another tenant terminated a run it cannot see")
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("and it is still running", func(t *testing.T) {
		// The refusals above would also be satisfied by a Terminate that failed
		// *after* stopping the run. What matters to the run's owner is that their
		// workload is still there.
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		require.NoError(t, err)
		require.Equal(t, v1.RunResponse_STATUS_RUNNING, resp.Msg.GetStatus(),
			"a refused stop stopped the run anyway")
	})

	t.Run("its owner still can", func(t *testing.T) {
		// The positive direction: a check that refused everyone would pass every
		// subtest above.
		_, err := fixture.teamA.Terminate(t.Context(), connect.NewRequest(&v1.TerminateRequest{
			WorkflowId: workflowID,
			Reason:     "done with it",
		}))
		require.NoError(t, err, "a run's own tenant could not stop it")

		require.Eventually(t, func() bool {
			resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
				WorkflowId: workflowID,
			}))
			return err == nil && resp.Msg.GetStatus() == v1.RunResponse_STATUS_TERMINATED
		}, 30*time.Second, 100*time.Millisecond, "the run was never actually terminated")
	})
}

// TestCancelLetsARunStop checks the cooperative half, and checks it precisely.
//
// This first asserted only that the run stopped, on the reasoning that a
// cooperative stop leaves the outcome to the workload. That was too generous by
// exactly the amount that mattered: the run did stop, and it stopped reporting
// STATUS_FAILED, because the engine wrapped Temporal's cancellation in a plain
// error and the type Temporal reads to record CANCELED was formatted away. A
// workload somebody stopped on purpose looked like a fault.
//
// Worse, the gate this workload waits on took the cancellation for its timeout —
// a cancelled selector leaves `received` false, the same shape as nobody
// answering — so the run recorded "not approved" and walked on to the next step
// instead of stopping. Both are invisible to an assertion that only asks whether
// the run is still going.
//
// So the status is pinned. A cancelled run reports cancelled, or this fails.
//
// And it is cancelled where the interesting branch is. This waited only for
// STATUS_RUNNING, which is true the instant Temporal accepts the run — before its
// first step is scheduled, let alone its gate reached. Cancelling then races the
// run's own progress, so the branch the test exists for was reached sometimes and
// by luck. It now waits for evidence the run is parked at the gate, and asserts
// afterwards that the step beyond the gate never ran: a cancellation taken for the
// gate's timeout would record "nobody approved" and walk on, which is well-formed
// enough that a status assertion alone cannot see it.
func TestCancelLetsARunStop(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	waitUntilParkedAtTheGate(t, fixture, workflowID)

	_, err = fixture.teamA.Cancel(t.Context(), connect.NewRequest(&v1.CancelRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)

	var final v1.RunResponse_Status
	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		if err != nil || resp.Msg.GetStatus() == v1.RunResponse_STATUS_RUNNING {
			return false
		}
		final = resp.Msg.GetStatus()
		return true
	}, 60*time.Second, 200*time.Millisecond, "a cancelled run never stopped")

	require.Equal(t, v1.RunResponse_STATUS_CANCELED, final,
		"a run stopped on purpose reported %s, so `flow get` tells whoever finds it that "+
			"something went wrong", final)

	// The run stopped at the gate rather than past it. `deploy` is conditional on
	// `approval.approved`, so it running at all would mean the cancellation was
	// read as an answer.
	ran, err := stepsScheduled(t.Context(), fixture, workflowID)
	require.NoError(t, err)
	require.Equal(t, []string{"requesting approval"}, ran,
		"a step ran after the run was cancelled at its gate")
}

// waitUntilParkedAtTheGate blocks until the run has actually reached its approval
// gate.
//
// Nothing on the RPC surface can answer this, which is why it reads history: a
// status says a run is going, not what it is doing. A gate with a timeout starts a
// durable timer and nothing else in this workload starts one, so a TimerStarted
// event is exactly the evidence wanted — and history is written when the workflow
// task completes, so unlike a listing there is no visibility lag to wait out.
func waitUntilParkedAtTheGate(t *testing.T, fixture *tenantFixture, workflowID string) {
	t.Helper()

	require.Eventually(t, func() bool {
		events, err := historyOf(t.Context(), fixture, workflowID)
		if err != nil {
			return false
		}

		return slices.ContainsFunc(events, func(event *historypb.HistoryEvent) bool {
			return event.GetEventType() == enumspb.EVENT_TYPE_TIMER_STARTED
		})
	}, 30*time.Second, 100*time.Millisecond, "the run never reached its approval gate")
}

// stepsScheduled reports the message each step this run scheduled was given, in
// the order they were scheduled.
//
// The message identifies the step because every step in gatedWorkflow is an echo
// with its own, which is cheaper than correlating scheduled events back to step
// ids and says the same thing.
func stepsScheduled(ctx context.Context, fixture *tenantFixture, workflowID string) ([]string, error) {
	events, err := historyOf(ctx, fixture, workflowID)
	if err != nil {
		return nil, err
	}

	var messages []string
	for _, event := range events {
		attributes := event.GetActivityTaskScheduledEventAttributes()
		if attributes == nil {
			continue
		}

		payloads := attributes.GetInput().GetPayloads()
		if len(payloads) == 0 {
			return nil, fmt.Errorf("a step was scheduled with no input")
		}

		// The resolved task is the first argument of every step activity, whichever
		// of the three was scheduled.
		var task v1.Task
		if err := converter.GetDefaultDataConverter().FromPayload(payloads[0], &task); err != nil {
			return nil, fmt.Errorf("reading what a step was scheduled with: %w", err)
		}

		messages = append(messages, task.GetInputs()["message"].GetLiteral().GetStringValue())
	}

	return messages, nil
}

// historyOf collects a run's history.
//
// It reports an error rather than asserting on one, because its callers run inside
// require.Eventually and testify evaluates that condition on its own goroutine — a
// failed assertion there is runtime.Goexit off the test goroutine, so the condition
// never returns and Eventually reports its own message a timeout later. The same
// trap listRunIDs documents, in the same package.
func historyOf(ctx context.Context, fixture *tenantFixture, workflowID string) ([]*historypb.HistoryEvent, error) {
	iterator := fixture.temporal.GetWorkflowHistory(
		ctx, workflowID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	var events []*historypb.HistoryEvent
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}

// TestListReturnsOnlyTheCallersRuns is the test the List implementation exists to
// pass.
//
// The tenant is a memo, which Temporal cannot filter on, so every run in the
// namespace comes back from the listing and the server drops the ones that are not
// the caller's. That filter is the only thing standing between a caller and every
// other tenant's run ids — and unlike Get, nothing about the request would look
// wrong if it were missing.
func TestListReturnsOnlyTheCallersRuns(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	teamARuns := make(map[string]bool)
	for range 2 {
		started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
			Workflow: gatedWorkflow(),
		}))
		require.NoError(t, err)
		teamARuns[started.Msg.GetWorkflowId()] = true
	}

	startedB, err := fixture.teamB.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)
	teamBRun := startedB.Msg.GetWorkflowId()

	// Temporal's visibility store is updated asynchronously, so a listing taken
	// immediately can legitimately be empty. Waiting for the run a tenant *should*
	// see is what makes the negative assertion below meaningful: an empty listing
	// would otherwise satisfy "sees none of team A's runs" while proving nothing.
	var listedForB []string
	var listErrB error
	require.Eventually(t, func() bool {
		listedForB, listErrB = listRunIDs(func() (*connect.Response[v1.ListResponse], error) {
			return fixture.teamB.List(t.Context(), connect.NewRequest(&v1.ListRequest{}))
		})
		return listErrB == nil && slices.Contains(listedForB, teamBRun)
	}, 30*time.Second, 200*time.Millisecond, "a tenant could not see its own run")
	require.NoError(t, listErrB, "listing a tenant's own runs was refused")

	// The direction that matters. Team B's listing must not name a run of team
	// A's, even though every one of them was in the listing the server read.
	for _, id := range listedForB {
		require.False(t, teamARuns[id],
			"a tenant's listing contained another tenant's run %q", id)
	}

	var listedForA []string
	var listErrA error
	require.Eventually(t, func() bool {
		listedForA, listErrA = listRunIDs(func() (*connect.Response[v1.ListResponse], error) {
			return fixture.teamA.List(t.Context(), connect.NewRequest(&v1.ListRequest{}))
		})
		if listErrA != nil {
			return false
		}
		for id := range teamARuns {
			if !slices.Contains(listedForA, id) {
				return false
			}
		}
		return true
	}, 30*time.Second, 200*time.Millisecond, "a tenant could not see all of its own runs")
	require.NoError(t, listErrA, "listing a tenant's own runs was refused")

	require.False(t, slices.Contains(listedForA, teamBRun),
		"a tenant's listing contained another tenant's run")
}

// TestListRefusesAPageTokenItDidNotIssue checks that a caller's token is parsed
// rather than trusted.
func TestListRefusesAPageTokenItDidNotIssue(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	_, err := fixture.teamA.List(t.Context(), connect.NewRequest(&v1.ListRequest{
		PageToken: "not a token!!",
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

// listRunIDs collects the workflow ids from one page of a listing, reporting an
// RPC failure rather than asserting on it.
//
// It must not call require.NoError, because its callers run it inside
// require.Eventually, and testify evaluates that condition on its own goroutine.
// A failed assertion there calls t.FailNow, which is runtime.Goexit off the test
// goroutine: the condition never returns, Eventually waits out its whole timeout,
// and reports its own message instead. A refused List would be diagnosed as
// Temporal visibility lag, thirty seconds later.
func listRunIDs(call func() (*connect.Response[v1.ListResponse], error)) ([]string, error) {
	resp, err := call()
	if err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(resp.Msg.GetRuns()))
	for _, run := range resp.Msg.GetRuns() {
		ids = append(ids, run.GetWorkflowId())
	}

	return ids, nil
}
