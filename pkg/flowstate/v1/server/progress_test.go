package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// `flow get` on a running run could say how long it had been going and not what it
// was doing, so a slow step and a wedged one looked identical — and those want
// opposite responses from whoever is looking.
//
// The position comes from a Temporal query against the live run. Two things about
// that shape the tests here. It reaches a *worker* rather than the service, so it
// fails for reasons that say nothing about the run; and it is the only part of a Get
// that can fail on its own, so what happens when it does is a decision worth pinning.

// TestAGetOnARunningRunSaysWhichStepItIsOn is the feature, end to end through the
// RPC rather than against the engine.
//
// A real worker, a real query, and a workload parked on a step long enough to be
// asked about. Engine tests already cover what the handler answers; what this adds is
// that the answer survives the server, the wire and the schema — the path a capability
// has to reach before anyone can use it.
func TestAGetOnARunningRunSaysWhichStepItIsOn(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name: "parked",
			Steps: []*v1.Node{
				bulky("first", 1),
				{
					Id: "waiting",
					Kind: &v1.Node_Wait{Wait: &v1.Wait{
						// Long enough that the run is reliably still on it, and
						// bounded so a failing test does not leave a worker
						// holding a run for an hour.
						Kind: &v1.Wait_Duration{Duration: durationpb.New(90 * time.Second)},
					}},
				},
			},
		},
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	// Eventually, because reaching the second step means scheduling and completing
	// an activity first. The assertion is inside the poll so that a run which never
	// arrives fails saying what it was doing instead.
	var progress *v1.RunProgress
	require.Eventually(t, func() bool {
		got, gerr := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		if gerr != nil {
			return false
		}
		progress = got.Msg.GetProgress()

		return progress.GetStepId() == "waiting"
	}, 60*time.Second, 250*time.Millisecond,
		"a running run never reported the step it was parked on; last answer was %v", progress)

	assert.Equal(t, int32(1), progress.GetCompletedSteps(),
		"the run had finished its first step and the count did not say so")
	assert.Empty(t, progress.GetPath(),
		"a top-level step reported a path into itself")

	// Cleaned up rather than left running for its full wait, since the fixture's
	// worker is shared with every other test in this package.
	_, err = fixture.teamA.Terminate(t.Context(), connect.NewRequest(&v1.TerminateRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)
}

// TestAFinishedRunReportsNoProgress is the boundary the field's meaning depends on.
//
// A finished run's position is its outputs, and a progress reported alongside them
// would be a second, staler answer to the same question. Unset is also what a caller
// reads as "not applicable", which is only true if it is never set here.
func TestAFinishedRunReportsNoProgress(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name:  "finishes",
			Steps: []*v1.Node{bulky("only", 1)},
		},
	}))
	require.NoError(t, err)

	var got *connect.Response[v1.GetResponse]
	require.Eventually(t, func() bool {
		var gerr error
		got, gerr = fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: started.Msg.GetWorkflowId(),
		}))

		return gerr == nil && got.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
	}, 60*time.Second, 200*time.Millisecond, "the run never finished")

	assert.Nil(t, got.Msg.GetProgress(),
		"a finished run reported a position, which is a second and staler answer to "+
			"the question its outputs already answer")
}

// TestAGetStillWorksWhenNoWorkerCanAnswer is the failure mode this must not have.
//
// A query reaches a worker, so it fails for reasons that are not about the run: none
// is polling the queue, the worker is busy, the run is pinned to an interpreter built
// before the handler existed. In every one of those the status, the times and the ids
// are still correct and still worth returning.
//
// Written with no worker started at all, which is the same absence from the query's
// point of view. If `Get` ever starts failing because a position could not be
// fetched, `flow get` on a healthy run breaks the moment a worker restarts — and it
// breaks looking like the run's fault.
func TestAGetStillWorksWhenNoWorkerCanAnswer(t *testing.T) {
	t.Parallel()

	// Built without a worker, unlike newTenantFixture, which starts one. Each
	// fixture gets its own Temporal namespace, so "no worker" here really means no
	// worker for this run rather than none in the package.
	temporal, _ := newTemporalNamespace(t)
	teamA := mustNew(t, temporal, server.WithNamespace("team-a"))

	started, err := teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name:  "nobody-home",
			Steps: []*v1.Node{bulky("only", 1)},
		},
	}))
	require.NoError(t, err)

	asked := time.Now()
	got, err := teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: started.Msg.GetWorkflowId(),
	}))
	require.NoError(t, err,
		"Get failed because no worker could answer where the run had got to")

	// The bound, asserted rather than assumed, because this is what it was added
	// for. Unbounded, the query took 10.5s to give up and every `flow get` on a run
	// whose worker was away wore all of it before printing a status the server had
	// already had. A generous ceiling rather than a tight one: an answering worker
	// replies in milliseconds, so this only bites when nothing will answer.
	assert.Less(t, time.Since(asked), 5*time.Second,
		"a Get on a run with no worker waits on the query long enough to look hung")

	assert.Equal(t, v1.RunResponse_STATUS_RUNNING, got.Msg.GetStatus(),
		"the status a Describe already knew was lost with the query")
	assert.NotNil(t, got.Msg.GetStartTime(),
		"the start time a Describe already knew was lost with the query")
	assert.Nil(t, got.Msg.GetProgress(),
		"a position was reported although nothing could have answered for it")
}

// TestAContinuedSegmentReportsNoPosition is the staleness this must not have.
//
// A segment that continued as new is closed, and `Get` reports it RUNNING because
// the workload is — the run id somebody holds still names the workload they asked
// about. But Temporal answers a query against a closed execution by replaying its
// history, so asking would return the position that segment finished at, presented
// as where the workload is now.
//
// That is worse than silence: a real step id, from the right workload, that the run
// left behind. Somebody looking because a run seems stuck would be shown a step it is
// provably no longer on.
func TestAContinuedSegmentReportsNoPosition(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	// One step per run, so the workload suspends after the first.
	flowstate := mustNew(t, temporal, server.WithNamespace("team-a"),
		server.WithMaxStepsPerRun(1))

	// A wait at the end, so the workload is still going when the closed segment is
	// asked about.
	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name: "suspends",
			Steps: []*v1.Node{
				bulky("one", 1),
				bulky("two", 1),
				{
					Id: "waiting",
					Kind: &v1.Node_Wait{Wait: &v1.Wait{
						Kind: &v1.Wait_Duration{Duration: durationpb.New(90 * time.Second)},
					}},
				},
			},
		},
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	firstRunID := started.Msg.GetRunId()

	// Wait until the workload has actually moved on, so the first segment is closed
	// rather than merely expected to close.
	require.Eventually(t, func() bool {
		got, gerr := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))

		return gerr == nil && got.Msg.GetRunId() != firstRunID
	}, 60*time.Second, 250*time.Millisecond,
		"the run never continued as new, so this test proves nothing")

	superseded, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: workflowID,
		RunId:      &firstRunID,
	}))
	require.NoError(t, err)

	assert.Equal(t, v1.RunResponse_STATUS_RUNNING, superseded.Msg.GetStatus(),
		"a continued segment stopped being reported as running, which is a separate "+
			"decision this test depends on")
	assert.Nil(t, superseded.Msg.GetProgress(),
		"a closed segment answered with the position it finished at, presented as "+
			"where the workload is now")

	_, err = flowstate.Terminate(t.Context(), connect.NewRequest(&v1.TerminateRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)
}
