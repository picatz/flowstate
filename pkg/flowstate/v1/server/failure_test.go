package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A run that failed used to be asked why, and answer with the status again.
//
// `Get` built `Error{Message: respStatus.String()}`, so a caller was told a run
// failed and, asked for the reason, told that it failed. The reason was never
// missing — Temporal had it, and the caller was already authorized to read the run's
// whole outputs, which are the workload's data rather than a sentence about it.
//
// The cost showed up two packages away: `flow watch` grew a `restatesStatus` helper
// whose only job was to notice that answer and drop it, so a terminal did not print
// `run "x" failed: STATUS_FAILED`. A workaround somewhere else for a sentence this
// server was choosing to produce is the clearest evidence there is that the sentence
// was wrong.

// TestAFailedRunSaysWhy is driven through the real client against a real Temporal,
// because what is under test is the shape of Temporal's own error — a fake would be
// asserting this repo's guess about that shape rather than the shape.
func TestAFailedRunSaysWhy(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	// An unknown task fails permanently and for a reason the engine words itself, so
	// the message this returns should be the engine's sentence rather than
	// Temporal's envelope around it.
	//
	// No `continue_on_error`: the point is the *run's* failure, not a step's
	// tolerated one.
	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name: "fails",
			Steps: []*v1.Node{{
				Id: "boom",
				// Inputs are required by the schema even for a task that does not
				// exist, so the run reaches the engine and fails there rather than
				// being refused at submit — which is the path under test.
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "nosuchtask",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")},
				}},
			}},
		},
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	var got *v1.GetResponse
	require.Eventually(t, func() bool {
		resp, gerr := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		if gerr != nil {
			return false
		}
		got = resp.Msg

		return got.GetStatus() != v1.RunResponse_STATUS_RUNNING
	}, 60*time.Second, 200*time.Millisecond, "the run never reached a terminal state")

	require.Equal(t, v1.RunResponse_STATUS_FAILED, got.GetStatus())

	failure := got.GetError().GetMessage()
	require.NotEmpty(t, failure, "a failed run reported no reason at all")

	// The claim, stated as what it must *not* be: the status name. Everything else
	// here is about the message being better than that, and this is the floor.
	assert.NotEqual(t, got.GetStatus().String(), failure,
		"the run's reason is its status restated, which is what this exists to stop")

	// And what it must be: the engine's own words, naming the step and what went
	// wrong with it. Asserted on the parts an author would search for rather than on
	// the whole sentence, since the wording around them belongs to the engine.
	assert.Contains(t, failure, "boom", "the reason does not name the step that failed")
	assert.Contains(t, failure, "nosuchtask", "the reason does not say what went wrong")

	// Temporal's envelope names the workflow type, the id and the run id — all of
	// which the caller already has, and none of which is a reason. The innermost
	// application error is what is wanted, so the envelope must not survive.
	assert.NotContains(t, failure, "workflow execution error",
		"Temporal's envelope reached the caller instead of the engine's own message")
	assert.NotContains(t, failure, workflowID,
		"the reason repeats the workflow id the caller asked with")
}

// TestACompletedRunReportsNoFailure is the negative direction.
//
// Reading a run's error is a second call to Temporal, made only on the terminal
// branches. A run that succeeded must still carry its outputs and no error at all —
// a `Get` that started reporting a failure for every finished run would be a far
// worse bug than the silence it replaced.
func TestACompletedRunReportsNoFailure(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name:  "succeeds",
			Steps: []*v1.Node{bulky("only", 8)},
		},
	}))
	require.NoError(t, err)

	var got *v1.GetResponse
	require.Eventually(t, func() bool {
		resp, gerr := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: started.Msg.GetWorkflowId(),
		}))
		if gerr != nil {
			return false
		}
		got = resp.Msg

		return got.GetStatus() != v1.RunResponse_STATUS_RUNNING
	}, 60*time.Second, 200*time.Millisecond, "the run never reached a terminal state")

	require.Equal(t, v1.RunResponse_STATUS_COMPLETED, got.GetStatus())
	assert.Nil(t, got.GetError(), "a run that succeeded reported a failure")
	assert.NotNil(t, got.GetOutputs(), "a run that succeeded reported no outputs")
}

// TestGetReportsWhenARunStartedAndFinished closes the other half of the same gap.
//
// A listing already answered both — `RunSummary` has carried `start_time` and
// `close_time` from the beginning — while `Get` on the same run answered three
// scalars. So `flow list` could tell you a run had been going for an hour and
// `flow get <id>` could only tell you it was running, which is the wrong way round:
// Get is the verb somebody reaches for when they care about a *particular* run.
//
// Both branches are asserted because they are different code paths and the running
// one is the one that used to say nothing at all.
func TestGetReportsWhenARunStartedAndFinished(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	// Parked at a gate, so there is a genuinely running run to ask about rather than
	// a race against one that finishes first.
	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	startWorker(t, fixture.temporal)
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	running, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, running.Msg.GetStatus())

	require.NotNil(t, running.Msg.GetStartTime(), "a running run does not say when it began")
	assert.False(t, running.Msg.GetStartTime().AsTime().IsZero(),
		"the start time is the zero instant, which reports the run as having begun in 1970")

	// Unset rather than zero, so "has not finished" and "finished at the epoch" stay
	// distinct — the same rule the listing follows, which is why both read it from
	// one place.
	assert.Nil(t, running.Msg.GetCloseTime(), "a run still going reported a close time")

	_, err = fixture.teamA.Cancel(t.Context(), connect.NewRequest(&v1.CancelRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)

	var finished *v1.GetResponse
	require.Eventually(t, func() bool {
		resp, gerr := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: workflowID,
		}))
		if gerr != nil {
			return false
		}
		finished = resp.Msg

		return finished.GetStatus() != v1.RunResponse_STATUS_RUNNING
	}, 60*time.Second, 200*time.Millisecond, "the run never reached a terminal state")

	require.NotNil(t, finished.GetStartTime(), "a finished run lost the time it began")
	require.NotNil(t, finished.GetCloseTime(), "a finished run does not say when it finished")
	assert.False(t, finished.GetCloseTime().AsTime().Before(finished.GetStartTime().AsTime()),
		"the run finished before it started")
}

// TestGetAndListAgreeAboutWhenARunStarted is the join, and the reason the mapping is
// shared rather than written twice.
//
// Two readings of one response eventually disagree. A run reported as having started
// at one time by `flow list` and another by `flow get` is a bug nobody can reproduce,
// and what stops it is that both verbs ask the same function.
//
// # Why this is not exact equality
//
// The two verbs read two different Temporal APIs — a listing comes from the visibility
// store and a Get from DescribeWorkflowExecution — and the visibility store keeps
// microseconds where Describe keeps nanoseconds. Measured, not assumed: this test
// first asserted exact equality and found 17:55:40.563523000 against
// 17:55:40.563523120, a difference of 120 nanoseconds.
//
// A microsecond is therefore the tightest claim that is about *this server* rather
// than about Temporal's storage precision. Rounding one side to make an exact
// assertion pass would be inventing agreement, and asserting nothing at all would let
// a genuinely different field — a close time read as a start time, say — slip through.
const listVisibilityPrecision = time.Microsecond

func TestGetAndListAgreeAboutWhenARunStarted(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name:  "agrees",
			Steps: []*v1.Node{bulky("only", 8)},
		},
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()

	var listed *v1.RunSummary
	require.Eventually(t, func() bool {
		resp, lerr := fixture.teamA.List(t.Context(), connect.NewRequest(&v1.ListRequest{}))
		if lerr != nil {
			return false
		}
		for _, run := range resp.Msg.GetRuns() {
			if run.GetWorkflowId() == workflowID && run.GetStatus() != v1.RunResponse_STATUS_RUNNING {
				listed = run

				return true
			}
		}

		return false
	}, 60*time.Second, 200*time.Millisecond, "the run never appeared in a listing as finished")

	got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)

	assert.WithinDuration(t, listed.GetStartTime().AsTime(), got.Msg.GetStartTime().AsTime(),
		listVisibilityPrecision, "a listing and a Get disagree about when the run started")
	assert.WithinDuration(t, listed.GetCloseTime().AsTime(), got.Msg.GetCloseTime().AsTime(),
		listVisibilityPrecision, "a listing and a Get disagree about when the run finished")
}
