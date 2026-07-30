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
