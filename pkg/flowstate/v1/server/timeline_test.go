package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTheTimelineAccountsForARunThatActuallyRan drives a real run to a real end
// and reads its account back.
//
// The claim is that the account is *about the run* rather than about the
// history's shape: the steps are named, in order, and the gate the run parked on
// appears as a wait that a signal then answered. A test that only counted
// entries would pass on a timeline that reported a workflow task being scheduled
// three times, which is the failure mode this mapping exists to prevent.
func TestTheTimelineAccountsForARunThatActuallyRan(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	// Read once while the run is still going, because a timeline that only
	// worked on finished runs would be useless on the runs somebody is actually
	// staring at.
	running, err := fixture.teamA.GetTimeline(t.Context(), connect.NewRequest(&v1.GetTimelineRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)

	assert.False(t, running.Msg.GetTruncated(),
		"a short run's whole account came back marked as not the whole of it")
	assert.Contains(t, timelineSteps(running.Msg), "`request`",
		"the first step ran and the account does not name it")
	assert.Contains(t, timelineKinds(running.Msg), v1.TimelineEntry_KIND_TIMER_STARTED,
		"the run is parked on a gate with a timeout and no wait was reported")

	_, err = fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
		}},
	}))
	require.NoError(t, err)

	var finished *v1.GetTimelineResponse
	require.Eventually(t, func() bool {
		resp, gerr := fixture.teamA.GetTimeline(t.Context(), connect.NewRequest(&v1.GetTimelineRequest{
			WorkflowId: workflowID,
		}))
		if gerr != nil {
			return false
		}
		finished = resp.Msg

		for _, kind := range timelineKinds(finished) {
			if kind == v1.TimelineEntry_KIND_RUN_ENDED {
				return true
			}
		}

		return false
	}, 60*time.Second, 200*time.Millisecond, "the run never reached an ending in its own account")

	assert.False(t, finished.GetTruncated(),
		"the account reached the run's ending and still says it is not the whole of it")

	steps := timelineSteps(finished)
	assert.Contains(t, steps, "`request`")
	assert.Contains(t, steps, "`deploy`",
		"the step the signal released ran and the account does not name it")

	// The signal is the fact that explains why the run moved on, and it is named
	// rather than carried: the payload is somebody's decision.
	assert.Contains(t, steps, "deploy-approved",
		"the gate was answered and the account does not say so")

	// Ordered, which is the whole of what an account is. Positions rather than
	// an exact sequence, because the events between them are not this test's
	// subject.
	requested := indexOfStep(finished, "`request`")
	deployed := indexOfStep(finished, "`deploy`")
	require.GreaterOrEqual(t, requested, 0)
	require.GreaterOrEqual(t, deployed, 0)
	assert.Less(t, requested, deployed, "the account has the run doing things out of order")

	// Every entry addresses itself, which is what makes two rows about one step
	// tellable apart.
	for i, entry := range finished.GetEntries() {
		assert.NotZero(t, entry.GetEventId(), "entry %d has no event id", i)
		assert.NotNil(t, entry.GetTime(), "entry %d has no time", i)
		assert.NotEqual(t, v1.TimelineEntry_KIND_UNSPECIFIED, entry.GetKind(),
			"entry %d was reported without saying what it is", i)
	}
}

// TestTheTimelineSaysWhenItIsNotTheWholeAccount is the bound, exercised rather
// than trusted: a bound nothing reaches is a bound nothing tests.
//
// The interesting half is not that a clipped answer is shorter. It is that it
// *says* it is clipped — a prefix that reads whole is the "not short, but
// claiming to be the whole of it" defect, and it is the one a caller cannot
// detect for itself.
func TestTheTimelineSaysWhenItIsNotTheWholeAccount(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	clipped, err := fixture.teamA.GetTimeline(t.Context(), connect.NewRequest(&v1.GetTimelineRequest{
		WorkflowId: workflowID,
		MaxEntries: 1,
	}))
	require.NoError(t, err)

	require.Len(t, clipped.Msg.GetEntries(), 1, "the bound was asked for and not applied")
	assert.True(t, clipped.Msg.GetTruncated(),
		"an account clipped to one entry came back claiming to be the whole of it")

	whole, err := fixture.teamA.GetTimeline(t.Context(), connect.NewRequest(&v1.GetTimelineRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err)

	assert.Greater(t, len(whole.Msg.GetEntries()), 1,
		"the unclipped answer is no longer than the clipped one, so the bound was never the reason")
	assert.False(t, whole.Msg.GetTruncated())
}

// TestATimelineIsNotReadableByAnotherTenant is the negative direction, which is
// the one that matters: an isolation test asserting that each party reaches its
// own resource is a functionality test wearing a security test's clothes.
//
// A history is the *whole* account of a workload — every step it ran, every
// failure's sentence, every gate somebody answered. So a timeline readable by
// whoever guessed an id would be a larger disclosure than Get's rather than a
// smaller one, and it is refused as not-found for the reason `lifecycle.go`
// gives: denied would confirm the run exists in some other tenant.
func TestATimelineIsNotReadableByAnotherTenant(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: gatedWorkflow(),
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	// The owner reads it, so the refusal below is about who is asking rather
	// than about the run being unreadable.
	_, err = fixture.teamA.GetTimeline(t.Context(), connect.NewRequest(&v1.GetTimelineRequest{
		WorkflowId: workflowID,
	}))
	require.NoError(t, err, "the tenant that started the run cannot read its own account")

	_, err = fixture.teamB.GetTimeline(t.Context(), connect.NewRequest(&v1.GetTimelineRequest{
		WorkflowId: workflowID,
	}))
	require.Error(t, err, "another tenant read the whole account of a run it does not own")
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err),
		"the refusal confirms the run exists somewhere, which is the fact a caller "+
			"in the wrong tenant must not learn")
}

// timelineSteps is the labels an account carries, in order.
func timelineSteps(msg *v1.GetTimelineResponse) []string {
	steps := make([]string, 0, len(msg.GetEntries()))
	for _, entry := range msg.GetEntries() {
		steps = append(steps, entry.GetStep())
	}

	return steps
}

// timelineKinds is what an account says happened, in order.
func timelineKinds(msg *v1.GetTimelineResponse) []v1.TimelineEntry_Kind {
	kinds := make([]v1.TimelineEntry_Kind, 0, len(msg.GetEntries()))
	for _, entry := range msg.GetEntries() {
		kinds = append(kinds, entry.GetKind())
	}

	return kinds
}

// indexOfStep is where a step first appears, or -1.
func indexOfStep(msg *v1.GetTimelineResponse, step string) int {
	for i, entry := range msg.GetEntries() {
		if entry.GetStep() == step {
			return i
		}
	}

	return -1
}
