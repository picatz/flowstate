package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// TestAContinuedWorkloadIsReportedFromWhereItBegan is #1690 end to end: a
// workload that ran as three segments is one row in a listing, dated from its
// first segment's start rather than its last, and a Get on it says the same.
//
// A real Temporal, because the claim is about what visibility reports. The
// interpreter writes the chain into the memo at every continued segment and
// Temporal carries the memo across Continue-As-New; the engine's own tests pin
// what is written, and this is the one that proves it arrives where the
// listing reads.
func TestAContinuedWorkloadIsReportedFromWhereItBegan(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	// One step per segment, so three steps run as three segments.
	flowstate := mustNew(t, temporal, server.WithMaxStepsPerRun(1))

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{
			Name:  "continues",
			Steps: []*v1.Node{bulky("a", 8), bulky("b", 8), bulky("c", 8)},
		},
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	firstRunID := started.Msg.GetRunId()

	// EventuallyWithT, not Eventually: the poll needs more than one boolean out
	// of a tick, including whether the workload showed up as more than one
	// row (#2104's own soak failure). Eventually's condition runs on its own
	// goroutine and reports the result over a channel the function itself
	// must write to; calling t.Fatalf from inside it invokes runtime.Goexit
	// before that write, which orphans the goroutine and starves the poll of
	// every tick from then on — the loop then only ever times out, on the
	// generic "condition never satisfied" rather than on what the tick
	// actually found. CollectT's FailNow calls runtime.Goexit the same way,
	// but the deferred write it leaves behind still runs during that unwind,
	// so a tick's real assertion failures reach the timeout's report intact.
	//
	// The duplicate check is recorded on t directly, not only on the tick's
	// CollectT: EventuallyWithT (testify v1.12.1) discards a failed tick and
	// simply retries, so a duplicate seen on one tick and gone by the next
	// would otherwise vanish along with it — silently retrying away the one
	// thing this test exists to catch. t.Errorf is documented safe to call
	// from any goroutine, unlike t.Fatalf, and fails the test outright rather
	// than leaving that to a tick that may not reproduce it; the tick's own
	// CollectT is still failed too, so the poll keeps going rather than
	// wrongly reading a duplicate as success.
	var listed *v1.RunSummary
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, lerr := flowstate.List(t.Context(), connect.NewRequest(&v1.ListRequest{}))
		if !assert.NoError(c, lerr) {
			return
		}

		var found *v1.RunSummary
		for _, run := range resp.Msg.GetRuns() {
			if run.GetWorkflowId() != workflowID {
				continue
			}
			if found != nil {
				t.Errorf("one workload is listed as more than one row")
				assert.Fail(c, "one workload is listed as more than one row")
				return
			}
			found = run
		}

		if !assert.NotNil(c, found, "the run has not appeared in a listing yet") {
			return
		}
		if !assert.Equal(c, v1.RunResponse_STATUS_COMPLETED, found.GetStatus(),
			"the listed run has not reached STATUS_COMPLETED yet") {
			return
		}

		listed = found
	}, 60*time.Second, 200*time.Millisecond, "the run never appeared in a listing as finished")

	// The first segment's own start, as Temporal recorded it: what the listing
	// has to date the workload from.
	first, err := temporal.DescribeWorkflowExecution(t.Context(), workflowID, firstRunID)
	require.NoError(t, err)
	firstStart := first.GetWorkflowExecutionInfo().GetStartTime().AsTime()

	require.NotEqual(t, firstRunID, listed.GetRunId(),
		"the run never continued as new, so this test proves nothing")
	assert.Equal(t, uint32(3), listed.GetSegments(), "three steps at one per segment is three segments")

	// Within the visibility store's own precision, for the reason
	// [listVisibilityPrecision] gives.
	assert.WithinDuration(t, firstStart, listed.GetStartTime().AsTime(), listVisibilityPrecision,
		"the listing dates the workload from somewhere other than its first segment's start")
	assert.True(t, listed.GetSegmentStartTime().AsTime().After(listed.GetStartTime().AsTime()),
		"the listed segment started at %s, which is not after the workload's start %s",
		listed.GetSegmentStartTime().AsTime(), listed.GetStartTime().AsTime())
	assert.False(t, listed.GetCloseTime().AsTime().Before(listed.GetStartTime().AsTime()),
		"the workload finished before it began")

	got, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
	require.NoError(t, err)

	assert.Equal(t, firstRunID, got.Msg.GetFirstRunId(), "Get does not say which run began the workload")
	assert.Equal(t, listed.GetRunId(), got.Msg.GetRunId(), "Get and the listing name different segments")
	assert.Equal(t, uint32(3), got.Msg.GetSegments())
	assert.WithinDuration(t, listed.GetStartTime().AsTime(), got.Msg.GetStartTime().AsTime(),
		listVisibilityPrecision, "a listing and a Get disagree about when the workload started")
}

// TestARunThatNeverContinuedReportsOneSegment is the other direction: the
// ordinary run, whose one segment is the workload, says so and is dated from
// itself — nothing about #1690 may cost it a memo write or move its start.
func TestARunThatNeverContinuedReportsOneSegment(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	flowstate := mustNew(t, temporal)

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: &v1.Workflow{Name: "once", Steps: []*v1.Node{bulky("only", 8)}},
	}))
	require.NoError(t, err)
	workflowID := started.Msg.GetWorkflowId()

	var listed *v1.RunSummary
	require.Eventually(t, func() bool {
		resp, lerr := flowstate.List(t.Context(), connect.NewRequest(&v1.ListRequest{}))
		if lerr != nil {
			return false
		}
		for _, run := range resp.Msg.GetRuns() {
			if run.GetWorkflowId() == workflowID && run.GetStatus() == v1.RunResponse_STATUS_COMPLETED {
				listed = run

				return true
			}
		}

		return false
	}, 60*time.Second, 200*time.Millisecond, "the run never appeared in a listing as finished")

	assert.Equal(t, started.Msg.GetRunId(), listed.GetRunId())
	assert.Zero(t, listed.GetSegments(), "a run with no chain memo reports a count it was never given")
	assert.True(t, listed.GetSegmentStartTime().AsTime().Equal(listed.GetStartTime().AsTime()),
		"a run that never continued is dated from somewhere other than its own start")

	got, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
	require.NoError(t, err)
	assert.Equal(t, got.Msg.GetRunId(), got.Msg.GetFirstRunId(), "one segment, and it is not its own first")
	assert.Zero(t, got.Msg.GetSegments())
}
