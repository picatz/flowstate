package server_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	enums "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
)

// #1966: two identical request_id submissions racing under
// `on_conflict: terminate_other`, whose probes both land against a stale
// incumbent before either reissue completes, used to each be able to destroy
// the run the *other* one started. Temporal's TERMINATE_EXISTING conflict
// policy destroys whatever currently holds the id and starts a new run,
// unconditionally, so the reissue that landed second destroyed the one that
// landed first, and one request_id was answered with two run ids.
//
// The fix sends the reissue with a request id derived from the submission,
// and Temporal answers a start whose request id the current run already
// carries with that run before it consults the conflict policy. These tests
// prove that against the pinned dev server rather than on the strength of
// reading its source, and prove that the server tells the caller whose start
// was folded that it was: Temporal's reply to both is identical, `Started`
// included, which is why the server reads the answer back from the run.
//
// The race is between client-side round trips this server itself makes to
// Temporal, not between goroutines in this test process, so reproducing it
// reliably means forcing the order those round trips cross the wire rather
// than hoping two goroutines interleave a particular way. [raceTerminateOther]
// does that with a gRPC interceptor rather than a sleep.

// racerIDKeyType tags a racing call's context so the interceptor below can
// tell the two racers' own calls apart. An unexported, unshared type so
// nothing outside this file can set or read it.
//
// It is also the evidence that [temporalclient.WithStartRequest] can work at
// all: the interceptor reads this value from the context the SDK hands the
// gRPC call, which carries it only because the SDK derives that context from
// the one [server.FlowstateServer.Run] passes to ExecuteWorkflow.
type racerIDKeyType struct{}

var racerIDKey racerIDKeyType

func withRacerID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, racerIDKey, id)
}

// reissue is what [raceTerminateOther]'s interceptor saw of one racer's
// TERMINATE_EXISTING reissue: the request id it went out with, and the run
// id the reply named.
type reissue struct {
	requestID string
	runID     string
}

// raceOutcome is each racer's reissue, keyed by racer.
type raceOutcome struct {
	mu       sync.Mutex
	reissues map[string]reissue
}

func (o *raceOutcome) set(racer string, r reissue) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.reissues == nil {
		o.reissues = map[string]reissue{}
	}
	o.reissues[racer] = r
}

// both returns the two racers' reissues, failing unless both reached one:
// the direct evidence that neither racer was answered through the ordinary
// retry arm because the other's run was already there when it probed.
func (o *raceOutcome) both(t *testing.T) []reissue {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()

	require.Len(t, o.reissues, 2, "both racers should have reached a TERMINATE_EXISTING reissue")
	return slices.Collect(maps.Values(o.reissues))
}

// raceTerminateOther dials a client whose gRPC interceptor forces two
// `on_conflict: terminate_other` reissues into #1966's interleaving, once
// armed with the workflow id both racers address.
//
// Two gates. Every reissue first waits for *both* racers' probes (their
// FAIL-policy starts) to have returned, which is the fact the issue's race
// depends on: "both probes land before either reissue completes." Without
// it, one racer could finish outright before the other probed, and the
// other would be answered by the ordinary retry arm. Then the first reissue
// to arrive proceeds, and the second is held until the first's reply is
// back, so the second necessarily lands on the run the first just started.
//
// Gated on [racerIDKey] and the armed workflow id, so only the two calls
// under test are ever held back; everything else this client sends,
// including the call that creates the incumbent, passes straight through.
// [temporalclient.StartInterceptor] sits after this one in the chain, so
// what this one reads back is the request as it went out and the reply as
// it came back.
func raceTerminateOther(t *testing.T, namespace string) (temporal client.Client, arm func(workflowID string), outcome *raceOutcome) {
	t.Helper()

	var (
		mu         sync.Mutex
		workflowID string
	)

	outcome = &raceOutcome{}

	var probesReturned atomic.Int32
	bothProbesReturned := make(chan struct{})
	var closeProbesOnce sync.Once

	var reissues atomic.Int32
	firstReissueReturned := make(chan struct{})
	var closeReissueOnce sync.Once

	watch := func(
		ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		racer, isRacer := ctx.Value(racerIDKey).(string)
		start, isStart := req.(*workflowservice.StartWorkflowExecutionRequest)

		mu.Lock()
		wfID := workflowID
		mu.Unlock()

		if !isRacer || !isStart || wfID == "" || start.GetWorkflowId() != wfID {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		switch start.GetWorkflowIdConflictPolicy() {
		case enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL:
			err := invoker(ctx, method, req, reply, cc, opts...)
			if probesReturned.Add(1) >= 2 {
				closeProbesOnce.Do(func() { close(bothProbesReturned) })
			}
			return err

		case enums.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING:
			select {
			case <-bothProbesReturned:
			case <-ctx.Done():
				return ctx.Err()
			}

			second := reissues.Add(1) == 2
			if second {
				select {
				case <-firstReissueReturned:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			err := invoker(ctx, method, req, reply, cc, opts...)
			if !second {
				closeReissueOnce.Do(func() { close(firstReissueReturned) })
			}
			if err == nil {
				resp := reply.(*workflowservice.StartWorkflowExecutionResponse)
				outcome.set(racer, reissue{
					requestID: start.GetRequestId(),
					runID:     resp.GetRunId(),
				})
			}
			return err
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}

	temporal, err := client.Dial(client.Options{
		HostPort:  devServer.FrontendHostPort(),
		Namespace: namespace,
		Logger:    newTestingLogger(t),
		ConnectionOptions: client.ConnectionOptions{
			DialOptions: []grpc.DialOption{
				grpc.WithChainUnaryInterceptor(watch, temporalclient.StartInterceptor()),
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(temporal.Close)

	arm = func(wfID string) {
		mu.Lock()
		workflowID = wfID
		mu.Unlock()
	}

	return temporal, arm, outcome
}

// runsUnder lists every run Temporal holds under workflowID, once visibility
// lists at least want of them.
func runsUnder(t *testing.T, temporal client.Client, workflowID string, want int) []*workflowpb.WorkflowExecutionInfo {
	t.Helper()

	var runs []*workflowpb.WorkflowExecutionInfo
	require.Eventually(t, func() bool {
		listed, err := temporal.ListWorkflow(t.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Query: fmt.Sprintf("WorkflowId = %q", workflowID),
		})
		if err != nil {
			return false
		}
		runs = listed.GetExecutions()
		return len(runs) >= want
	}, 30*time.Second, 50*time.Millisecond, "visibility never listed %d runs of %s", want, workflowID)

	return runs
}

// TestTwoIdenticalTerminateOtherSubmissionsConvergeOnOneRun is #1966's
// acceptance criterion: two concurrent submissions carrying the same
// request_id answer with the same run id, and neither terminates a run the
// other one started.
func TestTwoIdenticalTerminateOtherSubmissionsConvergeOnOneRun(t *testing.T) {
	t.Parallel()

	plain, namespace := newTemporalNamespace(t)
	startWorker(t, plain)

	racing, arm, outcome := raceTerminateOther(t, namespace)
	s := mustNew(t, racing, server.WithNamespace("acme"))

	// The stale incumbent both racers will probe and find genuinely
	// different from their own submission: a third, earlier request_id,
	// still live when the race below starts.
	incumbent, err := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow:  exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
		Inputs:    clusterInputs("checkout"),
		RequestId: proto.String("race-incumbent"),
	}))
	require.NoError(t, err)
	workflowID := incumbent.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, plain, workflowID)

	arm(workflowID)

	// Two byte-identical submissions (same request_id, same specification,
	// same inputs) racing the reissue this incumbent forces.
	responses := make([]*connect.Response[v1.RunResponse], 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, racer := range []string{"racer-1", "racer-2"} {
		wg.Go(func() {
			responses[i], errs[i] = s.Run(withRacerID(t.Context(), racer), connect.NewRequest(&v1.RunRequest{
				Workflow:  exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
				Inputs:    clusterInputs("checkout"),
				RequestId: proto.String("race-same-submission"),
			}))
		})
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	first, second := responses[0].Msg, responses[1].Msg
	require.Equal(t, first.GetRunId(), second.GetRunId(),
		"one request_id answered two different run ids")
	require.NotEqual(t, incumbent.Msg.GetRunId(), first.GetRunId(),
		"the incumbent, not the race, was answered")

	// Both reissues reached the cluster, under one request id derived from
	// the submission rather than the caller's own value, and the cluster
	// answered both with one run.
	reissues := outcome.both(t)
	require.Equal(t, reissues[0].requestID, reissues[1].requestID,
		"identical submissions reissued under different request ids")
	require.True(t, strings.HasPrefix(reissues[0].requestID, "flowstate-submission-"), reissues[0].requestID)
	require.NotContains(t, reissues[0].requestID, "race-same-submission")
	require.Equal(t, reissues[0].runID, reissues[1].runID)

	// Exactly one caller started that run, and the other is told its start
	// was folded onto it.
	require.True(t, first.GetReused() != second.GetReused(),
		"exactly one of the two responses should say reused; got %v and %v", first.GetReused(), second.GetReused())
	folded := first
	if second.GetReused() {
		folded = second
	}
	require.False(t, folded.GetSpecificationAsSubmitted(), "a folded start answered about a run it did not start")

	live, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
	require.NoError(t, err)
	require.Equal(t, first.GetRunId(), live.Msg.GetRunId())
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, live.Msg.GetStatus(),
		"the run both callers were answered with was terminated by the race")

	incumbentRunID := incumbent.Msg.GetRunId()
	replaced, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID, RunId: &incumbentRunID}))
	require.NoError(t, err)
	require.Equal(t, v1.RunResponse_STATUS_TERMINATED, replaced.Msg.GetStatus())

	// The incumbent and the survivor, and nothing between them: a third run
	// is a start the race made and then destroyed.
	runs := runsUnder(t, plain, workflowID, 2)
	require.Len(t, runs, 2, "the race started a run nobody was answered with")
	closed := slices.DeleteFunc(slices.Clone(runs), func(run *workflowpb.WorkflowExecutionInfo) bool {
		return run.GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_RUNNING
	})
	require.Len(t, closed, 1)
	require.Equal(t, incumbentRunID, closed[0].GetExecution().GetRunId())
}

// TestADifferentTerminateOtherSubmissionStillReplacesUnderTheRace is what
// #1966's fix must not break: two genuinely different submissions racing the
// same reissue still resolve to `on_conflict: terminate_other`'s ordinary
// meaning, whichever reissue lands second replacing everything before it, as
// [TestATerminateOtherSubmissionStillReplacesADifferentOne] proves
// sequentially, rather than the fix mistaking one for a retry of the other
// and joining them, which is the one answer an idempotency key must never
// give two unrelated submissions.
func TestADifferentTerminateOtherSubmissionStillReplacesUnderTheRace(t *testing.T) {
	t.Parallel()

	plain, namespace := newTemporalNamespace(t)
	startWorker(t, plain)

	racing, arm, outcome := raceTerminateOther(t, namespace)
	s := mustNew(t, racing, server.WithNamespace("acme"))

	incumbent, err := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow:  exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
		Inputs:    clusterInputs("checkout"),
		RequestId: proto.String("race-incumbent"),
	}))
	require.NoError(t, err)
	waitUntilParkedAtTheGate(t, plain, incumbent.Msg.GetWorkflowId())

	arm(incumbent.Msg.GetWorkflowId())

	responses := make([]*connect.Response[v1.RunResponse], 2)
	errs := make([]error, 2)
	requestIDs := []string{"race-different-a", "race-different-b"}
	var wg sync.WaitGroup
	for i, racer := range []string{"racer-1", "racer-2"} {
		wg.Go(func() {
			responses[i], errs[i] = s.Run(withRacerID(t.Context(), racer), connect.NewRequest(&v1.RunRequest{
				Workflow:  exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
				Inputs:    clusterInputs("checkout"),
				RequestId: proto.String(requestIDs[i]),
			}))
		})
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	first, second := responses[0].Msg, responses[1].Msg
	require.False(t, first.GetReused(), "a genuinely different submission was answered as a retry")
	require.False(t, second.GetReused(), "a genuinely different submission was answered as a retry")
	require.NotEqual(t, first.GetRunId(), second.GetRunId(),
		"two different submissions were joined onto the one run, rather than each starting its own")

	reissues := outcome.both(t)
	require.NotEqual(t, reissues[0].requestID, reissues[1].requestID)
	require.NotEqual(t, reissues[0].runID, reissues[1].runID)

	// Exactly one of the two runs this test started is the one still live;
	// the other, whichever caller was answered with it, was replaced in
	// turn, which is `on_conflict: terminate_other`'s ordinary cost and not
	// something #1966's fix should change.
	workflowID := first.GetWorkflowId()
	firstDesc, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID, RunId: proto.String(first.GetRunId())}))
	require.NoError(t, err)
	secondDesc, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID, RunId: proto.String(second.GetRunId())}))
	require.NoError(t, err)

	statuses := []v1.RunResponse_Status{firstDesc.Msg.GetStatus(), secondDesc.Msg.GetStatus()}
	require.ElementsMatch(t, []v1.RunResponse_Status{v1.RunResponse_STATUS_RUNNING, v1.RunResponse_STATUS_TERMINATED}, statuses,
		"exactly one of the two different submissions' runs should have survived the race")
}

// TestATerminateOtherReplacementRefusesAClientWithoutTheStartInterceptor is
// the fail-closed half of #1966's fix: an embedder's own client, dialed
// without [temporalclient.StartInterceptor], cannot send the derived request
// id, so a request_id replacement under it could race exactly as before the
// fix. It is refused before anything is terminated, and naming the fix; a
// submission without a request_id, which never depended on the id, still
// replaces as it always has.
func TestATerminateOtherReplacementRefusesAClientWithoutTheStartInterceptor(t *testing.T) {
	t.Parallel()

	plain, namespace := newTemporalNamespace(t)
	startWorker(t, plain)

	bare, err := client.Dial(client.Options{
		HostPort:  devServer.FrontendHostPort(),
		Namespace: namespace,
		Logger:    newTestingLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(bare.Close)
	s := mustNew(t, bare, server.WithNamespace("acme"))

	incumbent, err := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow:  exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
		Inputs:    clusterInputs("checkout"),
		RequestId: proto.String("bare-incumbent"),
	}))
	require.NoError(t, err)
	workflowID := incumbent.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, plain, workflowID)

	_, err = s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow:  exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
		Inputs:    clusterInputs("checkout"),
		RequestId: proto.String("bare-replacement"),
	}))
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "%v", err)
	require.ErrorContains(t, err, "temporalclient.StartInterceptor")

	incumbentRunID := incumbent.Msg.GetRunId()
	still, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID, RunId: &incumbentRunID}))
	require.NoError(t, err)
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, still.Msg.GetStatus(), "the refusal terminated the incumbent")
	live, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
	require.NoError(t, err)
	require.Equal(t, incumbentRunID, live.Msg.GetRunId(), "the refusal started a run anyway")

	// No request_id: the direct TERMINATE_EXISTING path, which never read the
	// id, is not refused.
	replaced, err := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
		Inputs:   clusterInputs("checkout"),
	}))
	require.NoError(t, err)
	require.NotEqual(t, incumbentRunID, replaced.Msg.GetRunId())
	stopped, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID, RunId: &incumbentRunID}))
	require.NoError(t, err)
	require.Equal(t, v1.RunResponse_STATUS_TERMINATED, stopped.Msg.GetStatus())
}
