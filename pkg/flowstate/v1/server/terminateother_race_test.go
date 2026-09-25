package server_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// #1966: two identical request_id submissions racing under
// `on_conflict: terminate_other`, whose probes both land against a stale
// incumbent before either reissue completes, used to each be able to destroy
// the run the *other* one started. Temporal's TERMINATE_EXISTING conflict
// policy has no compare-and-terminate — it destroys whatever currently holds
// the id and starts a new run, unconditionally — so the reissue that lost
// the race destroyed the reissue that won it, regardless of which caller was
// answered with which run id.
//
// The race is between client-side round trips this server itself makes to
// Temporal, not between goroutines in this test process, so reproducing it
// reliably means forcing the order those round trips cross the wire rather
// than hoping two goroutines interleave a particular way. [raceTerminateOther]
// does that with a gRPC interceptor rather than a sleep: it lets the first
// racer's terminate-the-incumbent call through immediately, holds the
// second's back until the first racer's own replacement start has gone out
// and returned, then releases it — which is exactly the interleaving the
// issue describes (both probes land before either reissue completes), made
// to happen every run rather than some of them.

// racerIDKeyType tags a racing call's context so the interceptor below can
// tell the two racers' own calls apart. An unexported, unshared type so
// nothing outside this file can set or read it.
type racerIDKeyType struct{}

var racerIDKey racerIDKeyType

func withRacerID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, racerIDKey, id)
}

// raceTerminateOther dials a client whose gRPC interceptor forces two
// `on_conflict: terminate_other` reissues into #1966's interleaving, once
// armed with the workflow id and the specific incumbent run id both racers
// will have observed.
//
// Gated on that specific run id, not on every TerminateWorkflowExecution
// call this test's setup also makes (starting the incumbent touches none,
// but a less careful gate would also catch a worker's own housekeeping) —
// and gated on [racerIDKey], so only the two calls under test are ever held
// back; everything else this client sends, including the call that creates
// the incumbent in the first place, passes straight through.
func raceTerminateOther(t *testing.T, namespace string) (temporal client.Client, arm func(workflowID, incumbentRunID string)) {
	t.Helper()

	var (
		mu             sync.Mutex
		workflowID     string
		incumbentRunID string
		terminated     = map[string]bool{}
	)

	var termCalls atomic.Int32
	firstClaimStarted := make(chan struct{})
	var closeOnce sync.Once

	watch := func(
		ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		racer, isRacer := ctx.Value(racerIDKey).(string)

		mu.Lock()
		wfID, runID := workflowID, incumbentRunID
		mu.Unlock()

		if isRacer && runID != "" {
			if term, ok := req.(*workflowservice.TerminateWorkflowExecutionRequest); ok &&
				term.GetWorkflowExecution().GetRunId() == runID {

				// The first of the two racers' terminate calls to arrive
				// proceeds immediately; the second waits for the first
				// racer's own replacement claim to have gone out and come
				// back — so the second racer's terminate, once released,
				// necessarily finds this run already gone rather than
				// racing to end it too.
				if termCalls.Add(1) == 2 {
					<-firstClaimStarted
				}

				err := invoker(ctx, method, req, reply, cc, opts...)

				mu.Lock()
				terminated[racer] = true
				mu.Unlock()

				return err
			}

			if start, ok := req.(*workflowservice.StartWorkflowExecutionRequest); ok &&
				start.GetWorkflowId() == wfID &&
				start.GetWorkflowIdConflictPolicy() == enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL {

				// Both this call's own initial probe and its post-terminate
				// claim have this exact shape; only the second — reached
				// after this racer's own terminate call above set the flag
				// — is the one #1966's fix hangs the second racer on.
				mu.Lock()
				wasTerminated := terminated[racer]
				mu.Unlock()

				err := invoker(ctx, method, req, reply, cc, opts...)

				if wasTerminated {
					closeOnce.Do(func() { close(firstClaimStarted) })
				}

				return err
			}
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}

	temporal, err := client.Dial(client.Options{
		HostPort:  devServer.FrontendHostPort(),
		Namespace: namespace,
		Logger:    newTestingLogger(t),
		ConnectionOptions: client.ConnectionOptions{
			DialOptions: []grpc.DialOption{grpc.WithChainUnaryInterceptor(watch)},
		},
	})
	require.NoError(t, err)
	t.Cleanup(temporal.Close)

	arm = func(wfID, runID string) {
		mu.Lock()
		workflowID, incumbentRunID = wfID, runID
		mu.Unlock()
	}

	return temporal, arm
}

// TestTwoIdenticalTerminateOtherSubmissionsConvergeOnOneRun is #1966's
// acceptance criterion: two concurrent submissions carrying the same
// request_id answer with the same run id, whatever order their reissues
// interleave in, and neither terminates a run the other one started.
func TestTwoIdenticalTerminateOtherSubmissionsConvergeOnOneRun(t *testing.T) {
	t.Parallel()

	plain, namespace := newTemporalNamespace(t)
	startWorker(t, plain)

	racing, arm := raceTerminateOther(t, namespace)
	s := mustNew(t, racing, server.WithNamespace("acme"))

	// The stale incumbent both racers will probe and find genuinely
	// different from their own submission — a third, earlier request_id,
	// still live when the race below starts.
	incumbent, err := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow:  exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
		Inputs:    clusterInputs("checkout"),
		RequestId: proto.String("race-incumbent"),
	}))
	require.NoError(t, err)
	waitUntilParkedAtTheGate(t, plain, incumbent.Msg.GetWorkflowId())

	arm(incumbent.Msg.GetWorkflowId(), incumbent.Msg.GetRunId())

	// Two byte-identical submissions — same request_id, same specification,
	// same inputs — racing the reissue this incumbent forces.
	responses := make([]*connect.Response[v1.RunResponse], 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, racer := range []string{"racer-1", "racer-2"} {
		wg.Add(1)
		go func(i int, racer string) {
			defer wg.Done()
			responses[i], errs[i] = s.Run(withRacerID(t.Context(), racer), connect.NewRequest(&v1.RunRequest{
				Workflow:  exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
				Inputs:    clusterInputs("checkout"),
				RequestId: proto.String("race-same-submission"),
			}))
		}(i, racer)
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	first, second := responses[0].Msg, responses[1].Msg
	require.Equal(t, first.GetRunId(), second.GetRunId(),
		"one request_id answered two different run ids")
	require.NotEqual(t, incumbent.Msg.GetRunId(), first.GetRunId(),
		"the incumbent, not the race, was answered")

	// Exactly one call started the surviving run; the other discovered it —
	// both must be true, or #1966's own scenario (both destroy what the
	// other started) is what actually happened.
	require.True(t, first.GetReused() != second.GetReused(),
		"exactly one of the two responses should name the run it itself started")

	live, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: first.GetWorkflowId()}))
	require.NoError(t, err)
	require.Equal(t, first.GetRunId(), live.Msg.GetRunId())
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, live.Msg.GetStatus(),
		"the run both callers were answered with was terminated by the race")
}

// TestADifferentTerminateOtherSubmissionStillReplacesUnderTheRace is what
// #1966's fix must not break: two genuinely different submissions racing the
// same reissue still resolve to `on_conflict: terminate_other`'s ordinary
// meaning — whichever reissue is left standing replaces everything before
// it, exactly as [TestATerminateOtherSubmissionStillReplacesADifferentOne]
// proves sequentially — rather than the fix mistaking one for a retry of the
// other and joining them, which is the one answer an idempotency key must
// never give two unrelated submissions.
func TestADifferentTerminateOtherSubmissionStillReplacesUnderTheRace(t *testing.T) {
	t.Parallel()

	plain, namespace := newTemporalNamespace(t)
	startWorker(t, plain)

	racing, arm := raceTerminateOther(t, namespace)
	s := mustNew(t, racing, server.WithNamespace("acme"))

	incumbent, err := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow:  exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
		Inputs:    clusterInputs("checkout"),
		RequestId: proto.String("race-incumbent"),
	}))
	require.NoError(t, err)
	waitUntilParkedAtTheGate(t, plain, incumbent.Msg.GetWorkflowId())

	arm(incumbent.Msg.GetWorkflowId(), incumbent.Msg.GetRunId())

	responses := make([]*connect.Response[v1.RunResponse], 2)
	errs := make([]error, 2)
	requestIDs := []string{"race-different-a", "race-different-b"}
	var wg sync.WaitGroup
	for i, racer := range []string{"racer-1", "racer-2"} {
		wg.Add(1)
		go func(i int, racer, requestID string) {
			defer wg.Done()
			responses[i], errs[i] = s.Run(withRacerID(t.Context(), racer), connect.NewRequest(&v1.RunRequest{
				Workflow:  exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_TERMINATE_OTHER),
				Inputs:    clusterInputs("checkout"),
				RequestId: proto.String(requestIDs[i]),
			}))
		}(i, racer, requestIDs[i])
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	first, second := responses[0].Msg, responses[1].Msg
	require.False(t, first.GetReused(), "a genuinely different submission was answered as a retry")
	require.False(t, second.GetReused(), "a genuinely different submission was answered as a retry")
	require.NotEqual(t, first.GetRunId(), second.GetRunId(),
		"two different submissions were joined onto the one run, rather than each starting its own")

	// Exactly one of the two runs this test started is the one still live —
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
