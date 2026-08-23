package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// Eager workflow start is the one optimization that is a claim about *topology*
// rather than about code: it only happens when the process that starts a run
// also hosts a worker, on the same [client.Client]. `flow server dev` is that
// process and nothing else in this repository is, which is why
// [server.WithEagerWorkflowStart] exists as an option rather than as an
// unconditional line in the start path.
//
// So the thing worth testing is not that a bool is copied. It is the wire: what
// the SDK actually asks Temporal for, given the option and given the topology,
// and what Temporal answers. The SDK decides that in
// `internal/internal_workflow_client.go` — the eager request is attached only
// when the option is set, the cluster advertises the capability, *and* the
// client's own dispatcher finds a registered worker on the run's task queue
// with a free slot — so a test that reads
// [workflowservice.StartWorkflowExecutionRequest.RequestEagerExecution] off the
// connection is reading all four facts at once, and a test that reads
// `EagerWorkflowTask` off the response is reading whether the cluster honored
// it.

// startCalls records what StartWorkflowExecution asked for, and what it got
// back, on one client connection.
type startCalls struct {
	mu         sync.Mutex
	requested  []bool // RequestEagerExecution, per start
	dispatched []bool // whether the response carried an eager workflow task
}

func (s *startCalls) record(requested, dispatched bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requested = append(s.requested, requested)
	s.dispatched = append(s.dispatched, dispatched)
}

// only returns the single start this test made, failing if there was not
// exactly one — a test asserting "no eager request" would otherwise pass on a
// connection that made no request at all.
func (s *startCalls) only(t *testing.T) (requested, dispatched bool) {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	require.Len(t, s.requested, 1, "expected exactly one StartWorkflowExecution on this connection")

	return s.requested[0], s.dispatched[0]
}

// newWatchedClient dials a second client into the namespace registered for this
// test, recording every StartWorkflowExecution that crosses it.
//
// A second client rather than an option on the shared helper because the eager
// path is per-connection: the worker below has to be registered on *this*
// client for the SDK to attach the request at all, which is the property under
// test rather than an implementation detail of the fixture.
func newWatchedClient(t *testing.T) (client.Client, string, *startCalls) {
	t.Helper()

	_, namespace := newTemporalNamespace(t)

	calls := &startCalls{}

	watch := func(
		ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		start, isStart := req.(*workflowservice.StartWorkflowExecutionRequest)
		err := invoker(ctx, method, req, reply, cc, opts...)
		if isStart {
			response, _ := reply.(*workflowservice.StartWorkflowExecutionResponse)
			calls.record(start.GetRequestEagerExecution(), response.GetEagerWorkflowTask() != nil)
		}
		return err
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

	return temporal, namespace, calls
}

// oneStep is the smallest thing that is a real durable run: a single `log`
// step, which has no outputs and so completes as fast as the substrate allows.
func oneStep(name string) *v1.Workflow {
	return &v1.Workflow{Name: name, Steps: []*v1.Node{bulky("only", 1)}}
}

// requireCompletes waits for the run to finish, and asserts it finished well.
//
// Every test here runs it, because the point of the option is that *nothing*
// observable changes: an eagerly dispatched run is the same run, with the same
// result, taken by a worker in this process instead of after a matching round
// trip.
func requireCompletes(t *testing.T, flowstate *server.FlowstateServer, workflowID string) {
	t.Helper()

	require.Eventually(t, func() bool {
		response, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		if err != nil {
			return false
		}
		return response.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
	}, 60*time.Second, 50*time.Millisecond, "the run never completed")
}

// TestEagerStartIsRequestedWhenTheOptionMeetsACoLocatedWorker is the feature, on
// the wire: the `flow server dev` shape, with the option that command sets.
func TestEagerStartIsRequestedWhenTheOptionMeetsACoLocatedWorker(t *testing.T) {
	t.Parallel()

	temporal, _, calls := newWatchedClient(t)
	startWorker(t, temporal)

	flowstate := mustNew(t, temporal, server.WithEagerWorkflowStart())

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: oneStep("eager"),
	}))
	require.NoError(t, err)

	requested, dispatched := calls.only(t)
	require.True(t, requested,
		"the start carried no eager request, so `flow run` against the dev stack still waits on matching")
	require.True(t, dispatched,
		"the cluster answered without an eager workflow task; a dev server that does not advertise the "+
			"capability makes this option a silent no-op, which is worth knowing rather than tolerating")

	requireCompletes(t, flowstate, started.Msg.GetWorkflowId())
}

// TestNoEagerStartIsRequestedWithoutTheOption is the production half, held in
// the topology that would otherwise take the shortcut.
//
// The worker here shares the client, exactly as in the test above — the *only*
// difference is the option — so a change that promoted eager start into an
// unconditional line in `prepareCreate` fails here rather than in whichever
// versioned deployment discovered it by running a step on the wrong build.
func TestNoEagerStartIsRequestedWithoutTheOption(t *testing.T) {
	t.Parallel()

	temporal, _, calls := newWatchedClient(t)
	startWorker(t, temporal)

	flowstate := mustNew(t, temporal)

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: oneStep("ordinary"),
	}))
	require.NoError(t, err)

	requested, dispatched := calls.only(t)
	require.False(t, requested, "a server nobody asked requested eager dispatch")
	require.False(t, dispatched)

	requireCompletes(t, flowstate, started.Msg.GetWorkflowId())
}

// TestTheOptionAsksForNothingWithoutACoLocatedWorker is the split deployment:
// `flow server` here, `flow worker` in another process.
//
// It is the reason the option is safe to hold rather than something to guard
// with configuration. The SDK attaches the eager request only after its own
// dispatcher reserves a slot on a worker registered against this client
// (`eagerWorkflowDispatcher.applyToRequest`), so a server whose workers live
// elsewhere makes an ordinary start and the run is dispatched ordinarily —
// asserted here rather than trusted, because "falls back gracefully" is the
// claim the option's doc makes to anyone considering adopting it.
func TestTheOptionAsksForNothingWithoutACoLocatedWorker(t *testing.T) {
	t.Parallel()

	temporal, namespace, calls := newWatchedClient(t)

	// The worker polls the same task queue in the same namespace from a
	// *different* client, which is what a separate `flow worker` process is
	// from this server's point of view.
	elsewhere, err := client.Dial(client.Options{
		HostPort:  devServer.FrontendHostPort(),
		Namespace: namespace,
		Logger:    newTestingLogger(t),
	})
	require.NoError(t, err)
	t.Cleanup(elsewhere.Close)
	startWorker(t, elsewhere)

	flowstate := mustNew(t, temporal, server.WithEagerWorkflowStart())

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: oneStep("split"),
	}))
	require.NoError(t, err)

	requested, dispatched := calls.only(t)
	require.False(t, requested,
		"an eager request was made with no worker on this client to receive it")
	require.False(t, dispatched)

	requireCompletes(t, flowstate, started.Msg.GetWorkflowId())
}
