package temporalclient

import (
	"context"
	"net"
	"sync"
	"testing"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
)

// invokeStart runs req through [StartInterceptor] under ctx and returns the
// request as the invoker received it.
func invokeStart(t *testing.T, ctx context.Context, req any) any {
	t.Helper()

	var sent any
	err := StartInterceptor()(ctx, "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution", req, nil, nil,
		func(_ context.Context, _ string, req, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			sent = req
			return nil
		})
	if err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	return sent
}

func TestStartInterceptorSendsTheContextsRequestID(t *testing.T) {
	r := &StartRequest{ID: "flowstate-submission-abc"}

	sent := invokeStart(t, WithStartRequest(t.Context(), r), &workflowservice.StartWorkflowExecutionRequest{RequestId: "sdk-random"})
	if got := sent.(*workflowservice.StartWorkflowExecutionRequest).GetRequestId(); got != "flowstate-submission-abc" {
		t.Fatalf("request id = %q, want the context's", got)
	}
	if !r.Applied() {
		t.Fatal("a start that passed through the interceptor was not recorded as applied")
	}
}

func TestStartInterceptorLeavesOtherCallsAlone(t *testing.T) {
	// No StartRequest on the context: the SDK's own id is what goes out.
	sent := invokeStart(t, t.Context(), &workflowservice.StartWorkflowExecutionRequest{RequestId: "sdk-random"})
	if got := sent.(*workflowservice.StartWorkflowExecutionRequest).GetRequestId(); got != "sdk-random" {
		t.Fatalf("request id = %q, want the SDK's", got)
	}

	// An empty ID keeps the SDK's id, and still records that the start was
	// seen: that is what lets a caller probe a client with no id to send.
	probe := &StartRequest{}
	sent = invokeStart(t, WithStartRequest(t.Context(), probe), &workflowservice.StartWorkflowExecutionRequest{RequestId: "sdk-random"})
	if got := sent.(*workflowservice.StartWorkflowExecutionRequest).GetRequestId(); got != "sdk-random" {
		t.Fatalf("request id = %q, want the SDK's", got)
	}
	if !probe.Applied() {
		t.Fatal("a start with no id to send was not recorded as seen")
	}

	// Any other request carrying a request id is not a start, and is neither
	// rewritten nor recorded, even under a context that carries one.
	r := &StartRequest{ID: "flowstate-submission-abc"}
	signal := &workflowservice.SignalWithStartWorkflowExecutionRequest{RequestId: "sdk-random"}
	sent = invokeStart(t, WithStartRequest(t.Context(), r), signal)
	if got := sent.(*workflowservice.SignalWithStartWorkflowExecutionRequest).GetRequestId(); got != "sdk-random" {
		t.Fatalf("signal-with-start request id = %q, want it untouched", got)
	}
	if r.Applied() {
		t.Fatal("a call that is not a start was recorded as applied")
	}
}

// startRecorder is a WorkflowService that answers the two calls a dial and an
// ExecuteWorkflow make, recording the request id every start arrives with.
type startRecorder struct {
	workflowservice.UnimplementedWorkflowServiceServer

	mu         sync.Mutex
	requestIDs []string
}

func (r *startRecorder) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (r *startRecorder) StartWorkflowExecution(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requestIDs = append(r.requestIDs, req.GetRequestId())
	return &workflowservice.StartWorkflowExecutionResponse{RunId: "run-1", Started: true}, nil
}

func (r *startRecorder) last(t *testing.T) string {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.requestIDs) == 0 {
		t.Fatal("no start reached the server")
	}
	return r.requestIDs[len(r.requestIDs)-1]
}

// TestConfigOptionsApplyTheStartRequestOnTheWire proves the seam end to end,
// through the SDK and a real gRPC connection: a client dialed from
// [Config.Options] sends [StartRequest.ID] as the start's request id, and a
// client dialed without it does not, and says so through
// [StartRequest.Applied].
func TestConfigOptionsApplyTheStartRequestOnTheWire(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	recorder := &startRecorder{}
	server := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(server, recorder)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	start := func(t *testing.T, c client.Client) *StartRequest {
		t.Helper()
		r := &StartRequest{ID: "flowstate-submission-abc"}
		_, err := c.ExecuteWorkflow(WithStartRequest(t.Context(), r),
			client.StartWorkflowOptions{ID: "w", TaskQueue: "q"}, "Workflow")
		if err != nil {
			t.Fatalf("ExecuteWorkflow: %v", err)
		}
		return r
	}

	t.Run("dialed from Config.Options", func(t *testing.T) {
		opts, err := Config{Address: listener.Addr().String()}.Options()
		if err != nil {
			t.Fatalf("Options() error: %v", err)
		}
		c, err := client.DialContext(t.Context(), opts)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(c.Close)

		r := start(t, c)
		if got := recorder.last(t); got != "flowstate-submission-abc" {
			t.Fatalf("request id on the wire = %q, want the StartRequest's", got)
		}
		if !r.Applied() {
			t.Fatal("the start was not recorded as applied")
		}
	})

	t.Run("dialed without it", func(t *testing.T) {
		c, err := client.DialContext(t.Context(), client.Options{HostPort: listener.Addr().String()})
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(c.Close)

		r := start(t, c)
		if got := recorder.last(t); got == "flowstate-submission-abc" {
			t.Fatal("a client without the interceptor sent the StartRequest's id")
		}
		if r.Applied() {
			t.Fatal("a start that never passed the interceptor was recorded as applied")
		}
	})
}
