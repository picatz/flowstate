package temporalclient

import (
	"context"
	"testing"

	"go.temporal.io/api/workflowservice/v1"
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
	ctx := WithStartRequestID(t.Context(), "flowstate-submission-abc")

	sent := invokeStart(t, ctx, &workflowservice.StartWorkflowExecutionRequest{RequestId: "sdk-random"})
	if got := sent.(*workflowservice.StartWorkflowExecutionRequest).GetRequestId(); got != "flowstate-submission-abc" {
		t.Fatalf("request id = %q, want the context's", got)
	}
}

func TestStartInterceptorLeavesOtherCallsAlone(t *testing.T) {
	// No request id on the context: the SDK's own is what goes out.
	sent := invokeStart(t, t.Context(), &workflowservice.StartWorkflowExecutionRequest{RequestId: "sdk-random"})
	if got := sent.(*workflowservice.StartWorkflowExecutionRequest).GetRequestId(); got != "sdk-random" {
		t.Fatalf("request id = %q, want the SDK's", got)
	}

	// An empty one is the same as none.
	sent = invokeStart(t, WithStartRequestID(t.Context(), ""), &workflowservice.StartWorkflowExecutionRequest{RequestId: "sdk-random"})
	if got := sent.(*workflowservice.StartWorkflowExecutionRequest).GetRequestId(); got != "sdk-random" {
		t.Fatalf("request id = %q, want the SDK's", got)
	}

	// Any other request carrying a request id is not a start, and is not
	// rewritten even under a context that carries one.
	signal := &workflowservice.SignalWithStartWorkflowExecutionRequest{RequestId: "sdk-random"}
	sent = invokeStart(t, WithStartRequestID(t.Context(), "flowstate-submission-abc"), signal)
	if got := sent.(*workflowservice.SignalWithStartWorkflowExecutionRequest).GetRequestId(); got != "sdk-random" {
		t.Fatalf("signal-with-start request id = %q, want it untouched", got)
	}
}

func TestConfigOptionsInstallTheStartInterceptor(t *testing.T) {
	// The one dial option Options adds is the interceptor; nothing else in a
	// zero Config's resolution adds dial options.
	opts, err := Config{}.Options()
	if err != nil {
		t.Fatalf("Options() error: %v", err)
	}
	if got := len(opts.ConnectionOptions.DialOptions); got != 1 {
		t.Fatalf("dial options = %d, want the start interceptor alone", got)
	}
}
