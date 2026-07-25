package server

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// New creates a new FlowstateServer instance with the provided Temporal client.
func New(temporalClient client.Client, opts ...Option) *FlowstateServer {
	s := &FlowstateServer{
		temporalClient: temporalClient,
		maxStepsPerRun: maxStepsPerRunFromEnv(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Option configures a [FlowstateServer].
type Option func(*FlowstateServer)

// WithNamespace records the Temporal namespace a workload runs in, which appears
// in the identity the run acts as.
func WithNamespace(name string) Option {
	return func(s *FlowstateServer) { s.namespace = name }
}

// WithDeployment records which Flowstate installation is running a workload.
//
// It appears in the identity a run acts as, so an assertion presented to an
// external system distinguishes a staging deployment from a production one.
func WithDeployment(name string) Option {
	return func(s *FlowstateServer) { s.deployment = name }
}

// WithIdentityClaims names the caller token claims to carry into a run's
// identity.
//
// Only named claims are copied, so the identity records what authorization
// decisions actually need — a repository, an environment, a team — rather than
// becoming a copy of whole tokens in workflow history.
func WithIdentityClaims(claims ...string) Option {
	return func(s *FlowstateServer) { s.identityClaims = claims }
}

// FlowstateServer implements the flowstatev1connect.WorkflowServiceHandler interface
// and provides methods to run and get the status of workflows using Temporal.
type FlowstateServer struct {
	temporalClient client.Client

	// maxStepsPerRun is read once at construction. Reading it per request would
	// let the step budget change between a run and its own Continue-As-New,
	// which must not vary once a run has started.
	maxStepsPerRun int

	namespace      string
	deployment     string
	identityClaims []string
}

// maxStepsPerRunFromEnv reads the optional step budget.
func maxStepsPerRunFromEnv() int {
	if s := os.Getenv("FLOWSTATE_MAX_STEPS_PER_RUN"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			return v
		}
	}
	return 0
}

// Ensure FlowstateServer implements the WorkflowServiceHandler interface.
var _ flowstatev1connect.WorkflowServiceHandler = (*FlowstateServer)(nil)

// Shutdown gracefully shuts down the FlowstateServer, closing the Temporal client.
func (s *FlowstateServer) Shutdown(ctx context.Context) error {
	s.temporalClient.Close()
	return nil
}

// Run starts a new workflow execution.
func (s *FlowstateServer) Run(ctx context.Context, req *connect.Request[v1.RunRequest]) (*connect.Response[v1.RunResponse], error) {
	workflowID := fmt.Sprintf("flowstate-workflow-%s", uuid.NewString())

	options := client.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          engine.RunTaskQueueName,
		WorkflowRunTimeout: 6 * time.Hour,
	}

	// Capture the identity now, while the authenticated caller is still in scope.
	// The run outlives this request, so anything a later step needs to know about
	// who asked for the work has to be recorded in the state it carries.
	run, err := s.temporalClient.ExecuteWorkflow(ctx, options, engine.Run, &v1.RunState{
		Workflow:    req.Msg.GetWorkflow(),
		StepsBudget: int32(s.maxStepsPerRun),
		Identity:    s.identityFor(ctx),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unable to execute workflow: %w", err))
	}

	return connect.NewResponse(
		&v1.RunResponse{
			WorkflowId: workflowID,
			RunId:      run.GetRunID(),
			Status:     v1.RunResponse_STATUS_RUNNING,
		},
	), nil
}

// identityFor builds the identity a run will act as from the authenticated
// caller.
//
// The run records who asked for the work so that later steps can act on their
// behalf, and so an assertion presented to an external system states something
// established rather than assumed. Only the configured claims are copied: this
// value is persisted in workflow history, which is durable and broadly readable,
// so it must carry no more than authorization decisions need and nothing secret.
//
// An unauthenticated caller yields an identity with no subject rather than an
// error, because whether to allow that at all is decided by the authenticator,
// not here.
func (s *FlowstateServer) identityFor(ctx context.Context) *v1.WorkloadIdentity {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok {
		// An unauthenticated caller yields an identity with no subject rather
		// than an error: whether to allow that at all is the authenticator's
		// decision, not this one's.
		return &v1.WorkloadIdentity{
			Namespace:  s.namespace,
			Deployment: s.deployment,
		}
	}

	// Derived rather than assembled here, so the namespace precedence has one
	// implementation. The verified caller's namespace wins; WithNamespace is only
	// the fallback for a deployment whose trust policy names none, meaning a
	// single tenant. The other order would make the tenant boundary decorative —
	// a namespace determined by how the server was started rather than by who the
	// caller is means every tenant shares one.
	derived := auth.IdentityFromPrincipal(principal, s.namespace, s.deployment, s.identityClaims...)

	return &v1.WorkloadIdentity{
		Subject:    derived.Subject,
		Issuer:     derived.Issuer,
		Claims:     derived.Claims,
		Namespace:  derived.Namespace,
		Deployment: derived.Deployment,
	}
}

// Get retrieves the status of a workflow execution by its ID (and optionally its run ID).
func (s *FlowstateServer) Get(ctx context.Context, req *connect.Request[v1.GetRequest]) (*connect.Response[v1.GetResponse], error) {
	resp, err := s.temporalClient.DescribeWorkflowExecution(ctx, req.Msg.GetWorkflowId(), req.Msg.GetRunId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to query workflow status: %w", err))
	}
	switch respStatus := getWorkflowExecutionStatus(resp); respStatus {
	case v1.RunResponse_STATUS_RUNNING:
		return connect.NewResponse(
			&v1.GetResponse{
				WorkflowId: req.Msg.GetWorkflowId(),
				RunId:      resp.WorkflowExecutionInfo.Execution.RunId,
				Status:     respStatus,
			},
		), nil
	case v1.RunResponse_STATUS_COMPLETED:
		var result v1.Workflow_StepOutputs
		if err := s.temporalClient.GetWorkflow(ctx, req.Msg.GetWorkflowId(), req.Msg.GetRunId()).Get(ctx, &result); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error getting workflow result: %w", err))
		}
		return connect.NewResponse(
			&v1.GetResponse{
				WorkflowId: req.Msg.GetWorkflowId(),
				RunId:      resp.WorkflowExecutionInfo.Execution.RunId,
				Status:     respStatus,
				Kind: &v1.GetResponse_Outputs{
					Outputs: &result,
				},
			},
		), nil
	case v1.RunResponse_STATUS_FAILED, v1.RunResponse_STATUS_CANCELED, v1.RunResponse_STATUS_TERMINATED, v1.RunResponse_STATUS_TIMED_OUT:
		return connect.NewResponse(
			&v1.GetResponse{
				WorkflowId: req.Msg.GetWorkflowId(),
				RunId:      resp.WorkflowExecutionInfo.Execution.RunId,
				Status:     respStatus,
				Kind: &v1.GetResponse_Error{
					Error: &v1.RunResponse_Error{
						Message: respStatus.String(),
					},
				},
			},
		), nil
	default:
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unknown workflow status: %d", respStatus))
	}
}

// getWorkflowExecutionStatus maps the Temporal workflow execution status to Flowstate's run response status.
func getWorkflowExecutionStatus(resp *workflowservice.DescribeWorkflowExecutionResponse) v1.RunResponse_Status {
	switch resp.GetWorkflowExecutionInfo().GetStatus() {
	case enums.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return v1.RunResponse_STATUS_CANCELED
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return v1.RunResponse_STATUS_COMPLETED
	case enums.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return v1.RunResponse_STATUS_RUNNING
	case enums.WORKFLOW_EXECUTION_STATUS_FAILED:
		return v1.RunResponse_STATUS_FAILED
	case enums.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return v1.RunResponse_STATUS_TERMINATED
	case enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return v1.RunResponse_STATUS_TIMED_OUT
	default:
		return v1.RunResponse_STATUS_UNSPECIFIED
	}
}
