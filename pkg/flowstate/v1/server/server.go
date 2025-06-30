package server

import (
	"context"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

// New creates a new FlowstateServer instance with the provided Temporal client.
func New(temporalClient client.Client) *FlowstateServer {
	return &FlowstateServer{
		temporalClient: temporalClient,
	}
}

// FlowstateServer implements the flowstatev1connect.WorkflowServiceHandler interface
// and provides methods to run and get the status of workflows using Temporal.
type FlowstateServer struct {
	temporalClient client.Client
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

	run, err := s.temporalClient.ExecuteWorkflow(ctx, options, engine.Run, req.Msg.GetWorkflow())
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
