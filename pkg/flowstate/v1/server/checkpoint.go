package server

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// GetCheckpoint reports whether a run segment started from a point a new run
// could be started from; see [v1.CheckpointInfo] for why it describes the point
// and does not return the state.
//
// Every segment's start input is the run's complete carried state: the
// Continue-As-New seam writes it there, so a checkpoint needs no interpreter
// support and no history of its own, and reading it changes nothing.
func (s *FlowstateServer) GetCheckpoint(
	ctx context.Context, req *connect.Request[v1.GetCheckpointRequest],
) (*connect.Response[v1.GetCheckpointResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Authorized, and the run resolved, before its history is read: an empty
	// run id means the latest, and the read must be of the execution the check
	// was made against.
	temporal, described, err := s.authorizeRun(ctx, "GetCheckpoint", req.Msg.GetWorkflowId(), req.Msg.GetRunId())
	if err != nil {
		return nil, err
	}
	execution := described.GetWorkflowExecutionInfo().GetExecution()

	state, err := s.startedRunStateVia(ctx, temporal, execution.GetWorkflowId(), execution.GetRunId())
	if err != nil {
		// Said rather than hidden: the caller asked about a run they may read,
		// and "could not read its start" is an answer, not a server fault.
		return unavailable(fmt.Sprintf("the segment's carried state could not be read: %v", err)), nil
	}

	// The same admission a resume applies, so a point is never reported
	// available that Verify would refuse.
	if err := v1.Validate(state); err != nil {
		return unavailable(fmt.Sprintf("the segment's carried state is invalid: %v", err)), nil
	}
	if err := v1.CheckRunStateSize(state); err != nil {
		return unavailable(err.Error()), nil
	}
	if err := v1.CheckpointUnavailable(state); err != nil {
		return unavailable(unavailableReason(err)), nil
	}

	return connect.NewResponse(&v1.GetCheckpointResponse{Result: &v1.GetCheckpointResponse_Checkpoint{
		Checkpoint: checkpointInfo(execution.GetWorkflowId(), execution.GetRunId(), state),
	}}), nil
}

// checkpointInfo describes the checkpoint state is, for the segment it started.
func checkpointInfo(workflowID, runID string, state *v1.RunState) *v1.CheckpointInfo {
	return &v1.CheckpointInfo{
		WorkflowId: workflowID,
		RunId:      runID,
		Segment:    state.GetSegment(),
		Step:       v1.CheckpointStep(state),
		SpecHash:   v1.CanonicalDigest(state.GetWorkflow()),
		SizeBytes:  int64(v1.RunStateEncodedSize(state)),
	}
}

// unavailable is the answer for a segment that is not a legal starting state.
func unavailable(reason string) *connect.Response[v1.GetCheckpointResponse] {
	return connect.NewResponse(&v1.GetCheckpointResponse{
		Result: &v1.GetCheckpointResponse_UnavailableReason{UnavailableReason: reason},
	})
}

// unavailableReason is the sentence a caller reads for a segment that is not a
// legal starting state. The checkpoint errors already say which rule refused.
func unavailableReason(err error) string {
	return err.Error()
}
