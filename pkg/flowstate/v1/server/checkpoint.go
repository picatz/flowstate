package server

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

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
	_, described, err := s.authorizeRun(ctx, "GetCheckpoint", req.Msg.GetWorkflowId(), req.Msg.GetRunId())
	if err != nil {
		return nil, err
	}
	execution := described.GetWorkflowExecutionInfo().GetExecution()

	state, err := s.startedRunState(ctx, s.identityFor(ctx).GetPrincipal().GetNamespace(),
		execution.GetWorkflowId(), execution.GetRunId())
	if err != nil {
		// Said rather than hidden: the caller asked about a run they may read,
		// and "could not read its start" is an answer, not a server fault.
		return connect.NewResponse(&v1.GetCheckpointResponse{
			UnavailableReason: fmt.Sprintf("the segment's carried state could not be read: %v", err),
		}), nil
	}

	if err := v1.CheckpointUnavailable(state); err != nil {
		return connect.NewResponse(&v1.GetCheckpointResponse{UnavailableReason: unavailableReason(err)}), nil
	}

	return connect.NewResponse(&v1.GetCheckpointResponse{
		Checkpoint: &v1.CheckpointInfo{
			WorkflowId: execution.GetWorkflowId(),
			RunId:      execution.GetRunId(),
			Segment:    state.GetSegment(),
			Step:       v1.CheckpointStep(state),
			SpecHash:   v1.CanonicalDigest(state.GetWorkflow()),
			SizeBytes:  int64(proto.Size(state)),
		},
	}), nil
}

// unavailableReason is the sentence a caller reads for a position that is not a
// legal starting state.
func unavailableReason(err error) string {
	if errors.Is(err, v1.ErrCheckpointUnsupported) {
		return "the segment starts inside a call, a loop or concurrent work; only a position between top-level steps can be started from"
	}

	return err.Error()
}
