package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/workflow"

	"github.com/picatz/flowstate/internal/textbound"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authz"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// DebugHistory reads a durable run as it was at one point of its recorded
// history (#2248). It is the live debugger's read path with the run's past in
// place of its present: the same action, the same tenancy and the same `debug:`
// policy decide who may ask, and the answer is a [v1.DebugSnapshot] the
// interpreter produced by replaying the history to the point.
//
// # What it cannot do
//
// The replay has no worker, so it can dispatch no task, plugin or other effect,
// and it writes nothing to the run: a closed run can be read as well as an open
// one, and a read of either changes neither. The cost is bounded where it is
// spent: the history is read only up to [engine.MaxReconstructionEvents],
// [maxDebugHistoryReplays] reconstructions run at once and the next is refused
// as unavailable rather than queued, and one read ends at
// [debugHistoryTimeout] or when its caller goes away. A history the current
// interpreter cannot replay to the point, because it was written by another
// build, is refused, never guessed.
const (
	// maxDebugHistoryReplays is how many reconstructions this server runs at
	// once: each is CPU and memory linear in the history it replays.
	maxDebugHistoryReplays = 4

	// debugHistoryTimeout bounds one read.
	debugHistoryTimeout = 30 * time.Second

	// maxDebugHistoryFailureBytes bounds the replay's own account of why it
	// could not reconstruct a point, which quotes the history.
	maxDebugHistoryFailureBytes = 512
)

// DebugHistory implements [flowstatev1connect.WorkflowServiceHandler].
func (s *FlowstateServer) DebugHistory(ctx context.Context, req *connect.Request[v1.DebugHistoryRequest]) (*connect.Response[v1.DebugHistoryResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	workflowID, runID := req.Msg.GetWorkflowId(), req.Msg.GetRunId()

	// An expression can test a sensitive value the printed answer withholds, so
	// asking any is the inspect action's, as a live inspection is, and the
	// audit trail carries a digest of what was asked.
	detail := &v1.AuditDebugDetail{Operation: "history", RunId: runID, Revision: uint64(req.Msg.GetEventId())}
	if asked := req.Msg.GetInspections(); len(asked) > 0 {
		detail.ExpressionDigest = historyInspectionsDigest(asked)
		if err := s.historyInspectAuthorized(ctx, workflowID, detail); err != nil {
			return nil, err
		}
	}

	// Resolved exactly, and judged by the memo of the execution whose history
	// is read: following the chain to its current execution, as a live session
	// does, would apply that execution's `debug:` policy to another's past.
	run, err := s.authorizeDebugRun(ctx, "DebugHistory", workflowID, runID, false, detail)
	if err != nil {
		return nil, err
	}
	if run.runID != runID {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %s of %s was not found", runID, workflowID))
	}

	// Held until the replay has finished, not until this call returns: a read
	// whose caller went away leaves its replay running, and that replay is
	// still the CPU and memory the slot counts.
	select {
	case s.historySlots <- struct{}{}:
	default:
		return nil, connect.NewError(connect.CodeResourceExhausted,
			errors.New("the server is reconstructing as many runs as it will at once; try again shortly"))
	}
	release := sync.OnceFunc(func() { <-s.historySlots })
	// Handed to the reconstruction once it is asked for, which then owns it:
	// until then, whichever way this call ends returns the slot.
	handedOver := false
	defer func() {
		if !handedOver {
			release()
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, debugHistoryTimeout)
	defer cancel()

	history, err := readHistory(ctx, run, runID)
	if err != nil {
		return nil, err
	}

	events := history.GetEvents()
	points := engine.Boundaries(history)
	if len(points) == 0 {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("the run has no point it can be read at yet"))
	}
	index := points[len(points)-1]
	if want := req.Msg.GetEventId(); want != 0 {
		at := slices.IndexFunc(points, func(i int) bool { return events[i].GetEventId() == want })
		if at < 0 {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("event %d is not a point this run can be read at; ask for one of the boundaries a previous answer lists, or 0 for the last", want))
		}
		index = points[at]
	}

	// The first record names the point asked for, and 0 asks for the last one;
	// this names the point actually read, so the trail answers which point of
	// which run was accessed.
	if err := s.auditDebugAllow(ctx, "DebugHistory", workflowID,
		&v1.AuditDebugDetail{
			Operation: "history/resolved", RunId: runID, Revision: uint64(events[index].GetEventId()),
			ExpressionDigest: detail.GetExpressionDigest(),
		}); err != nil {
		return nil, err
	}

	handedOver = true
	rec, err := engine.ReconstructWith(ctx, engine.ReconstructOptions{
		DataConverter: s.dataConverter, Done: release,
		AllowInspection: s.holderMayInspect(run, events),
	}, history, index,
		workflow.Execution{ID: workflowID, RunID: runID}, inspectionRequests(req.Msg.GetInspections())...)
	switch {
	case errors.Is(err, context.Canceled):
		return nil, connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return nil, connect.NewError(connect.CodeDeadlineExceeded, err)
	case err != nil:
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"the run could not be reconstructed at event %d: %s", events[index].GetEventId(),
			textbound.Cut(err.Error(), maxDebugHistoryFailureBytes)))
	}

	if rec.InspectDenied != nil {
		return nil, s.auditDebugDeny(ctx, "DebugHistory", workflowID,
			&v1.AuditDebugDetail{
				Operation: "history/inspect", RunId: runID, Revision: uint64(events[index].GetEventId()),
				ExpressionDigest: detail.GetExpressionDigest(),
			},
			v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED,
			connect.NewError(connect.CodePermissionDenied, rec.InspectDenied))
	}

	ids := make([]int64, len(points))
	for i, at := range points {
		ids[i] = events[at].GetEventId()
	}

	// A point before the interpreter had installed its debug session has no
	// snapshot to show; authorization already refused a run that declares none.
	snapshot := rec.Debug
	if snapshot != nil {
		snapshot = s.expressionsFor(ctx, snapshot)
	}

	return connect.NewResponse(&v1.DebugHistoryResponse{
		Snapshot:   snapshot,
		Progress:   rec.Progress,
		EventId:    rec.EventID,
		Fidelity:   v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED,
		Boundaries: ids,
		Outcome:    outcomeOf(events[index]),
		Inspected:  inspectedAnswers(req.Msg.GetInspections(), rec),
	}), nil
}

// historyInspectionsField is the request field the inspect action widens
// DebugHistory by.
const historyInspectionsField = "flowstate.v1.DebugHistoryRequest.inspections"

// historyInspectAuthorized requires workload.debug_inspect of a caller that
// asks for inspections, and records the decision under that action either way,
// as a reveal is. An expression can test a sensitive value the printed answer
// withholds, so it is a disclosure of its own beside reading the point.
func (s *FlowstateServer) historyInspectAuthorized(ctx context.Context, workflowID string, detail *v1.AuditDebugDetail) error {
	action, err := v1.AuthorizationActionForRequestField(historyInspectionsField)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	subject := s.debugAuditSubject(ctx, "DebugHistory", workflowID, detail)
	subject.RequestField = historyInspectionsField

	refusal := s.decide(ctx, action, authz.Implied).Refusal()
	if refusal == nil {
		return s.audit.Allow(ctx, subject)
	}
	if err := s.audit.Deny(ctx, subject, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED); err != nil {
		return err
	}

	return refusal
}

// holderMayInspect decides, once the point's session is known, whether the
// caller may read its scope. A live inspection is the holder's alone, and the
// past of a run still going keeps that: another caller the policy admits could
// not have read the scope while it was held. A closed run has no holder to
// protect. A point where the run held no session has no scope to read, so there
// is nothing to refuse.
func (s *FlowstateServer) holderMayInspect(run *debugRun, events []*historypb.HistoryEvent) func(*v1.DebugSnapshot) error {
	if closedAt(events) {
		return nil
	}
	caller := run.sender.GetIdentity()

	return func(snapshot *v1.DebugSnapshot) error {
		holder := snapshot.GetSession().GetAttachedBy()
		if holder != nil && v1.QualifiedSubject(holder.GetIssuer(), holder.GetSubject()) != v1.QualifiedSubject(caller.GetIssuer(), caller.GetSubject()) {
			return errors.New("only the session's holder may inspect the past of a run that is still going")
		}

		return nil
	}
}

// historyInspectionsDigest is the content digest of what was asked, in order,
// so one audit record names the whole batch, with how each was paged.
func historyInspectionsDigest(asked []*v1.DebugHistoryInspection) string {
	// Length-prefixed, so that two different batches never share bytes.
	var all []byte
	for _, one := range asked {
		all = binary.BigEndian.AppendUint64(all, uint64(len(one.GetExpression())))
		all = append(all, one.GetExpression()...)
		all = binary.BigEndian.AppendUint64(all, uint64(one.GetOffset())<<32|uint64(uint32(one.GetLimit())))
		if one.GetChildren() {
			all = append(all, 1)
		} else {
			all = append(all, 0)
		}
	}

	return v1.ContentDigest(all)
}

// inspectionRequests are the inspections as the interpreter answers them. The
// run, session and revision are the point's own, which the interpreter binds,
// so a caller cannot name another.
func inspectionRequests(asked []*v1.DebugHistoryInspection) []*v1.DebugInspectRequest {
	requests := make([]*v1.DebugInspectRequest, len(asked))
	for i, one := range asked {
		requests[i] = &v1.DebugInspectRequest{
			Expression: one.GetExpression(), Children: one.GetChildren(), Offset: one.GetOffset(), Limit: one.GetLimit(),
		}
	}

	return requests
}

// inspectedAnswers pairs each inspection with the reconstruction's answer. A
// refusal is the result's error, bounded like the replay's own account of a
// failure, which can quote the run.
func inspectedAnswers(asked []*v1.DebugHistoryInspection, rec *engine.Reconstruction) []*v1.DebugHistoryInspected {
	var answers []*v1.DebugHistoryInspected
	for i, one := range asked {
		result := &v1.DebugInspectResponse{}
		if i < len(rec.Inspected) && rec.Inspected[i] != nil {
			result = rec.Inspected[i]
		}
		if i < len(rec.InspectErrs) && rec.InspectErrs[i] != nil {
			result = &v1.DebugInspectResponse{Error: textbound.Cut(rec.InspectErrs[i].Error(), maxDebugHistoryFailureBytes)}
		} else if i >= len(rec.Inspected) {
			result = &v1.DebugInspectResponse{Error: "the run held no session at this point, so there is nothing to inspect"}
		}
		fidelity := v1.DebugFidelity_DEBUG_FIDELITY_HYPOTHETICAL
		if one.GetExpression() == "" {
			fidelity = v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED
		}
		answers = append(answers, &v1.DebugHistoryInspected{Result: result, Fidelity: fidelity})
	}

	return answers
}

// readHistory reads a run's history, refusing one over the bound as soon as it
// is seen to be: the read stops at the first event past it rather than finishing.
func readHistory(ctx context.Context, run *debugRun, runID string) (*historypb.History, error) {
	iter := run.temporal.GetWorkflowHistory(ctx, run.workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	history := &historypb.History{}
	for iter.HasNext() {
		if len(history.Events) >= engine.MaxReconstructionEvents {
			return nil, connect.NewError(connect.CodeResourceExhausted,
				fmt.Errorf("the run's history is over the %d events a reconstruction reads", engine.MaxReconstructionEvents))
		}
		event, err := iter.Next()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("reading the run's history: %w", err))
		}
		history.Events = append(history.Events, event)
	}

	return history, nil
}

// closedAt reports whether the history ends with the execution's closing event.
func closedAt(events []*historypb.HistoryEvent) bool {
	return len(events) > 0 && outcomeOf(events[len(events)-1]) != v1.DebugRunState_DEBUG_RUN_STATE_UNSPECIFIED
}

// outcomeOf is how the execution ended at event, or unspecified for a point
// that is not its closing event. A live read of a closed run says COMPLETED or
// FAILED, and this says the same of its past.
func outcomeOf(event *historypb.HistoryEvent) v1.DebugRunState {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		return v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return v1.DebugRunState_DEBUG_RUN_STATE_FAILED
	default:
		return v1.DebugRunState_DEBUG_RUN_STATE_UNSPECIFIED
	}
}
