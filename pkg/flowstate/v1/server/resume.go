package server

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// Memo keys recording where a resumed run came from, read afterwards by whoever
// asks how it came to happen. They sit beside the trigger and reason keys, and
// are present only on a run started by [FlowstateServer.ResumeRun].
const (
	checkpointOriginMemoKey = "flowstate.checkpoint.origin"
	checkpointStepMemoKey   = "flowstate.checkpoint.step"
	checkpointPatchMemoKey  = "flowstate.checkpoint.patch"
)

// ResumeRun starts a new run from the checkpoint a run segment started from;
// see the RPC's documentation for the contract.
//
// Every refusal here is a refusal to start, so none is recoverable by the run:
// the state is read, verified and bounded exactly as [FlowstateServer.GetCheckpoint]
// does before anything is built from it, and the new run goes through the same
// admission a submission does (specification validation, the workflow's own
// `manual:` policy) rather than a lighter one.
func (s *FlowstateServer) ResumeRun(
	ctx context.Context, req *connect.Request[v1.ResumeRunRequest],
) (*connect.Response[v1.ResumeRunResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	caller := s.identityFor(ctx)

	// Inheriting a run's state is reading it, so the caller needs the read action
	// as well as the run action the RPC is bound to. Asked before the run is
	// addressed, for [FlowstateServer.authorizeAction]'s reason.
	if err := s.authorizeAction(ctx, "GetCheckpoint", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, req.Msg.GetWorkflowId()); err != nil {
		return nil, err
	}
	temporal, described, err := s.authorizeRun(ctx, "ResumeRun", req.Msg.GetWorkflowId(), req.Msg.GetRunId())
	if err != nil {
		return nil, err
	}
	execution := described.GetWorkflowExecutionInfo().GetExecution()

	state, err := s.startedRunStateVia(ctx, temporal, execution.GetWorkflowId(), execution.GetRunId())
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("the segment's carried state could not be read"))
	}

	// The new run acts as the origin's principal and nothing here can change
	// that, so only that principal may start it. Anyone else would be borrowing
	// an identity, which is the one thing a resume must never do.
	if !samePrincipal(caller.GetPrincipal(), state.GetIdentity().GetPrincipal()) {
		return nil, s.auditDeny(ctx, "ResumeRun", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, execution.GetWorkflowId(),
			v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED,
			connect.NewError(connect.CodePermissionDenied, fmt.Errorf("a run can be resumed only by the principal it acts as")))
	}

	if err := v1.Validate(state); err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("the segment's carried state is invalid"))
	}
	patch := req.Msg.GetPatch()
	if patch != nil {
		patch = proto.Clone(patch).(*v1.Workflow)
		// A patch is a specification a caller sent, so it is admitted as one.
		if err := s.validateSpecification(patch); err != nil {
			return nil, err
		}
	}

	checkpoint, err := v1.NewCheckpoint(state, v1.CheckpointBuild, &v1.CheckpointOrigin{
		Run:     &v1.RunAddress{WorkflowId: execution.GetWorkflowId(), RunId: execution.GetRunId()},
		Segment: state.GetSegment(),
		Step:    v1.CheckpointStep(state),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	if want := req.Msg.GetExpectedStep(); want != "" && want != checkpoint.GetOrigin().GetStep() {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"the checkpoint stands before step %q, not %q", checkpoint.GetOrigin().GetStep(), want))
	}

	next, err := checkpoint.Resume(v1.CheckpointBuild, patch)
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}

	// A new run of its own: the origin's chain bookkeeping, debug session and
	// consumed deliveries describe the origin, not this run.
	next.Segment = 0
	next.WorkloadStartedAt = nil
	next.Debug = nil
	next.ConsumedDeliveryIds = nil
	next.StepsBudget = int32(s.maxStepsPerRun)
	patchDigest := ""
	if patch != nil {
		// The deployment's trusted name vouched for the origin's workflow, not
		// for an edit of it.
		next.MetricWorkflowName = ""
		patchDigest = v1.CanonicalDigest(next.GetWorkflow())
	}

	workflowID := fmt.Sprintf("flowstate-workflow-%s", uuid.New().String())
	if err := s.authorizeManualStart(ctx, "ResumeRun", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID,
		next.GetWorkflow(), caller, req.Msg.GetReason(), next.GetInputs()); err != nil {
		return nil, err
	}

	memo, startClient, options, err := s.prepareCreate(ctx, caller, next.GetWorkflow(), next.GetInputs())
	if err != nil {
		return nil, err
	}
	options.ID = workflowID
	memo[triggerMemoKey] = v1.TriggerKindManual
	memo[checkpointOriginMemoKey] = execution.GetWorkflowId() + "/" + execution.GetRunId()
	memo[checkpointStepMemoKey] = checkpoint.GetOrigin().GetStep()
	if patchDigest != "" {
		memo[checkpointPatchMemoKey] = patchDigest
	}
	if reason := req.Msg.GetReason(); reason != "" {
		memo[reasonMemoKey] = reason
	}
	options.Memo = memo

	// The record of the decision to start this run, under its own id. The one
	// above, from authorizing the origin, names the run that was read.
	if err := s.auditAllow(ctx, "ResumeRun", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID); err != nil {
		return nil, err
	}

	run, err := startClient.ExecuteWorkflow(ctx, options, engine.Run, next)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unable to execute workflow: %w", err))
	}

	return connect.NewResponse(&v1.ResumeRunResponse{
		WorkflowId:  workflowID,
		RunId:       run.GetRunID(),
		Origin:      checkpointInfo(execution.GetWorkflowId(), execution.GetRunId(), state),
		PatchDigest: patchDigest,
	}), nil
}

// samePrincipal reports whether two principals are the same authenticated
// identity: issuer, subject and namespace all equal.
func samePrincipal(a, b *v1.Principal) bool {
	return a.GetIssuer() == b.GetIssuer() && a.GetSubject() == b.GetSubject() && a.GetNamespace() == b.GetNamespace()
}
