package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"go.temporal.io/api/enums/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// The durable debugger's RPCs (#928 stage 3, #2126): attach, read, resume,
// breakpoints, and inspection, over the lease mechanics a run already has.
//
// # What each one is allowed to do, and by whom
//
// Every RPC maps to one authorization action — `workload.debug` for all but
// inspection, `workload.debug_inspect` for inspection — and every one then
// asks the run's own `debug:` policy, which fails closed: a workflow that
// declares none is debuggable by nobody. Tenancy is the ordinary run
// resolution every verb shares.
//
// Beyond that, the run decides. A command carries the session it is for, the
// server attests who sent it, and the workflow applies it only for the
// session's own holder and, for a resume, only at the revision the caller last
// saw. The server holds no session state at all: a server restart, a second
// server, or a client reconnecting through a different one all read the same
// session out of the run.
//
// # Commands are signals, reads are queries
//
// A command travels as a typed ask on the reserved debug signal, so it is
// ordered by history with every other ask; the server then reads the run's
// receipt for the request back through [v1.DebugQuery]. Delivery is not
// application: a receipt the run has not written yet is answered pending, and
// a retry with the same request id is answered from the run's receipt rather
// than applied again.
//
// # Inspection is a disclosure
//
// An expression can test any value in the held scope, whatever its rendering
// hides, so `DebugInspect` needs its own scope, may be asked only by the
// session's holder of a run held at the named revision, and is audited with a
// digest of the expression rather than the expression itself.

const (
	// defaultDebugWait is how long a command waits for its receipt when the
	// request names no wait.
	defaultDebugWait = 5 * time.Second

	// maxDebugWait bounds any wait a request asks for.
	maxDebugWait = 30 * time.Second

	// debugPollEvery paces the receipt and revision reads.
	debugPollEvery = 100 * time.Millisecond

	// debugQueryTimeout bounds one query, beside the request's own deadline.
	debugQueryTimeout = 5 * time.Second
)

// debugRun is one resolved, authorized run a debug RPC acts on.
type debugRun struct {
	temporal   client.Client
	describe   *workflowservice.DescribeWorkflowExecutionResponse
	workflowID string
	runID      string
	sender     *v1.SignalSender
}

// open reports whether the run can still be commanded.
func (r *debugRun) open() bool {
	return r.describe.GetWorkflowExecutionInfo().GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_RUNNING
}

// authorizeDebug resolves and authorizes one debug RPC: the action, tenancy,
// the reserved-channel protocol, and the run's own `debug:` policy. It audits
// the decision with detail.
func (s *FlowstateServer) authorizeDebug(ctx context.Context, rpc, workflowID, runID string, detail *v1.AuditDebugDetail) (*debugRun, error) {
	kind := v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN
	if err := s.authorizeAction(ctx, rpc, kind, workflowID); err != nil {
		return nil, err
	}

	temporal, resp, code, err := s.authorizeRunDecision(ctx, workflowID, runID)
	if runID != "" && (err != nil || resp.GetWorkflowExecutionInfo().GetFirstRunId() == runID) {
		// The same chain resolution [FlowstateServer.Signal] uses: a run id
		// names the chain, and a session follows the chain across
		// Continue-As-New, so the current execution of that same chain is the
		// one to address.
		if currentTemporal, currentResp, _, currentErr := s.authorizeRunDecision(ctx, workflowID, ""); currentErr == nil &&
			currentResp.GetWorkflowExecutionInfo().GetFirstRunId() == runID {
			temporal, resp, err = currentTemporal, currentResp, nil
			code = v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED
		}
	}
	if err != nil {
		if code == v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED {
			return nil, err
		}

		return nil, s.auditDebugDeny(ctx, rpc, workflowID, detail, code, err)
	}

	sender := &v1.SignalSender{Identity: s.identityFor(ctx), AcceptedAt: timestamppb.Now()}

	memo := resp.GetWorkflowExecutionInfo().GetMemo()
	current, err := s.usesCurrentSignalProtocol(memo)
	if err != nil {
		return nil, s.auditDebugDeny(ctx, rpc, workflowID, detail, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED,
			connect.NewError(connect.CodePermissionDenied, err))
	}
	if !current {
		return nil, s.auditDebugDeny(ctx, rpc, workflowID, detail, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED,
			connect.NewError(connect.CodeFailedPrecondition,
				errors.New("this run was submitted before the engine reserved its debug channel, so it cannot be debugged")))
	}
	if err := s.authorizeReservedSignal(resp, v1.DebugSignal, sender); err != nil {
		return nil, s.auditDebugDeny(ctx, rpc, workflowID, detail, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, err)
	}

	if err := s.audit.Allow(ctx, s.debugAuditSubject(ctx, rpc, workflowID, detail)); err != nil {
		return nil, err
	}

	return &debugRun{
		temporal:   temporal,
		describe:   resp,
		workflowID: workflowID,
		runID:      resp.GetWorkflowExecutionInfo().GetExecution().GetRunId(),
		sender:     sender,
	}, nil
}

func (s *FlowstateServer) debugAuditSubject(ctx context.Context, rpc, workflowID string, detail *v1.AuditDebugDetail) audit.Subject {
	subject := s.auditSubject(ctx, rpc, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, workflowID)
	subject.Debug = detail

	return subject
}

func (s *FlowstateServer) auditDebugDeny(ctx context.Context, rpc, workflowID string, detail *v1.AuditDebugDetail, code v1.AuditDenyCode, refusal error) error {
	if err := s.audit.Deny(ctx, s.debugAuditSubject(ctx, rpc, workflowID, detail), code); err != nil {
		return err
	}

	return refusal
}

// snapshot reads the run's debug state, naming a receipt to include. A run
// whose interpreter predates the protocol answers with an error the caller
// maps to incompatible.
func (r *debugRun) snapshot(ctx context.Context, request string) (*v1.DebugSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, debugQueryTimeout)
	defer cancel()

	encoded, err := r.temporal.QueryWorkflow(ctx, r.workflowID, r.runID, v1.DebugQuery, request)
	if err != nil {
		return nil, err
	}
	var snapshot v1.DebugSnapshot
	if err := encoded.Get(&snapshot); err != nil {
		return nil, err
	}
	if !r.open() {
		switch r.describe.GetWorkflowExecutionInfo().GetStatus() {
		case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
			snapshot.State = v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED
		default:
			snapshot.State = v1.DebugRunState_DEBUG_RUN_STATE_FAILED
		}
		snapshot.Reason = v1.DebugStopReason_DEBUG_STOP_REASON_UNSPECIFIED
		snapshot.Frames = nil
		snapshot.Message = "the run is closed: " + strings.ToLower(strings.TrimPrefix(
			r.describe.GetWorkflowExecutionInfo().GetStatus().String(), "WORKFLOW_EXECUTION_STATUS_"))
	}

	return &snapshot, nil
}

// workflowMetadataQuery is Temporal's built-in query listing the handlers a
// workflow registered (go.temporal.io/sdk internal/client.go,
// QueryTypeWorkflowMetadata). It is how a failed debug query is told apart by
// value: a run whose interpreter never registered [v1.DebugQuery] is
// incompatible, and one that did is merely unanswered.
const workflowMetadataQuery = "__temporal_workflow_metadata"

// speaksDebug reports whether the run's interpreter registered the debug
// query at all.
func (r *debugRun) speaksDebug(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, debugQueryTimeout)
	defer cancel()

	encoded, err := r.temporal.QueryWorkflow(ctx, r.workflowID, r.runID, workflowMetadataQuery)
	if err != nil {
		return false, err
	}
	var metadata sdkpb.WorkflowMetadata
	if err := encoded.Get(&metadata); err != nil {
		return false, err
	}
	for _, query := range metadata.GetDefinition().GetQueryDefinitions() {
		if query.GetName() == v1.DebugQuery {
			return true, nil
		}
	}

	return false, nil
}

// readFailure turns a failed snapshot read into a receipt a command answers
// with, or an error for a read.
func (r *debugRun) readFailure(ctx context.Context, request string, err error) (*v1.DebugReceipt, error) {
	if speaks, metaErr := r.speaksDebug(ctx); metaErr == nil && !speaks {
		return &v1.DebugReceipt{
			RequestId: request,
			Status:    v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_INCOMPATIBLE,
			Message:   "the run's interpreter predates typed debugging; nothing was sent that it would misread",
		}, nil
	}

	return nil, connect.NewError(connect.CodeUnavailable,
		fmt.Errorf("the run's worker did not answer the debug query: %w", err))
}

// readError is readFailure for a read, which has no receipt to answer with.
func (r *debugRun) readError(ctx context.Context, err error) error {
	receipt, readErr := r.readFailure(ctx, "", err)
	if readErr != nil {
		return readErr
	}

	return connect.NewError(connect.CodeFailedPrecondition, errors.New(receipt.GetMessage()))
}

// ready checks the run can take a typed command at all: open, and speaking the
// protocol.
func (r *debugRun) ready(ctx context.Context, request string) (*v1.DebugSnapshot, *v1.DebugReceipt, error) {
	snapshot, err := r.snapshot(ctx, request)
	if err != nil {
		receipt, err := r.readFailure(ctx, request, err)

		return nil, receipt, err
	}
	if snapshot.GetReceipt() != nil {
		// Answered before: a retry after a lost response.
		receipt := snapshot.GetReceipt()
		if receipt.GetStatus() == v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED {
			receipt.Status = v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE
		}
		snapshot.Receipt = nil

		return snapshot, receipt, nil
	}
	if snapshot.GetProtocol() < v1.DebugProtocol {
		return snapshot, &v1.DebugReceipt{
			RequestId: request,
			Status:    v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_INCOMPATIBLE,
			Message:   fmt.Sprintf("the run's interpreter speaks debug protocol %d, and this command needs %d", snapshot.GetProtocol(), v1.DebugProtocol),
		}, nil
	}
	if !r.open() {
		return snapshot, &v1.DebugReceipt{
			RequestId: request,
			Status:    v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED,
			Message:   snapshot.GetMessage(),
		}, nil
	}

	return snapshot, nil, nil
}

// command delivers one typed ask and waits, up to wait, for its receipt.
func (r *debugRun) command(ctx context.Context, ask *v1.DebugAsk, wait time.Duration) (*v1.DebugReceipt, *v1.DebugSnapshot, error) {
	snapshot, receipt, err := r.ready(ctx, ask.Request)
	if err != nil || receipt != nil {
		return receipt, snapshot, err
	}

	payload, err := v1.NewTypedDebugAsk(ask)
	if err != nil {
		return nil, nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := v1.CheckSignalPayloadSize(payload); err != nil {
		return nil, nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := r.temporal.SignalWorkflow(ctx, r.workflowID, r.runID, v1.DebugSignal,
		&v1.SignalDelivery{Payload: payload, Sender: r.sender}); err != nil {
		return nil, nil, actOnRunError("delivering a debug command to", r.workflowID, r.runID, err)
	}

	deadline := time.Now().Add(boundedWait(wait))
	for {
		snapshot, err := r.snapshot(ctx, ask.Request)
		if err == nil && snapshot.GetReceipt() != nil {
			receipt := snapshot.GetReceipt()
			snapshot.Receipt = nil

			return receipt, snapshot, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			pending := &v1.DebugReceipt{
				RequestId: ask.Request,
				Status:    v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING,
				Message: "delivered; the run applies commands at its next step boundary, and has not reached one yet. " +
					"Read DebugGet, or retry with the same request id",
			}
			if snapshot != nil {
				pending.Revision = snapshot.GetRevision()
				snapshot.Receipt = nil
			}

			return pending, snapshot, nil
		}
		if err := sleepCtx(ctx, debugPollEvery); err != nil {
			return nil, nil, err
		}
	}
}

func boundedWait(wait time.Duration) time.Duration {
	if wait <= 0 {
		return defaultDebugWait
	}

	return min(wait, maxDebugWait)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DebugAttach attaches a debug session to a durable run, or renews or
// re-pauses one the caller holds.
func (s *FlowstateServer) DebugAttach(ctx context.Context, req *connect.Request[v1.DebugAttachRequest]) (*connect.Response[v1.DebugAttachResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if req.Msg.GetRenew() && req.Msg.GetSessionId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("a renewal names the session it renews"))
	}

	session := req.Msg.GetSessionId()
	if session == "" {
		session = uuid.NewString()
	}
	verb := v1.DebugVerbPause
	operation := "attach"
	if req.Msg.GetRenew() {
		verb, operation = v1.DebugVerbRenew, "renew"
	}
	detail := &v1.AuditDebugDetail{SessionId: session, RequestId: req.Msg.GetRequestId(), Operation: operation}

	run, err := s.authorizeDebug(ctx, "DebugAttach", req.Msg.GetWorkflowId(), req.Msg.GetRunId(), detail)
	if err != nil {
		return nil, err
	}

	receipt, snapshot, err := run.command(ctx, &v1.DebugAsk{
		Verb:    verb,
		Session: session,
		Request: req.Msg.GetRequestId(),
		Lease:   req.Msg.GetLease().AsDuration(),
	}, req.Msg.GetWait().AsDuration())
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&v1.DebugAttachResponse{Receipt: receipt, Snapshot: snapshot, SessionId: session}), nil
}

// DebugGet reads a durable run's debug state, optionally waiting for a
// revision past the one the caller holds.
func (s *FlowstateServer) DebugGet(ctx context.Context, req *connect.Request[v1.DebugGetRequest]) (*connect.Response[v1.DebugGetResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	run, err := s.authorizeDebug(ctx, "DebugGet", req.Msg.GetWorkflowId(), req.Msg.GetRunId(),
		&v1.AuditDebugDetail{Operation: "get", Revision: req.Msg.GetAfterRevision()})
	if err != nil {
		return nil, err
	}

	var deadline time.Time
	if wait := req.Msg.GetWait().AsDuration(); wait > 0 {
		deadline = time.Now().Add(min(wait, maxDebugWait))
	}
	for {
		snapshot, err := run.snapshot(ctx, "")
		if err != nil {
			return nil, run.readError(ctx, err)
		}
		if snapshot.GetRevision() > req.Msg.GetAfterRevision() || deadline.IsZero() || time.Now().After(deadline) || !run.open() {
			return connect.NewResponse(&v1.DebugGetResponse{Snapshot: snapshot}), nil
		}
		if err := sleepCtx(ctx, debugPollEvery); err != nil {
			return nil, err
		}
	}
}

// DebugResume releases a held durable run: continue, step, run until, or
// detach.
func (s *FlowstateServer) DebugResume(ctx context.Context, req *connect.Request[v1.DebugResumeRequest]) (*connect.Response[v1.DebugResumeResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if req.Msg.GetAction() == v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL {
		if _, err := v1.ParseDebugTarget(req.Msg.GetUntil()); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
	}

	operation := "resume/" + strings.ToLower(strings.TrimPrefix(req.Msg.GetAction().String(), "DEBUG_RESUME_ACTION_"))
	run, err := s.authorizeDebug(ctx, "DebugResume", req.Msg.GetWorkflowId(), req.Msg.GetRunId(), &v1.AuditDebugDetail{
		SessionId: req.Msg.GetSessionId(), RequestId: req.Msg.GetRequestId(),
		Revision: req.Msg.GetExpectedRevision(), Operation: operation,
	})
	if err != nil {
		return nil, err
	}

	receipt, snapshot, err := run.command(ctx, &v1.DebugAsk{
		Verb:     v1.DebugVerbResume,
		Session:  req.Msg.GetSessionId(),
		Request:  req.Msg.GetRequestId(),
		Revision: req.Msg.GetExpectedRevision(),
		Action:   req.Msg.GetAction(),
		Until:    req.Msg.GetUntil(),
	}, req.Msg.GetWait().AsDuration())
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&v1.DebugResumeResponse{Receipt: receipt, Snapshot: snapshot}), nil
}

// DebugSetBreakpoints replaces a durable session's breakpoints.
func (s *FlowstateServer) DebugSetBreakpoints(ctx context.Context, req *connect.Request[v1.DebugSetBreakpointsRequest]) (*connect.Response[v1.DebugSetBreakpointsResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	run, err := s.authorizeDebug(ctx, "DebugSetBreakpoints", req.Msg.GetWorkflowId(), req.Msg.GetRunId(), &v1.AuditDebugDetail{
		SessionId: req.Msg.GetSessionId(), RequestId: req.Msg.GetRequestId(), Operation: "breakpoints",
	})
	if err != nil {
		return nil, err
	}

	receipt, snapshot, err := run.command(ctx, &v1.DebugAsk{
		Verb:        v1.DebugVerbBreakpoints,
		Session:     req.Msg.GetSessionId(),
		Request:     req.Msg.GetRequestId(),
		Breakpoints: req.Msg,
	}, req.Msg.GetWait().AsDuration())
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&v1.DebugSetBreakpointsResponse{
		Receipt:     receipt,
		Breakpoints: snapshot.GetBreakpoints(),
		Snapshot:    snapshot,
	}), nil
}

// DebugInspect evaluates a read-only expression against a held durable run.
func (s *FlowstateServer) DebugInspect(ctx context.Context, req *connect.Request[v1.DebugInspectRequest]) (*connect.Response[v1.DebugInspectResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	detail := &v1.AuditDebugDetail{
		SessionId: req.Msg.GetSessionId(), Revision: req.Msg.GetRevision(), Operation: "inspect",
	}
	if expression := req.Msg.GetExpression(); expression != "" {
		detail.ExpressionDigest = v1.ContentDigest([]byte(expression))
	}
	run, err := s.authorizeDebug(ctx, "DebugInspect", req.Msg.GetWorkflowId(), req.Msg.GetRunId(), detail)
	if err != nil {
		return nil, err
	}

	snapshot, err := run.snapshot(ctx, "")
	if err != nil {
		return nil, run.readError(ctx, err)
	}

	// Only the session's holder, only while held, only at the revision asked
	// about: an expression is a question about one stop, asked by the person
	// the run is stopped for.
	holder := snapshot.GetSession().GetAttachedBy()
	caller := run.sender.GetIdentity()
	switch {
	case snapshot.GetSession().GetSessionId() != req.Msg.GetSessionId():
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("the inspection does not name the session attached to this run"))
	case v1.QualifiedSubject(holder.GetIssuer(), holder.GetSubject()) != v1.QualifiedSubject(caller.GetIssuer(), caller.GetSubject()):
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("only the session's holder may inspect the held run"))
	case snapshot.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD:
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("the run is not held, so there is nothing to inspect"))
	case req.Msg.GetRevision() != 0 && req.Msg.GetRevision() != snapshot.GetRevision():
		stale := connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("that snapshot is stale: it was revision %d, and the session is at %d", req.Msg.GetRevision(), snapshot.GetRevision()))
		stale.Meta().Set(v1.DebugConditionHeader, v1.DebugConditionStale)

		return nil, stale
	}

	queryCtx, cancel := context.WithTimeout(ctx, debugQueryTimeout)
	defer cancel()
	encoded, err := run.temporal.QueryWorkflow(queryCtx, run.workflowID, run.runID, v1.DebugInspectQuery, req.Msg)
	if err != nil {
		var queryFailed *serviceerror.QueryFailed
		if errors.As(err, &queryFailed) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New(queryFailed.Message))
		}

		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("the run's worker did not answer the inspection: %w", err))
	}
	var answer v1.DebugInspectResponse
	if err := encoded.Get(&answer); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&answer), nil
}

// authorizeDebugChannel refuses a raw Signal onto the reserved debug channel
// from a caller without `workload.debug`, and records the refusal against the
// Signal it arrived as.
func (s *FlowstateServer) authorizeDebugChannel(ctx context.Context, workflowID string) error {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok || principal.Actions == nil {
		return nil
	}

	scope := v1.AuthorizationActionScope(v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_DEBUG)
	if slices.Contains(principal.Actions, scope) {
		return nil
	}

	refusal := connect.NewError(connect.CodePermissionDenied,
		fmt.Errorf("a signal on the reserved debug channel requires action %q", scope))
	refusal.Meta().Set("WWW-Authenticate", fmt.Sprintf(`Bearer error="insufficient_scope", scope=%q`, scope))

	return s.auditDebugDeny(ctx, "Signal", workflowID, &v1.AuditDebugDetail{Operation: "signal"},
		v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, refusal)
}
