package server_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// DebugHistory against a real Temporal server and worker: who may read a run's
// past, what the answer carries, and what it refuses (#2248).

// finishedDebuggableRun starts the debuggable workflow, releases its gate and
// waits for it to close, and returns its ids.
func finishedDebuggableRun(t *testing.T, fixture *tenantFixture) (workflowID, runID string) {
	t.Helper()

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: debuggableWorkflow()}))
	require.NoError(t, err)
	workflowID = started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	_, err = fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID, Name: "deploy-approved",
		Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(false)}},
	}))
	require.NoError(t, err)

	run := fixture.temporal.GetWorkflow(t.Context(), workflowID, "")
	require.NoError(t, run.Get(t.Context(), nil))

	return workflowID, run.GetRunID()
}

func TestDebugHistoryReadsAClosedRunAtEachOfItsPoints(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	workflowID, runID := finishedDebuggableRun(t, fixture)
	sre := as(t.Context(), "sre-1@example.com")

	last, err := fixture.teamA.DebugHistory(sre, connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID}))
	require.NoError(t, err)
	assert.Equal(t, v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED, last.Msg.GetFidelity())
	require.GreaterOrEqual(t, len(last.Msg.GetBoundaries()), 3, "a run with a gate has several workflow tasks")
	assert.Equal(t, last.Msg.GetBoundaries()[len(last.Msg.GetBoundaries())-1], last.Msg.GetEventId(), "zero names the last point")
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, last.Msg.GetOutcome(), "the closing point says how the execution ended")
	require.NotNil(t, last.Msg.GetSnapshot())
	assert.Equal(t, v1.WorkflowIRDigest(debuggableWorkflow()), last.Msg.GetSnapshot().GetIrDigest(),
		"the program the run executes is the one the snapshot names")

	// Every point the answer lists can be read, and answers for itself.
	for _, point := range last.Msg.GetBoundaries() {
		got, err := fixture.teamA.DebugHistory(sre, connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID, EventId: point}))
		require.NoError(t, err, "event %d", point)
		assert.Equal(t, point, got.Msg.GetEventId())
	}

	// An earlier point is the run as it was: before the gate was released, its
	// progress had completed fewer steps than at the end.
	first, err := fixture.teamA.DebugHistory(sre, connect.NewRequest(&v1.DebugHistoryRequest{
		WorkflowId: workflowID, RunId: runID, EventId: last.Msg.GetBoundaries()[0],
	}))
	require.NoError(t, err)
	assert.Less(t, first.Msg.GetProgress().GetCompletedSteps(), last.Msg.GetProgress().GetCompletedSteps())
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_UNSPECIFIED, first.Msg.GetOutcome(), "an earlier point is not an ending")
}

// historyDetails picks the debug-history records out of what a server emitted.
func historyDetails(sink *auditSink) []*v1.AuditDebugDetail {
	var out []*v1.AuditDebugDetail
	for _, record := range sink.records {
		if op := record.GetDebug().GetOperation(); op == "history" || op == "history/resolved" {
			out = append(out, record.GetDebug())
		}
	}

	return out
}

// TestDebugHistoryAuditsTheRunAndThePointRead: a read of the last point (event
// 0) is recorded with the exact run and with the event it resolved to, because
// event ids restart in every run of a chain and 0 means a different point each
// time it is asked.
func TestDebugHistoryAuditsTheRunAndThePointRead(t *testing.T) {
	t.Parallel()

	sink := &auditSink{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink))
	require.NoError(t, err)
	fixture := newTenantFixture(t, server.WithAudit(recorder))
	workflowID, runID := finishedDebuggableRun(t, fixture)

	got, err := fixture.teamA.DebugHistory(as(t.Context(), "sre-1@example.com"),
		connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID}))
	require.NoError(t, err)

	details := historyDetails(sink)
	require.Len(t, details, 2, "one record for the ask, one for the point read")
	assert.Equal(t, "history", details[0].GetOperation())
	assert.Equal(t, uint64(0), details[0].GetRevision(), "the ask was for the last point")
	assert.Equal(t, "history/resolved", details[1].GetOperation())
	assert.Equal(t, uint64(got.Msg.GetEventId()), details[1].GetRevision(), "the point actually read")
	for _, detail := range details {
		assert.Equal(t, runID, detail.GetRunId(), "the exact execution, not just the workflow")
	}
}

// TestAnInspectionIsAuditedUnderTheInspectActionAndItsDigestSeparatesBatches:
// asking for inspections is a decision under workload.debug_inspect, allowed or
// denied, and two different batches never share a digest.
func TestAnInspectionIsAuditedUnderTheInspectActionAndItsDigestSeparatesBatches(t *testing.T) {
	t.Parallel()

	sink := &auditSink{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink))
	require.NoError(t, err)
	fixture := newTenantFixture(t, server.WithAudit(recorder))
	workflowID, runID := finishedDebuggableRun(t, fixture)
	ask := func(ctx context.Context, expressions ...string) error {
		var asked []*v1.DebugHistoryInspection
		for _, expression := range expressions {
			asked = append(asked, &v1.DebugHistoryInspection{Expression: expression})
		}
		_, err := fixture.teamA.DebugHistory(ctx, connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID, Inspections: asked}))

		return err
	}
	inspectRecords := func(decision v1.AuditDecision) []*v1.AuditRecord {
		var out []*v1.AuditRecord
		for _, record := range sink.records {
			if record.GetAction() == v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_DEBUG_INSPECT && record.GetDecision() == decision {
				out = append(out, record)
			}
		}

		return out
	}

	require.Error(t, ask(as(t.Context(), "sre-1@example.com", "workload.debug"), "1"))
	assert.Len(t, inspectRecords(v1.AuditDecision_AUDIT_DECISION_DENY), 1, "the refusal is recorded for the action the caller lacked")

	require.NoError(t, ask(as(t.Context(), "sre-1@example.com", "workload.debug", "workload.debug_inspect"), "a", "b"))
	require.NoError(t, ask(as(t.Context(), "sre-1@example.com", "workload.debug", "workload.debug_inspect"), "a\x00b"))
	allowed := inspectRecords(v1.AuditDecision_AUDIT_DECISION_ALLOW)
	require.Len(t, allowed, 2, "each permitted ask is recorded for the inspect action")
	assert.NotEqual(t, allowed[0].GetDebug().GetExpressionDigest(), allowed[1].GetDebug().GetExpressionDigest(),
		"[a, b] and [a NUL b] are different batches")
}

// TestDebugHistoryWithholdsADeclaredSensitiveInputAtEveryPoint: a run whose
// input is declared sensitive and flows into a step's output is read at every
// boundary, and no answer, snapshot or progress, holds the value. The same
// engine handler that withholds it from the live session answers over the
// scope the replay rebuilds, and this is the proof of that rather than the
// claim.
func TestDebugHistoryWithholdsADeclaredSensitiveInputAtEveryPoint(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-sensitive-token-value"
	workflow := debuggableWorkflow()
	workflow.DeclaredInputs = []*v1.InputDeclaration{{Name: "token", Type: v1.InputDeclaration_TYPE_STRING, Required: true, Sensitive: true}}
	// The step that carries the value runs while a session is attached, so its
	// output is reported as an observation, the place a value could leak from.
	carry := &v1.Node{Id: "carry", Kind: &v1.Node_Value{Value: v1.NewExpr("inputs.token")}}
	workflow.Steps = slices.Insert(workflow.Steps, 2, carry)

	fixture := newTenantFixture(t)
	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: workflow, Inputs: map[string]*v1.Value{"token": v1.NewLiteral(secret)},
	}))
	require.NoError(t, err)
	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	sre := as(t.Context(), "sre-1@example.com")
	attached, err := fixture.teamA.DebugAttach(sre, connect.NewRequest(&v1.DebugAttachRequest{
		WorkflowId: workflowID, RequestId: "attach-sensitive", Lease: durationpb.New(5 * time.Minute), Wait: durationpb.New(time.Second),
	}))
	require.NoError(t, err)
	session := attached.Msg.GetSessionId()
	_, err = fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID, Name: "deploy-approved",
		Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
	}))
	require.NoError(t, err)
	held := waitForDebugState(t, fixture.teamA, sre, workflowID, v1.DebugRunState_DEBUG_RUN_STATE_HELD)
	require.Equal(t, "carry", held.GetOccurrence().GetAddress())
	_, err = fixture.teamA.DebugResume(sre, connect.NewRequest(&v1.DebugResumeRequest{
		WorkflowId: workflowID, SessionId: session, RequestId: "over-carry", ExpectedRevision: held.GetRevision(),
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER,
	}))
	require.NoError(t, err)
	held = waitForDebugState(t, fixture.teamA, sre, workflowID, v1.DebugRunState_DEBUG_RUN_STATE_HELD)
	require.Equal(t, "deploy", held.GetOccurrence().GetAddress())
	_, err = fixture.teamA.DebugResume(sre, connect.NewRequest(&v1.DebugResumeRequest{
		WorkflowId: workflowID, SessionId: session, RequestId: "bye-sensitive", ExpectedRevision: held.GetRevision(),
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH,
	}))
	require.NoError(t, err)
	run := fixture.temporal.GetWorkflow(t.Context(), workflowID, "")
	require.NoError(t, run.Get(t.Context(), nil))

	last, err := fixture.teamA.DebugHistory(sre, connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: run.GetRunID()}))
	require.NoError(t, err)

	// Asked at every point: the secret itself, the roots, and a sum.
	asked := []*v1.DebugHistoryInspection{{Expression: "inputs.token"}, {}, {Expression: "1 + 2"}}
	var sawSession, sawCarry, sawSum bool
	for _, point := range last.Msg.GetBoundaries() {
		got, err := fixture.teamA.DebugHistory(sre, connect.NewRequest(&v1.DebugHistoryRequest{
			WorkflowId: workflowID, RunId: run.GetRunID(), EventId: point, Inspections: asked,
		}))
		require.NoError(t, err, "event %d", point)
		require.Len(t, got.Msg.GetInspected(), len(asked), "event %d", point)
		if sum := got.Msg.GetInspected()[2].GetResult(); sum.GetError() == "" {
			sawSum = true
			assert.Equal(t, "3", sum.GetValue().GetRendered(), "event %d", point)
			assert.Equal(t, v1.DebugFidelity_DEBUG_FIDELITY_HYPOTHETICAL, got.Msg.GetInspected()[2].GetFidelity())
		}
		sawSession = sawSession || got.Msg.GetSnapshot() != nil
		for _, observation := range got.Msg.GetSnapshot().GetObservations() {
			sawCarry = sawCarry || strings.Contains(observation.GetStepId(), "carry")
		}
		rendered, err := protojson.Marshal(got.Msg)
		require.NoError(t, err)
		assert.NotContains(t, string(rendered), secret, "event %d carries a declared-sensitive input", point)
	}
	assert.True(t, sawSession, "no point held a debug session, so the scan covered progress only")
	assert.True(t, sawSum, "no point answered an inspection, so the scan could not have caught a leak through one")
	assert.True(t, sawCarry, "no point reported the step that carried the value, so the scan could not have caught a leak")
}

func TestAnOpenRunsPastIsInspectedOnlyByTheHolderOfItsSession(t *testing.T) {
	t.Parallel()

	workflow := debuggableWorkflow()
	sink := &auditSink{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink))
	require.NoError(t, err)
	fixture := newTenantFixture(t, server.WithAudit(recorder))
	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: workflow}))
	require.NoError(t, err)
	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	holder, other := as(t.Context(), "sre-1@example.com"), as(t.Context(), "sre-2@example.com")
	_, err = fixture.teamA.DebugAttach(holder, connect.NewRequest(&v1.DebugAttachRequest{
		WorkflowId: workflowID, RequestId: "attach-open", Lease: durationpb.New(5 * time.Minute), Wait: durationpb.New(time.Second),
	}))
	require.NoError(t, err)
	_, err = fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID, Name: "deploy-approved",
		Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
	}))
	require.NoError(t, err)
	held := waitForDebugState(t, fixture.teamA, holder, workflowID, v1.DebugRunState_DEBUG_RUN_STATE_HELD)
	// One step on, so a workflow task ends with the session held: the point the
	// past is read at below.
	_, err = fixture.teamA.DebugResume(holder, connect.NewRequest(&v1.DebugResumeRequest{
		WorkflowId: workflowID, SessionId: held.GetSession().GetSessionId(), RequestId: "step-open", ExpectedRevision: held.GetRevision(),
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OVER,
	}))
	require.NoError(t, err)
	waitForDebugState(t, fixture.teamA, holder, workflowID, v1.DebugRunState_DEBUG_RUN_STATE_HELD)
	runID := fixture.temporal.GetWorkflow(t.Context(), workflowID, "").GetRunID()

	asked := []*v1.DebugHistoryInspection{{Expression: "1 + 2"}}
	var point int64
	read := func(ctx context.Context, inspections []*v1.DebugHistoryInspection) (*v1.DebugHistoryResponse, error) {
		got, err := fixture.teamA.DebugHistory(ctx, connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID, EventId: point, Inspections: inspections}))
		if err != nil {
			return nil, err
		}

		return got.Msg, nil
	}

	// The latest point at which the run was held under the session.
	last, err := read(holder, nil)
	require.NoError(t, err)
	for _, boundary := range slices.Backward(last.GetBoundaries()) {
		point = boundary
		at, err := read(holder, nil)
		require.NoError(t, err)
		if at.GetSnapshot().GetSession().GetAttachedBy() != nil && at.GetSnapshot().GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD {
			break
		}
		point = 0
	}
	require.NotZero(t, point, "no point held the session")

	_, err = read(other, asked)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err), "another admitted caller read the scope of a session someone else holds")
	var denied []*v1.AuditDebugDetail
	for _, record := range sink.records {
		if record.GetDecision() == v1.AuditDecision_AUDIT_DECISION_DENY && record.GetDebug().GetOperation() == "history/inspect" {
			denied = append(denied, record.GetDebug())
		}
	}
	require.Len(t, denied, 1, "the refusal is audited as a denial, not only as the allows before it")
	assert.Equal(t, runID, denied[0].GetRunId())
	_, err = read(other, nil)
	assert.NoError(t, err, "the point itself is still readable")
	got, err := read(holder, asked)
	require.NoError(t, err)
	assert.Equal(t, "3", got.GetInspected()[0].GetResult().GetValue().GetRendered())
}

func TestDebugHistoryRefusesWhatItCannotRead(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	workflowID, runID := finishedDebuggableRun(t, fixture)
	sre := as(t.Context(), "sre-1@example.com")
	read := func(ctx context.Context, request *v1.DebugHistoryRequest) error {
		_, err := fixture.teamA.DebugHistory(ctx, connect.NewRequest(request))

		return err
	}

	t.Run("a caller the run's debug policy does not name", func(t *testing.T) {
		err := read(as(t.Context(), "intruder@example.com"), &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID})
		assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	})
	t.Run("an inspection without the inspect action", func(t *testing.T) {
		asked := []*v1.DebugHistoryInspection{{Expression: "1 + 1"}}
		err := read(as(t.Context(), "sre-1@example.com", "workload.debug"), &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID, Inspections: asked})
		assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
		// The same caller may read the point itself.
		require.NoError(t, read(as(t.Context(), "sre-1@example.com", "workload.debug"), &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID}))
	})
	t.Run("too many inspections", func(t *testing.T) {
		err := read(sre, &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID, Inspections: make([]*v1.DebugHistoryInspection, 17)})
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
	t.Run("a caller without the debug action", func(t *testing.T) {
		err := read(as(t.Context(), "sre-1@example.com", "workload.signal"), &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID})
		assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	})
	t.Run("another tenant", func(t *testing.T) {
		_, err := fixture.teamB.DebugHistory(sre, connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID}))
		require.Error(t, err)
		assert.NotEqual(t, connect.CodeUnknown, connect.CodeOf(err))
	})
	t.Run("a run id that names nothing", func(t *testing.T) {
		err := read(sre, &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: uuid.NewString()})
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})
	t.Run("a run id left out", func(t *testing.T) {
		err := read(sre, &v1.DebugHistoryRequest{WorkflowId: workflowID})
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
	t.Run("a point that is not a boundary", func(t *testing.T) {
		boundaries, err := fixture.teamA.DebugHistory(sre, connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID}))
		require.NoError(t, err)
		// The event after a boundary is inside its workflow task.
		inside := boundaries.Msg.GetBoundaries()[1] + 1
		require.NotContains(t, boundaries.Msg.GetBoundaries(), inside)
		err = read(sre, &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID, EventId: inside})
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		err = read(sre, &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID, EventId: 1 << 40})
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
	t.Run("a caller that has gone away", func(t *testing.T) {
		ctx, cancel := context.WithCancel(sre)
		cancel()
		err := read(ctx, &v1.DebugHistoryRequest{WorkflowId: workflowID, RunId: runID})
		require.Error(t, err)
	})
	t.Run("a run that declares no debug policy", func(t *testing.T) {
		started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: gatedWorkflow()}))
		require.NoError(t, err)
		waitUntilParkedAtTheGate(t, fixture.temporal, started.Msg.GetWorkflowId())
		run := fixture.temporal.GetWorkflow(t.Context(), started.Msg.GetWorkflowId(), "")
		err = read(sre, &v1.DebugHistoryRequest{WorkflowId: started.Msg.GetWorkflowId(), RunId: run.GetRunID()})
		assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	})
}
