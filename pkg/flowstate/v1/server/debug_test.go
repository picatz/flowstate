package server_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// The durable debugger's RPCs against a real Temporal server and worker:
// who may attach, read, move and inspect a run, and what a command's receipt
// says about whether the run acted on it.

const debugIssuer = "https://issuer.example.com"

func debuggableWorkflow() *v1.Workflow {
	wf := gatedWorkflow()
	wf.Name = "debuggable"
	wf.Steps = append(wf.Steps,
		&v1.Node{Id: "after", Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral("after")},
		}}},
	)
	wf.Debug = &v1.SignalPolicy{Allow: []*v1.SignalPolicyRule{
		{Subject: v1.QualifiedSubject(debugIssuer, "sre-1@example.com")},
		{Subject: v1.QualifiedSubject(debugIssuer, "sre-2@example.com")},
	}}

	return wf
}

func as(ctx context.Context, subject string, actions ...string) context.Context {
	principal := auth.Principal{Issuer: debugIssuer, Subject: subject}
	if len(actions) > 0 {
		principal.Actions = actions
	}

	return auth.ContextWithPrincipal(ctx, principal)
}

func TestTheDurableDebuggerEndToEnd(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: debuggableWorkflow()}))
	require.NoError(t, err)
	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	sre1 := as(t.Context(), "sre-1@example.com")
	sre2 := as(t.Context(), "sre-2@example.com")

	// Attach while the run waits at its gate: delivered, and not applied until
	// the run reaches a boundary.
	attached, err := fixture.teamA.DebugAttach(sre1, connect.NewRequest(&v1.DebugAttachRequest{
		WorkflowId: workflowID, RequestId: "attach-1", Lease: durationpb.New(5 * time.Minute), Wait: durationpb.New(time.Second),
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING, attached.Msg.GetReceipt().GetStatus(),
		"the run is inside a wait; a hold happens only at a step boundary")
	session := attached.Msg.GetSessionId()
	require.NotEmpty(t, session)

	// Somebody the run's debug policy does not name cannot even read it.
	_, err = fixture.teamA.DebugGet(as(t.Context(), "intruder@example.com"), connect.NewRequest(&v1.DebugGetRequest{WorkflowId: workflowID}))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// Another tenant cannot address the run at all.
	_, err = fixture.teamB.DebugGet(sre1, connect.NewRequest(&v1.DebugGetRequest{WorkflowId: workflowID}))
	require.Error(t, err, "another tenant read a run it cannot address")

	// Release the gate: the run reaches `after` and holds there.
	_, err = fixture.teamA.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID, Name: "deploy-approved",
		Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(false)}},
	}))
	require.NoError(t, err)

	held := waitForDebugState(t, fixture.teamA, sre1, workflowID, v1.DebugRunState_DEBUG_RUN_STATE_HELD)
	assert.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_PAUSE, held.GetReason())
	assert.Equal(t, "after", held.GetOccurrence().GetAddress())
	assert.Equal(t, session, held.GetSession().GetSessionId())
	assert.EqualValues(t, v1.DebugProtocol, held.GetProtocol())

	// The retry of the attach is answered from the run's receipt.
	retried, err := fixture.teamA.DebugAttach(sre1, connect.NewRequest(&v1.DebugAttachRequest{
		WorkflowId: workflowID, SessionId: session, RequestId: "attach-1",
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE, retried.Msg.GetReceipt().GetStatus())

	// Inspection: the holder may; another admitted identity may not; neither
	// may without the inspect action when actions are enforced.
	inspected, err := fixture.teamA.DebugInspect(sre1, connect.NewRequest(&v1.DebugInspectRequest{
		WorkflowId: workflowID, SessionId: session, Revision: held.GetRevision(), Expression: "steps.approval.payload.approved",
	}))
	require.NoError(t, err)
	assert.Equal(t, "bool", inspected.Msg.GetValue().GetType())
	assert.Equal(t, "false", inspected.Msg.GetValue().GetRendered())

	_, err = fixture.teamA.DebugInspect(sre2, connect.NewRequest(&v1.DebugInspectRequest{
		WorkflowId: workflowID, SessionId: session, Revision: held.GetRevision(), Expression: "1",
	}))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err), "only the holder may inspect")

	_, err = fixture.teamA.DebugInspect(as(t.Context(), "sre-1@example.com", "workload.debug"), connect.NewRequest(&v1.DebugInspectRequest{
		WorkflowId: workflowID, SessionId: session, Revision: held.GetRevision(), Expression: "1",
	}))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err), "inspection needs its own action")

	_, err = fixture.teamA.DebugInspect(sre1, connect.NewRequest(&v1.DebugInspectRequest{
		WorkflowId: workflowID, SessionId: session, Revision: held.GetRevision() - 1, Expression: "1",
	}))
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	var stale *connect.Error
	require.ErrorAs(t, err, &stale)
	assert.Equal(t, v1.DebugConditionStale, stale.Meta().Get(v1.DebugConditionHeader))

	// A raw Signal onto the debug channel needs workload.debug as well.
	_, err = fixture.teamA.Signal(as(t.Context(), "sre-1@example.com", "workload.signal"), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID, Name: v1.DebugSignal,
		Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{v1.DebugVerbInput: v1.NewLiteral(v1.DebugVerbResume)}},
	}))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// A command from an admitted identity that does not hold the session is
	// refused by the run itself.
	foreign, err := fixture.teamA.DebugResume(sre2, connect.NewRequest(&v1.DebugResumeRequest{
		WorkflowId: workflowID, SessionId: session, RequestId: "foreign", Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE,
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_CONFLICT, foreign.Msg.GetReceipt().GetStatus())

	stale2, err := fixture.teamA.DebugResume(sre1, connect.NewRequest(&v1.DebugResumeRequest{
		WorkflowId: workflowID, SessionId: session, RequestId: "stale", ExpectedRevision: held.GetRevision() + 7,
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE,
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE, stale2.Msg.GetReceipt().GetStatus())

	// Detach: applied, and the run finishes on its own.
	detached, err := fixture.teamA.DebugResume(sre1, connect.NewRequest(&v1.DebugResumeRequest{
		WorkflowId: workflowID, SessionId: session, RequestId: "bye", ExpectedRevision: held.GetRevision(),
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH,
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, detached.Msg.GetReceipt().GetStatus())

	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		return err == nil && resp.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
	}, 60*time.Second, 200*time.Millisecond, "a detached run must finish on its own")

	final, err := fixture.teamA.DebugGet(sre1, connect.NewRequest(&v1.DebugGetRequest{WorkflowId: workflowID}))
	require.NoError(t, err)
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, final.Msg.GetSnapshot().GetState())

	late, err := fixture.teamA.DebugResume(sre1, connect.NewRequest(&v1.DebugResumeRequest{
		WorkflowId: workflowID, SessionId: session, RequestId: "late", Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE,
	}))
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, late.Msg.GetReceipt().GetStatus(),
		"a command for a closed run is answered ended, and nothing is sent")
}

// TestABreakpointConditionIsAnInspection: a condition is an expression
// evaluated against the run's scope, and whether it held is a bit of what it
// read, so setting one needs the inspect action, by the RPC and by a raw signal
// alike. A plain breakpoint needs only the debug action. SignalWithStart is not
// a door onto the debug channel at all.
func TestABreakpointConditionIsAnInspection(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: debuggableWorkflow()}))
	require.NoError(t, err)
	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	debugOnly := as(t.Context(), "sre-1@example.com", "workload.debug", "workload.signal")
	conditional := []*v1.DebugBreakpoint{{Id: "peek", Step: "after", Condition: `steps.approval.payload.approved == true`}}

	_, err = fixture.teamA.DebugSetBreakpoints(debugOnly, connect.NewRequest(&v1.DebugSetBreakpointsRequest{
		WorkflowId: workflowID, SessionId: "s", RequestId: "conditional", Breakpoints: conditional,
	}))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err), "a condition was set without the inspect action")

	_, err = fixture.teamA.DebugSetBreakpoints(debugOnly, connect.NewRequest(&v1.DebugSetBreakpointsRequest{
		WorkflowId: workflowID, SessionId: "s", RequestId: "plain",
		Breakpoints: []*v1.DebugBreakpoint{{Id: "plain", Step: "after"}},
	}))
	require.NotEqual(t, connect.CodePermissionDenied, connect.CodeOf(err), "a plain breakpoint needs only the debug action")

	payload, err := v1.NewTypedDebugAsk(&v1.DebugAsk{
		Verb: v1.DebugVerbBreakpoints, Session: "s", Request: "raw",
		Breakpoints: &v1.DebugSetBreakpointsRequest{Breakpoints: conditional},
	})
	require.NoError(t, err)
	_, err = fixture.teamA.Signal(debugOnly, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID, Name: v1.DebugSignal, Payload: payload,
	}))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err), "a raw signal carried a condition without the inspect action")

	// The door that skipped the checks above: an existing entity signalled
	// again with a workflow that declares `concurrency:`, which resolves the
	// running entity and signals it directly.
	entity := entityWorkflow(nil)
	entity.Debug = debuggableWorkflow().GetDebug()
	created, err := fixture.teamA.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "debug-door", Workflow: entity, Name: "update", Payload: updatePayload(1, false),
	}))
	require.NoError(t, err)
	require.True(t, created.Msg.GetCreated())
	entity.Concurrency = &v1.Concurrency{Key: v1.NewLiteral("prod-eu")}
	_, err = fixture.teamA.SignalWithStart(as(t.Context(), "sre-1@example.com", "workload.run", "workload.signal"),
		connect.NewRequest(&v1.SignalWithStartRequest{
			EntityKey: "debug-door", Workflow: entity, Name: v1.DebugSignal, Payload: payload,
		}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "SignalWithStart delivered onto the reserved debug channel")
}

func TestARunWithoutADebugPolicyCannotBeAttached(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{Workflow: gatedWorkflow()}))
	require.NoError(t, err)
	waitUntilParkedAtTheGate(t, fixture.temporal, started.Msg.GetWorkflowId())

	_, err = fixture.teamA.DebugAttach(as(t.Context(), "sre-1@example.com"), connect.NewRequest(&v1.DebugAttachRequest{
		WorkflowId: started.Msg.GetWorkflowId(), RequestId: "attach",
	}))
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err), "a run that declares no `debug:` is debuggable by nobody")
}

func waitForDebugState(t *testing.T, client interface {
	DebugGet(context.Context, *connect.Request[v1.DebugGetRequest]) (*connect.Response[v1.DebugGetResponse], error)
}, ctx context.Context, workflowID string, want v1.DebugRunState,
) *v1.DebugSnapshot {
	t.Helper()

	var got *v1.DebugSnapshot
	require.Eventually(t, func() bool {
		resp, err := client.DebugGet(ctx, connect.NewRequest(&v1.DebugGetRequest{
			WorkflowId: workflowID, Wait: durationpb.New(2 * time.Second),
		}))
		if err != nil {
			return false
		}
		got = resp.Msg.GetSnapshot()

		return got.GetState() == want
	}, 60*time.Second, 100*time.Millisecond, "the run never reached %s", want)

	return got
}
