package server

import (
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

func gateRequest(name string) *connect.Request[v1.GetGateRequest] {
	return connect.NewRequest(&v1.GetGateRequest{WorkflowId: "orders-1", SignalName: name})
}

// TestGetGateReadsWithTheSignalActionAndNothingElse: a caller holding only
// `workload.signal` reads the gate, one holding only `workload.read` is refused
// before the run is addressed, and one holding neither is refused the same way.
func TestGetGateReadsWithTheSignalActionAndNothingElse(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		actions []string
		allowed bool
	}{
		"signal only": {actions: []string{"workload.signal"}, allowed: true},
		"read only":   {actions: []string{"workload.read"}},
		"neither":     {actions: []string{"workload.cancel"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := runningFake(t)
			fake.progress = &v1.RunProgress{PendingWaits: []*v1.PendingWait{{StepId: "approve", SignalName: "go"}}}
			sink := &recordingEmitter{}
			s := mustNew(t, fake, WithAudit(recorderFor(t, sink)))

			resp, err := s.GetGate(revealer(t.Context(), tc.actions...), gateRequest("go"))
			if !tc.allowed {
				require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
				require.Zero(t, fake.describes, "the run was addressed for a caller who may not read gates")
				require.Len(t, sink.records, 1)
				require.Equal(t, "GetGate", sink.records[0].GetRpc())
				require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, sink.records[0].GetDecision())

				return
			}

			require.NoError(t, err)
			require.Equal(t, "approve", resp.Msg.GetStepId())
			require.Equal(t, "r-1", resp.Msg.GetRunId())
			require.Equal(t, "go", resp.Msg.GetSignalName())
			require.True(t, resp.Msg.GetMayAnswer(), "a run declaring no policy for the name admits the caller, as Signal does")
			require.Zero(t, fake.signals, "reading a gate delivers nothing")
			require.Len(t, sink.records, 1, "one decision, one record")
			require.Equal(t, "GetGate", sink.records[0].GetRpc())
			require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, sink.records[0].GetDecision())
		})
	}
}

// TestGetGateWithholdsAPromptItCannotProveSafe: with no readable specification
// the prompt is withheld, as Get withholds it, and the gate itself is still
// reported.
func TestGetGateWithholdsAPromptItCannotProveSafe(t *testing.T) {
	t.Parallel()

	fake := runningFake(t)
	fake.progress = &v1.RunProgress{PendingWaits: []*v1.PendingWait{
		{StepId: "approve", SignalName: "go", Prompt: "approve synthetic-token-4c21?", PromptTruncated: true},
	}}
	s := mustNew(t, fake)

	resp, err := s.GetGate(revealer(t.Context(), "workload.signal"), gateRequest("go"))
	require.NoError(t, err)
	require.Equal(t, v1.PromptWithheldSensitive, resp.Msg.GetPrompt())
	require.False(t, resp.Msg.GetPromptTruncated())
}

// TestGetGateAnswersNotFoundOnceAndTellsTruncationApart: a run that is not
// running, and a run with no open wait by that name, are the same NotFound; a
// list cut at the bound that does not include the name is not, and neither is a
// run whose position could not be read.
func TestGetGateAnswersNotFoundOnceAndTellsTruncationApart(t *testing.T) {
	t.Parallel()

	var many []*v1.PendingWait
	for i := range v1.MaxPendingWaits {
		many = append(many, &v1.PendingWait{StepId: fmt.Sprintf("s%d", i), SignalName: fmt.Sprintf("n%d", i)})
	}

	for name, tc := range map[string]struct {
		mutate func(*fakeRunClient)
		name   string
		want   connect.Code
	}{
		"no open wait by that name": {
			mutate: func(f *fakeRunClient) {
				f.progress = &v1.RunProgress{PendingWaits: []*v1.PendingWait{{StepId: "x", SignalName: "other"}}}
			},
			want: connect.CodeNotFound,
		},
		"not running": {
			mutate: func(f *fakeRunClient) {
				f.describe.WorkflowExecutionInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			},
			want: connect.CodeNotFound,
		},
		"cut at the bound without the name": {
			mutate: func(f *fakeRunClient) {
				f.progress = &v1.RunProgress{PendingWaits: many, PendingWaitsTruncated: true}
			},
			want: connect.CodeFailedPrecondition,
		},
		"position unreadable": {
			want: connect.CodeUnavailable,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := runningFake(t)
			if tc.mutate != nil {
				tc.mutate(fake)
			}
			s := mustNew(t, fake)

			_, err := s.GetGate(revealer(t.Context(), "workload.signal"), gateRequest("go"))
			require.Equal(t, tc.want, connect.CodeOf(err))
		})
	}

	// The name inside a list that was cut is a gate that is open.
	fake := runningFake(t)
	fake.progress = &v1.RunProgress{PendingWaits: many, PendingWaitsTruncated: true}
	s := mustNew(t, fake)
	resp, err := s.GetGate(revealer(t.Context(), "workload.signal"), gateRequest("n3"))
	require.NoError(t, err)
	require.Equal(t, "s3", resp.Msg.GetStepId())
}

// TestGetGateValidatesLikeSignal: the same constraints on the same fields.
func TestGetGateValidatesLikeSignal(t *testing.T) {
	t.Parallel()

	s := mustNew(t, runningFake(t))
	for _, name := range []string{"", "-leading", "has space", "has.dot"} {
		_, err := s.GetGate(revealer(t.Context(), "workload.signal"), gateRequest(name))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), name)
	}
}

// TestGetGateShowsARefusedCallerNothingTheyWereNotGranted: a caller holding
// `workload.signal` that the gate's `signals:` rule refuses is told the gate
// exists and that they may not answer it, and is shown neither its prompt nor
// who started the run, because the author wrote those for the people the policy
// admits and this verb must not widen what `Get` would have refused them. A
// caller who also holds `workload.read` could read both anyway, and the one the
// policy admits is the audience.
func TestGetGateShowsARefusedCallerNothingTheyWereNotGranted(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		subject string
		actions []string
		may     bool
		shown   bool
	}{
		"admitted by the policy":      {subject: "approver", actions: []string{"workload.signal"}, may: true, shown: true},
		"refused, signal only":        {subject: "bystander", actions: []string{"workload.signal"}},
		"refused, with workload.read": {subject: "bystander", actions: []string{"workload.signal", "workload.read"}, shown: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := runningFake(t)
			fake.describe.WorkflowExecutionInfo.Memo.Fields[signalPolicyMemoKey] =
				memoWithSignalPolicy(t, map[string]*v1.SignalPolicy{
					"go": {Allow: `sender.identity.principal == "https://issuer.example#approver"`},
				}).GetWorkflowExecutionInfo().GetMemo().GetFields()[signalPolicyMemoKey]
			starter, err := converter.GetDefaultDataConverter().ToPayload("https://issuer.example#requester")
			require.NoError(t, err)
			fake.describe.WorkflowExecutionInfo.Memo.Fields[starterMemoKey] = starter
			fake.progress = &v1.RunProgress{PendingWaits: []*v1.PendingWait{
				{StepId: "approve", SignalName: "go", Prompt: "approve the production deploy?", Approvals: 1, ApprovalsNeeded: 2},
			}}
			s := mustNew(t, fake)

			ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
				Issuer: "https://issuer.example", Subject: tc.subject, Actions: tc.actions,
			})
			resp, err := s.GetGate(ctx, gateRequest("go"))
			require.NoError(t, err)

			require.Equal(t, "approve", resp.Msg.GetStepId(), "the gate is reported to a caller who holds workload.signal")
			require.Equal(t, tc.may, resp.Msg.GetMayAnswer())
			require.Equal(t, tc.shown, resp.Msg.GetPrompt() != "", "prompt shown: %q", resp.Msg.GetPrompt())
			require.Equal(t, tc.shown, resp.Msg.GetStarter() != "", "starter shown: %q", resp.Msg.GetStarter())
			require.Equal(t, tc.shown, resp.Msg.GetApprovalsNeeded() != 0, "tally shown")
			if tc.shown {
				require.EqualValues(t, 1, resp.Msg.GetApprovals())
				require.EqualValues(t, 2, resp.Msg.GetApprovalsNeeded())
			}
		})
	}
}

// TestGetGateAdvisesNoWhereSignalWillRefuseTheDebugChannel: a run begun before
// the debug channel's name was reserved may wait on it, and Signal asks for
// `workload.debug` there as well, so the advice must not say a signal-only
// caller may answer a delivery that is certain to be refused. A caller holding
// the action, and a legacy caller with no action list, may.
func TestGetGateAdvisesNoWhereSignalWillRefuseTheDebugChannel(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		actions []string
		may     bool
	}{
		"signal only":      {actions: []string{"workload.signal"}},
		"signal and debug": {actions: []string{"workload.signal", "workload.debug"}, may: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := runningFake(t)
			fake.progress = &v1.RunProgress{PendingWaits: []*v1.PendingWait{
				{StepId: "approve", SignalName: v1.DebugSignal},
			}}
			s := mustNew(t, fake)

			req := connect.NewRequest(&v1.GetGateRequest{WorkflowId: "orders-1", SignalName: v1.DebugSignal})
			resp, err := s.GetGate(revealer(t.Context(), tc.actions...), req)
			require.NoError(t, err)
			require.Equal(t, tc.may, resp.Msg.GetMayAnswer())
		})
	}
}
