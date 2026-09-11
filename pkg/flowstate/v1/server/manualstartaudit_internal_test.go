package server

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// #1889 and #1883: Run and SignalWithStart both write an admission ALLOW
// before reaching v1.CheckManualStart, which asks a second authorization
// question the admission record does not answer. A refusal there used to
// return PermissionDenied with no DENY record, leaving the trail reading as
// an allowed request. These pin the fix: the refused caller sees the same
// error, and the sink now holds an ALLOW followed by exactly one DENY.

func manualDeniedWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:     "manual-denied",
		Profile:  v1.CurrentProfile,
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{Denied: true}},
		Steps:    []*v1.Node{{Id: "record", Kind: &v1.Node_Value{Value: v1.NewLiteral("ok")}}},
	}
}

func manualDeniedEntityWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:     "manual-denied-entity",
		Profile:  v1.CurrentProfile,
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{Denied: true}},
		Steps: []*v1.Node{{
			Id:   "wait",
			Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "update"}}}},
		}},
	}
}

// TestRunAuditsAManualStartRefusal covers #1889's Run half.
func TestRunAuditsAManualStartRefusal(t *testing.T) {
	t.Parallel()

	sink := &recordingEmitter{}
	s := mustNew(t, &fakeRunClient{}, WithAudit(recorderFor(t, sink)))

	_, err := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: manualDeniedWorkflow(),
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	require.Len(t, sink.records, 2, "one admission ALLOW, one manual-start DENY")
	allow, deny := sink.records[0], sink.records[1]

	require.Equal(t, "Run", allow.GetRpc())
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, allow.GetDecision())
	require.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_NAMESPACE, allow.GetResourceKind())

	require.Equal(t, "Run", deny.GetRpc())
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, deny.GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, deny.GetDenyCode())
	require.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, deny.GetResourceKind())
}

// TestSignalWithStartAuditsAManualStartRefusalOnCreate covers #1889's
// SignalWithStart half: the same refusal, reached on the create arm before
// anything is started, so the fake client's unimplemented ExecuteWorkflow is
// never called — if it were, the test would panic through the embedded nil
// client.Client rather than fail cleanly.
func TestSignalWithStartAuditsAManualStartRefusalOnCreate(t *testing.T) {
	t.Parallel()

	sink := &recordingEmitter{}
	s := mustNew(t, &fakeRunClient{}, WithAudit(recorderFor(t, sink)))

	_, err := s.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: "order-1",
		Workflow:  manualDeniedEntityWorkflow(),
		Name:      "update",
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	require.Len(t, sink.records, 2, "one admission ALLOW, one manual-start DENY")
	allow, deny := sink.records[0], sink.records[1]

	require.Equal(t, "SignalWithStart", allow.GetRpc())
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, allow.GetDecision())

	require.Equal(t, "SignalWithStart", deny.GetRpc())
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, deny.GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, deny.GetDenyCode())
	require.Equal(t, allow.GetResourceKey(), deny.GetResourceKey(), "both decisions are about the same composed workflow id")
}
