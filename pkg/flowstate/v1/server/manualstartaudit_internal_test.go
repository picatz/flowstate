package server

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
)

// #1889 and #1883: Run and SignalWithStart both write an admission ALLOW
// before reaching v1.CheckManualStart, which asks a second authorization
// question the admission record does not answer. A refusal there used to
// return PermissionDenied with no DENY record, leaving the trail reading as
// an allowed request. These pin the fix: the refused caller sees the same
// error, and the sink now holds an ALLOW followed by exactly one DENY —
// under every rule CheckManualStart can refuse for, not only `denied`.

func manualStartWorkflow(name string, manual *v1.ManualTrigger) *v1.Workflow {
	return &v1.Workflow{
		Name:     name,
		Profile:  v1.CurrentProfile,
		Triggers: &v1.Triggers{Manual: manual},
		Steps:    []*v1.Node{{Id: "record", Kind: &v1.Node_Value{Value: v1.NewLiteral("ok")}}},
	}
}

func manualStartEntityWorkflow(name string, manual *v1.ManualTrigger) *v1.Workflow {
	return &v1.Workflow{
		Name:     name,
		Profile:  v1.CurrentProfile,
		Triggers: &v1.Triggers{Manual: manual},
		Steps: []*v1.Node{{
			Id:   "wait",
			Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "update"}}}},
		}},
	}
}

// manualStartRules is every rule [v1.CheckManualStart] can refuse under, each
// reachable with no request-side setup: no request here carries a reason (so
// require_reason refuses unconditionally) and none authenticates a principal
// (so allowed_principals refuses on the "nobody" branch rather than the
// wrong-principal one) — both are still real refusals through the same
// function, which is what these tests are pinning the audit shape of.
var manualStartRules = []struct {
	name   string
	manual *v1.ManualTrigger
}{
	{name: "denied", manual: &v1.ManualTrigger{Denied: true}},
	{name: "require_reason", manual: &v1.ManualTrigger{RequireReason: true}},
	{name: "allowed_principals", manual: &v1.ManualTrigger{AllowedPrincipals: []string{"https://issuer.example.com#someone-else"}}},
}

// TestRunAuditsAManualStartRefusal covers #1889's Run half, under each rule
// CheckManualStart can refuse for.
func TestRunAuditsAManualStartRefusal(t *testing.T) {
	t.Parallel()

	for _, rule := range manualStartRules {
		t.Run(rule.name, func(t *testing.T) {
			t.Parallel()

			sink := &recordingEmitter{}
			s := mustNew(t, &fakeRunClient{}, WithAudit(recorderFor(t, sink)))

			_, err := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
				Workflow: manualStartWorkflow("manual-"+rule.name, rule.manual),
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
		})
	}
}

// TestSignalWithStartAuditsAManualStartRefusalOnCreate covers #1889's
// SignalWithStart half: the same refusal, reached on the create arm before
// anything is started, so the fake client's unimplemented ExecuteWorkflow is
// never called — if it were, the test would panic through the embedded nil
// client.Client rather than fail cleanly.
func TestSignalWithStartAuditsAManualStartRefusalOnCreate(t *testing.T) {
	t.Parallel()

	for _, rule := range manualStartRules {
		t.Run(rule.name, func(t *testing.T) {
			t.Parallel()

			sink := &recordingEmitter{}
			s := mustNew(t, &fakeRunClient{}, WithAudit(recorderFor(t, sink)))

			_, err := s.SignalWithStart(t.Context(), connect.NewRequest(&v1.SignalWithStartRequest{
				EntityKey: "order-1",
				Workflow:  manualStartEntityWorkflow("manual-entity-"+rule.name, rule.manual),
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
		})
	}
}

// allowOnceThenFailEmitter succeeds on its first Emit — the admission ALLOW —
// and fails on every one after, so a test can isolate whether a *later*
// decision's own required-sink failure is what stops the RPC, distinct from
// the admission's own (already covered by
// TestARequiredRecordThatCannotBeWrittenStopsTheMutation).
type allowOnceThenFailEmitter struct {
	calls int
}

func (e *allowOnceThenFailEmitter) Emit(context.Context, *v1.AuditRecord) error {
	e.calls++
	if e.calls == 1 {
		return nil
	}

	return errors.New("the sink is down")
}

// TestAuthorizeManualStartSurfacesARequiredSinkFailure is the fail-closed
// claim for authorizeManualStart itself, not only for admission: under a
// required recorder, a DENY that cannot be written must stop the request
// with the sink's own failure, exactly as auditDeny's existing contract
// promises every other caller.
func TestAuthorizeManualStartSurfacesARequiredSinkFailure(t *testing.T) {
	t.Parallel()

	sink := &allowOnceThenFailEmitter{}
	required, err := audit.NewRecorder(audit.WithoutStderr(), audit.Required(), audit.WithEmitter(sink))
	require.NoError(t, err)

	s := mustNew(t, &fakeRunClient{}, WithAudit(required))

	_, err = s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: manualStartWorkflow("manual-denied", &v1.ManualTrigger{Denied: true}),
	}))
	require.Error(t, err)
	require.NotContains(t, err.Error(), "manual:",
		"the sink's own failure must surface before the refusal it could not record")
	require.Equal(t, 2, sink.calls, "the admission ALLOW succeeded and the manual-start DENY was attempted and failed")
}
