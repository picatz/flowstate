package server

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

func runningFake(t *testing.T) *fakeRunClient {
	t.Helper()

	return &fakeRunClient{describe: &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: "orders-1", RunId: "r-1"},
			Type:      &commonpb.WorkflowType{Name: flowstateRunWorkflowType},
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			Memo:      mineMemo(t),
		},
	}}
}

func revealer(ctx context.Context, actions ...string) context.Context {
	return auth.ContextWithPrincipal(ctx, auth.Principal{
		Issuer: "https://issuer.example", Subject: "reader", Actions: actions,
	})
}

// TestAnUnreadableSpecificationWithholds: the fake has no history, so the
// server cannot learn what the run declares. It must not answer "nothing
// declared", which a client trusts and renders in the clear, and the running
// run's pending failure, which can quote anything a task was given, is
// withheld whole.
func TestAnUnreadableSpecificationWithholds(t *testing.T) {
	t.Parallel()

	const quoted = `Get "https://api.example/synthetic-token-5d0c": connection refused`
	fake := runningFake(t)
	fake.describe.PendingActivities = []*workflowpb.PendingActivityInfo{{
		State:       enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:     3,
		LastFailure: &failurepb.Failure{Message: quoted},
	}}
	s := mustNew(t, fake)

	resp, err := s.Get(revealer(t.Context(), "workload.read"), connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1"}))
	require.NoError(t, err)
	require.Equal(t, v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD, resp.Msg.GetSensitiveDisclosure())
	require.Len(t, resp.Msg.GetPendingActivities(), 1)
	require.EqualValues(t, 3, resp.Msg.GetPendingActivities()[0].GetAttempt(), "the metadata beside the failure is not the workload's")
	require.NotContains(t, resp.Msg.GetPendingActivities()[0].GetLastFailure(), "synthetic-token-5d0c")
}

// TestEveryRevealRequestIsAuditedUnderItsOwnAction: the read and the reveal
// are two decisions, and each leaves one record; the reveal's names the field
// that widened the call, allowed or not.
func TestEveryRevealRequestIsAuditedUnderItsOwnAction(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		actions []string
		want    v1.AuditDecision
		reveals bool
	}{
		"held":     {actions: []string{"workload.read", "workload.reveal_sensitive"}, want: v1.AuditDecision_AUDIT_DECISION_ALLOW, reveals: true},
		"not held": {actions: []string{"workload.read"}, want: v1.AuditDecision_AUDIT_DECISION_DENY},
		// An entry with no action list holds every RPC action and not this one.
		"no action list": {want: v1.AuditDecision_AUDIT_DECISION_DENY},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sink := &recordingEmitter{}
			s := mustNew(t, runningFake(t), WithAudit(recorderFor(t, sink)))

			resp, err := s.Get(revealer(t.Context(), tc.actions...),
				connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1", RevealSensitive: true}))
			require.NoError(t, err, "a reveal the caller may not have is withheld, not refused")

			wantDisclosure := v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD
			if tc.reveals {
				wantDisclosure = v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_REVEALED
			}
			require.Equal(t, wantDisclosure, resp.Msg.GetSensitiveDisclosure())

			require.Len(t, sink.records, 2, "the read and the reveal are two decisions")
			reveal := revealRecord(t, sink)
			require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_REVEAL_SENSITIVE, reveal.GetAction())
			require.Equal(t, "Get", reveal.GetRpc())
			require.Equal(t, "orders-1", reveal.GetResourceKey())
			require.Equal(t, tc.want, reveal.GetDecision())
		})
	}
}

func revealRecord(t *testing.T, sink *recordingEmitter) *v1.AuditRecord {
	t.Helper()
	for _, r := range sink.records {
		if r.GetAction() == v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_REVEAL_SENSITIVE {
			return r
		}
	}
	t.Fatal("no reveal decision was recorded")
	return nil
}

// TestARevealOfARunThatCannotBeReadIsStillAudited: the reveal decision
// depends only on the caller and the field, so it is recorded before the run
// is read, and a read that fails (a missing run, another tenant's) does not
// leave the attempted elevated read out of the trail.
func TestARevealOfARunThatCannotBeReadIsStillAudited(t *testing.T) {
	t.Parallel()

	fake := &fakeRunClient{describeErr: errors.New("no such workflow execution")}
	sink := &recordingEmitter{}
	s := mustNew(t, fake, WithAudit(recorderFor(t, sink)))

	_, err := s.Get(revealer(t.Context(), "workload.read"),
		connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1", RevealSensitive: true}))
	require.Error(t, err)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, revealRecord(t, sink).GetDecision())

	// The fake serves no history, so the timeline read answers from nothing;
	// the reveal is recorded first all the same.
	sink.records = nil
	_, _ = s.GetTimeline(revealer(t.Context(), "workload.read", "workload.reveal_sensitive"),
		connect.NewRequest(&v1.GetTimelineRequest{WorkflowId: "orders-1", RevealSensitive: true}))
	require.NotEmpty(t, sink.records)
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_REVEAL_SENSITIVE, sink.records[0].GetAction(),
		"the reveal was not decided before the run was read")
	reveal := sink.records[0]
	require.Equal(t, "GetTimeline", reveal.GetRpc())
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, reveal.GetDecision())
}

// revealUnrecordable fails only the reveal's record, so the read before it
// is allowed and the reveal seam is the one under test.
type revealUnrecordable struct{}

func (revealUnrecordable) Emit(_ context.Context, record *v1.AuditRecord) error {
	if record.GetAction() == v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_REVEAL_SENSITIVE {
		return errors.New("the sink is down")
	}
	return nil
}

// TestARevealThatCannotBeRecordedReleasesNothing: a required recorder that
// cannot record the allow fails the call rather than disclose unrecorded.
func TestARevealThatCannotBeRecordedReleasesNothing(t *testing.T) {
	t.Parallel()

	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.Required(), audit.WithEmitter(revealUnrecordable{}))
	require.NoError(t, err)
	s := mustNew(t, runningFake(t), WithAudit(recorder))

	ctx := revealer(t.Context(), "workload.read", "workload.reveal_sensitive")
	_, err = s.Get(ctx, connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1", RevealSensitive: true}))
	require.Error(t, err)

	// The same caller not asking is served: only the reveal needed the record.
	resp, err := s.Get(ctx, connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1"}))
	require.NoError(t, err)
	require.Equal(t, v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD, resp.Msg.GetSensitiveDisclosure())
}
