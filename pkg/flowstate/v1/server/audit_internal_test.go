package server

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
)

// The behavioural half of the audit claims. auditseam_test.go proves every RPC
// reaches an emitter; these prove what reaches it, and when.
//
// The fakes here answer the two calls the decision itself makes — describe a
// run, and then act on it — and leave everything else to panic through an
// embedded nil interface, in the shape schedules_internal_test.go's fakes
// already use: a path this was not built for fails loudly rather than quietly
// returning a zero value.

// TestADecisionEmitsExactlyOneRecord covers both answers and the shapes of
// each, including the deny code that a caller is deliberately never told.
func TestADecisionEmitsExactlyOneRecord(t *testing.T) {
	t.Parallel()

	running := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: "orders-1", RunId: "r-1"},
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}

	t.Run("an authorized read", func(t *testing.T) {
		t.Parallel()

		sink := &recordingEmitter{}
		s := mustNew(t, &fakeRunClient{describe: running}, WithAudit(recorderFor(t, sink)))

		_, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1"}))
		require.NoError(t, err)

		record := sink.only(t)
		require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, record.GetDecision())
		require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_READ, record.GetAction())
		require.Equal(t, "Get", record.GetRpc())
		require.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, record.GetResourceKind())
		require.Equal(t, "orders-1", record.GetResourceKey())
		require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED, record.GetDenyCode())
		require.NotNil(t, record.GetDecidedAt())
	})

	t.Run("a run that cannot be read", func(t *testing.T) {
		t.Parallel()

		sink := &recordingEmitter{}
		s := mustNew(t, &fakeRunClient{describeErr: errors.New("no such execution")},
			WithAudit(recorderFor(t, sink)))

		_, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1"}))
		require.Error(t, err)

		record := sink.only(t)
		require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
		require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_RESOURCE_NOT_FOUND, record.GetDenyCode())

		// The record carries the code and the caller carries the words, and the
		// two say different amounts on purpose.
		require.Contains(t, err.Error(), "no such run")
		require.NotContains(t, record.String(), "no such run")
	})

	t.Run("a run belonging to another tenant", func(t *testing.T) {
		t.Parallel()

		// No memo on the described run, so [FlowstateServer.ownedBy] treats it
		// as reachable only from the empty namespace — and this caller is in
		// acme. The refusal the caller receives is identical to the one above,
		// which is exactly why the record must distinguish them.
		sink := &recordingEmitter{}
		s := mustNew(t, &fakeRunClient{describe: running},
			WithNamespace("acme"), WithAudit(recorderFor(t, sink)))

		_, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1"}))
		require.Error(t, err)

		record := sink.only(t)
		require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
		require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_TENANT_MISMATCH, record.GetDenyCode())
		require.Equal(t, "acme", record.GetIdentity().GetNamespace())
	})

	t.Run("a verb that reaches no resource", func(t *testing.T) {
		t.Parallel()

		sink := &recordingEmitter{}
		s := mustNew(t, &fakeRunClient{}, WithAudit(recorderFor(t, sink)))

		_, err := s.Validate(t.Context(), connect.NewRequest(&v1.ValidateRequest{
			Files: []*v1.SourceFile{{Name: "flow.yaml", Source: []byte("name: demo\n")}},
		}))
		require.NoError(t, err)

		record := sink.only(t)
		require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, record.GetDecision())
		require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_VALIDATE, record.GetAction())
		require.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_UNSPECIFIED, record.GetResourceKind())
		require.Empty(t, record.GetResourceKey())
	})
}

// TestSignalWalkingAChainRecordsOneDecision is the case that made the
// un-audited decision function necessary.
//
// Signal resolves a run twice when the caller pinned the first run id of a
// Continue-As-New chain: the pinned execution has closed, so the first lookup
// fails and the second finds the current one. Those are two lookups reaching
// one decision. Auditing each would write a denial into the trail for a
// request that was allowed — a record that is wrong, which is the one thing
// this artifact must not be.
func TestSignalWalkingAChainRecordsOneDecision(t *testing.T) {
	t.Parallel()

	fake := &fakeRunClient{
		describeByRun: map[string]*workflowservice.DescribeWorkflowExecutionResponse{
			"": {
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Execution:  &commonpb.WorkflowExecution{WorkflowId: "orders-1", RunId: "r-current"},
					FirstRunId: "r-first",
					Status:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
		},
		describeErr: errors.New("workflow execution already completed"),
	}

	sink := &recordingEmitter{}
	s := mustNew(t, fake, WithAudit(recorderFor(t, sink)))

	_, err := s.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: "orders-1",
		RunId:      "r-first",
		Name:       "approval",
	}))
	require.NoError(t, err)
	require.Equal(t, 1, fake.signals, "the signal was delivered, so the decision was an allow")

	record := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, record.GetDecision())
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_SIGNAL, record.GetAction())
	require.Equal(t, "Signal", record.GetRpc())
}

// TestARequiredRecordThatCannotBeWrittenStopsTheMutation is the fail-closed
// claim at the seam rather than in the recorder: an action that cannot be
// recorded does not happen, and "does not happen" means Temporal was never
// asked.
func TestARequiredRecordThatCannotBeWrittenStopsTheMutation(t *testing.T) {
	t.Parallel()

	fake := &fakeRunClient{
		describe: &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{WorkflowId: "orders-1", RunId: "r-1"},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		},
	}

	broken, err := audit.NewRecorder(audit.WithoutStderr(), audit.Required(),
		audit.WithEmitter(brokenEmitter{}))
	require.NoError(t, err)

	s := mustNew(t, fake, WithAudit(broken))

	_, err = s.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: "orders-1",
		Name:       "approval",
	}))
	require.Error(t, err)
	require.Zero(t, fake.signals, "the sink failed, so the signal must never have been delivered")

	// And the same deployment without the requirement serves the request: the
	// two halves are one decision an operator makes, not a default.
	advisory, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(brokenEmitter{}))
	require.NoError(t, err)

	s = mustNew(t, fake, WithAudit(advisory))
	_, err = s.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: "orders-1",
		Name:       "approval",
	}))
	require.NoError(t, err)
	require.Equal(t, 1, fake.signals)
}

// TestASignalPolicyDenialIsAuditedAsADenial: tenancy is only the first half of
// Signal's authorization. A name-level policy refusal must not leave behind an
// allow record for a delivery that never happened.
func TestASignalPolicyDenialIsAuditedAsADenial(t *testing.T) {
	t.Parallel()

	protocol, err := converter.GetDefaultDataConverter().ToPayload(currentSignalProtocol)
	require.NoError(t, err)

	fake := &fakeRunClient{
		describe: &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{WorkflowId: "orders-1", RunId: "r-1"},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
					signalProtocolMemoKey: protocol,
				}},
			},
		},
	}

	sink := &recordingEmitter{}
	s := mustNew(t, fake, WithAudit(recorderFor(t, sink)))

	_, err = s.Signal(t.Context(), connect.NewRequest(&v1.SignalRequest{
		WorkflowId: "orders-1",
		Name:       v1.DebugSignal,
	}))
	require.Error(t, err)
	require.Zero(t, fake.signals, "the policy refused the signal before delivery")

	record := sink.only(t)
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, record.GetDenyCode())
}

func recorderFor(t *testing.T, sink audit.Emitter) *audit.Recorder {
	t.Helper()

	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink))
	require.NoError(t, err)

	return recorder
}

type recordingEmitter struct {
	records []*v1.AuditRecord
}

func (e *recordingEmitter) Emit(_ context.Context, record *v1.AuditRecord) error {
	e.records = append(e.records, record)

	return nil
}

// only is the "exactly one record per decision" assertion, in the one place
// every test here makes it.
func (e *recordingEmitter) only(t *testing.T) *v1.AuditRecord {
	t.Helper()

	require.Len(t, e.records, 1, "one decision, one record")

	return e.records[0]
}

type brokenEmitter struct{}

func (brokenEmitter) Emit(context.Context, *v1.AuditRecord) error {
	return errors.New("the sink is down")
}

// fakeRunClient answers the two calls a run decision and its mutation make,
// and panics through the embedded nil interface on anything else.
type fakeRunClient struct {
	client.Client

	describe      *workflowservice.DescribeWorkflowExecutionResponse
	describeByRun map[string]*workflowservice.DescribeWorkflowExecutionResponse
	describeErr   error

	signals int
}

func (c *fakeRunClient) DescribeWorkflowExecution(_ context.Context, _, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if resp, ok := c.describeByRun[runID]; ok {
		return resp, nil
	}

	if c.describe != nil {
		return c.describe, nil
	}

	return nil, c.describeErr
}

// QueryWorkflow refuses: a running run's progress query is beside the point
// here, and [runProgress] treating an unavailable answer as "no progress" is
// what a real worker that has not started answering looks like.
func (c *fakeRunClient) QueryWorkflow(context.Context, string, string, string, ...any) (converter.EncodedValue, error) {
	return nil, errors.New("no worker is answering queries")
}

func (c *fakeRunClient) SignalWorkflow(context.Context, string, string, string, any) error {
	c.signals++

	return nil
}
