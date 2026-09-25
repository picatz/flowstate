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
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
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
			Type:      &commonpb.WorkflowType{Name: flowstateRunWorkflowType},
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			Memo:      mineMemo(t),
		},
	}

	t.Run("an authorized read", func(t *testing.T) {
		t.Parallel()

		sink := &recordingEmitter{}
		s := mustNew(t, &fakeRunClient{describe: running}, WithAudit(recorderFor(t, sink)))
		ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
			Issuer:     "https://issuer.example",
			IssuerName: "production-issuer",
			Subject:    "agent-1",
			Role:       "operator",
			Claims:     map[string]any{"private": "claim-value"},
		})

		_, err := s.Get(ctx, connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1"}))
		require.NoError(t, err)

		record := sink.only(t)
		require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, record.GetDecision())
		require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_READ, record.GetAction())
		require.Equal(t, "Get", record.GetRpc())
		require.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, record.GetResourceKind())
		require.Equal(t, "orders-1", record.GetResourceKey())
		require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_UNSPECIFIED, record.GetDenyCode())
		require.Equal(t, "production-issuer", record.GetIssuerName())
		require.Equal(t, "operator", record.GetRole())
		require.Empty(t, record.GetIdentity().GetClaims())
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

		// running's memo positively records the default (empty-string) tenant
		// as its owner (mineMemo(t), above), and this caller is in acme —
		// a genuine tenant mismatch, not the no-memo case "a run that cannot
		// be read" above covers. The refusal the caller receives is
		// identical to that one, which is exactly why the record must
		// distinguish them.
		sink := &recordingEmitter{}
		s := mustNew(t, &fakeRunClient{describe: running},
			WithNamespace("acme"), WithAudit(recorderFor(t, sink)))
		// A caller who holds the action, so the refusal below is the tenant
		// comparison rather than the action check that now runs ahead of it.
		ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
			Issuer: "https://issuer.example", Subject: "reader", Actions: auth.ActionScopes{"workload.read"},
		})

		_, err := s.Get(ctx, connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1"}))
		require.Error(t, err)
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err),
			"a run in another tenant was distinguishable from one that does not exist")

		record := sink.only(t)
		require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
		require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_TENANT_MISMATCH, record.GetDenyCode())
		require.Equal(t, "acme", record.GetIdentity().GetNamespace())
	})

	t.Run("an execution of another workflow type", func(t *testing.T) {
		t.Parallel()

		foreign := &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{WorkflowId: "orders-1", RunId: "r-1"},
				Type:      &commonpb.WorkflowType{Name: "AnotherApplication"},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		}
		sink := &recordingEmitter{}
		s := mustNew(t, &fakeRunClient{describe: foreign}, WithAudit(recorderFor(t, sink)))
		ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
			Issuer: "https://issuer.example", Subject: "reader", Actions: auth.ActionScopes{"workload.read"},
		})

		_, err := s.Get(ctx, connect.NewRequest(&v1.GetRequest{WorkflowId: "orders-1"}))
		require.Error(t, err)
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

		record := sink.only(t)
		require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
		require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_RESOURCE_NOT_FOUND, record.GetDenyCode())
	})

	t.Run("a reader cannot terminate", func(t *testing.T) {
		t.Parallel()

		sink := &recordingEmitter{}
		s := mustNew(t, &fakeRunClient{describe: running}, WithAudit(recorderFor(t, sink)))
		ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
			Issuer:  "https://issuer.example",
			Subject: "dashboard",
			Role:    "reader",
			Actions: auth.ActionScopes{"workload.read"},
		})

		_, err := s.Terminate(ctx, connect.NewRequest(&v1.TerminateRequest{
			WorkflowId: "orders-1",
			Reason:     "must not happen",
		}))
		require.Error(t, err)
		require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
		var connectErr *connect.Error
		require.ErrorAs(t, err, &connectErr)
		require.Contains(t, connectErr.Meta().Get("WWW-Authenticate"), `scope="workload.terminate"`)

		record := sink.only(t)
		require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
		require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_TERMINATE, record.GetAction())
		require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, record.GetDenyCode())
		require.Equal(t, "reader", record.GetRole())
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

func TestValidateAuthorizationPolicyUsesTheCanonicalVocabulary(t *testing.T) {
	t.Parallel()

	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name:    "reader",
		Actions: auth.ActionScopes{"workload.read"},
	}}}
	require.NoError(t, ValidateAuthorizationPolicy(policy))

	policy.Issuers[0].Actions[0] = "workload.raed"
	err := ValidateAuthorizationPolicy(policy)
	require.Error(t, err, "a misspelled action silently produced a deny-all principal")
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	require.ErrorContains(t, err, "workload.raed")
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
					Type:       &commonpb.WorkflowType{Name: flowstateRunWorkflowType},
					FirstRunId: "r-first",
					Status:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					Memo:       mineMemo(t),
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
				Type:      &commonpb.WorkflowType{Name: flowstateRunWorkflowType},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Memo:      mineMemo(t),
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

	namespace, err := converter.GetDefaultDataConverter().ToPayload("")
	require.NoError(t, err)

	fake := &fakeRunClient{
		describe: &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{WorkflowId: "orders-1", RunId: "r-1"},
				Type:      &commonpb.WorkflowType{Name: flowstateRunWorkflowType},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
					signalProtocolMemoKey: protocol,
					namespaceMemoKey:      namespace,
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

	// describes counts the lookups a request actually spent, which is what
	// makes "the run was never addressed" assertable rather than inferred from
	// a status code that has more than one reason to be what it is.
	describes int
}

func (c *fakeRunClient) DescribeWorkflowExecution(_ context.Context, _, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	c.describes++
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

// TestAnActionRefusalNeverAddressesTheResource is #1119's oracle.
//
// The per-action check sat at the allow seam, which for every verb that
// addresses an existing resource is reached only after a Describe and a tenant
// comparison have already decided the answer. The two refusals differ: a run
// that exists in the caller's own tenant reached the action check and came back
// permission denied, while an absent, foreign, or non-Flowstate id had already
// come back not found. A caller holding no action at all could therefore sort
// guessed ids into "names something in my tenant" and "does not" from the
// status alone — without holding the action that reads one, and with the same
// answer for every id being the whole point of the not-found refusal.
//
// The assertion is the lookup count as well as the status, because a uniform
// status reached by two different routes is still two routes: the timing and
// the audit record would separate them even where the code does not.
func TestAnActionRefusalNeverAddressesTheResource(t *testing.T) {
	t.Parallel()

	// The three run shapes whose refusals used to differ, plus the absent one.
	// Every row is the same caller, holding no action, probing one id.
	owned := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: "candidate", RunId: "r-1"},
			Type:      &commonpb.WorkflowType{Name: flowstateRunWorkflowType},
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			Memo:      mineMemo(t),
		},
	}

	for name, temporal := range map[string]*fakeRunClient{
		"a run in the caller's own tenant": {describe: owned},
		"a run that cannot be read":        {describeErr: errors.New("no such workflow execution")},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sink := &recordingEmitter{}
			s := mustNew(t, temporal, WithAudit(recorderFor(t, sink)))
			ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
				Issuer: "https://issuer.example", Subject: "disabled", Actions: auth.ActionScopes{},
			})

			_, err := s.Get(ctx, connect.NewRequest(&v1.GetRequest{WorkflowId: "candidate"}))
			require.Error(t, err)
			require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err),
				"the answer has to be the same whatever the id names")
			require.Zero(t, temporal.describes,
				"a caller with no action addressed the run, so the refusal is still about what it found")

			record := sink.only(t)
			require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
			require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, record.GetDenyCode())
		})
	}
}

// TestSignalRefusesAnUnheldActionBeforeResolvingTheRun is the same claim for
// the verb that resolves twice.
//
// Signal walks the Continue-As-New chain, so a caller holding `workload.read`
// but not `workload.signal` spent two lookups on this server before being told
// they may not signal at all — and learned from the ordering whether the id
// named a live run.
func TestSignalRefusesAnUnheldActionBeforeResolvingTheRun(t *testing.T) {
	t.Parallel()

	owned := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: "candidate", RunId: "r-1"},
			Type:      &commonpb.WorkflowType{Name: flowstateRunWorkflowType},
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			Memo:      mineMemo(t),
		},
	}

	sink := &recordingEmitter{}
	temporal := &fakeRunClient{describe: owned}
	s := mustNew(t, temporal, WithAudit(recorderFor(t, sink)))
	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer: "https://issuer.example", Subject: "reader", Actions: auth.ActionScopes{"workload.read"},
	})

	_, err := s.Signal(ctx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: "candidate", Name: "approval",
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	require.Zero(t, temporal.describes, "a caller who may not signal resolved the run anyway")
	require.Zero(t, temporal.signals, "a caller who may not signal delivered one")

	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, sink.only(t).GetDenyCode())
}
