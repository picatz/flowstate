package server_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// [RunRequest.request_id] end to end, against a real Temporal.
//
// The defect this field closes is a retry after a lost answer starting the
// workload twice, so every positive case here submits the identical request
// twice and asserts on how many runs exist afterwards — through Get, which
// names one execution, and through the ids the two answers carry.

// keyedWorkflow is [gatedWorkflow] with a declared input, so the same key can
// be submitted with a different payload and the refusal exercised.
func keyedWorkflow() *v1.Workflow {
	wf := gatedWorkflow()
	wf.DeclaredInputs = []*v1.InputDeclaration{{
		Name:     "target",
		Type:     v1.InputDeclaration_TYPE_STRING,
		Required: true,
	}}

	return wf
}

func targetInputs(target string) map[string]*v1.Value {
	return map[string]*v1.Value{"target": v1.NewLiteral(target)}
}

func keyed(wf *v1.Workflow, inputs map[string]*v1.Value, requestID string) *connect.Request[v1.RunRequest] {
	return connect.NewRequest(&v1.RunRequest{
		Workflow:  wf,
		Inputs:    inputs,
		RequestId: &requestID,
	})
}

// auditSink collects what a server records, for the one assertion here about
// the audit trail: a reuse is a decision about a run, written down as one.
type auditSink struct {
	records []*v1.AuditRecord
}

func (s *auditSink) Emit(_ context.Context, record *v1.AuditRecord) error {
	s.records = append(s.records, record)

	return nil
}

// TestRequestIDRetryReusesTheRun is the acceptance case: the same request
// twice under one key observes one run, and the second answer says so.
func TestRequestIDRetryReusesTheRun(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)

	sink := &auditSink{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink))
	require.NoError(t, err)
	flowstate := mustNew(t, temporal, server.WithNamespace(teamANamespace), server.WithAudit(recorder))

	first, err := flowstate.Run(t.Context(), keyed(keyedWorkflow(), targetInputs("prod"), "job-1"))
	require.NoError(t, err)
	require.False(t, first.Msg.GetReused(), "the first submission started the run; nothing existed to reuse")
	require.True(t, strings.HasPrefix(first.Msg.GetWorkflowId(), "flowstate-request-"),
		"a request-addressed run is started under its own id namespace: %q", first.Msg.GetWorkflowId())
	require.True(t, first.Msg.GetSpecificationAsSubmitted())
	waitUntilParkedAtTheGate(t, temporal, first.Msg.GetWorkflowId())

	second, err := flowstate.Run(t.Context(), keyed(keyedWorkflow(), targetInputs("prod"), "job-1"))
	require.NoError(t, err, "a retry is answered with the run, never refused")
	require.True(t, second.Msg.GetReused(),
		"a reuse is a fact the server states; a caller cannot tell it from a fresh start by looking")
	require.Equal(t, first.Msg.GetWorkflowId(), second.Msg.GetWorkflowId())
	require.Equal(t, first.Msg.GetRunId(), second.Msg.GetRunId(), "the run returned is the one the first attempt started")
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, second.Msg.GetStatus())
	require.False(t, second.Msg.GetJoined(), "a reuse is not a join: nobody else holds this run")
	require.NotNil(t, second.Msg.SpecificationAsSubmitted, "the attestation is answered, never left unset")
	require.False(t, second.Msg.GetSpecificationAsSubmitted(),
		"the specification that ran is the earlier attempt's; this server did not establish it now")

	// Three records so far: two admissions ("may start work in this namespace",
	// one per call) and the reuse, which names the run the retry was answered
	// with. Asserted before the Get below adds its own.
	require.Len(t, sink.records, 3, "two admissions and one reuse decision")
	reuse := sink.records[2]
	require.Equal(t, "Run", reuse.GetRpc())
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, reuse.GetDecision())
	require.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, reuse.GetResourceKind())
	require.Equal(t, first.Msg.GetWorkflowId(), reuse.GetResourceKey(), "the reuse record names the original run")
	for _, record := range sink.records[:2] {
		require.Equal(t, v1.AuditResourceKind_AUDIT_RESOURCE_KIND_NAMESPACE, record.GetResourceKind())
	}

	// Exactly one run exists under that id: the retry started nothing.
	got, err := flowstate.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: first.Msg.GetWorkflowId(),
	}))
	require.NoError(t, err)
	require.Equal(t, first.Msg.GetRunId(), got.Msg.GetRunId())
	require.Equal(t, v1.RunResponse_STATUS_RUNNING, got.Msg.GetStatus())
}

// TestRequestIDWithADifferentSubmissionIsRefused is the acceptance case's
// negative direction: a key reused for a different payload is refused, naming
// the run, rather than silently attached to a run that will do something else.
func TestRequestIDWithADifferentSubmissionIsRefused(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	first, err := fixture.teamA.Run(t.Context(), keyed(keyedWorkflow(), targetInputs("prod"), "job-2"))
	require.NoError(t, err)
	waitUntilParkedAtTheGate(t, fixture.temporal, first.Msg.GetWorkflowId())

	t.Run("different inputs", func(t *testing.T) {
		_, err := fixture.teamA.Run(t.Context(), keyed(keyedWorkflow(), targetInputs("staging"), "job-2"))
		require.Error(t, err)
		require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
		require.ErrorContains(t, err, first.Msg.GetRunId(), "the refusal names the run the key already started")
		require.ErrorContains(t, err, "new request_id")
	})

	t.Run("different specification", func(t *testing.T) {
		edited := keyedWorkflow()
		edited.Steps[0].GetTask().Inputs["message"] = v1.NewLiteral("edited between attempts")

		_, err := fixture.teamA.Run(t.Context(), keyed(edited, targetInputs("prod"), "job-2"))
		require.Error(t, err)
		require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
		require.ErrorContains(t, err, first.Msg.GetRunId())
	})

	// Still one run, and still the first one: neither refusal started anything.
	got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
		WorkflowId: first.Msg.GetWorkflowId(),
	}))
	require.NoError(t, err)
	require.Equal(t, first.Msg.GetRunId(), got.Msg.GetRunId())
}

// TestRequestIDRetryAfterTheRunFinishedStillReuses is the half a dedupe that
// only thought about concurrent retries gets wrong: a retry arriving after the
// run ended finds that run, reported with its finished status, rather than
// starting the workload a second time.
func TestRequestIDRetryAfterTheRunFinishedStillReuses(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	brief := func() *v1.Workflow {
		return &v1.Workflow{
			Name: "brief",
			Steps: []*v1.Node{{
				Id: "work",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("done")},
				}},
			}},
		}
	}

	first, err := fixture.teamA.Run(t.Context(), keyed(brief(), nil, "job-3"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		got, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{
			WorkflowId: first.Msg.GetWorkflowId(),
		}))

		return err == nil && got.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
	}, 30*time.Second, 100*time.Millisecond, "the first run did not finish")

	second, err := fixture.teamA.Run(t.Context(), keyed(brief(), nil, "job-3"))
	require.NoError(t, err, "a request id names one submission forever, finished or not")
	require.True(t, second.Msg.GetReused())
	require.Equal(t, first.Msg.GetRunId(), second.Msg.GetRunId(), "the finished run, not a second one")
	require.Equal(t, v1.RunResponse_STATUS_COMPLETED, second.Msg.GetStatus(),
		"a reused run reports its current status, which here is the finished one")
}

// TestRequestIDsDoNotCrossTenants is the negative direction tenancy asks for:
// the same key chosen by two tenants is two submissions, and neither is
// answered with the other's run.
func TestRequestIDsDoNotCrossTenants(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	teamA, err := fixture.teamA.Run(t.Context(), keyed(keyedWorkflow(), targetInputs("prod"), "shared-key"))
	require.NoError(t, err)
	teamB, err := fixture.teamB.Run(t.Context(), keyed(keyedWorkflow(), targetInputs("prod"), "shared-key"))
	require.NoError(t, err)

	require.False(t, teamB.Msg.GetReused(), "team B's first submission is its own, not a retry of team A's")
	require.NotEqual(t, teamA.Msg.GetWorkflowId(), teamB.Msg.GetWorkflowId(),
		"two tenants naming the identical request id must never resolve to the same run")
}

// TestRunWithoutARequestIDIsUnchanged pins the byte-identical-to-today case:
// no key, a fresh `flowstate-workflow-<uuid>` id per request, a second run.
func TestRunWithoutARequestIDIsUnchanged(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	first, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: keyedWorkflow(), Inputs: targetInputs("prod"),
	}))
	require.NoError(t, err)
	second, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: keyedWorkflow(), Inputs: targetInputs("prod"),
	}))
	require.NoError(t, err)

	for _, started := range []*v1.RunResponse{first.Msg, second.Msg} {
		require.True(t, strings.HasPrefix(started.GetWorkflowId(), "flowstate-workflow-"), started.GetWorkflowId())
		require.False(t, started.GetReused())
	}
	require.NotEqual(t, first.Msg.GetWorkflowId(), second.Msg.GetWorkflowId(),
		"without a key there is nothing to deduplicate on, and two submissions are two runs")
}

// TestRequestIDComposesWithAConcurrencyKey checks the composition rule where
// it matters most: `flow run` sends a request id on every invocation, so a
// workflow declaring `concurrency:` must keep its `on_conflict:` semantics for
// a *different* submission while a retry of the same one is recognized.
func TestRequestIDComposesWithAConcurrencyKey(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	first, err := fixture.teamA.Run(t.Context(),
		keyed(exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_JOIN), clusterInputs("ledger-1"), "attempt-1"))
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(first.Msg.GetWorkflowId(), "flowstate-lock-"),
		"the permit decides the address; the request id does not displace it")
	waitUntilParkedAtTheGate(t, fixture.temporal, first.Msg.GetWorkflowId())

	t.Run("a retry is a reuse, not a join", func(t *testing.T) {
		retry, err := fixture.teamA.Run(t.Context(),
			keyed(exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_JOIN), clusterInputs("ledger-1"), "attempt-1"))
		require.NoError(t, err)
		require.True(t, retry.Msg.GetReused())
		require.False(t, retry.Msg.GetJoined())
		require.Equal(t, first.Msg.GetRunId(), retry.Msg.GetRunId())
	})

	t.Run("a different submission is the permit's own answer", func(t *testing.T) {
		other, err := fixture.teamA.Run(t.Context(),
			keyed(exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_JOIN), clusterInputs("ledger-1"), "attempt-2"))
		require.NoError(t, err)
		require.True(t, other.Msg.GetJoined(), "`on_conflict: join` decides for a submission that is not a retry")
		require.False(t, other.Msg.GetReused())
		require.Equal(t, first.Msg.GetRunId(), other.Msg.GetRunId())
	})

	t.Run("a different submission under a rejecting permit is still refused", func(t *testing.T) {
		rejecting, err := fixture.teamA.Run(t.Context(),
			keyed(exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_REJECT), clusterInputs("ledger-2"), "attempt-3"))
		require.NoError(t, err)
		waitUntilParkedAtTheGate(t, fixture.temporal, rejecting.Msg.GetWorkflowId())

		_, err = fixture.teamA.Run(t.Context(),
			keyed(exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_REJECT), clusterInputs("ledger-2"), "attempt-4"))
		require.Error(t, err)
		require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
		require.ErrorContains(t, err, "on_conflict: reject")

		retry, err := fixture.teamA.Run(t.Context(),
			keyed(exclusiveWorkflow(v1.Concurrency_ON_CONFLICT_REJECT), clusterInputs("ledger-2"), "attempt-3"))
		require.NoError(t, err, "the submission that holds the permit may retry")
		require.True(t, retry.Msg.GetReused())
	})
}

// TestRequestIDComposesWithAnEntityKey is the same rule under the third
// address: the entity decides which run is live, the request id decides whether
// this submission is the one that started it.
func TestRequestIDComposesWithAnEntityKey(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	startWorker(t, fixture.temporal)

	key := "order-77"
	first, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow:  entityWorkflow(nil),
		EntityKey: &key,
		RequestId: new("create-77"),
	}))
	require.NoError(t, err)
	require.Equal(t, "flowstate-entity-team-a_order-77", first.Msg.GetWorkflowId(),
		"the entity decides the address; the request id does not displace it")

	retry, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow:  entityWorkflow(nil),
		EntityKey: &key,
		RequestId: new("create-77"),
	}))
	require.NoError(t, err)
	require.True(t, retry.Msg.GetReused())
	require.Equal(t, first.Msg.GetRunId(), retry.Msg.GetRunId())

	_, err = fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow:  entityWorkflow(nil),
		EntityKey: &key,
		RequestId: new("create-77-again"),
	}))
	require.Error(t, err, "a second submission against a live entity is not the one that started it")
	require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
	require.ErrorContains(t, err, first.Msg.GetRunId())
}
