package server

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"

	v1types "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A predicate that reads the run's inputs or starter needs what submit
// recorded for it. With that record absent it must deny rather than be
// evaluated over an empty scope, where an absence test reads as true.
func TestAuthorizeSignalDeniesWhenTheScopeAPredicateReadsWasNeverRecorded(t *testing.T) {
	t.Parallel()

	starterPayload, err := converter.GetDefaultDataConverter().ToPayload(
		v1types.QualifiedSubject("https://issuer.example.com", "requester@example.com"))
	require.NoError(t, err)

	lead := sender("https://issuer.example.com", "lead@example.com", "", map[string]string{"team": "payments"})

	for _, expression := range []string{
		`!has(inputs.x) && sender.identity.claims["team"] == "payments"`,
		`"x" in inputs || sender.identity.claims["team"] == "payments"`,
		`sender.identity.claims["team"] == "payments" || sender.identity.principal != run.identity.principal`,
	} {
		resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{"deploy-approved": {Allow: expression}})
		resp.GetWorkflowExecutionInfo().GetMemo().GetFields()[starterMemoKey] = starterPayload

		err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved", lead)
		require.Error(t, err, expression)
		assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err), expression)
		assert.Contains(t, err.Error(), "recorded nothing", expression)
	}

	// A predicate that reads neither needs no record, and is unaffected.
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: `sender.identity.claims["team"] == "payments"`},
	})
	require.NoError(t, mustNew(t, nil).authorizeSignal(resp, "deploy-approved", lead))
}

// The same, against a memo written by submit itself: the control admits with
// the scope recorded, and the one difference in the denial is that the record
// was removed. A predicate over `!has(inputs.x)` does not dereference a missing
// input, so only the absent-record rule can refuse it.
func TestAuthorizeSignalDeniesAPredicateWhoseRecordedScopeWasRemoved(t *testing.T) {
	t.Parallel()

	wf := &v1types.Workflow{
		Name: "gate",
		Signals: map[string]*v1types.SignalPolicy{
			"deploy-approved": {Allow: `!has(inputs.x) && sender.identity.claims["team"] == "payments"`},
		},
	}
	entries, err := policyMemoEntries(wf, nil, &v1types.WorkloadIdentity{Issuer: "i", Subject: "s"})
	require.NoError(t, err)
	require.Contains(t, entries, signalPolicyScopeMemoKey)

	build := func(drop bool) *workflowservice.DescribeWorkflowExecutionResponse {
		fields := map[string]*common.Payload{}
		for key, value := range entries {
			if drop && key == signalPolicyScopeMemoKey {
				continue
			}
			payload, err := converter.GetDefaultDataConverter().ToPayload(value)
			require.NoError(t, err)
			fields[key] = payload
		}

		return &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Memo: &common.Memo{Fields: fields}},
		}
	}

	lead := sender("https://issuer.example.com", "lead@example.com", "", map[string]string{"team": "payments"})
	srv := mustNew(t, nil)

	require.NoError(t, srv.authorizeSignal(build(false), "deploy-approved", lead), "the control, with the scope recorded")

	err = srv.authorizeSignal(build(true), "deploy-approved", lead)
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "recorded nothing")
}
