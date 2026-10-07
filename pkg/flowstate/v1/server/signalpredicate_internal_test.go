package server

import (
	"maps"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	v1types "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The shared conformance table (TestRehearsalSignalCasesDurably) already runs
// every rule-list case through its predicate twin and the fail-closed arms
// through `authorizeSignal`. What it cannot say is about the memo: what submit
// records for a predicate, and what delivery does with a record it cannot trust.

func predicateWorkflow(expression string) *v1types.Workflow {
	return &v1types.Workflow{
		Name: "gate",
		Signals: map[string]*v1types.SignalPolicy{
			"deploy-approved": {Allow: expression},
		},
	}
}

func TestSignalPolicyScopeIsRecordedOnlyForWhatAPredicateReads(t *testing.T) {
	t.Parallel()

	starter := &v1types.WorkloadIdentity{
		Issuer: "https://issuer.example.com", Subject: "requester@example.com",
		Claims: map[string]string{"team": "payments"},
	}
	inputs := map[string]*v1types.Value{"expected_approver": v1types.NewLiteral("lead@example.com")}

	read := func(expression string) *v1types.Scope {
		entries, err := policyMemoEntries(predicateWorkflow(expression), inputs, starter)
		require.NoError(t, err)

		raw, ok := entries[signalPolicyScopeMemoKey]
		if !ok {
			return nil
		}

		scope := &v1types.Scope{}
		require.NoError(t, proto.Unmarshal(raw.([]byte), scope))

		return scope
	}

	assert.Nil(t, read(`sender.identity.claims["team"] == "x"`),
		"a predicate that reads neither inputs nor the starter must not copy either into the memo")

	withRun := read(`sender.identity.principal != run.identity.principal`)
	require.NotNil(t, withRun)
	assert.Empty(t, withRun.GetInputs(), "inputs were recorded for a predicate that does not read them")
	assert.Equal(t, "payments", withRun.GetIdentity().GetClaims()["team"],
		"the starter's identity, claims included, is what run.identity reads at delivery")

	withInputs := read(`sender.identity.principal == "https://issuer.example.com#" + inputs.expected_approver && sender.identity.claims["team"] == "x"`)
	require.NotNil(t, withInputs)
	assert.Contains(t, withInputs.GetInputs(), "expected_approver")
	assert.Len(t, withInputs.GetInputs(), 1)
	assert.Nil(t, withInputs.GetIdentity(), "the starter was recorded for a predicate that does not read it")

	// A rule-list policy records no scope at all.
	entries, err := policyMemoEntries(&v1types.Workflow{
		Name: "gate",
		Signals: map[string]*v1types.SignalPolicy{
			"deploy-approved": {Allow: `sender.identity.namespace == "n"`},
		},
	}, inputs, starter)
	require.NoError(t, err)
	assert.NotContains(t, entries, signalPolicyScopeMemoKey)
}

func TestSignalPolicyScopeOverItsBoundRefusesTheRunInsteadOfTruncating(t *testing.T) {
	t.Parallel()

	big := map[string]*v1types.Value{"blob": v1types.NewLiteral(strings.Repeat("x", v1types.MaxSignalPolicyScopeBytes+1))}

	_, err := policyMemoEntries(
		predicateWorkflow(`inputs.blob == "x" && sender.identity.claims["team"] == "y"`), big, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bound")

	// The same inputs are fine when no predicate reads them: the bound is spent
	// only where it is used.
	_, err = policyMemoEntries(predicateWorkflow(`sender.identity.claims["team"] == "y"`), big, nil)
	require.NoError(t, err)
}

func TestAuthorizeSignalReadsAPredicateOverTheRecordedScope(t *testing.T) {
	t.Parallel()

	starter := &v1types.WorkloadIdentity{
		Issuer: "https://issuer.example.com", Subject: "requester@example.com",
		Claims: map[string]string{"team": "payments"},
	}
	wf := predicateWorkflow(`sender.identity.claims["team"] == run.identity.claims["team"]` +
		` && sender.identity.principal == "https://issuer.example.com#" + inputs.approver`)
	inputs := map[string]*v1types.Value{"approver": v1types.NewLiteral("lead@example.com")}

	entries, err := policyMemoEntries(wf, inputs, starter)
	require.NoError(t, err)

	resp := memoWithSignalPolicy(t, wf.GetSignals())
	fields := resp.GetWorkflowExecutionInfo().GetMemo().GetFields()
	for _, key := range []string{signalPolicyScopeMemoKey} {
		payload, err := converter.GetDefaultDataConverter().ToPayload(entries[key])
		require.NoError(t, err)
		fields[key] = payload
	}
	starterPayload, err := converter.GetDefaultDataConverter().ToPayload(
		v1types.QualifiedSubject(starter.GetIssuer(), starter.GetSubject()))
	require.NoError(t, err)
	fields[starterMemoKey] = starterPayload

	srv := mustNew(t, nil)
	lead := sender("https://issuer.example.com", "lead@example.com", "", map[string]string{"team": "payments"})
	require.NoError(t, srv.authorizeSignal(resp, "deploy-approved", lead, nil))

	err = srv.authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "lead@example.com", "", map[string]string{"team": "other"}), nil)
	require.Error(t, err, "a claim that differs from the starter's was admitted")
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// A scope entry that cannot be decoded denies, never evaluates the
	// predicate over inputs that silently vanished.
	corrupt := memoWithSignalPolicy(t, wf.GetSignals())
	corrupt.GetWorkflowExecutionInfo().GetMemo().GetFields()[starterMemoKey] = starterPayload
	garbage, err := converter.GetDefaultDataConverter().ToPayload([]byte{0xff, 0xff, 0xff})
	require.NoError(t, err)
	corrupt.GetWorkflowExecutionInfo().GetMemo().GetFields()[signalPolicyScopeMemoKey] = garbage
	err = srv.authorizeSignal(corrupt, "deploy-approved", lead, nil)
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// And one over its bound is the same refusal.
	oversized, err := converter.GetDefaultDataConverter().ToPayload(make([]byte, v1types.MaxSignalPolicyScopeBytes+1))
	require.NoError(t, err)
	corrupt.GetWorkflowExecutionInfo().GetMemo().GetFields()[signalPolicyScopeMemoKey] = oversized
	require.Error(t, srv.authorizeSignal(corrupt, "deploy-approved", lead, nil))

	// A run with no scope recorded and a predicate that needs one denies.
	missing := memoWithSignalPolicy(t, wf.GetSignals())
	missing.GetWorkflowExecutionInfo().GetMemo().GetFields()[starterMemoKey] = starterPayload
	require.Error(t, srv.authorizeSignal(missing, "deploy-approved", lead, nil))
}

func TestAuthorizeSignalPredicateOnARunWithNoStarterRecordedDenies(t *testing.T) {
	t.Parallel()

	policies := map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: `sender.identity.principal != run.identity.principal`},
	}
	err := mustNew(t, nil).authorizeSignal(memoWithSignalPolicy(t, policies), "deploy-approved",
		sender("https://issuer.example.com", "lead@example.com", "", nil), nil)
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

func TestAuthorizeSignalRefusalNamesNoClaimValue(t *testing.T) {
	t.Parallel()

	const secret = "CLAIMSECRET123"
	policies := map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: `int(sender.identity.claims["n"]) == 1`},
	}
	err := mustNew(t, nil).authorizeSignal(memoWithSignalPolicy(t, policies), "deploy-approved",
		sender("https://issuer.example.com", "lead@example.com", "", map[string]string{"n": secret}), nil)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret)
}

// Secrets never enter durable history: a run records the inputs a predicate
// names and nothing else, and a predicate that reads a `sensitive:` input is
// refused rather than recorded.
func TestSignalPolicyScopeRecordsOnlyTheInputsAPredicateNames(t *testing.T) {
	t.Parallel()

	wf := predicateWorkflow(`sender.identity.claims["team"] == "x" && sender.identity.principal == "i#" + inputs.approver`)
	wf.DeclaredInputs = []*v1types.InputDeclaration{{Name: "approver"}, {Name: "token", Sensitive: true}, {Name: "note"}}
	inputs := map[string]*v1types.Value{
		"approver": v1types.NewLiteral("lead@example.com"),
		"token":    v1types.NewLiteral("hunter2"),
		"note":     v1types.NewLiteral("unrelated"),
	}

	entries, err := policyMemoEntries(wf, inputs, nil)
	require.NoError(t, err)

	scope := &v1types.Scope{}
	require.NoError(t, proto.Unmarshal(entries[signalPolicyScopeMemoKey].([]byte), scope))
	assert.Equal(t, []string{"approver"}, slices.Collect(maps.Keys(scope.GetInputs())),
		"inputs no predicate names must not be copied into durable history")
	assert.NotContains(t, string(entries[signalPolicyScopeMemoKey].([]byte)), "hunter2")

	// Fail closed at the place the memo is written, for a specification that
	// never met the validator.
	wf.Signals["deploy-approved"].Allow = `sender.identity.claims["team"] == "x" && inputs.token == "hunter2"`
	_, err = policyMemoEntries(wf, inputs, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `reads the input "token"`)
	assert.NotContains(t, err.Error(), "hunter2")

	// And at submit's validation, beside the other policy checks.
	parsed, _, err := flowfile.Parse([]byte(`edition: v2026.4
name: gate
inputs:
  token:
    type: string
    sensitive: true
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
signals:
  deploy-approved:
    allow: ${sender.identity.claims.team == "x" && inputs.token == "y"}
`))
	require.NoError(t, err)
	err = (&FlowstateServer{}).validateSpecification(parsed)
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	assert.Contains(t, err.Error(), `reads the input "token"`)
}
