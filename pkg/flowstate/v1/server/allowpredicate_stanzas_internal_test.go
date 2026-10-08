package server

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	v1types "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// `debug: allow: ${...}` and `triggers: manual: allow: ${...}` at the server's
// decision sites. The evaluator's own semantics are pinned in package v1; what
// is here is that each site reaches it with the right caller, starter and
// inputs, and refuses when what the predicate reads was never recorded.

func debugPredicateWorkflow(expression string) *v1types.Workflow {
	return &v1types.Workflow{
		Name:    "dbg",
		Profile: v1types.CurrentProfile,
		Debug:   &v1types.SignalPolicy{Allow: expression},
	}
}

// debugRunMemo is the memo a run submitted with wf, inputs and starter carries,
// built by the one function submit uses, so what is decided is what submit wrote.
func debugRunMemo(t *testing.T, wf *v1types.Workflow, inputs map[string]*v1types.Value, starter *v1types.WorkloadIdentity, drop ...string) *workflowservice.DescribeWorkflowExecutionResponse {
	t.Helper()

	entries, err := policyMemoEntries(wf, inputs, starter)
	require.NoError(t, err)
	for _, key := range drop {
		delete(entries, key)
	}

	resp := memoWithCurrentSignalProtocol(t)
	fields := resp.GetWorkflowExecutionInfo().GetMemo().GetFields()
	for key, value := range entries {
		payload, err := converter.GetDefaultDataConverter().ToPayload(value)
		require.NoError(t, err)
		fields[key] = payload
	}
	if starter != nil {
		payload, err := converter.GetDefaultDataConverter().ToPayload(
			v1types.QualifiedSubject(starter.GetPrincipal().GetIssuer(), starter.GetPrincipal().GetSubject()))
		require.NoError(t, err)
		fields[starterMemoKey] = payload
	}

	return resp
}

func TestADebugPredicateReadsTheRunScopeSubmitRecorded(t *testing.T) {
	t.Parallel()

	starter := &v1types.WorkloadIdentity{Principal: &v1types.Principal{Issuer: "https://issuer.example.com", Subject: "requester@example.com", Claims: v1types.StringClaimValues(map[string]string{"team": "payments"})}}
	inputs := map[string]*v1types.Value{"debugger": v1types.NewLiteral("sre-1@example.com")}
	wf := debugPredicateWorkflow(`sender.identity.claims.team == run.identity.claims.team` +
		` && sender.identity.principal == "https://issuer.example.com#" + inputs.debugger`)

	srv := mustNew(t, nil)
	sre := sender("https://issuer.example.com", "sre-1@example.com", "", map[string]string{"team": "payments"})
	memo := debugRunMemo(t, wf, inputs, starter)

	require.NoError(t, srv.authorizeReservedSignal(memo, v1types.DebugSignal, sre),
		"the starter's claims and the run's inputs, as recorded, admit the matching caller")

	for name, caller := range map[string]*v1types.SignalSender{
		"a team that is not the starter's":      sender("https://issuer.example.com", "sre-1@example.com", "", map[string]string{"team": "other"}),
		"the right subject from another issuer": sender("https://other.example.com", "sre-1@example.com", "", map[string]string{"team": "payments"}),
		"a subject the inputs do not name":      sender("https://issuer.example.com", "intruder@example.com", "", map[string]string{"team": "payments"}),
		"a caller with no claims (key missing)": sender("https://issuer.example.com", "sre-1@example.com", "", nil),
	} {
		err := srv.authorizeReservedSignal(memo, v1types.DebugSignal, caller)
		require.Error(t, err, name)
		assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err), name)
	}
}

func TestADebugPredicateDeniesWhenWhatItReadsWasNeverRecorded(t *testing.T) {
	t.Parallel()

	starter := &v1types.WorkloadIdentity{Principal: &v1types.Principal{Issuer: "https://issuer.example.com", Subject: "requester@example.com"}}
	wf := debugPredicateWorkflow(`sender.identity.principal != run.identity.principal`)
	srv := mustNew(t, nil)
	caller := sender("https://issuer.example.com", "sre-1@example.com", "", nil)

	require.NoError(t, srv.authorizeReservedSignal(debugRunMemo(t, wf, nil, starter), v1types.DebugSignal, caller),
		"the control: with the scope recorded the predicate admits")

	// Scope absent while the predicate reads the starter: denied, not evaluated
	// over an empty scope.
	err := srv.authorizeReservedSignal(debugRunMemo(t, wf, nil, starter, signalPolicyScopeMemoKey), v1types.DebugSignal, caller)
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// Same for inputs.
	inputsWF := debugPredicateWorkflow(`!has(inputs.x) && sender.identity.claims.team == "sre"`)
	sre := sender("https://issuer.example.com", "sre-1@example.com", "", map[string]string{"team": "sre"})
	require.NoError(t, srv.authorizeReservedSignal(
		debugRunMemo(t, inputsWF, map[string]*v1types.Value{}, starter), v1types.DebugSignal, sre))
	require.Error(t, srv.authorizeReservedSignal(
		debugRunMemo(t, inputsWF, map[string]*v1types.Value{}, starter, signalPolicyScopeMemoKey), v1types.DebugSignal, sre),
		"`!has(inputs.x)` is true over an empty scope; an unrecorded scope must deny rather than admit")

	// A run with no recorded starter denies a predicate that reads it.
	require.Error(t, srv.authorizeReservedSignal(debugRunMemo(t, wf, nil, nil), v1types.DebugSignal, caller))

	// A scope entry that cannot be decoded denies.
	corrupt := debugRunMemo(t, wf, nil, starter)
	garbage, err := converter.GetDefaultDataConverter().ToPayload([]byte{0xff, 0xff, 0xff})
	require.NoError(t, err)
	corrupt.GetWorkflowExecutionInfo().GetMemo().GetFields()[signalPolicyScopeMemoKey] = garbage
	require.Error(t, srv.authorizeReservedSignal(corrupt, v1types.DebugSignal, caller))
}

func TestADebugPredicateThatReadsNothingOfTheRunNeedsNoScope(t *testing.T) {
	t.Parallel()

	wf := debugPredicateWorkflow(`sender.identity.claims.team == "sre"`)
	entries, err := policyMemoEntries(wf,
		map[string]*v1types.Value{"secretish": v1types.NewLiteral("v")},
		&v1types.WorkloadIdentity{Principal: &v1types.Principal{Issuer: "i", Subject: "s", Claims: v1types.StringClaimValues(map[string]string{"team": "x"})}})
	require.NoError(t, err)

	assert.NotContains(t, entries, signalPolicyScopeMemoKey,
		"no inputs or claims are copied into a memo nothing evaluates")
	assert.Contains(t, entries, debugPolicyMemoKey)

	srv := mustNew(t, nil)
	memo := debugRunMemo(t, wf, nil, nil)
	require.NoError(t, srv.authorizeReservedSignal(memo, v1types.DebugSignal,
		sender("https://i", "s", "", map[string]string{"team": "sre"})))
	require.Error(t, srv.authorizeReservedSignal(memo, v1types.DebugSignal,
		sender("https://i", "s", "", map[string]string{"team": "dev"})))
}

func TestOneRecordedScopeServesSignalsAndDebugTogether(t *testing.T) {
	t.Parallel()

	starter := &v1types.WorkloadIdentity{Principal: &v1types.Principal{Issuer: "https://i", Subject: "s", Claims: v1types.StringClaimValues(map[string]string{"team": "payments"})}}
	inputs := map[string]*v1types.Value{"approver": v1types.NewLiteral("lead")}
	wf := &v1types.Workflow{
		Name:    "both",
		Profile: v1types.CurrentProfile,
		// signals reads only inputs; debug reads only the starter: the one entry
		// must hold both.
		Signals: map[string]*v1types.SignalPolicy{
			"approved": {Allow: `sender.identity.principal == "https://i#" + inputs.approver && sender.identity.claims.team == "x"`},
		},
		Debug: &v1types.SignalPolicy{Allow: `sender.identity.claims.team == run.identity.claims.team`},
	}

	entries, err := policyMemoEntries(wf, inputs, starter)
	require.NoError(t, err)

	scope := &v1types.Scope{}
	require.NoError(t, proto.Unmarshal(entries[signalPolicyScopeMemoKey].([]byte), scope))
	assert.Contains(t, scope.GetInputs(), "approver", "the signal predicate's inputs were dropped when debug joined the scope")
	assert.Equal(t, "payments", scope.GetIdentity().GetPrincipal().GetClaims()["team"].GetStringValue(), "the debug predicate's starter was not recorded")
}

func TestADebugPredicateScopeOverItsBoundRefusesTheRun(t *testing.T) {
	t.Parallel()

	big := map[string]*v1types.Value{"blob": v1types.NewLiteral(string(make([]byte, v1types.MaxSignalPolicyScopeBytes+1)))}

	_, err := policyMemoEntries(
		debugPredicateWorkflow(`inputs.blob == "x" && sender.identity.claims.team == "y"`), big, nil)
	require.Error(t, err, "a debug predicate over a partial copy of its inputs would be a different predicate")

	_, err = policyMemoEntries(debugPredicateWorkflow(`sender.identity.claims.team == "y"`), big, nil)
	require.NoError(t, err, "the bound is spent only where a predicate reads")
}

// manual

func manualPredicateServer(t *testing.T) (*FlowstateServer, *recordingEmitter) {
	t.Helper()

	sink := &recordingEmitter{}
	opts := []Option{WithAudit(recorderFor(t, sink))}

	return mustNew(t, &fakeRunClient{}, opts...), sink
}

func TestAuthorizeManualStartDecidesAPredicateOverTheCallerAndSubmittedInputs(t *testing.T) {
	t.Parallel()

	srv, sink := manualPredicateServer(t)
	ops := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer: "https://issuer.example.com", Subject: "ops@example.com", Claims: map[string]any{"team": "ops"},
	})
	dev := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer: "https://issuer.example.com", Subject: "dev@example.com", Claims: map[string]any{"team": "dev"},
	})

	wf := manualStartWorkflow("manual-predicate", &v1types.ManualTrigger{
		Allow: `sender.identity.claims.team == "ops" && (!has(inputs.target) || inputs.target != "prod")`,
	})
	asOps := func(inputs map[string]*v1types.Value) error {
		return srv.authorizeManualStart(ops, "Run", v1types.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, "id", wf,
			srv.identityFor(ops), "", inputs)
	}

	require.NoError(t, asOps(nil), "a start with no inputs is an empty set, so the predicate sees `!has(inputs.target)`")
	require.NoError(t, asOps(map[string]*v1types.Value{"target": v1types.NewLiteral("staging")}))

	err := asOps(map[string]*v1types.Value{"target": v1types.NewLiteral("prod")})
	require.Error(t, err, "the SUBMITTED inputs are what the predicate reads")
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	err = srv.authorizeManualStart(dev, "Run", v1types.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, "id", wf,
		srv.identityFor(dev), "", nil)
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// Unauthenticated: refused, and audited as every manual-start refusal is.
	err = srv.authorizeManualStart(t.Context(), "Run", v1types.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, "id", wf,
		srv.identityFor(t.Context()), "", nil)
	require.Error(t, err)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	var denies int
	for _, r := range sink.records {
		if r.GetDecision() == v1types.AuditDecision_AUDIT_DECISION_DENY {
			denies++
			assert.Equal(t, v1types.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, r.GetDenyCode())
		}
	}
	assert.Equal(t, 3, denies, "each refusal leaves one DENY under the RPC's own name")
}

func TestAManualPredicateRefusalNamesNoInputOrClaimValue(t *testing.T) {
	t.Parallel()

	const secret = "CLAIMSECRET123"
	srv, _ := manualPredicateServer(t)
	ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer: "https://issuer.example.com", Subject: "ops@example.com", Claims: map[string]any{"n": secret},
	})
	wf := manualStartWorkflow("manual-secret", &v1types.ManualTrigger{Allow: `int(sender.identity.claims["n"]) == 1`})

	err := srv.authorizeManualStart(ctx, "Run", v1types.AuditResourceKind_AUDIT_RESOURCE_KIND_RUN, "id", wf,
		srv.identityFor(ctx), "", nil)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret)
}
