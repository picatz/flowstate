package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// legacyManualTrigger decodes real wire bytes an earlier release wrote for
// `manual: allowed_principals: [...]`: field 3, a repeated string, now reserved.
func legacyManualTrigger(t *testing.T) *v1.ManualTrigger {
	t.Helper()

	raw := protowire.AppendTag(nil, 3, protowire.BytesType)
	raw = protowire.AppendString(raw, "https://issuer.example.com#ops")

	manual := &v1.ManualTrigger{}
	require.NoError(t, proto.Unmarshal(raw, manual))
	require.NotEmpty(t, manual.ProtoReflect().GetUnknown(), "the legacy field must survive as an unknown field")

	return manual
}

// legacySignalPolicy decodes real wire bytes for a rule-list policy with
// `distinct_from_starter: true`: field 1 a rule message, field 2 a varint.
func legacySignalPolicy(t *testing.T) *v1.SignalPolicy {
	t.Helper()

	rule := protowire.AppendTag(nil, 1, protowire.BytesType)
	rule = protowire.AppendString(rule, "https://issuer.example.com#approver")

	raw := protowire.AppendTag(nil, 1, protowire.BytesType)
	raw = protowire.AppendBytes(raw, rule)
	raw = protowire.AppendTag(raw, 2, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 1)

	policy := &v1.SignalPolicy{}
	require.NoError(t, proto.Unmarshal(raw, policy))
	require.NotEmpty(t, policy.ProtoReflect().GetUnknown(), "the legacy fields must survive as unknown fields")

	return policy
}

// TestALegacyManualBlockIsRefusedNotReadAsOpen is the fail-closed line for a
// workflow decoded from bytes frozen before `allowed_principals` was retired:
// the block reads as having no predicate, which would admit every authenticated
// caller where it used to admit only the listed principals.
func TestALegacyManualBlockIsRefusedNotReadAsOpen(t *testing.T) {
	t.Parallel()

	manual := legacyManualTrigger(t)
	require.Error(t, v1.CheckManualTrigger(manual))

	workflow := &v1.Workflow{Name: "legacy", Triggers: &v1.Triggers{Manual: manual}}
	caller := &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://issuer.example.com", Subject: "anyone"}}
	err := v1.CheckManualStart(t.Context(), workflow, caller, principal.Qualified(caller.GetPrincipal().GetIssuer(), caller.GetPrincipal().GetSubject()), "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flow fix")
	assert.NotContains(t, err.Error(), "ops", "a refusal must not echo the retired values")
}

// TestALegacySignalPolicyIsRefusedByEveryDecisionPoint covers a rule-list policy
// decoded from frozen bytes: the shape check and the decision both deny.
func TestALegacySignalPolicyIsRefusedByEveryDecisionPoint(t *testing.T) {
	t.Parallel()

	policy := legacySignalPolicy(t)
	require.Error(t, v1.CheckPolicyShape("signals[\"go\"]", policy))
	require.Error(t, v1.CheckSignalPolicyShape(map[string]*v1.SignalPolicy{"go": policy}))

	sender := &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://issuer.example.com", Subject: "approver"}}
	require.Error(t, v1.SignalPolicyCheck(t.Context(), policy, sender, nil, false, nil))
	require.Error(t, v1.DebugPolicyCheck(t.Context(), policy, sender, nil, false, nil))
}
