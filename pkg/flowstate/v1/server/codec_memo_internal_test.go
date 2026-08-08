package server

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	common "go.temporal.io/api/common/v1"
	workflow "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/toycodec"
)

// The memo policy this server runs under, stated as a test because it is the
// whole of what makes a payload codec safe to turn on.
//
// # The mechanism
//
// A memo is not exempt from the data converter. The Go SDK encodes memo values
// with the *user's* converter and falls back to the default one only if that
// fails (go.temporal.io/sdk@v1.47.0 internal/internal_workflow_client.go:1998
// encodeMemoValue, reached from getWorkflowMemo at :2023), gated on
// SDKFlagMemoUserDCEncode, which defaults to true (internal/internal_flags.go:56).
// So on a deployment with a payload codec, every memo this server writes is
// ciphertext.
//
// Search attributes behave the opposite way: they are always encoded with
// converter.GetDefaultDataConverter() and never with the user's
// (internal/internal_search_attributes.go:391 and :405), because the cluster
// has to index them. A codec cannot cover them, which is why nothing
// payload-derived may ever be projected into one.
//
// # The policy
//
// Memos are ciphertext at rest, and every read this server performs goes
// through the converter it was constructed with. The two halves below are both
// load bearing. The positive half is the outage this fixes: a server given the
// codec's converter reads tenant, starter, signal policy, workflow name,
// schedule state and heartbeat phase back off codec-written values, so
// [FlowstateServer.ownedBy] answers true for the tenant that owns the run
// rather than hiding every run in the deployment from its owner. The negative
// half is what makes them ciphertext rather than merely encoded: the default
// converter alone reads none of it.
func TestCodecMemosAreReadThroughTheConfiguredConverter(t *testing.T) {
	t.Parallel()

	toy, err := toycodec.New(bytes.Repeat([]byte{0x2a}, 32))
	require.NoError(t, err)
	dc := payloadcodec.Config{Codec: toy}.DataConverter()

	// The server an operator gets from `flow server` on a deployment with a
	// codec configured: the same converter the Temporal client was built with.
	s := New(nil, WithDataConverter(dc))

	// And the server every other test in this package gets, kept alongside so
	// the negative direction is asserted rather than assumed.
	plainServer := New(nil)

	// Exactly what the SDK does with the maps namespaceMemoEntry,
	// starterMemoEntry, signalPolicyMemoEntry and workflowNameMemoEntry return,
	// when the client carries a codec converter.
	encode := func(value any) *common.Payload {
		t.Helper()
		payload, err := dc.ToPayload(value)
		require.NoError(t, err)
		return payload
	}

	policy := map[string]*v1.SignalPolicy{
		"deploy-approved": {Allow: []*v1.SignalPolicyRule{
			{Subject: "https://issuer.example.com#release-bot@example.com"},
		}},
	}
	encodedPolicy, err := proto.Marshal(&v1.Workflow{Signals: policy})
	require.NoError(t, err)

	memo := &common.Memo{Fields: map[string]*common.Payload{
		namespaceMemoKey:    encode("acme"),
		starterMemoKey:      encode(v1.QualifiedSubject("https://issuer.example.com", "release-bot@example.com")),
		signalPolicyMemoKey: encode(encodedPolicy),
		workflowNameMemoKey: encode("deploy"),
	}}

	t.Run("the configured converter reads every memo this server writes", func(t *testing.T) {
		recorded, err := s.memoTenant(memo)
		require.NoError(t, err)
		require.Equal(t, "acme", recorded)

		require.True(t, s.ownedBy("acme", memo),
			"a codec-configured server hid a run from the tenant that owns it")
		require.False(t, s.ownedBy("someone-else", memo),
			"reading through the codec must not weaken the tenancy comparison itself")

		starter, hasStarter, err := s.memoStarter(memo)
		require.NoError(t, err)
		require.True(t, hasStarter)
		require.Equal(t, "https://issuer.example.com#release-bot@example.com", starter)

		declared, hasPolicy, err := s.signalPolicies(memo)
		require.NoError(t, err)
		require.True(t, hasPolicy)
		require.Contains(t, declared, "deploy-approved")
		require.Equal(t, "https://issuer.example.com#release-bot@example.com",
			declared["deploy-approved"].GetAllow()[0].GetSubject())

		require.Equal(t, "deploy", s.workflowNameOf(&workflow.WorkflowExecutionInfo{Memo: memo}))
	})

	t.Run("the configured converter reads a schedule's stored run state", func(t *testing.T) {
		state := &v1.RunState{Workflow: &v1.Workflow{Name: "nightly"}}
		payload, err := dc.ToPayload(state)
		require.NoError(t, err)

		action := &client.ScheduleWorkflowAction{Args: []any{payload}}

		stored := s.storedRunState(action)
		require.NotNil(t, stored, "a codec-configured server could not read back the schedule it wrote")
		require.Equal(t, "nightly", stored.GetWorkflow().GetName())

		// The same argument through the default converter is the silent nil this
		// path answers with for anything it cannot decode, which is exactly why
		// the converter has to be the configured one.
		require.Nil(t, plainServer.storedRunState(action),
			"the default converter decoded codec-written schedule arguments, so this no longer describes the seam")
	})

	t.Run("the configured converter reads a heartbeat phase", func(t *testing.T) {
		details := &common.Payloads{Payloads: []*common.Payload{encode("uploading")}}

		require.Equal(t, "uploading", s.heartbeatPhase(details))
		require.Equal(t, "", plainServer.heartbeatPhase(details),
			"the default converter decoded a codec-written heartbeat detail")
	})

	// The negative direction, which is what makes the memos above ciphertext at
	// rest rather than merely a different encoding. Every one of these is the
	// outage this change exists to prevent: fail-closed is the right direction,
	// and a silent one is still every run hidden from its own tenant.
	t.Run("the default converter alone reads none of it", func(t *testing.T) {
		_, err := plainServer.memoTenant(memo)
		require.Error(t, err,
			"the default converter read a codec-encoded memo, so this no longer describes the seam")

		require.False(t, plainServer.ownedBy("acme", memo),
			"a codec-encoded memo authorized its own tenant, which would mean the decode silently succeeded")

		_, _, err = plainServer.memoStarter(memo)
		require.Error(t, err)

		_, _, err = plainServer.signalPolicies(memo)
		require.Error(t, err)

		require.Equal(t, "", plainServer.workflowNameOf(&workflow.WorkflowExecutionInfo{Memo: memo}),
			"the default converter read a codec-encoded workflow name")
	})

	// Sanity: the plaintext path every unconfigured deployment runs is
	// unaffected, byte for byte, which is what [WithDataConverter]'s zero value
	// promises.
	t.Run("an unconfigured deployment is unchanged", func(t *testing.T) {
		plain, err := converter.GetDefaultDataConverter().ToPayload("acme")
		require.NoError(t, err)

		plainMemo := &common.Memo{Fields: map[string]*common.Payload{namespaceMemoKey: plain}}

		recorded, err := plainServer.memoTenant(plainMemo)
		require.NoError(t, err)
		require.Equal(t, "acme", recorded)
		require.True(t, plainServer.ownedBy("acme", plainMemo))
	})
}
