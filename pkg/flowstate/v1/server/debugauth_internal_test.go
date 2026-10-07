package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	common "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	v1types "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Unit-level coverage of the door a durable debug lease arrives at: which
// callers [authorizeReservedSignal] admits, and the fail-closed rules it
// applies that its `signals:` neighbour deliberately does not.
//
// Isolated from a real Temporal server for signalauth_internal_test.go's
// reason: a memo carrying a policy this server would never have written can be
// constructed here and cannot be fought into existence through the SDK.

// memoWithDebugPolicy builds a Describe response carrying an encoded debug
// policy, the wire shape [debugPolicyMemoEntry] writes.
func memoWithDebugPolicy(t *testing.T, policy *v1types.SignalPolicy) *workflowservice.DescribeWorkflowExecutionResponse {
	t.Helper()

	encoded, err := proto.Marshal(&v1types.Workflow{Debug: policy})
	require.NoError(t, err)

	payload, err := converter.GetDefaultDataConverter().ToPayload(encoded)
	require.NoError(t, err)

	protocol, err := converter.GetDefaultDataConverter().ToPayload(currentSignalProtocol)
	require.NoError(t, err)

	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Memo: &common.Memo{Fields: map[string]*common.Payload{
				debugPolicyMemoKey:    payload,
				signalProtocolMemoKey: protocol,
			}},
		},
	}
}

func memoWithCurrentSignalProtocol(t *testing.T) *workflowservice.DescribeWorkflowExecutionResponse {
	t.Helper()

	protocol, err := converter.GetDefaultDataConverter().ToPayload(currentSignalProtocol)
	require.NoError(t, err)

	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Memo: &common.Memo{Fields: map[string]*common.Payload{signalProtocolMemoKey: protocol}},
		},
	}
}

// TestOnlyADeclaredDebugPolicyAdmitsAPauseAsk is the zero case, and it is the
// one place this door's answer is the opposite of the signal door's.
//
// The permitted direction runs first. Without it, every refusal below would
// also be satisfied by a door that refuses everybody, which is a gate that
// looks identical from the outside and is not the one being claimed.
func TestOnlyADeclaredDebugPolicyAdmitsAPauseAsk(t *testing.T) {
	allowed := sender("https://issuer.example.com", "sre-1@example.com", "team-a",
		map[string]string{"role": "sre"})

	declared := memoWithDebugPolicy(t, &v1types.SignalPolicy{Allow: `sender.identity.claims["role"] == "sre"`})

	require.NoError(t,
		mustNew(t, nil).authorizeSignal(declared, v1types.DebugSignal, allowed, nil),
		"a caller matching the declared debug policy may pause the run")
	require.NoError(t,
		mustNew(t, nil).authorizeSignal(declared, v1types.DebugSignal, allowed, nil),
		"and may resume it — both asks are governed by the one stanza")

	// The negative direction, which is the claim that matters: a run that says
	// nothing about debugging is not debuggable, and an ordinary signal on the
	// same run still is.
	silent := memoWithCurrentSignalProtocol(t)

	require.Error(t,
		mustNew(t, nil).authorizeSignal(silent, v1types.DebugSignal, allowed, nil),
		"a run with no `debug:` stanza refused nobody, so anyone could pause production")
	require.NoError(t,
		mustNew(t, nil).authorizeSignal(silent, "deploy-approved", allowed, nil),
		"an ordinary signal on the same run keeps its own fail-open zero case")
}

// TestALegacyRunKeepsItsWorkflowOwnedSignalNamespace: before the protocol
// marker existed, `flowstate_*` was an ordinary signal name. The server must
// keep routing those deliveries even though newly submitted runs reserve the
// same prefix for the engine.
func TestALegacyRunKeepsItsWorkflowOwnedSignalNamespace(t *testing.T) {
	legacy := memoWithNoSignalPolicy()
	caller := sender("https://issuer.example.com", "sre-1@example.com", "team-a", nil)

	require.NoError(t, mustNew(t, nil).authorizeSignal(legacy, v1types.DebugSignal, caller, nil),
		"an absent protocol marker identifies a run submitted before the prefix was reserved")
	require.NoError(t, mustNew(t, nil).authorizeSignal(legacy,
		v1types.ReservedSignalPrefix+"custom", caller, nil),
		"the entire prefix belonged to legacy workflows, not only today's debug spelling")

	policed := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		v1types.DebugSignal: {Allow: `sender.identity.principal == "https://issuer.example.com#somebody-else@example.com"`},
	})
	require.Error(t, mustNew(t, nil).authorizeSignal(policed, v1types.DebugSignal, caller, nil),
		"legacy routing must not bypass an ordinary signal policy declared for the old workflow-owned name")
}

func TestAnUnknownOrUnreadableSignalProtocolRefusesReservedNames(t *testing.T) {
	caller := sender("https://issuer.example.com", "sre-1@example.com", "team-a", nil)

	for name, value := range map[string]any{
		"unknown version": currentSignalProtocol + 1,
		"wrong type":      "one",
	} {
		t.Run(name, func(t *testing.T) {
			protocol, err := converter.GetDefaultDataConverter().ToPayload(value)
			require.NoError(t, err)

			memo := &workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
					Memo: &common.Memo{Fields: map[string]*common.Payload{signalProtocolMemoKey: protocol}},
				},
			}

			require.Error(t, mustNew(t, nil).authorizeSignal(memo, v1types.DebugSignal, caller, nil),
				"a reserved signal must not be guessed onto an unknown protocol")
		})
	}
}

// TestACallerTheDebugPolicyDoesNotNameCannotPause is the sender direction:
// a policy exists, and somebody it does not describe is refused.
func TestACallerTheDebugPolicyDoesNotNameCannotPause(t *testing.T) {
	declared := memoWithDebugPolicy(t, &v1types.SignalPolicy{Allow: `sender.identity.claims["role"] == "sre"`})

	require.Error(t, mustNew(t, nil).authorizeSignal(declared, v1types.DebugSignal,
		sender("https://issuer.example.com", "dev-1@example.com", "team-a",
			map[string]string{"role": "developer"}), nil),
		"a caller carrying the wrong claim may not pause the run")

	require.Error(t, mustNew(t, nil).authorizeSignal(declared, v1types.DebugSignal,
		sender("https://issuer.example.com", "anon@example.com", "team-a", nil), nil),
		"a caller carrying no claims at all may not pause the run")
}

// TestARehearsalIdentityNeverTakesADebugLease: the sender-shape refusal holds
// for a reserved name too. A rehearsal stands in for an approver on a local run
// and attests nobody; it may no more hold a production run than answer a gate.
func TestARehearsalIdentityNeverTakesADebugLease(t *testing.T) {
	declared := memoWithDebugPolicy(t, &v1types.SignalPolicy{Allow: `sender.identity.namespace == "team-a"`})

	local := sender("https://issuer.example.com", "sre-1@example.com", "team-a", nil)
	require.NoError(t, mustNew(t, nil).authorizeSignal(declared, v1types.DebugSignal, local, nil),
		"the same sender is admitted while it is not marked local")

	local.Local = true
	require.Error(t, mustNew(t, nil).authorizeSignal(declared, v1types.DebugSignal, local, nil),
		"a sender marked as a local rehearsal identity may not take a durable debug lease")
}

// TestAReservedNameThisBuildDoesNotKnowIsRefused: the prefix is the engine's,
// and delivering onto a channel nothing reads would report a success that did
// nothing.
func TestAReservedNameThisBuildDoesNotKnowIsRefused(t *testing.T) {
	declared := memoWithDebugPolicy(t, &v1types.SignalPolicy{Allow: `sender.identity.namespace == "team-a"`})

	require.Error(t, mustNew(t, nil).authorizeSignal(declared,
		v1types.ReservedSignalPrefix+"whatever", sender("https://issuer.example.com", "sre-1@example.com", "team-a", nil), nil),
		"a reserved name with no channel behind it is refused rather than delivered")
}

// TestAnUnreadableDebugPolicyRefusesEverybody is the fail-closed rule for
// corruption: "I could not read it" and "it says nothing" are different
// sentences, and only one of them permits a caller to act.
func TestAnUnreadableDebugPolicyRefusesEverybody(t *testing.T) {
	payload, err := converter.GetDefaultDataConverter().ToPayload([]byte{0xff, 0xff, 0xff, 0xff})
	require.NoError(t, err)
	protocol, err := converter.GetDefaultDataConverter().ToPayload(currentSignalProtocol)
	require.NoError(t, err)

	corrupt := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Memo: &common.Memo{Fields: map[string]*common.Payload{
				debugPolicyMemoKey:    payload,
				signalProtocolMemoKey: protocol,
			}},
		},
	}

	require.Error(t, mustNew(t, nil).authorizeSignal(corrupt, v1types.DebugSignal,
		sender("https://issuer.example.com", "sre-1@example.com", "team-a", nil), nil),
		"a debug policy that cannot be decoded authorizes nobody")
}

// TestADebugPolicyThisServerWouldNotHaveWrittenIsRefused covers the shapes
// [debugPolicyMemoEntry] never produces: a present key holding an empty policy,
// and one holding a predicate that does not compile.
//
// Both are refused rather than read, because a memo that decodes to either is
// truncation or a bit flip, and reading "matches everybody" out of one would be
// the widest possible misreading of a policy that fails closed by design.
func TestADebugPolicyThisServerWouldNotHaveWrittenIsRefused(t *testing.T) {
	caller := sender("https://issuer.example.com", "sre-1@example.com", "team-a", nil)

	t.Run("a present key holding nothing", func(t *testing.T) {
		require.Error(t, mustNew(t, nil).authorizeSignal(
			memoWithDebugPolicy(t, nil), v1types.DebugSignal, caller, nil))
	})

	t.Run("a policy with no predicate", func(t *testing.T) {
		require.Error(t, mustNew(t, nil).authorizeSignal(
			memoWithDebugPolicy(t, &v1types.SignalPolicy{}), v1types.DebugSignal, caller, nil))
	})

	t.Run("a predicate that does not compile", func(t *testing.T) {
		require.Error(t, mustNew(t, nil).authorizeSignal(
			memoWithDebugPolicy(t, &v1types.SignalPolicy{Allow: `sender.identity.principal ==`}),
			v1types.DebugSignal, caller, nil))
	})
}

// TestTheDebugPolicyTravelsOnItsOwnMemoKey: a workflow declaring only `debug:`
// must not have to write a signal-policy entry, because a present-but-empty
// signal policy is exactly the corruption `signalPolicies` refuses.
func TestTheDebugPolicyTravelsOnItsOwnMemoKey(t *testing.T) {
	entries, err := policyMemoEntries(&v1types.Workflow{
		Name:    "debug-only",
		Profile: v1types.CurrentProfile,
		Debug:   &v1types.SignalPolicy{Allow: `sender.identity.claims["role"] == "sre"`},
	}, nil, nil)
	require.NoError(t, err)

	require.Contains(t, entries, debugPolicyMemoKey, "the debug policy is recorded")
	require.Equal(t, currentSignalProtocol, entries[signalProtocolMemoKey],
		"every newly submitted run records which reserved-signal protocol it uses")
	require.NotContains(t, entries, signalPolicyMemoKey,
		"and nothing is written for a `signals:` block the workflow does not have")

	// The other direction, so this is not a test about one workflow: a
	// signals-only workflow writes no debug key, and a run with no debug key is
	// not debuggable.
	entries, err = policyMemoEntries(&v1types.Workflow{
		Name:    "signals-only",
		Profile: v1types.CurrentProfile,
		Signals: map[string]*v1types.SignalPolicy{
			"deploy-approved": {Allow: `sender.identity.namespace == "team-a"`},
		},
	}, nil, nil)
	require.NoError(t, err)

	require.Contains(t, entries, signalPolicyMemoKey)
	require.Equal(t, currentSignalProtocol, entries[signalProtocolMemoKey])
	require.NotContains(t, entries, debugPolicyMemoKey)
}
