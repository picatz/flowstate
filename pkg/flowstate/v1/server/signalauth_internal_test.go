package server

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	common "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	v1types "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Unit-level coverage of #206 gap 1's enforcement: [signalPolicies] decoding
// a run's memo, and [authorizeSignal]'s fail-closed and zero-case rules —
// isolated from a real Temporal server so a corrupted memo can be constructed
// directly rather than fought into existence through the SDK.
//
// The end-to-end shape — a real server, a real sender, a signal that actually
// does or does not reach a waiting workflow — lives in signalauth_test.go.

// memoWithSignalPolicy builds a Describe response carrying an encoded signal
// policy, the same wire shape [FlowstateServer.Run] writes.
func memoWithSignalPolicy(t *testing.T, policies map[string]*v1types.SignalPolicy) *workflowservice.DescribeWorkflowExecutionResponse {
	t.Helper()

	encoded, err := proto.Marshal(&v1types.Workflow{Signals: policies})
	require.NoError(t, err)

	payload, err := converter.GetDefaultDataConverter().ToPayload(encoded)
	require.NoError(t, err)

	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Memo: &common.Memo{Fields: map[string]*common.Payload{
				signalPolicyMemoKey: payload,
			}},
		},
	}
}

// memoWithNoSignalPolicy is what a run that declared no policy at all
// carries — the overwhelmingly common case, and indistinguishable from a run
// that predates the field entirely (invariant 10: absent means absent, no
// compatibility arm to reach for).
func memoWithNoSignalPolicy() *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Memo: &common.Memo{Fields: map[string]*common.Payload{}},
		},
	}
}

func sender(issuer, subject, namespace string, claims map[string]string) *v1types.SignalSender {
	return &v1types.SignalSender{
		Identity: &v1types.WorkloadIdentity{
			Issuer:    issuer,
			Subject:   subject,
			Namespace: namespace,
			Claims:    claims,
		},
	}
}

// TestAuthorizeSignalZeroCaseNoMemoKey is invariant 10's own case: a run
// whose memo carries no signal policy at all — because none was declared, or
// because the run predates the field — keeps today's behavior. Every sender
// is authorized.
func TestAuthorizeSignalZeroCaseNoMemoKey(t *testing.T) {
	resp := memoWithNoSignalPolicy()

	err := authorizeSignal(resp, "deploy-approved", sender("https://issuer.example.com", "anybody@example.com", "team-a", nil))
	require.NoError(t, err, "a run with no declared signal policy refused an ordinary sender")
}

// TestAuthorizeSignalZeroCasePerName checks that the opt-in is per signal
// name: a policy declared for one name leaves every other name on this same
// run unconstrained.
func TestAuthorizeSignalZeroCasePerName(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: []*v1types.SignalPolicyRule{
			{Subject: "https://issuer.example.com#release-manager@example.com"},
		}},
	})

	err := authorizeSignal(resp, "cancel", sender("https://issuer.example.com", "anybody@example.com", "team-a", nil))
	require.NoError(t, err, "a policy declared for one signal name constrained a different, undeclared name")
}

// TestAuthorizeSignalAllowsTheDeclaredSubject is the positive direction: the
// exact subject a rule names is authorized.
func TestAuthorizeSignalAllowsTheDeclaredSubject(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: []*v1types.SignalPolicyRule{
			{Subject: "https://issuer.example.com#release-manager@example.com"},
		}},
	})

	err := authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "release-manager@example.com", "team-a", nil))
	require.NoError(t, err, "the declared subject was refused")
}

// TestAuthorizeSignalDeniesEveryoneElse is the negative direction, and the one
// worth having: an authenticated, in-tenant caller who is not the declared
// subject is refused with PermissionDenied — the "all fifty engineers" hole
// #206 names, closed.
func TestAuthorizeSignalDeniesEveryoneElse(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: []*v1types.SignalPolicyRule{
			{Subject: "https://issuer.example.com#release-manager@example.com"},
		}},
	})

	err := authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "some-other-engineer@example.com", "team-a", nil))
	require.Error(t, err, "a sender who is not the declared subject was authorized")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestAuthorizeSignalIssuerQualifiesTheSubject is #215's lesson, restated for
// signal policy: the same subject string, minted by a different issuer, must
// not be authorized. Comparing subject alone would let a second identity
// provider mint the same "release-manager@example.com" string and approve.
func TestAuthorizeSignalIssuerQualifiesTheSubject(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: []*v1types.SignalPolicyRule{
			{Subject: "https://issuer.example.com#release-manager@example.com"},
		}},
	})

	err := authorizeSignal(resp, "deploy-approved",
		sender("https://a-different-issuer.example.com", "release-manager@example.com", "team-a", nil))
	require.Error(t, err, "a different issuer's identically-named subject was authorized")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestAuthorizeSignalAllowsByClaim checks the claim-equality form: a sender
// need not be named individually when a claim identifies the whole group.
func TestAuthorizeSignalAllowsByClaim(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: []*v1types.SignalPolicyRule{
			{Claims: map[string]string{"team": "release-managers"}},
		}},
	})

	err := authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "whoever@example.com", "team-a", map[string]string{
			"team": "release-managers",
		}))
	require.NoError(t, err, "a sender carrying the required claim was refused")

	err = authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "whoever@example.com", "team-a", map[string]string{
			"team": "some-other-team",
		}))
	require.Error(t, err, "a sender carrying the wrong claim value was authorized")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestAuthorizeSignalFailsClosedOnUnreadableMemo proves the fail-closed
// direction CLAUDE.md requires: a memo that is present but cannot be decoded
// must refuse every sender, never fall back to "no policy declared". Reading
// a decode failure as the zero case would be exactly the substitution
// fail-closed forbids — "I could not read the policy" silently becoming "no
// policy exists".
func TestAuthorizeSignalFailsClosedOnUnreadableMemo(t *testing.T) {
	payload, err := converter.GetDefaultDataConverter().ToPayload([]byte("not a valid encoded Workflow"))
	require.NoError(t, err)

	resp := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Memo: &common.Memo{Fields: map[string]*common.Payload{
				signalPolicyMemoKey: payload,
			}},
		},
	}

	err = authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "release-manager@example.com", "team-a", nil))
	require.Error(t, err, "a corrupted signal policy memo authorized a signal instead of refusing it")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err),
		"a corrupted memo must be refused the same way an unauthorized sender is, not with some other code "+
			"that a caller might read as retryable")
}

// TestAuthorizeSignalFailsClosedOnWrongPayloadShape covers the other decode
// failure: the memo field decodes as bytes, but the bytes are not even
// attempted as an encoded Workflow — proving the check is on the actual
// content, not merely on the outer payload envelope succeeding.
func TestAuthorizeSignalFailsClosedOnWrongPayloadShape(t *testing.T) {
	// A payload holding a type FromPayload cannot even coerce into []byte —
	// an integer, say — so the outer decode itself fails.
	payload, err := converter.GetDefaultDataConverter().ToPayload(12345)
	require.NoError(t, err)

	resp := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Memo: &common.Memo{Fields: map[string]*common.Payload{
				signalPolicyMemoKey: payload,
			}},
		},
	}

	err = authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "release-manager@example.com", "team-a", nil))
	require.Error(t, err)
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}
