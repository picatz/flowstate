package server

import (
	"strings"
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

	err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved", sender("https://issuer.example.com", "anybody@example.com", "team-a", nil))
	require.NoError(t, err, "a run with no declared signal policy refused an ordinary sender")
}

// TestAuthorizeSignalZeroCasePerName checks that the opt-in is per signal
// name: a policy declared for one name leaves every other name on this same
// run unconstrained.
func TestAuthorizeSignalZeroCasePerName(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: `sender.identity.principal == "https://issuer.example.com#release-manager@example.com"`},
	})

	err := mustNew(t, nil).authorizeSignal(resp, "cancel", sender("https://issuer.example.com", "anybody@example.com", "team-a", nil))
	require.NoError(t, err, "a policy declared for one signal name constrained a different, undeclared name")
}

// TestAuthorizeSignalAllowsTheDeclaredSubject is the positive direction: the
// exact subject a rule names is authorized.
func TestAuthorizeSignalAllowsTheDeclaredSubject(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: `sender.identity.principal == "https://issuer.example.com#release-manager@example.com"`},
	})

	err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "release-manager@example.com", "team-a", nil))
	require.NoError(t, err, "the declared subject was refused")
}

// TestAuthorizeSignalDeniesEveryoneElse is the negative direction, and the one
// worth having: an authenticated, in-tenant caller who is not the declared
// subject is refused with PermissionDenied — the "all fifty engineers" hole
// #206 names, closed.
func TestAuthorizeSignalDeniesEveryoneElse(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: `sender.identity.principal == "https://issuer.example.com#release-manager@example.com"`},
	})

	err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "some-other-engineer@example.com", "team-a", nil))
	require.Error(t, err, "a sender who is not the declared subject was authorized")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestAuthorizeSignalRefusesAVetoFromAnUnadmittedSender is the durable twin of
// the v1 package's TestAVetoFromAnUnadmittedSenderNeverReachesAQuorumLocally.
// A `quorum:` veto counts whoever's delivery reaches the wait, so what keeps a
// stranger from ending an approval is admission, here, before the signal is ever
// sent: mallory's rejection is refused at the door, the two admitted approvers
// are not.
func TestAuthorizeSignalRefusesAVetoFromAnUnadmittedSender(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"release-approved": {Allow: `(sender.identity.principal == "https://issuer.example.com#alice") || (sender.identity.principal == "https://issuer.example.com#bob")`},
	})
	srv := mustNew(t, nil)

	err := srv.authorizeSignal(resp, "release-approved",
		sender("https://issuer.example.com", "mallory", "team-a", nil))
	require.Error(t, err, "a sender the policy does not admit reached a quorum")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	for _, subject := range []string{"alice", "bob"} {
		require.NoError(t, srv.authorizeSignal(resp, "release-approved",
			sender("https://issuer.example.com", subject, "team-a", nil)),
			"the admitted approver %q was refused", subject)
	}
}

// TestAuthorizeSignalIssuerQualifiesTheSubject is #215's lesson, restated for
// signal policy: the same subject string, minted by a different issuer, must
// not be authorized. Comparing subject alone would let a second identity
// provider mint the same "release-manager@example.com" string and approve.
func TestAuthorizeSignalIssuerQualifiesTheSubject(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: `sender.identity.principal == "https://issuer.example.com#release-manager@example.com"`},
	})

	err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
		sender("https://a-different-issuer.example.com", "release-manager@example.com", "team-a", nil))
	require.Error(t, err, "a different issuer's identically-named subject was authorized")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestAuthorizeSignalAllowsByClaim checks the claim-equality form: a sender
// need not be named individually when a claim identifies the whole group.
func TestAuthorizeSignalAllowsByClaim(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: `sender.identity.claims["team"] == "release-managers"`},
	})

	err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "whoever@example.com", "team-a", map[string]string{
			"team": "release-managers",
		}))
	require.NoError(t, err, "a sender carrying the required claim was refused")

	err = mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
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

	err = mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
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

	err = mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "release-manager@example.com", "team-a", nil))
	require.Error(t, err)
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// P2's regression coverage: a memo key that is *present* but decodes to
// something that is not a legitimately declared policy must deny, exactly
// like a key that fails to decode at all. [signalPolicyMemoEntry] — the only
// function that writes this key — never writes it for an empty policy map,
// so "the key is present" and "a non-empty, well-formed policy was
// recorded" are the same fact on every path that legitimately writes it. A
// present key that decodes to nothing is corruption, and the bug this
// closes was reading it as the zero case ("no policy declared") instead —
// the one substitution fail-closed forbids, and the most dangerous shape a
// decode failure can take, because it looks identical to the ordinary,
// overwhelmingly common case of no policy at all.

// TestAuthorizeSignalFailsClosedOnPresentButEmptyPayload is P2's first
// required case: the memo key exists, decodes cleanly as bytes and then as a
// `*v1.Workflow`, but that Workflow declares no signals at all — the shape
// `proto.Unmarshal` produces for zero bytes, for a truncated payload missing
// the signals field, or for any payload that happens to decode to an empty
// message. Before this fix, [signalPolicies] returned `(nil, true, nil)` for
// this — "hasMemo" true, zero policies — and [authorizeSignal] read the
// empty map, found no entry for the signal name, and allowed the sender:
// exactly the substitution "policy exists but corrupted" must never make.
func TestAuthorizeSignalFailsClosedOnPresentButEmptyPayload(t *testing.T) {
	// The wire shape a present-but-empty payload actually takes: an encoded
	// zero-value Workflow, indistinguishable at the proto level from an
	// empty/zero-length payload (proto3's empty message and the zero-length
	// byte string decode identically).
	encoded, err := proto.Marshal(&v1types.Workflow{})
	require.NoError(t, err)
	require.Empty(t, encoded, "an empty Workflow encodes to zero bytes, which is the shape under test")

	payload, err := converter.GetDefaultDataConverter().ToPayload(encoded)
	require.NoError(t, err)

	resp := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Memo: &common.Memo{Fields: map[string]*common.Payload{
				signalPolicyMemoKey: payload,
			}},
		},
	}

	err = mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "some-other-engineer@example.com", "team-a", nil))
	require.Error(t, err,
		"a present-but-empty signal policy payload authorized a sender instead of refusing — the key's "+
			"presence proves a policy was recorded, so an empty decode must deny, not fall through to "+
			"the zero case")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestAuthorizeSignalFailsClosedOnAnUnauthorizingPolicyShape is P2's second
// required case: the memo decodes to a non-empty, structurally well-formed
// Workflow message, but the policy it carries is not something this server
// would ever have written — a declared name with no `allow:` predicate at all
// (what a run frozen by a release that still wrote the retired rule list
// decodes to), which authorizes nobody but also is not "no policy", and
// (separately) a predicate that does not compile, which
// [v1.CheckSignalPolicyShape] refuses for the identical reason
// `flow validate`/`CheckSignalPolicies` refuse it at submit. Both must deny.
func TestAuthorizeSignalFailsClosedOnAnUnauthorizingPolicyShape(t *testing.T) {
	t.Run("a declared name with no allow predicate", func(t *testing.T) {
		resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
			"deploy-approved": {}, // Allow is empty
		})

		err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
			sender("https://issuer.example.com", "release-manager@example.com", "team-a", nil))
		require.Error(t, err, "a policy with no allow predicate authorized a sender instead of refusing")
		require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	})

	t.Run("a predicate that does not compile", func(t *testing.T) {
		resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
			"deploy-approved": {Allow: `sender.identity.principal ==`},
		})

		err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
			sender("https://issuer.example.com", "anybody-at-all@example.com", "team-a", nil))
		require.Error(t, err,
			"a predicate that does not compile was accepted from the memo instead of refused — this "+
				"shape is refused at submit (CheckSignalPolicies) and must be refused identically if it "+
				"somehow reaches a run's memo anyway")
		require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	})
}

// TestAuthorizeSignalZeroCaseStillAllowsWhenTheKeyIsGenuinelyAbsent is the
// control for P2: the fix above must not have widened fail-closed to cover
// the legitimate zero case. A memo with no [signalPolicyMemoKey] entry at
// all — never written, never present — still allows every signal name.
// [TestAuthorizeSignalZeroCaseNoMemoKey] already covers this; restated here,
// beside the corruption cases it must stay distinguishable from, so the two
// are read together rather than trusted to agree from opposite ends of the
// file.
func TestAuthorizeSignalZeroCaseStillAllowsWhenTheKeyIsGenuinelyAbsent(t *testing.T) {
	resp := memoWithNoSignalPolicy()

	err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "anybody-at-all@example.com", "team-a", nil))
	require.NoError(t, err,
		"a memo with no signal-policy key at all must still allow — the zero case must survive the "+
			"present-but-corrupt fix, not be swallowed by it")
}

// Per-run signal authorization (#207's slice 1): a predicate comparing the
// sender with the run's recorded starter, and what it does when nothing was
// recorded.

// memoWithSignalPolicyAndStarter is [memoWithSignalPolicy] plus a
// [starterMemoKey] entry — the shape [starterMemoEntry] writes at submit,
// built directly here rather than through it so a test can construct a memo
// [starterMemoEntry] would never produce (a starter recorded, but no policy;
// or vice versa) when that is exactly the shape under test.
func memoWithSignalPolicyAndStarter(t *testing.T, policies map[string]*v1types.SignalPolicy, starter string) *workflowservice.DescribeWorkflowExecutionResponse {
	t.Helper()

	resp := memoWithSignalPolicy(t, policies)

	payload, err := converter.GetDefaultDataConverter().ToPayload(starter)
	require.NoError(t, err)
	resp.WorkflowExecutionInfo.Memo.Fields[starterMemoKey] = payload

	// And what a predicate reading `run` finds at delivery, recorded the way
	// submit records it: without it the run recorded nothing for the predicate
	// to read and every sender is refused, which would make the negative test
	// below pass for a reason that is not its own.
	issuer, subject, _ := strings.Cut(starter, "#")
	scope, err := signalPolicyScopeMemoEntry(policies, nil, &v1types.WorkloadIdentity{Issuer: issuer, Subject: subject})
	require.NoError(t, err)
	for key, value := range scope {
		scopePayload, err := converter.GetDefaultDataConverter().ToPayload(value)
		require.NoError(t, err)
		resp.WorkflowExecutionInfo.Memo.Fields[key] = scopePayload
	}

	return resp
}

// TestAuthorizeSignalComparingWithTheStarterRefusesTheStartersOwnSignal is the
// negative direction #207's decision record calls out by name: a policy
// requiring separation of duties must refuse the run's own starter, even
// when the starter satisfies every rule in `allow`.
func TestAuthorizeSignalComparingWithTheStarterRefusesTheStartersOwnSignal(t *testing.T) {
	starter := v1types.QualifiedSubject("https://issuer.example.com", "release-manager@example.com")

	resp := memoWithSignalPolicyAndStarter(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {
			Allow: `sender.identity.principal == "` + starter + `" && sender.identity.principal != run.identity.principal`,
		},
	}, starter)

	err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "release-manager@example.com", "team-a", nil))
	require.Error(t, err, "the run's own starter delivered a signal a policy comparing the sender with the starter should have refused")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestAuthorizeSignalComparingWithTheStarterAllowsADistinctSender is the positive
// half of the same policy, in its own test so a check that refused
// everyone would still pass the negative one above.
func TestAuthorizeSignalComparingWithTheStarterAllowsADistinctSender(t *testing.T) {
	starter := v1types.QualifiedSubject("https://issuer.example.com", "requester@example.com")
	approver := v1types.QualifiedSubject("https://issuer.example.com", "release-manager@example.com")

	resp := memoWithSignalPolicyAndStarter(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {
			Allow: `sender.identity.principal == "` + approver + `" && sender.identity.principal != run.identity.principal`,
		},
	}, starter)

	err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "release-manager@example.com", "team-a", nil))
	require.NoError(t, err, "a sender distinct from the run's starter was refused by the starter comparison")
}

// TestAuthorizeSignalComparingWithTheStarterRefusesARunPredatingTheStarterKey
// checks the fail-closed side of #207's decision record: a run whose memo
// has no [starterMemoKey] entry — because it started before this field
// existed — has nothing to compare a sender against, and a policy demanding
// the comparison must refuse rather than treat "unknown" as "distinct".
func TestAuthorizeSignalComparingWithTheStarterRefusesARunPredatingTheStarterKey(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {
			Allow: `sender.identity.principal == "` + v1types.QualifiedSubject("https://issuer.example.com", "release-manager@example.com") + `" && sender.identity.principal != run.identity.principal`,
		},
	}) // no starterMemoKey entry at all

	err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "release-manager@example.com", "team-a", nil))
	require.Error(t, err,
		"a run predating the starter memo key was authorized under a starter comparison instead of "+
			"refused — a run that cannot prove separation must not get it")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestAuthorizeSignalRefusesASenderMatchingClaimsButNotThePrincipal
// checks that the principal comparison is still ANDed with the claim
// comparison rather than treated as satisfied by it: a sender carrying the
// right claim but the wrong principal is refused.
func TestAuthorizeSignalRefusesASenderMatchingClaimsButNotThePrincipal(t *testing.T) {
	resp := memoWithSignalPolicy(t, map[string]*v1types.SignalPolicy{
		"deploy-approved": {Allow: `sender.identity.principal == "` + v1types.QualifiedSubject("https://issuer.example.com", "release-manager@example.com") + `" && sender.identity.claims["team"] == "release-managers"`},
	})

	err := mustNew(t, nil).authorizeSignal(resp, "deploy-approved",
		sender("https://issuer.example.com", "some-other-engineer@example.com", "team-a",
			map[string]string{"team": "release-managers"}))
	require.Error(t, err,
		"a sender carrying the right claim but the wrong subject was authorized — the two are an AND, "+
			"not a fallback to whichever field matches")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestSignalPolicyMemoEntryRecordsThePredicateAsWritten is the shared helper
// both submit paths use, tested directly: the entry it encodes carries the
// predicate verbatim (nothing is resolved at submit any more; what a predicate
// reads of the run is recorded beside it), and it passes the same read-path
// check the server applies when it later decodes this memo back, so the two
// halves of the fail-closed design agree with each other.
func TestSignalPolicyMemoEntryRecordsThePredicateAsWritten(t *testing.T) {
	const expression = `sender.identity.principal == "https://issuer.example.com#release-manager@example.com" && sender.identity.namespace == "release-managers-ns"`

	wf := &v1types.Workflow{
		Name:    "gate",
		Signals: map[string]*v1types.SignalPolicy{"deploy-approved": {Allow: expression}},
	}

	entry, err := signalPolicyMemoEntry(wf)
	require.NoError(t, err)
	require.Contains(t, entry, signalPolicyMemoKey)

	encoded, ok := entry[signalPolicyMemoKey].([]byte)
	require.True(t, ok)

	var decoded v1types.Workflow
	require.NoError(t, proto.Unmarshal(encoded, &decoded))
	require.Equal(t, expression, decoded.GetSignals()["deploy-approved"].GetAllow())

	require.NoError(t, v1types.CheckSignalPolicyShape(decoded.GetSignals()))
}

// TestStarterMemoEntryRecordsTheQualifiedIdentity checks the second shared
// helper submit uses: the starter entry is the same "<issuer>#<subject>"
// form [v1.QualifiedSubject] produces everywhere else, so [FlowstateServer.memoStarter] and
// a policy's predicate compares read identically shaped strings.
func TestStarterMemoEntryRecordsTheQualifiedIdentity(t *testing.T) {
	identity := &v1types.WorkloadIdentity{Issuer: "https://issuer.example.com", Subject: "requester@example.com"}

	entry := starterMemoEntry(identity)
	require.Equal(t,
		v1types.QualifiedSubject("https://issuer.example.com", "requester@example.com"),
		entry[starterMemoKey])
}
