package flowstatev1

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAuthorizationPlanAndBoundary(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	approvalPub, approvalPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	issuerPub, issuerPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	holderPub, holderPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	plan := testAuthorizationPlan(now)
	approval, err := SignPlanApproval(plan, "policy:release", now.Add(time.Hour), approvalPriv, now)
	require.NoError(t, err)
	capability, err := IssueExecutionCapability(plan, approval, approvalPub, issuerPriv, "worker", holderPub, []byte("capability-nonce"), now, now.Add(5*time.Minute))
	require.NoError(t, err)
	digest, err := DigestAuthorizationPlan(plan)
	require.NoError(t, err)
	makeAttempt := func(op *PlannedOperation, id string) *OperationAttempt {
		a := &OperationAttempt{PlanDigest: digest, OperationId: op.GetId(), Sequence: op.GetSequence(), Action: op.GetAction(), Resource: op.GetResource(), Parameters: cloneMap(op.GetParameters()), Artifacts: cloneMessages(op.GetArtifacts()), NetworkDestinations: cloneMessages(op.GetNetworkDestinations()), SecretReferences: cloneMessages(op.GetSecretReferences()), Dependencies: cloneMessages(plan.GetDependencies()), PolicyRevision: plan.GetPolicyRevision(), DelegationChain: cloneMessages(plan.GetDelegationChain()), AttemptId: id}
		require.NoError(t, SignOperationAttempt(a, capability, holderPriv))
		return a
	}
	newBoundary := func(at time.Time) *BoundaryAuthorizer {
		return &BoundaryAuthorizer{IssuerKey: issuerPub, Audience: "worker", Now: func() time.Time { return at }}
	}

	t.Run("parameter substitution is denied", func(t *testing.T) {
		a := makeAttempt(plan.GetOperations()[0], "substitute")
		a.Parameters["ref"] = "refs/heads/other"
		require.NoError(t, SignOperationAttempt(a, capability, holderPriv))
		require.ErrorIs(t, newBoundary(now).Authorize(plan, capability, a, "run-1"), ErrAuthorizationDenied)
	})
	t.Run("reordered operations are denied", func(t *testing.T) {
		require.ErrorIs(t, newBoundary(now).Authorize(plan, capability, makeAttempt(plan.GetOperations()[1], "second-first"), "run-1"), ErrAuthorizationDenied)
	})
	t.Run("artifact changes are denied", func(t *testing.T) {
		a := makeAttempt(plan.GetOperations()[0], "artifact-change")
		a.Artifacts[0].Digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		require.NoError(t, SignOperationAttempt(a, capability, holderPriv))
		require.ErrorIs(t, newBoundary(now).Authorize(plan, capability, a, "run-1"), ErrAuthorizationDenied)
	})
	t.Run("partial execution and retries remain authorized", func(t *testing.T) {
		b := newBoundary(now)
		require.NoError(t, b.Authorize(plan, capability, makeAttempt(plan.GetOperations()[0], "first"), "run-1"))
		require.NoError(t, b.Authorize(plan, capability, makeAttempt(plan.GetOperations()[0], "retry"), "run-1"))
	})
	t.Run("approved compensation can unwind out of forward order", func(t *testing.T) {
		require.NoError(t, newBoundary(now).Authorize(plan, capability, makeAttempt(plan.GetOperations()[2], "undo"), "run-1"))
	})
	t.Run("approval and capability expiry fail closed", func(t *testing.T) {
		require.ErrorIs(t, newBoundary(now.Add(6*time.Minute)).Authorize(plan, capability, makeAttempt(plan.GetOperations()[0], "late"), "run-1"), ErrAuthorizationDenied)
		require.ErrorIs(t, VerifyPlanApproval(plan, approval, approvalPub, now.Add(2*time.Hour)), ErrAuthorizationDenied)
	})
	t.Run("agent replacement changes the plan digest", func(t *testing.T) {
		changed := proto.Clone(plan).(*AuthorizationPlan)
		changed.Agent = "agent-replacement"
		changedDigest, err := DigestAuthorizationPlan(changed)
		require.NoError(t, err)
		require.NotEqual(t, digest, changedDigest)
	})
	t.Run("capability attempt reuse is denied", func(t *testing.T) {
		b := newBoundary(now)
		a := makeAttempt(plan.GetOperations()[0], "one-shot")
		require.NoError(t, b.Authorize(plan, capability, a, "run-1"))
		require.ErrorIs(t, b.Authorize(plan, capability, a, "run-1"), ErrAuthorizationDenied)
	})
}

func testAuthorizationPlan(now time.Time) *AuthorizationPlan {
	artifact := func() []*Artifact {
		return []*Artifact{{Identity: "oci:deploy", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}
	}
	return &AuthorizationPlan{Version: "v1", Tenant: "tenant-a", Workload: "deploy", Agent: "agent-7", Nonce: []byte("authorization-plan"), ExpiresAt: timestamppb.New(now.Add(2 * time.Hour)), MaximumDuration: durationpb.New(30 * time.Minute), MaximumCostMicrounits: 10_000, PolicyRevision: "policy-git-sha", Profile: &ComputeStorageProfile{CpuMillis: 1000, MemoryBytes: 1 << 30, EphemeralStorageBytes: 1 << 30, ExecutionClass: "isolated"}, Dependencies: []*Dependency{{Identity: "policy-bundle", Revision: "sha256:policy"}}, DelegationChain: []*Delegation{{Delegator: "human:owner", Delegate: "agent-7", ScopeDigest: "sha256:scope"}}, Operations: []*PlannedOperation{
		{Id: "fetch", Sequence: 1, Action: "git.fetch", Resource: "repo:flowstate", Parameters: map[string]string{"ref": "refs/heads/main"}, Artifacts: artifact(), NetworkDestinations: []*NetworkDestination{{Scheme: "https", Host: "github.com", Port: 443}}, SecretReferences: []*SecretReference{{Provider: "vault", Name: "github-token", Version: "3"}}, HighRisk: true},
		{Id: "deploy", Sequence: 2, Action: "deployment.apply", Resource: "cluster:production", Parameters: map[string]string{"namespace": "flowstate"}, Artifacts: artifact(), HighRisk: true},
		{Id: "undo-deploy", Sequence: 3, Action: "deployment.rollback", Resource: "cluster:production", Parameters: map[string]string{"namespace": "flowstate"}, Artifacts: artifact(), CompensationFor: "deploy", HighRisk: true},
	}}
}

func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func cloneMessages[T proto.Message](in []T) []T {
	out := make([]T, len(in))
	for i, value := range in {
		out[i] = proto.Clone(value).(T)
	}
	return out
}
