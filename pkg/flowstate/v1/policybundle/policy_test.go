package policybundle

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type memoryStore struct {
	bundles map[string]*flowstatev1.PolicyBundle
	err     error
}

func (s *memoryStore) Get(_ context.Context, d string) (*flowstatev1.PolicyBundle, error) {
	if s.err != nil {
		return nil, s.err
	}
	b, ok := s.bundles[d]
	if !ok {
		return nil, errors.New("not found")
	}
	return b, nil
}

type fixture struct {
	manager *Manager
	store   *memoryStore
	keys    map[string]ed25519.PrivateKey
	now     time.Time
}

func newFixture(t *testing.T, nodes ...*FleetNode) fixture {
	t.Helper()
	pub1, key1, e := ed25519.GenerateKey(nil)
	require.NoError(t, e)
	pub2, key2, e := ed25519.GenerateKey(nil)
	require.NoError(t, e)
	s := &memoryStore{bundles: map[string]*flowstatev1.PolicyBundle{}}
	root := RootTrust{Authorities: map[string]Authority{"security": {pub1, "security"}, "service-owner": {pub2, "owner"}}, Requirements: map[string]ApprovalRequirement{"sensitive": {Threshold: 2, DistinctGroups: 2}}}
	return fixture{NewManager(root, s, nodes), s, map[string]ed25519.PrivateKey{"security": key1, "service-owner": key2}, time.Unix(1700000000, 0)}
}
func bundle(t *testing.T, name, revision string, allow bool) *flowstatev1.PolicyBundle {
	t.Helper()
	b, e := Canonicalize(&flowstatev1.PolicyBundle{Name: name, Revision: revision, PolicyClass: "sensitive", Rules: []*flowstatev1.PolicyRule{{Name: "access", Revision: revision, Action: "read", Resource: "document", Condition: `request.subject == "alice"`, Allow: allow}}, DependencyRevisions: map[string]string{"claims": "v1"}})
	require.NoError(t, e)
	return b
}
func (f fixture) approval(t *testing.T, b *flowstatev1.PolicyBundle) *flowstatev1.PolicyApproval {
	t.Helper()
	var sigs []*flowstatev1.PolicySignature
	for _, id := range []string{"security", "service-owner"} {
		s, e := Sign(b, id, f.keys[id], f.now)
		require.NoError(t, e)
		sigs = append(sigs, s)
	}
	a, e := f.manager.Approve(b, sigs, f.now)
	require.NoError(t, e)
	return a
}
func activation(a *flowstatev1.PolicyApproval, now time.Time, percent uint32) *flowstatev1.PolicyActivation {
	return &flowstatev1.PolicyActivation{ActivationId: "rollout-1", ApprovedDigest: a.BundleDigest, AppliedDigest: a.BundleDigest, CanaryPercent: percent, StartsAt: timestamppb.New(now.Add(-time.Minute)), ExpiresAt: timestamppb.New(now.Add(time.Hour))}
}

func TestValidateSimulationReplayAndSemanticDiff(t *testing.T) {
	deny := bundle(t, "docs", "1", false)
	allow := bundle(t, "docs", "2", true)
	v := Vocabulary{Actions: map[string]bool{"read": true}, Resources: map[string]bool{"document": true}, Relations: map[string]bool{}, Claims: map[string]bool{}, Attributes: map[string]bool{}}
	require.True(t, Validate(allow, v).Valid)
	req := &flowstatev1.AuthorizationRequest{Tenant: "a", Subject: "alice", Action: "read", Resource: "document"}
	sim := Simulate(context.Background(), allow, []*flowstatev1.PolicyScenario{{Name: "alice reads", Request: req, WantAllowed: true}})
	require.True(t, sim.Passed)
	require.Equal(t, []string{"access@2"}, sim.Cases[0].Decision.RuleRevisions)
	diff := SemanticDiff(context.Background(), deny, allow, []*flowstatev1.AuthorizationRequest{req})
	require.Len(t, diff.NewlyAllowed, 1)
	require.Empty(t, diff.NewlyDenied)
}

func TestStaleApprovalRefused(t *testing.T) {
	f := newFixture(t, &FleetNode{ID: "n", Tenant: "a"})
	b := bundle(t, "docs", "1", true)
	a := f.approval(t, b)
	f.store.bundles[b.Digest] = bundle(t, "docs", "changed", true)
	require.ErrorIs(t, f.manager.Apply(context.Background(), a, activation(a, f.now, 100), f.now), ErrUnapprovedDigest)
}

func TestSignerRemovalInvalidatesApproval(t *testing.T) {
	f := newFixture(t)
	b := bundle(t, "docs", "1", true)
	a := f.approval(t, b)
	f.store.bundles[b.Digest] = b
	delete(f.manager.root.Authorities, "service-owner")
	require.ErrorContains(t, f.manager.Apply(context.Background(), a, activation(a, f.now, 100), f.now), "approval no longer valid")
}

func TestPartialFleetRolloutFailsClosed(t *testing.T) {
	nodes := []*FleetNode{{ID: "a", Tenant: "t"}, {ID: "b", Tenant: "t"}, {ID: "c", Tenant: "t"}, {ID: "d", Tenant: "t"}}
	f := newFixture(t, nodes...)
	b := bundle(t, "docs", "1", true)
	a := f.approval(t, b)
	f.store.bundles[b.Digest] = b
	require.NoError(t, f.manager.Apply(context.Background(), a, activation(a, f.now, 50), f.now))
	allowed, denied := 0, 0
	for _, n := range nodes {
		d := f.manager.Decision(context.Background(), n.ID, &flowstatev1.AuthorizationRequest{Subject: "alice", Action: "read", Resource: "document"}, f.now)
		if d.Allowed {
			allowed++
		} else {
			denied++
		}
	}
	require.Positive(t, allowed)
	require.Positive(t, denied)
}

func TestRollbackAndStoreUnavailable(t *testing.T) {
	f := newFixture(t, &FleetNode{ID: "n", Tenant: "t"})
	old := bundle(t, "docs", "1", false)
	a1 := f.approval(t, old)
	f.store.bundles[old.Digest] = old
	require.NoError(t, f.manager.Apply(context.Background(), a1, activation(a1, f.now, 100), f.now))
	next := bundle(t, "docs", "2", true)
	a2 := f.approval(t, next)
	f.store.bundles[next.Digest] = next
	require.NoError(t, f.manager.Apply(context.Background(), a2, activation(a2, f.now, 100), f.now))
	r, e := f.manager.ObserveSafetySignal(&flowstatev1.PolicySafetySignal{Name: "denial spike", Value: 11, RollbackThreshold: 10}, f.now)
	require.NoError(t, e)
	require.Equal(t, old.Digest, r.ToDigest)
	f.store.err = errors.New("down")
	d := f.manager.Decision(context.Background(), "n", &flowstatev1.AuthorizationRequest{Subject: "alice", Action: "read", Resource: "document"}, f.now)
	require.False(t, d.Allowed)
	require.Equal(t, "policy store unavailable", d.Reason)
}

func TestTrustRootIsOutsideGovernedPolicy(t *testing.T) {
	f := newFixture(t)
	require.ErrorIs(t, f.manager.UpdateRoot(bundle(t, "self-authorizing", "1", true)), ErrBootstrapBoundary)
}

func TestBreakGlassRequiresEvidenceAndShortExpiry(t *testing.T) {
	f := newFixture(t)
	b := bundle(t, "docs", "1", true)
	a := f.approval(t, b)
	f.store.bundles[b.Digest] = b
	act := activation(a, f.now, 100)
	act.BreakGlass = true
	require.ErrorContains(t, f.manager.Apply(context.Background(), a, act, f.now), "audit evidence")
	act.AuditEvidence = "incident INC-42 commander approval"
	act.ExpiresAt = timestamppb.New(f.now.Add(5 * time.Hour))
	require.ErrorContains(t, f.manager.Apply(context.Background(), a, act, f.now), "four hours")
}
