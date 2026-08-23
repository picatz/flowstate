package flowstatev1_test

import (
	"errors"
	"testing"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type governanceFunc func(*v1.IdentityAliasMigration) error

func (f governanceFunc) VerifyIdentityMigration(m *v1.IdentityAliasMigration) error { return f(m) }

func identity(issuer, subject, tenant string, attributes map[string]string) *v1.CanonicalIdentity {
	return &v1.CanonicalIdentity{Issuer: issuer, Subject: subject, Tenant: tenant, Kind: v1.PrincipalKind_PRINCIPAL_KIND_HUMAN, Attributes: attributes}
}

func migration(old, next *v1.CanonicalIdentity, now time.Time) *v1.IdentityAliasMigration {
	return &v1.IdentityAliasMigration{Id: "migration-1", OldIdentity: old, NewIdentity: next,
		IssuerEvidence: []byte("signed issuer statement"), EffectiveTime: timestamppb.New(now),
		AffectedTenants: []string{old.GetTenant()}, ApprovedBy: "identity-admin",
		ApprovalReference: "change/123", RollbackDeadline: timestamppb.New(now.Add(time.Hour)), RollbackRequiresApproval: true}
}

func TestMutableAttributesNeverDefineIdentity(t *testing.T) {
	tests := []struct {
		name          string
		first, second *v1.CanonicalIdentity
		equal         bool
	}{
		{"deleted and recreated user", identity("issuer", "object-17", "team", map[string]string{"email": "a@example.com"}), identity("issuer", "object-91", "team", map[string]string{"email": "a@example.com"}), false},
		{"reassigned email", identity("issuer", "user-a", "team", map[string]string{"email": "shared@example.com"}), identity("issuer", "user-b", "team", map[string]string{"email": "shared@example.com"}), false},
		{"renamed repository", identity("issuer", "repository-id-42", "team", map[string]string{"repository_path": "acme/old"}), identity("issuer", "repository-id-42", "team", map[string]string{"repository_path": "acme/new"}), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first, err := v1.IdentityKey(tt.first)
			require.NoError(t, err)
			second, err := v1.IdentityKey(tt.second)
			require.NoError(t, err)
			require.Equal(t, tt.equal, first == second)
		})
	}
}

func TestAliasesSupportHistoryButNeverCurrentAuthority(t *testing.T) {
	now := time.Now().UTC()
	old := identity("issuer", "pairwise-subject", "team", nil)
	next := identity("issuer", "public-subject", "team", nil)
	ledger := v1.NewAliasLedger(governanceFunc(func(*v1.IdentityAliasMigration) error { return nil }), nil)
	require.NoError(t, ledger.Add(migration(old, next, now)))

	oldKey, _ := v1.IdentityKey(old)
	nextKey, _ := v1.IdentityKey(next)
	for _, use := range []v1.AliasUse{v1.AliasUseResourceOwnership, v1.AliasUseRelationship, v1.AliasUseAuditSearch, v1.AliasUsePolicySimulation} {
		resolved, err := ledger.Resolve(old, use, "team", "", now.Add(time.Minute))
		require.NoError(t, err)
		require.Equal(t, nextKey, resolved)
	}
	delegation, err := ledger.Resolve(old, v1.AliasUseDelegation, "team", "", now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, oldKey, delegation)
	authorized, err := ledger.Authorizes(old, next)
	require.NoError(t, err)
	require.False(t, authorized)
	search, err := ledger.SearchKeys(next)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{oldKey, nextKey}, search)
}

func TestIssuerTenantMigrationRequiresExplicitCrossIssuerPolicy(t *testing.T) {
	now := time.Now().UTC()
	old := identity("old-issuer", "subject", "old-tenant", nil)
	next := identity("new-issuer", "subject", "new-tenant", nil)
	record := migration(old, next, now)
	record.AffectedTenants = []string{"old-tenant", "new-tenant"}
	governance := governanceFunc(func(*v1.IdentityAliasMigration) error { return nil })
	require.ErrorIs(t, v1.NewAliasLedger(governance, nil).Add(record), v1.ErrCrossIssuerPolicy)
	require.NoError(t, v1.NewAliasLedger(governance, func(a, b *v1.CanonicalIdentity) bool {
		return a.GetTenant() == "old-tenant" && b.GetTenant() == "new-tenant"
	}).Add(record))
}

func TestMaliciousAliasClaimIsNotSelfAuthorizing(t *testing.T) {
	now := time.Now().UTC()
	record := migration(identity("issuer", "victim", "team", nil), identity("issuer", "attacker", "team", nil), now)
	record.ApprovedBy = "attacker-asserted-admin"
	ledger := v1.NewAliasLedger(governanceFunc(func(*v1.IdentityAliasMigration) error { return errors.New("bad signature") }), nil)
	require.ErrorIs(t, ledger.Add(record), v1.ErrMigrationGovernance)
}

func TestRollbackAfterPartialResourceMigrationIsAtomicAndConstrained(t *testing.T) {
	now := time.Now().UTC()
	old, next := identity("issuer", "old", "team", nil), identity("issuer", "new", "team", nil)
	record := migration(old, next, now)
	record.AffectedResources = []string{"repo:1", "repo:2"}
	ledger := v1.NewAliasLedger(governanceFunc(func(*v1.IdentityAliasMigration) error { return nil }), nil)
	require.NoError(t, ledger.Add(record))
	require.ErrorIs(t, ledger.Rollback(old, false, now.Add(time.Minute)), v1.ErrRollbackConstrained)
	require.NoError(t, ledger.Rollback(old, true, now.Add(time.Minute)))
	oldKey, _ := v1.IdentityKey(old)
	for _, resource := range record.AffectedResources {
		resolved, err := ledger.Resolve(old, v1.AliasUseResourceOwnership, "team", resource, now.Add(2*time.Minute))
		require.NoError(t, err)
		require.Equal(t, oldKey, resolved)
	}
}
