package flowstatev1

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

// IdentityKey returns the opaque, versioned key used by ownership and
// relationship stores. Attributes are deliberately absent from the digest.
func IdentityKey(identity *CanonicalIdentity) (string, error) {
	if err := Validate(identity); err != nil {
		return "", fmt.Errorf("canonical identity: %w", err)
	}
	h := sha256.New()
	for _, component := range []string{identity.GetIssuer(), identity.GetSubject(), identity.GetTenant(), identity.GetKind().String()} {
		fmt.Fprintf(h, "%d:%s", len(component), component)
	}
	return "identity:v1:" + base64.RawURLEncoding.EncodeToString(h.Sum(nil)), nil
}

// AliasUse names the non-authoritative surfaces which may consult history.
type AliasUse uint8

const (
	AliasUseResourceOwnership AliasUse = iota + 1
	AliasUseRelationship
	AliasUseAuditSearch
	AliasUsePolicySimulation
	AliasUseDelegation
)

var (
	ErrAliasNotFound       = errors.New("identity alias not found")
	ErrAliasNotAuthority   = errors.New("historical identity alias is not current authority")
	ErrMigrationGovernance = errors.New("identity alias migration lacks verified governance")
	ErrCrossIssuerPolicy   = errors.New("cross-issuer identity linking requires explicit policy")
	ErrRollbackConstrained = errors.New("identity alias migration cannot be rolled back")
)

// MigrationGovernance verifies either issuer signature evidence or an
// administrator approval against configuration outside the migration record.
type MigrationGovernance interface {
	VerifyIdentityMigration(*IdentityAliasMigration) error
}

// CrossIssuerLinkPolicy makes cross-issuer linking an explicit decision.
type CrossIssuerLinkPolicy func(oldIdentity, newIdentity *CanonicalIdentity) bool

// AliasLedger is an administrator-managed, concurrency-safe migration index.
// It never infers links from attributes or claims.
type AliasLedger struct {
	mu          sync.RWMutex
	governance  MigrationGovernance
	crossIssuer CrossIssuerLinkPolicy
	byOld       map[string]aliasEntry
}

type aliasEntry struct {
	record *IdentityAliasMigration
	newKey string
}

func NewAliasLedger(governance MigrationGovernance, crossIssuer CrossIssuerLinkPolicy) *AliasLedger {
	return &AliasLedger{governance: governance, crossIssuer: crossIssuer, byOld: make(map[string]aliasEntry)}
}

// Add verifies and atomically publishes a migration. Evidence and approval
// strings are never trusted merely because they are present in the message.
func (l *AliasLedger) Add(record *IdentityAliasMigration) error {
	if err := Validate(record); err != nil {
		return fmt.Errorf("identity alias migration: %w", err)
	}
	if l.governance == nil || l.governance.VerifyIdentityMigration(record) != nil {
		return ErrMigrationGovernance
	}
	if record.GetOldIdentity().GetIssuer() != record.GetNewIdentity().GetIssuer() &&
		(l.crossIssuer == nil || !l.crossIssuer(record.GetOldIdentity(), record.GetNewIdentity())) {
		return ErrCrossIssuerPolicy
	}
	oldKey, _ := IdentityKey(record.GetOldIdentity())
	newKey, _ := IdentityKey(record.GetNewIdentity())
	if oldKey == newKey {
		return errors.New("identity alias migration does not change the canonical identity")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.byOld[oldKey]; exists {
		return errors.New("identity alias migration already exists for old identity")
	}
	l.byOld[oldKey] = aliasEntry{record: record, newKey: newKey}
	return nil
}

// Resolve returns the current key for historical lookup surfaces. Delegation
// and every other authority check must use the presented canonical key itself.
func (l *AliasLedger) Resolve(identity *CanonicalIdentity, use AliasUse, tenant, resource string, at time.Time) (string, error) {
	key, err := IdentityKey(identity)
	if err != nil {
		return "", err
	}
	if use == AliasUseDelegation {
		return key, nil
	}
	l.mu.RLock()
	entry, ok := l.byOld[key]
	l.mu.RUnlock()
	if !ok {
		return key, nil
	}
	if at.Before(entry.record.GetEffectiveTime().AsTime()) ||
		(tenant != "" && !slices.Contains(entry.record.GetAffectedTenants(), tenant)) ||
		(resource != "" && len(entry.record.GetAffectedResources()) > 0 && !slices.Contains(entry.record.GetAffectedResources(), resource)) {
		return key, nil
	}
	return entry.newKey, nil
}

// Authorizes reports only exact current identity equality. It intentionally
// does not resolve aliases: history is discoverable, not reusable authority.
func (l *AliasLedger) Authorizes(presented, required *CanonicalIdentity) (bool, error) {
	presentedKey, err := IdentityKey(presented)
	if err != nil {
		return false, err
	}
	requiredKey, err := IdentityKey(required)
	if err != nil {
		return false, err
	}
	return presentedKey == requiredKey, nil
}

// Rollback removes a migration atomically, including all tenant/resource
// scopes. Partial migration cannot leave a subset of aliases authoritative.
func (l *AliasLedger) Rollback(oldIdentity *CanonicalIdentity, approved bool, now time.Time) error {
	key, err := IdentityKey(oldIdentity)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.byOld[key]
	if !ok {
		return ErrAliasNotFound
	}
	deadline := entry.record.GetRollbackDeadline()
	if deadline != nil && now.After(deadline.AsTime()) {
		return ErrRollbackConstrained
	}
	if entry.record.GetRollbackRequiresApproval() && !approved {
		return ErrRollbackConstrained
	}
	delete(l.byOld, key)
	return nil
}

// SearchKeys returns both the canonical and historical keys audit storage may
// query, without changing which identity currently authorizes an action.
func (l *AliasLedger) SearchKeys(identity *CanonicalIdentity) ([]string, error) {
	key, err := IdentityKey(identity)
	if err != nil {
		return nil, err
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	keys := []string{key}
	for old, entry := range l.byOld {
		if entry.newKey == key {
			keys = append(keys, old)
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys), nil
}
