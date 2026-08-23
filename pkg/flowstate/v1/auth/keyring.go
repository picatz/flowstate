package auth

// The types in this file are deliberately not protobuf messages. A Signer is a
// process boundary which private key material must not cross; making it
// serializable would defeat that boundary.

import (
	"context"
	"crypto"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
)

const DefaultMaxLocalPublicKeys = 32

var (
	ErrGenerationRollback = errors.New("signing generation rollback refused")
	ErrSignerNotPublished = errors.New("active signer is absent from published key set")
)

// Signer is the signing boundary implemented by local keys, cloud KMS clients,
// and HSM clients. Sign receives claims rather than private material; remote
// implementations perform the complete JWS operation in the protected service.
type Signer interface {
	KeyID() string
	Algorithm() jwa.Algorithm
	Sign(context.Context, jwt.ClaimsSet) (string, error)
}

// PublicKey describes one verification key. Public is necessarily safe to
// publish; implementations must never put a private key in this value.
type PublicKey struct {
	ID        string
	Algorithm jwa.Algorithm
	Public    crypto.PublicKey
}

// PublicKeySet is one atomic, fleet-wide view of published verification keys.
type PublicKeySet struct {
	Generation uint64
	Keys       []PublicKey
}

// PublicKeySetProvider loads an atomic public view from a bounded local file,
// configuration service, KMS, or HSM.
type PublicKeySetProvider interface {
	PublicKeySet(context.Context) (PublicKeySet, error)
}

// LocalPublicKeySet is an in-memory provider for bounded configuration sources.
// Its constructor copies input so callers cannot mutate a published snapshot.
type LocalPublicKeySet struct{ set PublicKeySet }

func NewLocalPublicKeySet(generation uint64, keys []PublicKey) (*LocalPublicKeySet, error) {
	if generation == 0 {
		return nil, fmt.Errorf("%w: generation must be positive", ErrInvalidPolicy)
	}
	if len(keys) == 0 || len(keys) > DefaultMaxLocalPublicKeys {
		return nil, fmt.Errorf("%w: public key count %d must be between 1 and %d", ErrInvalidPolicy, len(keys), DefaultMaxLocalPublicKeys)
	}
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key.ID == "" || key.Public == nil {
			return nil, fmt.Errorf("%w: public key needs an id and public material", ErrInvalidPolicy)
		}
		algorithm, _, err := publishValue(key.ID, key.Public)
		if err != nil {
			return nil, err
		}
		if algorithm != key.Algorithm {
			return nil, fmt.Errorf("%w: public key %q algorithm %q does not match %q", ErrInvalidPolicy, truncate(key.ID, 64), key.Algorithm, algorithm)
		}
		if _, ok := seen[key.ID]; ok {
			return nil, fmt.Errorf("%w: duplicate public key id %q", ErrInvalidPolicy, truncate(key.ID, 64))
		}
		seen[key.ID] = struct{}{}
	}
	return &LocalPublicKeySet{set: PublicKeySet{Generation: generation, Keys: slices.Clone(keys)}}, nil
}

func (p *LocalPublicKeySet) PublicKeySet(context.Context) (PublicKeySet, error) {
	return PublicKeySet{Generation: p.set.Generation, Keys: slices.Clone(p.set.Keys)}, nil
}

type KeyEventKind string

const (
	KeyReloaded  KeyEventKind = "reload"
	KeyActivated KeyEventKind = "activation"
	KeyRetired   KeyEventKind = "retirement"
	KeyRefused   KeyEventKind = "refusal"
	KeyPublished KeyEventKind = "publication"
)

// KeyEvent contains identifiers only, never key or assertion material.
type KeyEvent struct {
	Kind       KeyEventKind
	Generation uint64
	KeyID      string
	Reason     string
}

type KeyObserver interface {
	ObserveKeyEvent(context.Context, KeyEvent)
}
type KeyMetrics interface {
	RecordKeyEvent(context.Context, KeyEventKind)
}

type retainedPublicKey struct {
	key      PublicKey
	retireAt time.Time
}

// SigningKeyring atomically coordinates the signer used by servers and workers
// with the public set they advertise. Reload builds and validates a complete
// snapshot before taking the lock, so minting sees either configuration, never
// a mixture.
type SigningKeyring struct {
	mu         sync.RWMutex
	retention  time.Duration
	clock      func() time.Time
	observer   KeyObserver
	metrics    KeyMetrics
	generation uint64
	signer     Signer
	keys       map[string]retainedPublicKey
}

func NewSigningKeyring(retention time.Duration, observer KeyObserver, metrics KeyMetrics) (*SigningKeyring, error) {
	if retention < 0 {
		return nil, fmt.Errorf("%w: key retention must not be negative", ErrInvalidPolicy)
	}
	return &SigningKeyring{retention: retention, clock: time.Now, observer: observer, metrics: metrics, keys: make(map[string]retainedPublicKey)}, nil
}

func (r *SigningKeyring) emit(ctx context.Context, event KeyEvent) {
	if r.observer != nil {
		r.observer.ObserveKeyEvent(ctx, event)
	}
	if r.metrics != nil {
		r.metrics.RecordKeyEvent(ctx, event.Kind)
	}
}

// Reload activates a strictly newer generation. The provider generation must
// match it and contain the active key, making partial fleet configuration fail
// closed. The previous active public key is retained for the configured window.
func (r *SigningKeyring) Reload(ctx context.Context, generation uint64, signer Signer, provider PublicKeySetProvider) error {
	if signer == nil || provider == nil || generation == 0 {
		err := fmt.Errorf("%w: incomplete signing configuration", ErrInvalidPolicy)
		r.emit(ctx, KeyEvent{Kind: KeyRefused, Generation: generation, Reason: err.Error()})
		return err
	}
	set, err := provider.PublicKeySet(ctx)
	if err != nil {
		r.emit(ctx, KeyEvent{Kind: KeyRefused, Generation: generation, KeyID: signer.KeyID(), Reason: "public key load failed"})
		return fmt.Errorf("loading public keys: %w", err)
	}
	r.emit(ctx, KeyEvent{Kind: KeyReloaded, Generation: set.Generation, KeyID: signer.KeyID()})
	if set.Generation != generation || !containsPublicKey(set.Keys, signer.KeyID(), signer.Algorithm()) {
		err := fmt.Errorf("%w: signer %q generation %d, published generation %d", ErrSignerNotPublished, truncate(signer.KeyID(), 64), generation, set.Generation)
		r.emit(ctx, KeyEvent{Kind: KeyRefused, Generation: generation, KeyID: signer.KeyID(), Reason: err.Error()})
		return err
	}
	now := r.clock()
	r.mu.Lock()
	if generation <= r.generation {
		current := r.generation
		r.mu.Unlock()
		err := fmt.Errorf("%w: current %d, proposed %d", ErrGenerationRollback, current, generation)
		r.emit(ctx, KeyEvent{Kind: KeyRefused, Generation: generation, KeyID: signer.KeyID(), Reason: err.Error()})
		return err
	}
	oldSigner := r.signer
	var oldPublic retainedPublicKey
	var hadOldPublic bool
	if oldSigner != nil {
		oldPublic, hadOldPublic = r.keys[oldSigner.KeyID()]
	}
	for id, key := range r.keys {
		if !key.retireAt.IsZero() && !now.Before(key.retireAt) {
			delete(r.keys, id)
			r.emit(ctx, KeyEvent{Kind: KeyRetired, Generation: generation, KeyID: id})
		}
	}
	for _, key := range set.Keys {
		r.keys[key.ID] = retainedPublicKey{key: key}
	}
	if oldSigner != nil && hadOldPublic && oldSigner.KeyID() != signer.KeyID() {
		oldPublic.retireAt = now.Add(r.retention)
		r.keys[oldSigner.KeyID()] = oldPublic
	}
	r.generation, r.signer = generation, signer
	r.mu.Unlock()
	r.emit(ctx, KeyEvent{Kind: KeyActivated, Generation: generation, KeyID: signer.KeyID()})
	return nil
}

func containsPublicKey(keys []PublicKey, id string, alg jwa.Algorithm) bool {
	for _, key := range keys {
		if key.ID == id && key.Algorithm == alg && key.Public != nil {
			return true
		}
	}
	return false
}

// Ready fails when no signer is active or its key is not in this generation's
// published snapshot. It is suitable for a readiness probe.
func (r *SigningKeyring) Ready() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.signer == nil || !containsRetained(r.keys, r.signer.KeyID(), r.signer.Algorithm()) {
		return ErrSignerNotPublished
	}
	return nil
}

func containsRetained(keys map[string]retainedPublicKey, id string, alg jwa.Algorithm) bool {
	key, ok := keys[id]
	return ok && key.key.Algorithm == alg
}

func (r *SigningKeyring) PublicKeySet(ctx context.Context) PublicKeySet {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	keys := make([]PublicKey, 0, len(r.keys))
	for id, key := range r.keys {
		if !key.retireAt.IsZero() && !now.Before(key.retireAt) {
			delete(r.keys, id)
			r.emit(ctx, KeyEvent{Kind: KeyRetired, Generation: r.generation, KeyID: id})
			continue
		}
		keys = append(keys, key.key)
	}
	slices.SortFunc(keys, func(a, b PublicKey) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	r.emit(ctx, KeyEvent{Kind: KeyPublished, Generation: r.generation})
	return PublicKeySet{Generation: r.generation, Keys: keys}
}

// Revoke immediately removes a verification key. Revoking the active key also
// makes readiness fail, so no instance continues minting unverifiable tokens.
func (r *SigningKeyring) Revoke(ctx context.Context, id string) {
	r.mu.Lock()
	delete(r.keys, id)
	generation := r.generation
	r.mu.Unlock()
	r.emit(ctx, KeyEvent{Kind: KeyRetired, Generation: generation, KeyID: id, Reason: "emergency revocation"})
}
