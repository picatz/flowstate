package secrets

import (
	"fmt"
	"maps"
	"slices"
	"sync"
)

// A Registry holds the secret providers a deployment permits.
//
// It is the extension point: a deployment registers providers for the schemes it
// wants — environment variables and files out of the box, an OS keychain or a
// password manager for local development, a vault or a cloud manager in production
// — and a reference to any other scheme is refused rather than guessed at. A
// deployment that permits no secrets registers none, and every reference fails.
//
// A Registry is safe for concurrent use. Workers register at startup; a [Store]
// takes a snapshot so what it resolves cannot change under a running worker.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]Provider
}

// NewRegistry returns an empty [Registry].
func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]Provider)}
}

// Register adds a provider under the scheme it reports.
//
// Unlike the task registry, this refuses to replace an existing provider. Two
// backends claiming one scheme is a configuration mistake with a security
// consequence — whichever registered last would silently answer every reference
// for that scheme — so it is reported rather than resolved by ordering.
//
// It reports an error for a nil provider, a malformed scheme, or a duplicate, so a
// misconfigured worker fails at startup rather than mid-run.
func (r *Registry) Register(provider Provider) error {
	if provider == nil {
		return fmt.Errorf("secrets: provider must not be nil")
	}

	scheme := provider.Scheme()
	switch {
	case scheme == "":
		return fmt.Errorf("secrets: provider %T reports an empty scheme", provider)
	case len(scheme) > MaxSchemeLen:
		return fmt.Errorf("secrets: provider %T reports a scheme longer than %d characters", provider, MaxSchemeLen)
	case !validScheme(scheme):
		return fmt.Errorf(
			"secrets: provider %T reports scheme %q, which may only contain lowercase letters, digits, and dashes",
			provider, scheme,
		)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.providers[scheme]; ok {
		return fmt.Errorf(
			"secrets: scheme %q is already registered by %T, so %T cannot also claim it",
			scheme, existing, provider,
		)
	}

	r.providers[scheme] = provider

	return nil
}

// MustRegister adds a provider, panicking if it cannot be registered.
//
// It is meant for package initialization, where a duplicate or malformed scheme is
// a programming error rather than a runtime condition.
func (r *Registry) MustRegister(provider Provider) {
	if err := r.Register(provider); err != nil {
		panic(err.Error())
	}
}

// Lookup returns the provider registered for a scheme.
func (r *Registry) Lookup(scheme string) (Provider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	provider, ok := r.providers[scheme]

	return provider, ok
}

// Schemes returns the registered schemes, sorted, so an operator can be shown what
// a deployment permits. It reports schemes, never anything a provider holds.
func (r *Registry) Schemes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return slices.Sorted(maps.Keys(r.providers))
}

// All returns the registered providers, ordered by scheme.
func (r *Registry) All() []Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()

	providers := make([]Provider, 0, len(r.providers))
	for _, scheme := range slices.Sorted(maps.Keys(r.providers)) {
		providers = append(providers, r.providers[scheme])
	}

	return providers
}

// Len returns how many providers are registered.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.providers)
}

// clone returns a copy, so a [Store] is unaffected by later registrations.
func (r *Registry) clone() *Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &Registry{providers: maps.Clone(r.providers)}
}
