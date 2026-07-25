package secrets

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// Cache defaults, chosen so that a rotated secret is picked up on its own without
// anyone intervening.
const (
	// DefaultCacheTTL bounds how stale a cached secret may be. A minute is about
	// how long a Kubernetes secret update takes to appear in a mounted volume
	// anyway, so a shorter TTL mostly buys extra syscalls.
	DefaultCacheTTL = time.Minute

	// DefaultCacheMaxEntries bounds how many secrets a cache holds, so that a
	// workflow naming an unbounded set of references cannot grow it without
	// limit.
	DefaultCacheMaxEntries = 256
)

// Cache wraps a [Provider] with a bounded, expiring cache. It implements
// [Provider] itself, so it can stand in for the provider it wraps:
//
//	store, err := secrets.NewStore(secrets.NewCache(files))
//
// # What is cached, and what is not
//
// Caching is worth it when resolution costs something. A file read is a syscall
// per use, and a cloud KMS or vault lookup is a network round trip that is rate
// limited and often billed, so both belong behind a cache. Wrapping
// [EnvProvider] is pointless — the environment is already an in-memory map — and
// this package does not do it.
//
// Failures are never cached. A missing secret usually means someone is in the
// middle of creating it, and caching the miss would keep it invisible for the
// whole TTL after it appears. The cost of not caching a failure is a retried
// syscall; the cost of caching one is a worker that stays broken after the
// operator has fixed the problem.
//
// The TTL is what makes rotation work without a restart: an entry is used until it
// expires and then re-read, so a secret replaced in place takes effect within one
// TTL. There is no way to cache forever, because a credential that can never be
// re-read is a credential that can never be rotated.
//
// Concurrent resolutions of the same reference are collapsed into one call to the
// wrapped provider. Without that, every task execution on a worker resolving the
// same secret at startup, or at the moment an entry expires, would each make the
// network round trip the cache exists to avoid.
//
// A Cache is safe for concurrent use.
type Cache struct {
	provider Provider
	ttl      time.Duration
	max      int

	// flight collapses concurrent resolutions of one reference.
	flight singleflight.Group

	// now is the clock, replaced in tests so expiry can be exercised without
	// waiting for it.
	now func() time.Time

	mu sync.Mutex

	// entries is keyed by namespace and reference text, not by the reference
	// itself: a Ref is an interface, and one holding a pointer would compare by
	// identity, so two messages naming the same secret would occupy two entries
	// and neither would ever be found by the other. See cacheKey.
	entries map[string]cacheEntry
}

// cacheEntry is a resolved secret and the time it stops being usable.
type cacheEntry struct {
	secret  Secret
	expires time.Time
}

// CacheOption configures a [Cache].
type CacheOption func(*Cache)

// WithCacheTTL sets how long a resolved secret may be reused. A non-positive TTL
// disables caching entirely, so every resolution reaches the provider; that is a
// supported way to turn a cache off by configuration without restructuring the
// providers.
func WithCacheTTL(ttl time.Duration) CacheOption {
	return func(c *Cache) {
		c.ttl = ttl
	}
}

// WithCacheMaxEntries bounds how many secrets the cache holds. A non-positive
// value restores [DefaultCacheMaxEntries].
func WithCacheMaxEntries(n int) CacheOption {
	return func(c *Cache) {
		if n <= 0 {
			n = DefaultCacheMaxEntries
		}
		c.max = n
	}
}

// NewCache wraps provider in a cache using [DefaultCacheTTL] and
// [DefaultCacheMaxEntries].
//
// It panics if provider is nil, which is a programming error rather than a
// configuration one: a cache with nothing to fall back to could only ever fail.
func NewCache(provider Provider, opts ...CacheOption) *Cache {
	if provider == nil {
		panic("secrets: NewCache requires a provider")
	}

	cache := &Cache{
		provider: provider,
		ttl:      DefaultCacheTTL,
		max:      DefaultCacheMaxEntries,
		now:      time.Now,
		entries:  make(map[string]cacheEntry),
	}

	for _, opt := range opts {
		opt(cache)
	}

	return cache
}

// Scheme implements [Provider], reporting the wrapped provider's scheme.
func (c *Cache) Scheme() string {
	return c.provider.Scheme()
}

// Resolve implements [Provider], returning a cached value when one is still valid
// and otherwise resolving through the wrapped provider.
func (c *Cache) Resolve(ctx context.Context, req Request) (Secret, error) {
	if c.ttl <= 0 {
		return c.provider.Resolve(ctx, req)
	}

	key := cacheKey(req)

	if secret, ok := c.lookup(key); ok {
		return secret, nil
	}

	// Only one caller per key reaches the provider; the rest wait for it and share
	// the result, which is safe because a Secret is immutable.
	resolved, err, _ := c.flight.Do(key, func() (any, error) {
		// Another caller may have populated the entry while this one waited.
		if secret, ok := c.lookup(key); ok {
			return secret, nil
		}

		secret, err := c.provider.Resolve(ctx, req)
		if err != nil {
			// Deliberately not cached: see the type's documentation.
			return Secret{}, err
		}

		c.store(key, secret)

		return secret, nil
	})
	if err != nil {
		return Secret{}, err
	}

	secret, ok := resolved.(Secret)
	if !ok {
		return Secret{}, fmt.Errorf("secrets: cache produced %T, want a Secret", resolved)
	}

	return secret, nil
}

// cacheKey derives the entry key for a request.
//
// The namespace is part of the key, and that is a security boundary rather than a
// performance detail: two tenants may name the same reference and mean different
// secrets, so a key that omitted the namespace would hand one tenant's value to the
// other. The parts are length-prefixed so that no combination of namespace and
// reference can be spelled two ways — without it, namespace "a" with reference
// "b:c" and namespace "a:b" with reference "c" would collide.
func cacheKey(req Request) string {
	namespace, ref := req.Namespace, RefString(req.Ref)

	return fmt.Sprintf("%d:%s|%d:%s", len(namespace), namespace, len(ref), ref)
}

// lookup returns a cached secret that has not expired.
func (c *Cache) lookup(key string) (Secret, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return Secret{}, false
	}

	if !c.now().Before(entry.expires) {
		delete(c.entries, key)
		return Secret{}, false
	}

	return entry.secret, true
}

// store caches a resolved secret, making room first if the cache is full.
func (c *Cache) store(key string, secret Secret) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) >= c.max {
		c.evictLocked()
	}

	c.entries[key] = cacheEntry{
		secret:  secret,
		expires: c.now().Add(c.ttl),
	}
}

// evictLocked makes room for one entry. It drops everything expired first, and if
// that frees nothing it drops the entry closest to expiring, which is the one with
// the least remaining value.
//
// The caller must hold c.mu.
func (c *Cache) evictLocked() {
	now := c.now()

	for ref, entry := range c.entries {
		if !now.Before(entry.expires) {
			delete(c.entries, ref)
		}
	}

	if len(c.entries) < c.max {
		return
	}

	var (
		oldest    string
		oldestExp time.Time
		found     bool
	)

	for key, entry := range c.entries {
		if !found || entry.expires.Before(oldestExp) {
			oldest, oldestExp, found = key, entry.expires, true
		}
	}

	if found {
		delete(c.entries, oldest)
	}
}

// Len returns how many secrets are currently cached, including any that have
// expired but not yet been evicted. It is for tests and for reporting.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.entries)
}

// Forget drops any cached value for a request, so the next resolution reaches the
// provider. Use it when a secret is known to have changed and waiting out the TTL
// is not acceptable.
//
// It forgets one namespace's entry, not every namespace's: a secret rotated for one
// tenant says nothing about another's.
func (c *Cache) Forget(req Request) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.entries, cacheKey(req))
}

// Clear drops every cached value.
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]cacheEntry)
}
