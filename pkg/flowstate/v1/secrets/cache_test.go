package secrets

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// countingProvider records how often it is asked, and can be pointed at a
// different value or made to fail between calls, so a test can tell a cache hit
// from a fresh resolution.
type countingProvider struct {
	mu    sync.Mutex
	calls int
	value string
	err   error

	// delay holds a resolution open long enough for concurrent callers to pile up
	// behind it.
	delay time.Duration
}

func (p *countingProvider) Scheme() string { return "test" }

func (p *countingProvider) Resolve(_ context.Context, req Request) (Secret, error) {
	p.mu.Lock()
	p.calls++
	delay, err, value := p.delay, p.err, p.value
	p.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	if err != nil {
		return Secret{}, err
	}

	return NewSecret(req.Ref, value), nil
}

func (p *countingProvider) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.calls
}

func (p *countingProvider) set(value string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.value, p.err = value, err
}

// clock is a manual clock, so expiry is exercised without waiting for it.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *clock {
	return &clock{now: time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)}
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

// newTestCache wraps provider in a cache driven by a manual clock.
func newTestCache(t *testing.T, provider Provider, opts ...CacheOption) (*Cache, *clock) {
	t.Helper()

	cache := NewCache(provider, opts...)
	clk := newClock()
	cache.now = clk.Now

	return cache, clk
}

func Test_Cache_Resolve(t *testing.T) {
	ref := NewRef("test", "key")

	t.Run("a failure is never cached", func(t *testing.T) {
		// Caching a miss would keep a secret invisible for the whole TTL after
		// someone creates it, leaving a worker broken after the problem is fixed.
		provider := &countingProvider{err: &ResolveError{Ref: ref, Err: ErrNotFound}}
		cache, _ := newTestCache(t, provider)

		for range 3 {
			_, err := cache.Resolve(t.Context(), Request{Ref: ref})
			require.ErrorIs(t, err, ErrNotFound)
		}

		require.Equal(t, 3, provider.count(), "each attempt must reach the provider")
		require.Zero(t, cache.Len(), "a failure must not occupy the cache")

		// Once the secret exists, it is visible immediately.
		provider.set("now-present", nil)

		secret, err := cache.Resolve(t.Context(), Request{Ref: ref})
		require.NoError(t, err)
		require.Equal(t, "now-present", secret.Reveal())
	})

	t.Run("a hit does not reach the provider", func(t *testing.T) {
		provider := &countingProvider{value: "cached"}
		cache, _ := newTestCache(t, provider)

		for range 5 {
			secret, err := cache.Resolve(t.Context(), Request{Ref: ref})
			require.NoError(t, err)
			require.Equal(t, "cached", secret.Reveal())
		}

		require.Equal(t, 1, provider.count())
		require.Equal(t, 1, cache.Len())
	})

	t.Run("an entry expires after the TTL", func(t *testing.T) {
		provider := &countingProvider{value: "first"}
		cache, clk := newTestCache(t, provider, WithCacheTTL(time.Minute))

		secret, err := cache.Resolve(t.Context(), Request{Ref: ref})
		require.NoError(t, err)
		require.Equal(t, "first", secret.Reveal())

		// Still inside the TTL: the rotated value is not seen yet.
		provider.set("second", nil)
		clk.advance(59 * time.Second)

		secret, err = cache.Resolve(t.Context(), Request{Ref: ref})
		require.NoError(t, err)
		require.Equal(t, "first", secret.Reveal())
		require.Equal(t, 1, provider.count())

		// Past the TTL: the secret is re-read, which is how rotation takes effect
		// without restarting the worker.
		clk.advance(2 * time.Second)

		secret, err = cache.Resolve(t.Context(), Request{Ref: ref})
		require.NoError(t, err)
		require.Equal(t, "second", secret.Reveal())
		require.Equal(t, 2, provider.count())
	})

	t.Run("an entry exactly at its expiry is treated as expired", func(t *testing.T) {
		provider := &countingProvider{value: "first"}
		cache, clk := newTestCache(t, provider, WithCacheTTL(time.Minute))

		_, err := cache.Resolve(t.Context(), Request{Ref: ref})
		require.NoError(t, err)

		provider.set("second", nil)
		clk.advance(time.Minute)

		secret, err := cache.Resolve(t.Context(), Request{Ref: ref})
		require.NoError(t, err)
		require.Equal(t, "second", secret.Reveal())
	})

	t.Run("a non-positive TTL disables caching", func(t *testing.T) {
		provider := &countingProvider{value: "uncached"}
		cache, _ := newTestCache(t, provider, WithCacheTTL(0))

		for range 3 {
			secret, err := cache.Resolve(t.Context(), Request{Ref: ref})
			require.NoError(t, err)
			require.Equal(t, "uncached", secret.Reveal())
		}

		require.Equal(t, 3, provider.count())
		require.Zero(t, cache.Len())
	})

	t.Run("distinct references are cached separately", func(t *testing.T) {
		provider := &countingProvider{value: "v"}
		cache, _ := newTestCache(t, provider)

		for i := range 3 {
			_, err := cache.Resolve(t.Context(), Request{Ref: NewRef("test", fmt.Sprint(i))})
			require.NoError(t, err)
		}

		require.Equal(t, 3, provider.count())
		require.Equal(t, 3, cache.Len())
	})
}

// Test_Cache_collapsesConcurrentResolutions pins the behavior that makes the cache
// worth having for a network-backed provider: N task executions resolving the same
// secret at once cost one lookup, not N.
func Test_Cache_collapsesConcurrentResolutions(t *testing.T) {
	ref := NewRef("test", "hot")

	t.Run("a cold start costs one provider call", func(t *testing.T) {
		provider := &countingProvider{value: "shared"}
		cache := NewCache(provider, WithCacheTTL(time.Minute))

		// The provider is slow enough that every goroutine is waiting at once,
		// which is exactly the cold-start stampede a worker sees at startup.
		provider.delay = 20 * time.Millisecond

		var wg sync.WaitGroup
		for range 50 {
			wg.Go(func() {
				secret, err := cache.Resolve(context.Background(), Request{Ref: ref})
				require.NoError(t, err)
				require.Equal(t, "shared", secret.Reveal())
			})
		}
		wg.Wait()

		require.Equal(t, 1, provider.count(),
			"concurrent resolutions of one reference must collapse into a single lookup")
	})

	t.Run("expiry does not stampede either", func(t *testing.T) {
		provider := &countingProvider{value: "first"}
		cache, clk := newTestCache(t, provider, WithCacheTTL(time.Minute))

		_, err := cache.Resolve(t.Context(), Request{Ref: ref})
		require.NoError(t, err)
		require.Equal(t, 1, provider.count())

		// Every entry created together expires together, so the TTL boundary is a
		// second stampede if it is not collapsed.
		provider.delay = 20 * time.Millisecond
		clk.advance(2 * time.Minute)

		var wg sync.WaitGroup
		for range 50 {
			wg.Go(func() {
				_, err := cache.Resolve(context.Background(), Request{Ref: ref})
				require.NoError(t, err)
			})
		}
		wg.Wait()

		require.Equal(t, 2, provider.count(), "one re-read, not fifty")
	})

	t.Run("a failure is still not cached under concurrency", func(t *testing.T) {
		provider := &countingProvider{err: &ResolveError{Ref: ref, Err: ErrNotFound}}
		cache := NewCache(provider, WithCacheTTL(time.Minute))

		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				_, err := cache.Resolve(context.Background(), Request{Ref: ref})
				require.ErrorIs(t, err, ErrNotFound)
			})
		}
		wg.Wait()

		require.Zero(t, cache.Len())
	})
}

func Test_NewCache_nilProvider(t *testing.T) {
	// A cache with nothing behind it could only ever fail, so it is refused at
	// construction rather than panicking on the first resolve.
	require.PanicsWithValue(t, "secrets: NewCache requires a provider", func() {
		NewCache(nil)
	})
}

func Test_Cache_bounds(t *testing.T) {
	t.Run("the cache does not grow past its limit", func(t *testing.T) {
		// A workflow naming an unbounded set of references must not be able to grow
		// the cache without limit.
		provider := &countingProvider{value: "v"}
		cache, _ := newTestCache(t, provider, WithCacheMaxEntries(8))

		for i := range 100 {
			_, err := cache.Resolve(t.Context(), Request{Ref: NewRef("test", fmt.Sprint(i))})
			require.NoError(t, err)
		}

		require.LessOrEqual(t, cache.Len(), 8)
		require.Equal(t, 100, provider.count())
	})

	t.Run("expired entries are reclaimed before anything live is evicted", func(t *testing.T) {
		provider := &countingProvider{value: "v"}
		cache, clk := newTestCache(t, provider, WithCacheMaxEntries(4), WithCacheTTL(time.Minute))

		for i := range 4 {
			_, err := cache.Resolve(t.Context(), Request{Ref: NewRef("test", fmt.Sprint(i))})
			require.NoError(t, err)
		}
		require.Equal(t, 4, cache.Len())

		// Everything expires, so inserting one more clears the lot rather than
		// evicting a live entry.
		clk.advance(2 * time.Minute)

		_, err := cache.Resolve(t.Context(), Request{Ref: NewRef("test", "fresh")})
		require.NoError(t, err)
		require.Equal(t, 1, cache.Len())
	})

	t.Run("a non-positive limit restores the default", func(t *testing.T) {
		cache := NewCache(&countingProvider{value: "v"}, WithCacheMaxEntries(0))
		require.Equal(t, DefaultCacheMaxEntries, cache.max)
	})
}

func Test_Cache_Forget(t *testing.T) {
	ref := NewRef("test", "key")

	t.Run("Forget drops one entry", func(t *testing.T) {
		provider := &countingProvider{value: "first"}
		cache, _ := newTestCache(t, provider)

		_, err := cache.Resolve(t.Context(), Request{Ref: ref})
		require.NoError(t, err)

		provider.set("second", nil)
		cache.Forget(Request{Ref: ref})

		secret, err := cache.Resolve(t.Context(), Request{Ref: ref})
		require.NoError(t, err)
		require.Equal(t, "second", secret.Reveal(), "a forgotten secret is re-read at once")
	})

	t.Run("Forget on an absent reference is harmless", func(t *testing.T) {
		cache, _ := newTestCache(t, &countingProvider{value: "v"})
		cache.Forget(Request{Ref: ref})
		require.Zero(t, cache.Len())
	})

	t.Run("Clear drops everything", func(t *testing.T) {
		provider := &countingProvider{value: "v"}
		cache, _ := newTestCache(t, provider)

		for i := range 3 {
			_, err := cache.Resolve(t.Context(), Request{Ref: NewRef("test", fmt.Sprint(i))})
			require.NoError(t, err)
		}
		require.Equal(t, 3, cache.Len())

		cache.Clear()
		require.Zero(t, cache.Len())
	})
}

func Test_Cache_Scheme(t *testing.T) {
	// The cache stands in for the provider it wraps, so it must report the same
	// scheme or a store would register it under the wrong name.
	cache := NewCache(&countingProvider{})
	require.Equal(t, "test", cache.Scheme())

	store, err := NewStore(cache)
	require.NoError(t, err)
	require.Equal(t, []string{"test"}, store.Schemes())
}

func Test_Cache_concurrentUse(t *testing.T) {
	provider := &countingProvider{value: "shared"}
	cache := NewCache(provider, WithCacheTTL(time.Minute), WithCacheMaxEntries(4))

	// Many task executions resolving through one cache at once, including eviction
	// pressure from more references than the cache holds. Run under -race.
	var wg sync.WaitGroup

	for i := range 64 {
		wg.Go(func() {
			ref := NewRef("test", fmt.Sprint(i%8))

			secret, err := cache.Resolve(context.Background(), Request{Ref: ref})
			require.NoError(t, err)
			require.Equal(t, "shared", secret.Reveal())

			if i%3 == 0 {
				cache.Forget(Request{Ref: ref})
			}
			_ = cache.Len()
		})
	}

	wg.Wait()

	require.LessOrEqual(t, cache.Len(), 4)
}
