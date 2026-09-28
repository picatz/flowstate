package envelope

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/local"
)

type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC).UnixNano())
	return c
}

func (c *fakeClock) now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

// TestUnwrapsOfUnseenDataKeysAreRateLimited: a flood of wrapped keys nobody
// made cannot turn into a flood of provider calls.
func TestUnwrapsOfUnseenDataKeysAreRateLimited(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	c := newDecodeCache(0, clock.now, true)
	for range unwrapBurst {
		require.True(t, c.admit())
	}
	require.False(t, c.admit(), "past the burst, an unwrap must wait")
	clock.advance(time.Second)
	for range unwrapRate {
		require.True(t, c.admit())
	}
	require.False(t, c.admit())

	// A worker's keyring is not limited: its unwraps are of history
	// processes holding the keys wrote, and a replay burst is legitimate.
	worker := newDecodeCache(0, clock.now, false)
	for range 2 * unwrapBurst {
		require.True(t, worker.admit())
	}
}

// TestTheNegativeCacheCannotBeFlushed: a flood of distinct refusals does not
// wash out the ones already held.
func TestTheNegativeCacheCannotBeFlushed(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	c := newDecodeCache(0, clock.now, false)
	held := [32]byte{1}
	c.refuse(held, ErrKeyDenied)
	for i := range maxNegativeCacheEntries * 2 {
		c.refuse([32]byte{2, byte(i), byte(i >> 8)}, ErrKeyDenied)
	}
	_, err, ok := c.get(held)
	require.True(t, ok)
	require.ErrorIs(t, err, ErrKeyDenied)
}

type countingUnwraps struct {
	keyprovider.Key
	unwraps atomic.Int64
}

func (k *countingUnwraps) Unwrap(ctx context.Context, w keyprovider.Wrapped, ectx keyprovider.Context) ([]byte, error) {
	k.unwraps.Add(1)
	return k.Key.Unwrap(ctx, w, ectx)
}

// TestEachNamespaceKeepsItsOwnWindow: sharing a cache, a namespace with a one
// minute window re-asks its provider after a minute even though another
// namespace in the keyring keeps data keys for a day. A short window is a
// revocation promise, and another namespace's configuration does not break it.
func TestEachNamespaceKeepsItsOwnWindow(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	cache := newDecodeCache(0, clock.now, false)
	material := bytes.Repeat([]byte{7}, local.KeyBytes)

	codec := func(ns, id string, maxAge time.Duration, key keyprovider.Key) *Codec {
		c, err := New(t.Context(), Options{Binding: ns, Current: id, Keys: []Recipient{{ID: id, Key: key}},
			DataKey: policyWithAge(maxAge), now: clock.now, cache: cache})
		require.NoError(t, err)
		return c
	}
	shortKey, err := local.NewKey(material)
	require.NoError(t, err)
	longKey, err := local.NewKey(material)
	require.NoError(t, err)

	writerShort := codec("short", "s1", time.Minute, shortKey)
	writerLong := codec("long", "l1", 24*time.Hour, longKey)
	short, err := writerShort.Encode([]*commonpb.Payload{{Data: []byte("x")}})
	require.NoError(t, err)
	long, err := writerLong.Encode([]*commonpb.Payload{{Data: []byte("x")}})
	require.NoError(t, err)

	// Readers sharing the cache, counting what reaches the provider.
	countShort, countLong := &countingUnwraps{Key: shortKey}, &countingUnwraps{Key: longKey}
	readerShort := codec("short", "s1", time.Minute, countShort)
	readerLong := codec("long", "l1", 24*time.Hour, countLong)

	// Two minutes on, the short window's writer entry is gone and the
	// reader unwraps; the long window's is still cached.
	clock.advance(2 * time.Minute)
	for _, read := range []struct {
		c *Codec
		p []*commonpb.Payload
	}{{readerShort, short}, {readerLong, long}} {
		_, err = read.c.Decode(read.p)
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, countShort.unwraps.Load(), "the short window's data key outlived its minute")
	require.EqualValues(t, 0, countLong.unwraps.Load(), "the long window's data key was dropped early")

	// And what the reader itself cached expires by the same minute.
	clock.advance(2 * time.Minute)
	_, err = readerShort.Decode(short)
	require.NoError(t, err)
	_, err = readerLong.Decode(long)
	require.NoError(t, err)
	require.EqualValues(t, 2, countShort.unwraps.Load(), "a data key the short namespace read outlived its minute")
	require.EqualValues(t, 0, countLong.unwraps.Load())
}

func policyWithAge(d time.Duration) *v1.PayloadDataKeyPolicy {
	return &v1.PayloadDataKeyPolicy{MaxAge: durationpb.New(d)}
}

// slowWraps takes delay to wrap, or gives up when its context does.
type slowWraps struct {
	keyprovider.Key
	delay time.Duration
}

func (k slowWraps) Wrap(ctx context.Context, dk []byte, ectx keyprovider.Context) (keyprovider.Wrapped, error) {
	select {
	case <-time.After(k.delay):
		return k.Key.Wrap(ctx, dk, ectx)
	case <-ctx.Done():
		return keyprovider.Wrapped{}, ctx.Err()
	}
}

// TestEachWrapHasItsOwnDeadline: a data key wrapped to a primary and an escrow
// key asks two providers, and each may take what the timeout allows one call.
// Sharing one deadline would let a slow primary spend the escrow's budget and
// fail a rollover in which every call was within its bound.
func TestEachWrapHasItsOwnDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		primary, err := local.Parse(local.Generate())
		require.NoError(t, err)
		escrow, err := local.Parse(local.Generate())
		require.NoError(t, err)

		const timeout = time.Second
		_, err = New(t.Context(), Options{
			Binding:         "ns",
			Current:         "k1",
			Keys:            []Recipient{{ID: "k1", Key: slowWraps{Key: primary, delay: timeout * 3 / 4}}},
			Escrow:          []Recipient{{ID: "e1", Key: slowWraps{Key: escrow, delay: timeout * 3 / 4}}},
			ProviderTimeout: timeout,
		})
		require.NoError(t, err, "the escrow wrap inherited the deadline the primary spent")
	})
}

// TestAnUnsetCacheSizeAsksForTheDefault: the keyring shares one cache, sized
// by the most generous namespace, and a namespace that sets no size is asking
// for the default rather than for nothing.
func TestAnUnsetCacheSizeAsksForTheDefault(t *testing.T) {
	t.Parallel()

	cfg, err := ParseConfig([]byte(`
namespaces:
  small:
    current: s1
    keys: [{id: s1, env: KEY_S}]
    data_key: {decode_cache_entries: 1}
  plain:
    current: p1
    keys: [{id: p1, env: KEY_P}]
`))
	require.NoError(t, err)
	keys := map[string]string{"KEY_S": string(local.Generate()), "KEY_P": string(local.Generate())}
	kr, err := Open(t.Context(), cfg, OpenOptions{Getenv: func(k string) string { return keys[k] }})
	require.NoError(t, err)
	small, _ := kr.Codec("small")
	require.Equal(t, DefaultDecodeCacheEntries, small.cache.capacity, "an unset size shrank the shared cache")
}

// TestStartupWrapsHoldToTheCallersDeadline: each wrap has its own timeout,
// and all of them stay under the context New was given, so opening a keyring
// against a stalled provider ends when the caller's bound does.
func TestStartupWrapsHoldToTheCallersDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		primary, err := local.Parse(local.Generate())
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		_, err = New(ctx, Options{
			Binding:         "ns",
			Current:         "k1",
			Keys:            []Recipient{{ID: "k1", Key: slowWraps{Key: primary, delay: time.Minute}}},
			ProviderTimeout: 30 * time.Second,
		})
		require.Error(t, err)
		require.LessOrEqual(t, time.Since(start), time.Second, "startup outlived the caller's deadline")
	})
}

// clockedWraps wraps normally until failing is set; then it spends advance on
// the clock, as a provider timing out would, and refuses.
type clockedWraps struct {
	keyprovider.Key
	clock   *fakeClock
	advance time.Duration
	failing atomic.Bool
}

func (k *clockedWraps) Wrap(ctx context.Context, dk []byte, ectx keyprovider.Context) (keyprovider.Wrapped, error) {
	if !k.failing.Load() {
		return k.Key.Wrap(ctx, dk, ectx)
	}
	k.clock.advance(k.advance)
	return keyprovider.Wrapped{}, keyprovider.ErrUnavailable
}

// TestAFailedRolloverDoesNotSealPastTheGrace: a rollover that starts inside
// stale_grace and fails after the grace has passed must not seal with the old
// data key, since the revocation bound is max_age + stale_grace of real time,
// not of the moment the rollover began.
func TestAFailedRolloverDoesNotSealPastTheGrace(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	primary, err := local.Parse(local.Generate())
	require.NoError(t, err)
	key := &clockedWraps{Key: primary, clock: clock, advance: 30 * time.Second}

	o := Options{
		Binding: "ns",
		Current: "k1",
		Keys:    []Recipient{{ID: "k1", Key: key}},
		DataKey: &v1.PayloadDataKeyPolicy{MaxAge: durationpb.New(time.Minute), StaleGrace: durationpb.New(time.Minute)},
	}
	SetClock(&o, clock.now)
	c, err := New(t.Context(), o)
	require.NoError(t, err)

	// Past max_age, inside the grace by ten seconds.
	clock.advance(time.Minute + 50*time.Second)
	key.failing.Store(true)

	_, err = c.Encode([]*commonpb.Payload{{Data: []byte("x")}})
	require.Error(t, err, "sealed with the old data key after the grace had passed during the rollover")
}

// TestConcurrentSealsReserveTheBudget: callers arriving together as a data
// key reaches max_messages cannot all pass the bound, since each reserves its
// payload before sealing; the key seals exactly as many as it allows.
func TestConcurrentSealsReserveTheBudget(t *testing.T) {
	t.Parallel()

	p := dataKeyPolicy{maxAge: time.Hour, maxMessages: 3, maxBytes: 1 << 20}
	k := &activeKey{created: time.Now()}

	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			if k.fresh(p, time.Now(), 10) {
				admitted.Add(1)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 3, admitted.Load(), "concurrent seals passed max_messages together")
	require.EqualValues(t, 3, k.messages.Load())
	require.EqualValues(t, 30, k.bytes.Load())

	// A reservation that is not sealed is given back.
	k.release(10)
	require.True(t, k.fresh(p, time.Now(), 10))

	// And the byte bound is reserved the same way.
	byBytes := &activeKey{created: time.Now()}
	bp := dataKeyPolicy{maxAge: time.Hour, maxMessages: 100, maxBytes: 25}
	require.True(t, byBytes.fresh(bp, time.Now(), 20))
	require.False(t, byBytes.fresh(bp, time.Now(), 10))
	require.EqualValues(t, 1, byBytes.messages.Load(), "a refused reservation kept its message")
}
