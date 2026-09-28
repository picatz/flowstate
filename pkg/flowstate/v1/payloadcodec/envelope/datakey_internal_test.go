package envelope

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
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
	c := newDecodeCache(0, clock.now)
	for range unwrapBurst {
		require.True(t, c.admit())
	}
	require.False(t, c.admit(), "past the burst, an unwrap must wait")
	clock.advance(time.Second)
	for range unwrapRate {
		require.True(t, c.admit())
	}
	require.False(t, c.admit())
}

// TestTheNegativeCacheCannotBeFlushed: a flood of distinct refusals does not
// wash out the ones already held.
func TestTheNegativeCacheCannotBeFlushed(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	c := newDecodeCache(0, clock.now)
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
	cache := newDecodeCache(0, clock.now)
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
