package envelope

import (
	"container/list"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
)

// Data key bounds, and their ceilings. The defaults are the AWS Encryption
// SDK's caching guidance scaled for history: a provider call every ten minutes
// per namespace in steady state, and a disabled wrapping key stops working in
// a running process within the same ten minutes.
const (
	DefaultDataKeyMaxAge      = 10 * time.Minute
	DefaultDataKeyMaxMessages = 1 << 20
	DefaultDataKeyMaxBytes    = 64 << 30
	DefaultDecodeCacheEntries = 4096
	DefaultProviderTimeout    = 5 * time.Second
	maxDataKeyAge             = 24 * time.Hour
	maxDataKeyMessages        = 1 << 32
	maxStaleGrace             = time.Hour
	maxDecodeCacheEntries     = 1 << 16
	negativeCacheTTL          = 5 * time.Second
	maxNegativeCacheEntries   = 1024
)

// ErrProviderUnavailable is a data key that could not be wrapped or unwrapped
// because the key provider did not answer. Transient: Temporal retries the
// task that needed it, and nothing is written unsealed meanwhile.
var ErrProviderUnavailable = errors.New("envelope: the key provider is unavailable")

// dataKeyPolicy is a PayloadDataKeyPolicy with its defaults applied.
type dataKeyPolicy struct {
	maxAge, staleGrace time.Duration
	maxMessages        uint64
	maxBytes           uint64
}

func resolvePolicy(p *v1.PayloadDataKeyPolicy) dataKeyPolicy {
	d := dataKeyPolicy{
		maxAge:      DefaultDataKeyMaxAge,
		maxMessages: DefaultDataKeyMaxMessages,
		maxBytes:    DefaultDataKeyMaxBytes,
	}
	if v := p.GetMaxAge().AsDuration(); p.GetMaxAge() != nil && v > 0 {
		d.maxAge = min(v, maxDataKeyAge)
	}
	if v := p.GetMaxMessages(); v > 0 {
		d.maxMessages = min(v, maxDataKeyMessages)
	}
	if v := p.GetMaxBytes(); v > 0 {
		d.maxBytes = v
	}
	if v := p.GetStaleGrace().AsDuration(); p.GetStaleGrace() != nil && v > 0 {
		d.staleGrace = min(v, maxStaleGrace)
	}
	return d
}

// proto is the policy as the status message reports it, defaults filled in.
func (p dataKeyPolicy) proto() *v1.PayloadDataKeyPolicy {
	return &v1.PayloadDataKeyPolicy{
		MaxAge:      durationpb.New(p.maxAge),
		MaxMessages: p.maxMessages,
		MaxBytes:    p.maxBytes,
		StaleGrace:  durationpb.New(p.staleGrace),
	}
}

// activeKey is the data key a codec is sealing under, with its wrapped
// copies. Immutable but for its counters, and replaced whole at rollover, so
// an encode that loaded it keeps a consistent key even as another rolls it.
type activeKey struct {
	dataKey []byte
	wrapped keyprovider.Wrapped
	escrow  []*v1.PayloadEscrowRecipient
	created time.Time

	messages atomic.Uint64
	bytes    atomic.Uint64
}

// fresh reports whether k may seal another payload of size bytes now.
func (k *activeKey) fresh(p dataKeyPolicy, now time.Time, size int) bool {
	return now.Sub(k.created) < p.maxAge && k.withinCounts(p, size)
}

// withinGrace reports whether k may keep sealing because its successor could
// not be wrapped: only on age, and only by the configured grace. The message
// and byte bounds are never stretched.
func (k *activeKey) withinGrace(p dataKeyPolicy, now time.Time, size int) bool {
	return p.staleGrace > 0 && now.Sub(k.created) < p.maxAge+p.staleGrace && k.withinCounts(p, size)
}

func (k *activeKey) withinCounts(p dataKeyPolicy, size int) bool {
	return k.messages.Load() < p.maxMessages && k.bytes.Load()+uint64(size) <= p.maxBytes
}

// use charges one payload of size bytes against k.
func (k *activeKey) use(size int) {
	k.messages.Add(1)
	k.bytes.Add(uint64(size))
}

// newDataKey returns 32 fresh random bytes.
func newDataKey() []byte {
	dk := make([]byte, keyprovider.DataKeyBytes)
	// crypto/rand.Read never fails on supported platforms (Go 1.24+).
	_, _ = rand.Read(dk)
	return dk
}

// decodeCache holds unwrapped data keys for reading, shared by every codec a
// keyring builds, so a process pays one provider round trip per data key it
// reads rather than one per payload. Bounded in entries and in age; an entry
// is keyed by everything its unwrap was bound to, so a hit is exactly the
// answer the provider would have given.
//
// Definitive refusals (denied, unknown key, invalid wrap) are remembered
// briefly too, so a flood of payloads under a revoked key is not a flood of
// provider calls. Unavailability never is: the next payload asks again.
type decodeCache struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	now      func() time.Time
	entries  map[[sha256.Size]byte]*list.Element
	order    *list.List // front is most recently used
	negative map[[sha256.Size]byte]negativeEntry
	flight   singleflight.Group
}

type cacheEntry struct {
	key     [sha256.Size]byte
	dataKey []byte
	expires time.Time
}

type negativeEntry struct {
	err     error
	expires time.Time
}

func newDecodeCache(capacity int, ttl time.Duration, now func() time.Time) *decodeCache {
	if capacity <= 0 {
		capacity = DefaultDecodeCacheEntries
	}
	return &decodeCache{
		capacity: min(capacity, maxDecodeCacheEntries),
		ttl:      ttl,
		now:      now,
		entries:  make(map[[sha256.Size]byte]*list.Element),
		order:    list.New(),
		negative: make(map[[sha256.Size]byte]negativeEntry),
	}
}

// cacheKey is what one unwrap was bound to: the wrapping key, its version,
// the wrapped bytes, and the context, each length-prefixed.
func cacheKey(ectx keyprovider.Context, w keyprovider.Wrapped) [sha256.Size]byte {
	h := sha256.New()
	h.Write(ectx.Bytes())
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], w.Version)
	h.Write(buf[:])
	h.Write(w.Bytes)
	var out [sha256.Size]byte
	h.Sum(out[:0])
	return out
}

// get returns a copy of a cached data key, or a remembered refusal.
func (c *decodeCache) get(key [sha256.Size]byte) ([]byte, error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if neg, ok := c.negative[key]; ok {
		if now.Before(neg.expires) {
			return nil, neg.err, true
		}
		delete(c.negative, key)
	}
	el, ok := c.entries[key]
	if !ok {
		return nil, nil, false
	}
	e := el.Value.(*cacheEntry)
	if !now.Before(e.expires) {
		c.remove(el)
		return nil, nil, false
	}
	c.order.MoveToFront(el)
	return clone(e.dataKey), nil, true
}

// put stores a copy of dataKey under key, evicting the least recently used
// entry when full.
func (c *decodeCache) put(key [sha256.Size]byte, dataKey []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		c.remove(el)
	}
	for c.order.Len() >= c.capacity {
		c.remove(c.order.Back())
	}
	c.entries[key] = c.order.PushFront(&cacheEntry{key: key, dataKey: clone(dataKey), expires: c.now().Add(c.ttl)})
}

// refuse remembers a definitive refusal for a few seconds.
func (c *decodeCache) refuse(key [sha256.Size]byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.negative) >= maxNegativeCacheEntries {
		clear(c.negative)
	}
	c.negative[key] = negativeEntry{err: err, expires: c.now().Add(negativeCacheTTL)}
}

// remove drops an entry and clears its data key.
func (c *decodeCache) remove(el *list.Element) {
	e := c.order.Remove(el).(*cacheEntry)
	clear(e.dataKey)
	delete(c.entries, e.key)
}

// unwrap returns the data key w wraps under ectx, from the cache or from key,
// with concurrent misses for one wrapped key coalesced into one provider call.
func (c *decodeCache) unwrap(key keyprovider.Key, timeout time.Duration, w keyprovider.Wrapped, ectx keyprovider.Context) ([]byte, error) {
	ck := cacheKey(ectx, w)
	if dk, err, ok := c.get(ck); ok {
		return dk, err
	}

	v, err, _ := c.flight.Do(string(ck[:]), func() (any, error) {
		if dk, err, ok := c.get(ck); ok {
			return dk, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		dk, err := key.Unwrap(ctx, w, ectx)
		if err != nil {
			err = classifyProviderError(err)
			if !errors.Is(err, ErrProviderUnavailable) {
				c.refuse(ck, err)
			}
			return nil, err
		}
		if len(dk) != keyprovider.DataKeyBytes {
			clear(dk)
			return nil, fmt.Errorf("%w: the provider returned a data key of the wrong length", ErrAuthentication)
		}
		c.put(ck, dk)
		return dk, nil
	})
	if err != nil {
		return nil, err
	}
	// Every caller of a coalesced unwrap gets its own copy to clear.
	return clone(v.([]byte)), nil
}

// classifyProviderError maps a provider's sentinel onto the envelope's
// refusals. Anything unclassified is treated as unavailable: retried, never
// remembered, and never a reason to report a payload as tampered.
func classifyProviderError(err error) error {
	switch {
	case errors.Is(err, keyprovider.ErrInvalidWrapped):
		return fmt.Errorf("%w: the key provider did not accept the wrapped data key", ErrAuthentication)
	case errors.Is(err, keyprovider.ErrUnknownKey), errors.Is(err, keyprovider.ErrCannotUnwrap):
		return fmt.Errorf("%w: %w", ErrUnknownKey, err)
	case errors.Is(err, keyprovider.ErrDenied):
		return fmt.Errorf("%w: %w", ErrKeyDenied, err)
	default:
		return fmt.Errorf("%w: %w", ErrProviderUnavailable, err)
	}
}

// ErrKeyDenied is a key provider refusing to unwrap: the key is disabled, or
// this process may not use it.
var ErrKeyDenied = errors.New("envelope: the key provider refused the key")

func clone(b []byte) []byte { return append([]byte(nil), b...) }
