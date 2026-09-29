package envelope

import (
	"container/list"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
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

	// unwrapRate and unwrapBurst bound how often a keyring opened with
	// [OpenOptions.LimitUnwraps] asks providers to unwrap a data key it has
	// not seen. Legitimate reads miss once per data key, which is one per
	// namespace per window per writer; what exceeds this is a flood of
	// wrapped keys nobody made, sent to turn the provider into a quota or
	// audit-log sink. Past it, an unwrap is refused as unavailable, which a
	// codec server's caller retries. Only the codec server is limited: its
	// callers choose what it unwraps, while a worker reads history that
	// processes holding the keys wrote, where a replay burst after a restart
	// is legitimate and a refusal would fail a run.
	unwrapRate  = 200
	unwrapBurst = 2000
)

// ErrProviderUnavailable is a data key that could not be wrapped or unwrapped
// because the key provider did not answer. Transient, and nothing is written
// unsealed meanwhile. It matches [payloadcodec.ErrUnavailable], which is how
// workflow-side decoding tells it from a corrupt payload.
var ErrProviderUnavailable error = &classifiedError{
	msg: "envelope: the key provider is unavailable", class: payloadcodec.ErrUnavailable,
}

// classifiedError is one of this package's sentinels that also matches a
// codec-neutral class in [payloadcodec], so a caller that knows only that
// package can tell a payload this process cannot read from a corrupt one.
type classifiedError struct {
	msg   string
	class error
}

func (e *classifiedError) Error() string { return e.msg }

func (e *classifiedError) Is(target error) bool { return target == e.class }

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

// fresh reports whether k may seal another payload of size bytes now, and if
// so reserves it: see [activeKey.reserve].
func (k *activeKey) fresh(p dataKeyPolicy, now time.Time, size int) bool {
	return now.Sub(k.created) < p.maxAge && k.reserve(p, size)
}

// withinGrace is [activeKey.fresh] for a key whose successor could not be
// wrapped: only its age is stretched, and only by the configured grace. The
// message and byte bounds never are.
func (k *activeKey) withinGrace(p dataKeyPolicy, now time.Time, size int) bool {
	return p.staleGrace > 0 && now.Sub(k.created) < p.maxAge+p.staleGrace && k.reserve(p, size)
}

// reserve charges one payload of size bytes against k if its message and
// byte bounds allow it, atomically, so concurrent seals cannot all pass a
// bound one of them reaches: a key with max_messages 1 seals one payload
// however many callers arrive together. A caller that reserves and then does
// not seal gives it back with [activeKey.release].
func (k *activeKey) reserve(p dataKeyPolicy, size int) bool {
	for {
		m := k.messages.Load()
		if m >= p.maxMessages {
			return false
		}
		if k.messages.CompareAndSwap(m, m+1) {
			break
		}
	}
	for {
		b := k.bytes.Load()
		if b+uint64(size) > p.maxBytes {
			k.messages.Add(^uint64(0))
			return false
		}
		if k.bytes.CompareAndSwap(b, b+uint64(size)) {
			return true
		}
	}
}

// release returns a reservation that was not used.
func (k *activeKey) release(size int) {
	k.messages.Add(^uint64(0))
	k.bytes.Add(^uint64(size - 1))
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
	now      func() time.Time
	entries  map[[sha256.Size]byte]*list.Element
	order    *list.List // front is most recently used
	negative map[[sha256.Size]byte]negativeEntry
	flight   singleflight.Group

	// tokens and refilled are a token bucket over provider unwraps, spent
	// only when limited.
	limited  bool
	tokens   float64
	refilled time.Time
}

type cacheEntry struct {
	key     [sha256.Size]byte
	dataKey []byte
	expires time.Time
	timer   *time.Timer // removes the entry at expires even if nothing asks for it again
}

type negativeEntry struct {
	err     error
	expires time.Time
}

func newDecodeCache(capacity int, now func() time.Time, limited bool) *decodeCache {
	if capacity <= 0 {
		capacity = DefaultDecodeCacheEntries
	}
	return &decodeCache{
		capacity: min(capacity, maxDecodeCacheEntries),
		now:      now,
		entries:  make(map[[sha256.Size]byte]*list.Element),
		order:    list.New(),
		negative: make(map[[sha256.Size]byte]negativeEntry),
		limited:  limited,
		tokens:   unwrapBurst,
		refilled: now(),
	}
}

// admit takes one token for a provider unwrap, or reports that the bucket is
// empty. An unlimited cache always admits.
func (c *decodeCache) admit() bool {
	if !c.limited {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	c.tokens = min(unwrapBurst, c.tokens+now.Sub(c.refilled).Seconds()*unwrapRate)
	c.refilled = now
	if c.tokens < 1 {
		return false
	}
	c.tokens--
	return true
}

// cacheKey is what one unwrap was bound to: the key that unwraps (the
// primary or an escrow key, by id), the context, the wrapping key's version,
// and the wrapped bytes, each length-prefixed or fixed-width. Naming the
// unwrapping key keeps one key's refusal from being remembered against
// another's wrapped copy.
func cacheKey(unwrapper string, ectx keyprovider.Context, w keyprovider.Wrapped) [sha256.Size]byte {
	h := sha256.New()
	h.Write(appendPrefixed(nil, unwrapper))
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

// put stores a copy of dataKey under key until ttl from now, evicting the
// least recently used entry when full. The ttl is the caching namespace's own,
// so one namespace's short window is not stretched by another's long one.
//
// The entry is removed and its key cleared when the ttl passes, whether or
// not anything asks for it again: an idle namespace's data key does not stay
// in memory past the window that bounds a disabled wrapping key.
func (c *decodeCache) put(key [sha256.Size]byte, dataKey []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		c.remove(el)
	}
	for c.order.Len() >= c.capacity {
		c.remove(c.order.Back())
	}
	e := &cacheEntry{key: key, dataKey: clone(dataKey), expires: c.now().Add(ttl)}
	el := c.order.PushFront(e)
	c.entries[key] = el
	e.timer = time.AfterFunc(ttl, func() { c.expire(el) })
}

// expire removes el if it is still the entry cached under its key.
func (c *decodeCache) expire(el *list.Element) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur, ok := c.entries[el.Value.(*cacheEntry).key]; ok && cur == el {
		c.remove(el)
	}
}

// refuse remembers a definitive refusal for a few seconds. When full, it
// drops what has expired and otherwise remembers nothing new: flushing the
// whole set would let a flood of bad wraps wash out the refusals that are
// holding back calls for a revoked key.
func (c *decodeCache) refuse(key [sha256.Size]byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if len(c.negative) >= maxNegativeCacheEntries {
		for k, e := range c.negative {
			if !now.Before(e.expires) {
				delete(c.negative, k)
			}
		}
		if len(c.negative) >= maxNegativeCacheEntries {
			return
		}
	}
	c.negative[key] = negativeEntry{err: err, expires: now.Add(negativeCacheTTL)}
}

// remove drops an entry and clears its data key.
func (c *decodeCache) remove(el *list.Element) {
	e := c.order.Remove(el).(*cacheEntry)
	e.timer.Stop()
	clear(e.dataKey)
	delete(c.entries, e.key)
}

// unwrap returns the data key w wraps under ectx, from the cache or from the
// entry's key, with concurrent misses for one wrapped key coalesced into one
// provider call. A data key it fetches is kept for the entry's ttl.
func (c *decodeCache) unwrap(e ringEntry, timeout time.Duration, w keyprovider.Wrapped, ectx keyprovider.Context) ([]byte, error) {
	ck := cacheKey(e.id, ectx, w)
	if dk, err, ok := c.get(ck); ok {
		return dk, err
	}

	v, err, _ := c.flight.Do(string(ck[:]), func() (any, error) {
		if dk, err, ok := c.get(ck); ok {
			return sharedDataKey(dk), err
		}
		if !c.admit() {
			return nil, fmt.Errorf("%w: more unwraps of unseen data keys than %d a second", ErrProviderUnavailable, unwrapRate)
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		dk, err := e.key.Unwrap(ctx, w, ectx)
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
		c.put(ck, dk, e.ttl)
		return sharedDataKey(dk), nil
	})
	if err != nil {
		return nil, err
	}
	// Every caller of a coalesced unwrap gets its own copy to clear.
	return clone(v.(*sharedKey).dataKey), nil
}

// sharedKey is the one copy of a data key a coalesced unwrap hands to every
// caller waiting on it, each of which clones it. The copy itself belongs to
// no caller, so none clears it: it is cleared when the last of them has let
// go, which is when nothing reaches this any more. Without that, the
// provider's own slice outlived the cache entry it was copied into, and a
// data key the cache had expired stayed in the heap (Codex, #2167).
type sharedKey struct{ dataKey []byte }

func sharedDataKey(dk []byte) *sharedKey {
	shared := &sharedKey{dataKey: dk}
	runtime.AddCleanup(shared, func(dk []byte) { clear(dk) }, dk)
	return shared
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
// this process may not use it. It matches [payloadcodec.ErrNotReadableHere]:
// the refusal is this process's, not the payload's, and another worker whose
// policy allows the key reads the same payload, so workflow code fails the run
// on it rather than dropping a signal as corrupt or failing a step that
// succeeded, either of which would diverge from that worker's replay.
var ErrKeyDenied error = &classifiedError{
	msg: "envelope: the key provider refused the key", class: payloadcodec.ErrNotReadableHere,
}

func clone(b []byte) []byte { return append([]byte(nil), b...) }
