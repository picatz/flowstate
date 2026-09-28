// Package envelope is Flowstate's production payload codec: envelope
// encryption of every payload a run writes to durable history, under keys the
// deployment holds and the substrate never sees.
//
// # The key hierarchy
//
//	wrapping key   held by a key provider (package keyprovider): a local key,
//	               a Vault or OpenBao Transit key, an HPKE recipient. The
//	               envelope never sees it.
//	data key (DK)  32 random bytes this process generates per namespace,
//	               wrapped by the wrapping key once and reused for a bounded
//	               window: ten minutes, 2²⁰ payloads or 64 GiB by default.
//	content key    derived per payload from the data key and a fresh salt, and
//	               the only key an AEAD is ever keyed with.
//
// The wrapped data key travels in every payload it sealed, so history is
// self-describing: reading a payload needs the wrapping key's provider and
// nothing Flowstate stores anywhere else. A provider is consulted once per data
// key rather than once per payload, so a Temporal workflow task never waits on
// a KMS round trip it did not need, and the provider's own quotas stay far
// away; unwrapped data keys are cached, bounded in count and age, for reading.
//
// # The construction
//
// For a payload sealed with suite s under data key DK, in namespace b, by the
// wrapping key with id k:
//
//	salt       = 32 random bytes, fresh per payload
//	tail       = u32(s) ‖ u16|k ‖ u16|b                  (length-prefixed)
//	prk        = HKDF-Extract(SHA-256, salt, DK)          (RFC 5869)
//	CK         = HKDF-Expand(prk, "…/v1/content-key" ‖ 0 ‖ tail, 32)
//	C          = HKDF-Expand(prk, "…/v1/commitment"  ‖ 0 ‖ tail, 32)
//	H          = PayloadEnvelopeHeader{s, k, salt, C, wrapped DK, …}
//	AAD        = "…/v1/aad" ‖ 0 ‖ u16|b ‖ "FSE1" ‖ uvarint(|H|) ‖ H
//	data       = "FSE1" ‖ uvarint(|H|) ‖ H ‖ AEAD_s(CK, plaintext, AAD)
//
// where the AEAD draws its own random nonce: AES-256-GCM through the FIPS 140-3
// module's NewGCMWithRandomNonce, or XChaCha20-Poly1305 with a 192-bit nonce.
//
// Each content key seals exactly one message, so no nonce is ever reused
// under a key (SP 800-38D §8) whatever the nonce source, and the per-key
// message bounds of the AEADs are never approached; how much one data key may
// seal is bounded by policy far below the multi-key bounds of
// draft-irtf-cfrg-aead-limits.
//
// # Key commitment
//
// AES-GCM and ChaCha20-Poly1305 are not committing: one ciphertext can be
// made to open under two keys (Albertini et al., "How to Abuse and Fix
// Authenticated Encryption Without Key Commitment", USENIX Security 2022).
// That matters here because a reader holds many keys (every namespace's, and
// escrow recipients'), and a wrapped data key reaches it through a provider
// the envelope does not control. C is derived from the data key beside the
// content key and checked, in constant time, before the AEAD is opened; a
// payload opens under exactly the data key that sealed it or not at all. It
// is the construction of the AWS Encryption SDK's committing suites.
//
// # What is authenticated
//
// The header, as the bytes on the wire, and the binding: the Temporal
// namespace, taken from trusted configuration and never from the payload. A
// payload moved to another namespace fails, even under a shared key, because
// the binding is in the AAD, the content key, the commitment and the provider's
// wrap context. What the binding is not, stated so nobody builds on it: it does
// not bind a payload to a workflow, a run, or a position in history. The SDK
// hands a codec payloads and nothing else (go.temporal.io/sdk@v1.48.0
// converter/codec.go), so a party able to rewrite history can move a sealed
// payload between runs of one namespace, and replay an old one. What that party
// cannot do is read it, move it across namespaces, or forge one that a writing
// codec accepts; see Escrow for the one reader that can be handed a forgery.
//
// The header is also held to its canonical encoding: Decode re-marshals it
// deterministically and refuses one whose bytes differ, so a non-minimal
// varint or a repeated field cannot make two readers disagree about it.
//
// # Escrow
//
// A namespace may name up to three escrow keys every data key is also wrapped
// to, typically an HPKE recipient whose private key is kept offline (package
// keyprovider/hpke, ML-KEM-768 + X25519 by default). A worker holding only the
// public half can wrap to it and unwrap nothing with it; a recovery process
// holding the private half reads history whose primary wrapping key is gone.
// The commitment is what makes several wrapped copies safe: whichever copy a
// reader unwraps, only the data key that sealed the payload opens it.
//
// What escrow cannot do is vouch for a writer. An HPKE public key is not a
// secret, so a party able to rewrite history and holding it can wrap a data
// key of its own choosing and seal a payload a recovery process will open.
// Three rules keep that from mattering: a namespace's own keys must
// authenticate the wrapper ([keyprovider.KeyInfo.Authenticates]), so an HPKE
// key can be only an escrow key; only a decode-only codec, one that writes
// nothing and so drives no workflow, unwraps through escrow; and what it reads
// that way is documented as confidential, not authentic.
//
// # What stays readable
//
// Two metadata entries, necessarily in the clear because Decode needs them:
// "encoding", which marks the payload as this envelope, and
// [payloadcodec.KeyIDMetadataKey], which names the wrapping key. The header
// names the suite, the wrapping key and its version, and the escrow keys. The
// ciphertext's length is its plaintext's length plus a bounded constant, and
// everything Temporal records outside payloads stays visible. See
// docs/ENCRYPTION.md for the full inventory.
//
// # Fail closed
//
// Decode refuses, and never passes through or guesses, on: a payload claiming
// a version of this envelope it does not know; a malformed or oversized
// header; a suite the namespace does not accept; a wrapping key it does not
// hold; a provider that refuses or cannot be reached; a commitment or AEAD
// that fails; and, unless [Options.AcceptUnencrypted] is set, a payload that is
// not encrypted at all. Encode refuses rather than write in plaintext when a
// data key cannot be wrapped.
package envelope

import (
	"bytes"
	"cmp"
	"context"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
)

// Encoding is the value of the "encoding" metadata entry on every payload this
// codec writes, and names the envelope's version.
//
// The "encoding" entry is the SDK's own name for "what shape are these bytes",
// which is the question this answers, and it is what Temporal's UI and CLI show
// for a payload they cannot decode.
const Encoding = "binary/flowstate-envelope-v1"

// encodingFamily is the prefix every version of this envelope shares. A payload
// carrying it with a version this build does not know is refused rather than
// passed through as unencrypted: it is encrypted, by something newer.
const encodingFamily = "binary/flowstate-envelope"

// encodingMetadataKey is the SDK's metadata name for a payload's encoding.
const encodingMetadataKey = "encoding"

// magic opens every sealed payload's data.
const magic = "FSE1"

// MaxBindingBytes bounds a binding. A Temporal namespace name is at most 255
// bytes, and the binding is folded into every derivation.
const MaxBindingBytes = 255

// MaxHeaderBytes bounds a payload's header, checked before it is parsed and
// at startup against the largest header a configuration can produce.
const MaxHeaderBytes = 16 << 10

// MaxEscrow bounds how many escrow keys a data key is wrapped to.
const MaxEscrow = 3

const (
	saltBytes       = 32
	commitmentBytes = 32
	contentKeyBytes = 32

	contentKeyLabel = "flowstate/payload-envelope/v1/content-key"
	commitmentLabel = "flowstate/payload-envelope/v1/commitment"
	aadLabel        = "flowstate/payload-envelope/v1/aad"
)

// maxSealedBytes bounds the data Decode will spend work on. Nothing Temporal
// stores can be larger than its blob limit, and the codec server hands Decode
// input an outside party chose, so anything larger is refused on its length
// before any cryptographic work.
const maxSealedBytes = v1.TemporalDefaultBlobLimitBytes

// maxWrittenBytes bounds the whole payload Encode produces. Temporal weighs a
// payload inside the Payloads, command, and history-event framing it wraps
// around it, so a payload written right at the blob limit would be refused
// there; the ceiling leaves the same reserve [payloadcodec] checks a codec's
// expansion against.
const maxWrittenBytes = v1.TemporalDefaultBlobLimitBytes - v1.ContinueAsNewFramingReserveBytes

// Recipient is a wrapping key under its keyring id.
type Recipient struct {
	ID  string
	Key keyprovider.Key
}

// Options configures a [Codec] for one namespace.
type Options struct {
	// Binding is authenticated into every payload and must be supplied, on
	// both sides, from trusted configuration: in Flowstate, the Temporal
	// namespace the client is dialed for. It is never read off a payload or a
	// request. See the package doc for what it does and does not bind.
	Binding string

	// Keys is every wrapping key this namespace's history may be sealed
	// under.
	Keys []Recipient

	// Current is the id of the key new data keys are wrapped under. Empty
	// makes the codec decode-only.
	Current string

	// Escrow is the recipients every new data key is also wrapped to, and
	// that Decode may unwrap with when it cannot use the primary key.
	Escrow []Recipient

	// Suite is what Encode seals with; zero is [DefaultSuite].
	Suite Suite

	// DecryptSuites is what Decode accepts; empty accepts every suite this
	// process may use.
	DecryptSuites []Suite

	// AcceptUnencrypted lets Decode return a payload that carries no
	// encryption at all, unchanged. Off, such a payload is refused. It exists
	// for a namespace whose history was written before encryption was turned
	// on. Encode is unaffected; everything written is still sealed.
	AcceptUnencrypted bool

	// DataKey bounds how long, and for how much, one data key seals.
	DataKey *v1.PayloadDataKeyPolicy

	// ProviderTimeout bounds each wrap and unwrap; zero is
	// [DefaultProviderTimeout]. The codec interface carries no context, so
	// this is the only deadline a provider call gets.
	ProviderTimeout time.Duration

	// now, for tests; cache, shared by a keyring's codecs.
	now   func() time.Time
	cache *decodeCache
}

// Codec is the envelope codec. It implements [payloadcodec.Codec] and is safe
// for concurrent use.
//
// A codec built by [New] serves one binding. A [Keyring]'s reader is the
// other shape: decode-only, holding every namespace's keys each with its own
// binding; see [Keyring.Reader].
type Codec struct {
	binding           string
	current           *ringEntry
	ring              map[string]ringEntry
	escrowIDs         []string
	escrow            map[string]ringEntry
	suite             Suite
	decryptSuites     []Suite
	acceptUnencrypted bool
	policy            dataKeyPolicy
	timeout           time.Duration
	cache             *decodeCache
	now               func() time.Time

	envelopeSize int
	maxHeader    int

	rollover sync.Mutex
	active   atomic.Pointer[activeKey]

	// retryAt and lastErr, guarded by rollover, hold a failed rollover's
	// answer until the next attempt, so concurrent encodes during an outage
	// get it at once instead of each waiting out a provider timeout.
	retryAt time.Time
	lastErr error
}

// rolloverBackoff is how long a failed rollover's answer stands before the
// provider is asked again.
const rolloverBackoff = 2 * time.Second

// ringEntry is a key together with what payloads sealed under it are
// authenticated to and may be. These travel with the key rather than with the
// codec so a reader holding several namespaces' keys opens each payload
// against the namespace its key belongs to, and nothing on the payload gets a
// say in which that is.
type ringEntry struct {
	id            string
	key           keyprovider.Key
	info          keyprovider.KeyInfo
	binding       string
	decryptSuites []Suite

	// ttl is how long a data key this entry unwrapped is cached: its
	// namespace's max_age, the bound on how long a disabled wrapping key
	// keeps working in a running process.
	ttl time.Duration
}

// New returns a codec for one namespace, having asked every key's provider to
// describe it and, when the codec encodes, wrapped its first data key: a
// provider that is misconfigured or unreachable is found here, at startup,
// rather than on the first payload.
func New(ctx context.Context, opts Options) (*Codec, error) {
	if err := validateBinding(opts.Binding); err != nil {
		return nil, err
	}
	if len(opts.Keys) == 0 && len(opts.Escrow) == 0 {
		return nil, errors.New("envelope: no keys: a codec needs at least one key to seal or open with")
	}
	if len(opts.Escrow) > MaxEscrow {
		return nil, fmt.Errorf("envelope: %d escrow keys, and a data key is wrapped to at most %d", len(opts.Escrow), MaxEscrow)
	}
	suite, decrypt, err := resolveSuites(opts.Suite, opts.DecryptSuites, opts.Current != "")
	if err != nil {
		return nil, err
	}

	c := &Codec{
		binding:           opts.Binding,
		ring:              make(map[string]ringEntry, len(opts.Keys)),
		escrow:            make(map[string]ringEntry, len(opts.Escrow)),
		suite:             suite,
		decryptSuites:     decrypt,
		acceptUnencrypted: opts.AcceptUnencrypted,
		policy:            resolvePolicy(opts.DataKey),
		timeout:           cmp.Or(opts.ProviderTimeout, DefaultProviderTimeout),
		now:               opts.now,
		cache:             opts.cache,
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.cache == nil {
		c.cache = newDecodeCache(int(opts.DataKey.GetDecodeCacheEntries()), c.now)
	}

	seen := map[string]bool{}
	for _, set := range []struct {
		into       map[string]ringEntry
		recipients []Recipient
	}{{c.ring, opts.Keys}, {c.escrow, opts.Escrow}} {
		for _, r := range set.recipients {
			if seen[r.ID] {
				return nil, fmt.Errorf("envelope: key id %q is configured twice: an id names exactly one key, "+
					"or Decode could not tell which one sealed a payload", r.ID)
			}
			seen[r.ID] = true
			e, err := describe(ctx, r, opts.Binding, decrypt)
			if err != nil {
				return nil, err
			}
			e.ttl = c.policy.maxAge
			set.into[r.ID] = e
		}
	}
	for _, r := range opts.Escrow {
		c.escrowIDs = append(c.escrowIDs, r.ID)
		if !c.escrow[r.ID].info.CanWrap {
			return nil, fmt.Errorf("envelope: escrow key %q cannot wrap", r.ID)
		}
	}
	// A decode-only codec exists to read, so one with nothing to unwrap with
	// (only an escrow public key, say) is a configuration error now rather
	// than at every read.
	if opts.Current == "" {
		canRead := func(e ringEntry) bool { return e.info.CanUnwrap }
		if !slices.ContainsFunc(slices.Collect(maps.Values(c.ring)), canRead) &&
			!slices.ContainsFunc(slices.Collect(maps.Values(c.escrow)), canRead) {
			return nil, fmt.Errorf("envelope: namespace %q is decode-only and holds no key that can unwrap: "+
				"a recovery keyring needs an escrow private key, or a primary key", opts.Binding)
		}
	}

	for id, e := range c.ring {
		if !e.info.Authenticates {
			return nil, fmt.Errorf("envelope: key %q is a %s key, which anyone holding its public half can wrap "+
				"to, so it cannot vouch for who sealed a payload; name it as an escrow key instead", id, e.info.Kind)
		}
	}

	if opts.Current != "" {
		cur, ok := c.ring[opts.Current]
		if !ok {
			return nil, fmt.Errorf("envelope: the current key %q is not among the configured keys", opts.Current)
		}
		if !cur.info.CanWrap {
			return nil, fmt.Errorf("envelope: the current key %q cannot wrap data keys", opts.Current)
		}
		c.current = &cur
	}

	c.envelopeSize = maxMetadataSize()
	if c.current != nil {
		c.envelopeSize = proto.Size(&commonpb.Payload{Metadata: metadataFor(c.current.id)})
	}
	c.maxHeader = c.maxHeaderSize()
	if c.maxHeader > MaxHeaderBytes {
		return nil, fmt.Errorf("envelope: this namespace's keys can produce a %d-byte header, over the %d-byte bound",
			c.maxHeader, MaxHeaderBytes)
	}

	if c.current != nil {
		// Warm: wrap the first data key now, so a provider that cannot wrap
		// refuses startup instead of the first write.
		a, err := c.activeFor(ctx, 0)
		if err != nil {
			return nil, fmt.Errorf("envelope: wrapping the first data key for %q: %w", opts.Binding, err)
		}
		a.release(0)
	}
	return c, nil
}

// describe asks a key's provider to describe it, bounded by ctx, and holds the
// answer to the limits the envelope relies on.
func describe(ctx context.Context, r Recipient, binding string, decrypt []Suite) (ringEntry, error) {
	if err := payloadcodec.ValidateKeyID(r.ID); err != nil {
		return ringEntry{}, fmt.Errorf("envelope: key id: %w", err)
	}
	if r.Key == nil {
		return ringEntry{}, fmt.Errorf("envelope: key %q has no provider", r.ID)
	}
	info, err := r.Key.Describe(ctx)
	if err != nil {
		return ringEntry{}, fmt.Errorf("envelope: key %q: %w", r.ID, classifyProviderError(err))
	}
	if info.MaxWrappedBytes <= 0 || info.MaxWrappedBytes > keyprovider.MaxWrappedBytes {
		return ringEntry{}, fmt.Errorf("envelope: key %q wraps to %d bytes, outside 1 to %d",
			r.ID, info.MaxWrappedBytes, keyprovider.MaxWrappedBytes)
	}
	return ringEntry{id: r.ID, key: r.Key, info: info, binding: binding, decryptSuites: decrypt}, nil
}

func validateBinding(binding string) error {
	if binding == "" {
		return errors.New("envelope: a binding is required: it is the Temporal namespace (or other trusted " +
			"context) authenticated into every payload, so a payload cannot be moved to another one")
	}
	if len(binding) > MaxBindingBytes {
		return fmt.Errorf("envelope: a binding is at most %d bytes, and this one is %d", MaxBindingBytes, len(binding))
	}
	return nil
}

// maxHeaderSize is the largest header this codec can write: every field at its
// largest, measured with proto.Size so it cannot drift from what Encode builds.
// A decode-only codec writes nothing, and answers with the bound every header
// it reads is held to.
func (c *Codec) maxHeaderSize() int {
	if c.current == nil {
		return MaxHeaderBytes
	}
	h := &v1.PayloadEnvelopeHeader{
		Suite: uint32(c.suite), KeyId: c.current.id,
		Salt: make([]byte, saltBytes), Commitment: make([]byte, commitmentBytes),
		WrappedKey: make([]byte, c.current.info.MaxWrappedBytes), KeyVersion: math.MaxUint32,
	}
	for _, id := range c.escrowIDs {
		e := c.escrow[id]
		h.Escrow = append(h.Escrow, &v1.PayloadEscrowRecipient{
			KeyId: id, WrappedKey: make([]byte, e.info.MaxWrappedBytes), KeyVersion: math.MaxUint32,
		})
	}
	return proto.Size(h)
}

// Name implements [payloadcodec.Codec].
func (c *Codec) Name() string { return "envelope-v1" }

// CurrentKeyID implements [payloadcodec.Codec]. A decode-only codec answers
// with the empty string.
func (c *Codec) CurrentKeyID() string {
	if c.current == nil {
		return ""
	}
	return c.current.id
}

// Binding is the namespace this codec authenticates into every payload. A
// reader, which serves every namespace, answers with the empty string.
func (c *Codec) Binding() string { return c.binding }

// ProviderTimeout is the deadline c puts on one wrap or unwrap. A server
// answering with c sizes its own response deadline from it.
func (c *Codec) ProviderTimeout() time.Duration { return c.timeout }

// DecodeOnly reports whether c has no current key, so that its Encode refuses
// every payload: a keyring reader, or a namespace configured without
// `current`, such as a recovery keyring's. See [payloadcodec.DecodeOnly].
func (c *Codec) DecodeOnly() bool { return c.current == nil }

// AcceptsUnencrypted reports whether Decode passes unencrypted payloads through.
func (c *Codec) AcceptsUnencrypted() bool { return c.acceptUnencrypted }

// Suite is what Encode seals with, and DecryptSuites what Decode accepts.
func (c *Codec) Suite() Suite { return c.suite }

// DecryptSuites is the suites Decode accepts, in order.
func (c *Codec) DecryptSuites() []Suite { return slices.Clone(c.decryptSuites) }

// KeyStatus describes one key a codec holds without revealing it.
type KeyStatus struct {
	ID          string
	Kind        string
	Fingerprint string
	Binding     string
	Version     uint32
	Current     bool
	Escrow      bool
	CanUnwrap   bool
}

// Keys lists the keys this codec holds, current first, then its own keys,
// then escrow keys, for status output. It carries no material.
func (c *Codec) Keys() []KeyStatus {
	out := make([]KeyStatus, 0, len(c.ring)+len(c.escrow))
	for _, set := range []struct {
		entries map[string]ringEntry
		escrow  bool
	}{{c.ring, false}, {c.escrow, true}} {
		for id, e := range set.entries {
			out = append(out, KeyStatus{
				ID: id, Kind: e.info.Kind, Fingerprint: e.info.Fingerprint, Binding: e.binding,
				Version: e.info.Version, Current: c.current != nil && id == c.current.id,
				Escrow: set.escrow, CanUnwrap: e.info.CanUnwrap,
			})
		}
	}
	slices.SortFunc(out, func(a, b KeyStatus) int {
		rank := func(k KeyStatus) int {
			switch {
			case k.Current:
				return 0
			case !k.Escrow:
				return 1
			default:
				return 2
			}
		}
		return cmp.Or(cmp.Compare(rank(a), rank(b)), cmp.Compare(a.Binding, b.Binding), cmp.Compare(a.ID, b.ID))
	})
	return out
}

// maxMetadataSize is the size of the metadata a payload under the longest key
// id carries, for a codec with no single key to measure.
func maxMetadataSize() int {
	return proto.Size(&commonpb.Payload{Metadata: metadataFor(strings.Repeat("x", payloadcodec.MaxKeyIDBytes))})
}

func metadataFor(keyID string) map[string][]byte {
	return map[string][]byte{
		encodingMetadataKey:           []byte(Encoding),
		payloadcodec.KeyIDMetadataKey: []byte(keyID),
	}
}

// appendPrefixed appends s behind its length as two big-endian bytes. Every
// variable term in a derivation is length-prefixed, so no two sets of terms
// encode alike.
func appendPrefixed(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

// info is a derivation's info: its label, then the suite, key id and binding.
func info(label string, suite uint32, keyID, binding string) string {
	b := make([]byte, 0, len(label)+1+4+2+len(keyID)+2+len(binding))
	b = append(b, label...)
	b = append(b, 0)
	b = binary.BigEndian.AppendUint32(b, suite)
	b = appendPrefixed(b, keyID)
	b = appendPrefixed(b, binding)
	return string(b)
}

// aad is the AEAD's associated data: the binding, then the payload's framing
// and header exactly as they are on the wire.
//
// Sized by the fixed part alone: framed is at most a header's length on
// decode, and appending it grows the buffer once.
func aad(binding string, framed []byte) []byte {
	b := make([]byte, 0, len(aadLabel)+1+2+len(binding))
	b = append(b, aadLabel...)
	b = append(b, 0)
	b = appendPrefixed(b, binding)
	return append(b, framed...)
}

// derive returns a payload's content key and key commitment.
func derive(dataKey, salt []byte, suite uint32, keyID, binding string) (contentKey, commitment []byte, err error) {
	prk, err := hkdf.Extract(sha256.New, dataKey, salt)
	if err != nil {
		return nil, nil, err
	}
	defer clear(prk)
	if contentKey, err = hkdf.Expand(sha256.New, prk, info(contentKeyLabel, suite, keyID, binding), contentKeyBytes); err != nil {
		return nil, nil, err
	}
	if commitment, err = hkdf.Expand(sha256.New, prk, info(commitmentLabel, suite, keyID, binding), commitmentBytes); err != nil {
		clear(contentKey)
		return nil, nil, err
	}
	return contentKey, commitment, nil
}

// ErrReaderCannotEncode is what a decode-only codec answers to Encode: a
// [Keyring.Reader], which holds every namespace's keys and so has no single
// namespace to seal for, or a namespace configured with no current key.
var ErrReaderCannotEncode = errors.New("envelope: this codec is decode-only: a keyring reader, which decodes " +
	"for every configured namespace and encodes for none, or a namespace with no current key")

// activeFor returns the data key to seal a size-byte payload with, the
// payload already reserved against its bounds (release it if the seal does
// not happen), rolling over when the window requires it. ctx bounds any wrap
// a rollover makes:
// New's startup context for the warm-up, and none beyond the per-call
// timeout for Encode, whose interface carries none.
func (c *Codec) activeFor(ctx context.Context, size int) (*activeKey, error) {
	now := c.now()
	if a := c.active.Load(); a != nil && a.fresh(c.policy, now, size) {
		return a, nil
	}

	c.rollover.Lock()
	defer c.rollover.Unlock()
	// Read again after every wait: another caller's rollover, or this one's,
	// can take several provider timeouts, and a decision about whether the old
	// key is still within its window made with a time from before the wait
	// could seal past max_age + stale_grace.
	now = c.now()
	a := c.active.Load()
	if a != nil && a.fresh(c.policy, now, size) {
		return a, nil
	}
	err := c.lastErr
	if now.Before(c.retryAt) && err != nil {
		// Asked recently and refused: answer from that, not the provider.
	} else {
		var next *activeKey
		if next, err = c.newActive(ctx, now); err == nil {
			c.lastErr, c.retryAt = nil, time.Time{}
			next.charge(size)
			c.active.Store(next)
			return next, nil
		}
		now = c.now()
		c.lastErr, c.retryAt = err, now.Add(rolloverBackoff)
	}
	if a != nil && a.withinGrace(c.policy, now, size) {
		return a, nil
	}
	return nil, err
}

// newActive generates a data key and wraps it to the current key and every
// escrow key.
func (c *Codec) newActive(ctx context.Context, now time.Time) (*activeKey, error) {
	dk := newDataKey()
	ectx := keyprovider.Context{Namespace: c.binding, KeyID: c.current.id, Suite: uint32(c.suite)}

	wrapped, err := c.wrap(ctx, *c.current, dk, ectx)
	if err != nil {
		clear(dk)
		return nil, err
	}
	a := &activeKey{dataKey: dk, wrapped: wrapped, created: now}
	// Cleared once nothing holds the key any more: after rollover, when the
	// last in-flight seal that loaded it has finished.
	runtime.AddCleanup(a, func(dk []byte) { clear(dk) }, dk)
	for _, id := range c.escrowIDs {
		w, err := c.wrap(ctx, c.escrow[id], dk, ectx)
		if err != nil {
			clear(dk)
			return nil, fmt.Errorf("escrow key %q: %w", id, err)
		}
		a.escrow = append(a.escrow, &v1.PayloadEscrowRecipient{KeyId: id, WrappedKey: w.Bytes, KeyVersion: w.Version})
	}
	// This process will read what it writes, for as long as it may write
	// with this key, grace included; it need not ask the provider.
	c.cache.put(cacheKey(c.current.id, ectx, wrapped), dk, c.policy.maxAge+c.policy.staleGrace)
	return a, nil
}

// wrap wraps dk to one key under a deadline of its own, so a slow provider
// spends only its own budget: a data key wrapped to a primary and three escrow
// keys waits at most four timeouts, and no provider inherits a deadline an
// earlier one used up. Each is still bounded by parent, so a caller's own
// deadline (startup's) holds across all of them.
func (c *Codec) wrap(parent context.Context, e ringEntry, dk []byte, ectx keyprovider.Context) (keyprovider.Wrapped, error) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	w, err := e.key.Wrap(ctx, dk, ectx)
	if err != nil {
		return keyprovider.Wrapped{}, classifyProviderError(err)
	}
	if len(w.Bytes) == 0 || len(w.Bytes) > e.info.MaxWrappedBytes {
		return keyprovider.Wrapped{}, fmt.Errorf("envelope: key %q wrapped to %d bytes, over the %d it declared",
			e.id, len(w.Bytes), e.info.MaxWrappedBytes)
	}
	return w, nil
}

// Encode implements [payloadcodec.Codec].
func (c *Codec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	if c.current == nil {
		return nil, ErrReaderCannotEncode
	}
	spec := suites[c.suite]

	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		plaintext, err := proto.MarshalOptions{Deterministic: true}.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("envelope: marshaling a payload: %w", err)
		}
		sealed, err := c.seal(spec, plaintext)
		clear(plaintext)
		if err != nil {
			return nil, err
		}
		out[i] = &commonpb.Payload{Metadata: metadataFor(c.current.id), Data: sealed}
	}
	return out, nil
}

func (c *Codec) seal(spec suiteSpec, plaintext []byte) ([]byte, error) {
	// What Temporal would refuse is never written: the whole payload,
	// metadata and framing included, stays under the blob limit less the
	// framing Temporal adds above it, and bounding it here keeps every
	// allocation below sized off a bounded value. The first comparison keeps
	// the sum from overflowing.
	if len(plaintext) > maxWrittenBytes || c.MaxEncodedSize(len(plaintext)) > maxWrittenBytes {
		return nil, fmt.Errorf("envelope: a %d-byte payload seals past the %d-byte limit history holds",
			len(plaintext), maxWrittenBytes)
	}

	a, err := c.activeFor(context.Background(), len(plaintext))
	if err != nil {
		return nil, err
	}
	sealed := false
	defer func() {
		if !sealed {
			a.release(len(plaintext))
		}
	}()

	salt := make([]byte, saltBytes)
	// crypto/rand never fails on supported platforms (Go 1.24+).
	_, _ = rand.Read(salt)
	contentKey, commitment, err := derive(a.dataKey, salt, uint32(c.suite), c.current.id, c.binding)
	if err != nil {
		return nil, fmt.Errorf("envelope: deriving a content key: %w", err)
	}
	defer clear(contentKey)

	header, err := proto.MarshalOptions{Deterministic: true}.Marshal(&v1.PayloadEnvelopeHeader{
		Suite:      uint32(c.suite),
		KeyId:      c.current.id,
		Salt:       salt,
		Commitment: commitment,
		WrappedKey: a.wrapped.Bytes,
		KeyVersion: a.wrapped.Version,
		Escrow:     a.escrow,
	})
	if err != nil {
		return nil, fmt.Errorf("envelope: marshaling a header: %w", err)
	}
	// Startup checked that every header this codec writes fits; checked again
	// here, against the constant Decode enforces, because the wrapped keys in
	// it are what the providers returned.
	if len(header) > MaxHeaderBytes {
		return nil, fmt.Errorf("envelope: a %d-byte header is past the %d-byte limit Decode reads", len(header), MaxHeaderBytes)
	}

	framed := make([]byte, 0, len(magic)+binary.MaxVarintLen32+len(header)+spec.overhead+len(plaintext))
	framed = append(framed, magic...)
	framed = binary.AppendUvarint(framed, uint64(len(header)))
	framed = append(framed, header...)

	body, err := spec.seal(contentKey, plaintext, aad(c.binding, framed))
	if err != nil {
		return nil, fmt.Errorf("envelope: sealing: %w", err)
	}
	sealed = true
	return append(framed, body...), nil
}

// dataField is Payload.data's field number, for the size bound.
var dataField = (&commonpb.Payload{}).ProtoReflect().Descriptor().Fields().ByName("data").Number()

// MaxEncodedSize implements [payloadcodec.Codec]: the metadata, and the data
// field holding the framing, the largest header this codec writes, the
// suite's nonce and tag, and the plaintext payload. An upper bound, and exact
// but for the few bytes a provider's shorter wrap or smaller key version saves.
func (c *Codec) MaxEncodedSize(plain int) int {
	plain = max(plain, 0)
	overhead := maxOverhead
	if c.current != nil {
		overhead = suites[c.suite].overhead
	}
	data := len(magic) + protowire.SizeVarint(uint64(c.maxHeader)) + c.maxHeader + overhead + plain
	return c.envelopeSize + protowire.SizeTag(dataField) + protowire.SizeBytes(data)
}

// Decode errors. Each is a distinct sentinel so callers, and the codec server,
// can classify a refusal without parsing its text, and none carries payload
// bytes.
var (
	// ErrUnencrypted is a payload with no encryption, refused because the
	// codec does not accept unencrypted payloads.
	ErrUnencrypted = errors.New("envelope: payload is not encrypted, and this deployment requires encryption")

	// ErrUnknownVersion is a payload marked as a version of this envelope
	// this build cannot read.
	ErrUnknownVersion = errors.New("envelope: payload uses an envelope version this build cannot read")

	// ErrUnknownKey is a payload whose data key this codec holds no key to
	// unwrap.
	ErrUnknownKey = errors.New("envelope: payload was sealed under a key this codec does not hold")

	// ErrSuiteRefused is a payload sealed with a suite the namespace does not
	// accept.
	ErrSuiteRefused = errors.New("envelope: payload was sealed with a suite this namespace does not accept")

	// ErrMalformed is a payload whose envelope is structurally invalid.
	ErrMalformed = errors.New("envelope: malformed payload")

	// ErrAuthentication is a payload that failed authentication: the wrong key
	// material under a matching id, a payload moved from another namespace, or
	// a payload altered after it was sealed.
	ErrAuthentication = errors.New("envelope: payload failed authentication")
)

// Decode implements [payloadcodec.Codec].
func (c *Codec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		decoded, err := c.decodeOne(p)
		if err != nil {
			return nil, err
		}
		out[i] = decoded
	}
	return out, nil
}

func (c *Codec) decodeOne(p *commonpb.Payload) (*commonpb.Payload, error) {
	encoding := string(p.GetMetadata()[encodingMetadataKey])
	_, stamped := p.GetMetadata()[payloadcodec.KeyIDMetadataKey]
	switch {
	case encoding == Encoding:
	case strings.HasPrefix(encoding, encodingFamily):
		return nil, ErrUnknownVersion
	case stamped:
		// A payload naming a Flowstate key is one some Flowstate codec sealed,
		// whatever its encoding now says. Letting it through as unencrypted
		// would let an edit to one unauthenticated metadata entry move a
		// sealed payload onto the plaintext path.
		return nil, fmt.Errorf("%w: it names a Flowstate key but is not marked as this envelope", ErrMalformed)
	case c.acceptUnencrypted:
		return p, nil
	default:
		return nil, ErrUnencrypted
	}

	keyID := string(p.GetMetadata()[payloadcodec.KeyIDMetadataKey])
	// The id is outside input and refusals below quote it, so it is held to
	// the grammar first; ValidateKeyID's own errors never echo it.
	if err := payloadcodec.ValidateKeyID(keyID); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMalformed, err)
	}

	data := p.GetData()
	if len(data) > maxSealedBytes {
		return nil, fmt.Errorf("%w: %d bytes of sealed data, over the %d an envelope can hold", ErrMalformed, len(data), maxSealedBytes)
	}
	header, framed, body, err := parse(data)
	if err != nil {
		return nil, err
	}
	if header.GetKeyId() != keyID {
		return nil, fmt.Errorf("%w: the header and the metadata name different keys", ErrMalformed)
	}

	unwrapper, binding, allowed, w, err := c.unwrapperFor(header)
	if err != nil {
		return nil, err
	}
	// Every accepted suite is a registered one: resolveSuites admits no
	// other.
	if !slices.Contains(allowed, Suite(header.GetSuite())) {
		return nil, fmt.Errorf("%w: suite %d", ErrSuiteRefused, header.GetSuite())
	}
	spec := suites[Suite(header.GetSuite())]

	ectx := keyprovider.Context{Namespace: binding, KeyID: keyID, Suite: header.GetSuite()}
	dataKey, err := c.cache.unwrap(unwrapper, c.timeout, w, ectx)
	if err != nil {
		return nil, fmt.Errorf("key %q: %w", keyID, err)
	}
	defer clear(dataKey)

	contentKey, commitment, err := derive(dataKey, header.GetSalt(), header.GetSuite(), keyID, binding)
	if err != nil {
		return nil, fmt.Errorf("envelope: deriving a content key: %w", err)
	}
	defer clear(contentKey)

	// The commitment first: a data key that is not the one that sealed this
	// payload never reaches the AEAD, whichever wrapped copy it came from.
	// Deliberately undifferentiated from an AEAD failure: which of wrong key,
	// wrong namespace, or tampering is not something the ciphertext can say,
	// and an oracle that tried would be telling an attacker which edit got
	// further.
	if subtle.ConstantTimeCompare(commitment, header.GetCommitment()) != 1 {
		return nil, authFailure(keyID)
	}
	plaintext, err := spec.open(contentKey, body, aad(binding, framed))
	if err != nil {
		return nil, authFailure(keyID)
	}
	defer clear(plaintext)

	var decoded commonpb.Payload
	if err := proto.Unmarshal(plaintext, &decoded); err != nil {
		return nil, fmt.Errorf("%w: the authenticated plaintext is not a payload", ErrMalformed)
	}
	return &decoded, nil
}

func authFailure(keyID string) error {
	return fmt.Errorf("%w under key %q: either the key differs from the one that sealed it, it was sealed "+
		"for a different namespace, or it was altered", ErrAuthentication, keyID)
}

// parse splits sealed data into its header, the framing the AAD covers, and
// the AEAD body, bounding every length before it is trusted.
func parse(data []byte) (*v1.PayloadEnvelopeHeader, []byte, []byte, error) {
	if !bytes.HasPrefix(data, []byte(magic)) {
		return nil, nil, nil, fmt.Errorf("%w: the data does not open with the envelope's magic", ErrMalformed)
	}
	rest := data[len(magic):]
	n, read := binary.Uvarint(rest)
	if read <= 0 || n == 0 || n > MaxHeaderBytes || n > uint64(len(rest)-read) {
		return nil, nil, nil, fmt.Errorf("%w: the header length is invalid", ErrMalformed)
	}
	end := len(magic) + read + int(n)
	wire := data[len(magic)+read : end]
	var header v1.PayloadEnvelopeHeader
	if err := proto.Unmarshal(wire, &header); err != nil {
		return nil, nil, nil, fmt.Errorf("%w: the header does not parse", ErrMalformed)
	}
	if err := v1.Validate(&header); err != nil {
		return nil, nil, nil, fmt.Errorf("%w: the header is invalid", ErrMalformed)
	}
	if canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&header); err != nil || !bytes.Equal(canonical, wire) {
		return nil, nil, nil, fmt.Errorf("%w: the header is not in its canonical encoding", ErrMalformed)
	}
	return &header, data[:end], data[end:], nil
}

// unwrapperFor chooses the key that will unwrap a payload's data key, the
// binding the payload is authenticated to, and the suites it may use: the
// primary key if this codec holds it and can unwrap with it, and otherwise the
// first escrow recipient on the payload that this codec holds and can unwrap
// with. The choice is by id, never by trial.
func (c *Codec) unwrapperFor(h *v1.PayloadEnvelopeHeader) (ringEntry, string, []Suite, keyprovider.Wrapped, error) {
	primary, held := c.ring[h.GetKeyId()]
	binding, allowed := c.binding, c.decryptSuites
	if held {
		// A reader's keys each carry their own namespace; a namespace codec's
		// carry its own, the same as c.binding.
		binding, allowed = primary.binding, primary.decryptSuites
		if primary.info.CanUnwrap {
			return primary, binding, allowed, keyprovider.Wrapped{Bytes: h.GetWrappedKey(), Version: h.GetKeyVersion()}, nil
		}
	}
	// Escrow is for a decode-only namespace codec: a recovery process, whose
	// binding is its own and which writes nothing. A codec that writes never
	// reads through escrow, because an escrow wrap does not prove who made it
	// and what a writer reads drives its workflows. A reader that does not
	// hold the primary key cannot say which namespace a payload belongs to,
	// so it does not guess either.
	if binding != "" && c.current == nil && c.binding != "" {
		for _, r := range h.GetEscrow() {
			if e, ok := c.escrow[r.GetKeyId()]; ok && e.info.CanUnwrap {
				return e, binding, allowed, keyprovider.Wrapped{Bytes: r.GetWrappedKey(), Version: r.GetKeyVersion()}, nil
			}
		}
	}
	return ringEntry{}, "", nil, keyprovider.Wrapped{}, fmt.Errorf("%w: the key is %q. If that key was retired "+
		"from the ring or destroyed, history sealed under it can be read only through an escrow key; "+
		"otherwise this process was started without it", ErrUnknownKey, h.GetKeyId())
}

var (
	_ payloadcodec.Codec      = (*Codec)(nil)
	_ payloadcodec.DecodeOnly = (*Codec)(nil)
)
