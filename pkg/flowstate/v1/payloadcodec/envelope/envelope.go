// Package envelope is Flowstate's production payload codec: authenticated
// encryption of every payload a run writes to durable history, under keys the
// deployment holds and the substrate never sees.
//
// # The construction
//
// Each payload is sealed with its own content key. The content key is derived
// from the deployment's key-encryption key (KEK) with HKDF-SHA256 (RFC 5869)
// over a fresh 256-bit random salt, and the payload is sealed under it with
// AES-256-GCM (NIST SP 800-38D):
//
//	salt        = 32 random bytes, fresh per payload
//	content key = HKDF-SHA256(ikm = KEK, salt, info = label ‖ key id ‖ binding)
//	sealed      = AES-256-GCM(content key, nonce = 0¹², plaintext, aad)
//	data        = salt ‖ sealed
//
// Deriving rather than wrapping is what lets the nonce be fixed. GCM's one
// catastrophic failure is two messages under one key with one nonce
// (SP 800-38D §8); a content key is used for exactly one message, so the nonce
// is unique per key by construction rather than by probability, and the KEK is
// never used as a GCM key at all, so the random-nonce bound of 2³² messages per
// key (SP 800-38D §8.3) does not cap how much one KEK may protect. Two payloads
// share a content key only if their salts collide, which at 256 bits is not a
// consideration.
//
// The plaintext is the whole marshaled payload, metadata included, as it is for
// every codec in this tree: a payload's metadata names the converter that wrote
// it and, for a proto payload, the message type, and nothing of the original
// should be readable without the key.
//
// # What is authenticated
//
// The associated data covers the envelope version, the key id, the binding, and
// the salt, so none of them can be edited without the open failing. The binding
// is the one term that is not on the payload: it is configuration, the Temporal
// namespace the codec was built for, supplied by whoever constructs the codec
// from trusted deployment configuration. A payload sealed in one namespace and
// spliced into another therefore fails authentication even when both
// namespaces happen to be configured with the same key.
//
// What the binding is not, stated so nobody builds on it: it does not bind a
// payload to a workflow, a run, or a position in history. The SDK hands a codec
// payloads and nothing else (go.temporal.io/sdk@v1.48.0 converter/codec.go),
// so no codec can authenticate where in a namespace a payload was written. A
// party able to rewrite history can move a sealed payload between runs of one
// namespace, and replay an old one. What that party cannot do is read it, forge
// one, or move it across namespaces.
//
// # What stays readable
//
// Two metadata entries, necessarily in the clear because Decode needs them to
// choose a key: "encoding", which marks the payload as this envelope and names
// its version, and [payloadcodec.KeyIDMetadataKey], which names the key. The
// ciphertext's length is its plaintext's length plus a constant, so size is
// visible, as are everything Temporal records outside payloads: workflow and
// activity types, ids, task queues, timestamps, search attributes, and the
// shape of the event history. See docs/ENCRYPTION.md for the full inventory.
//
// # Fail closed
//
// Decode refuses, and never passes through or guesses, on: a payload claiming a
// version of this envelope it does not know; a key id it does not hold; a
// payload whose authentication fails; and, unless [Options.AcceptUnencrypted] is
// set, a payload that is not encrypted at all. That last refusal is what makes
// "required" mean required on the read side too: without it, anyone able to
// write a plaintext payload into history, or a client started without the
// codec, would be read back as though it were protected.
package envelope

import (
	"bytes"
	"cmp"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
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

// KeyBytes is the length of a key-encryption key: 256 bits.
const KeyBytes = 32

// MaxBindingBytes bounds a binding. A Temporal namespace name is at most 255
// bytes, and the binding is folded into every content key derivation.
const MaxBindingBytes = 255

const (
	saltBytes = 32
	tagBytes  = 16

	// contentKeyLabel and aadLabel domain-separate the two uses of the
	// envelope's context, and fingerprintLabel separates the one other thing
	// derived from a KEK, so publishing a fingerprint says nothing about any
	// content key.
	contentKeyLabel  = "flowstate/payload-envelope/v1/content-key"
	aadLabel         = "flowstate/payload-envelope/v1/aad"
	fingerprintLabel = "flowstate/payload-envelope/v1/fingerprint"

	fingerprintBytes = 8
)

// maxSealedBytes bounds the data Decode will spend a derivation and an AEAD
// open on. Nothing Temporal stores can be larger than its blob limit, and the
// codec server hands Decode input an outside party chose, so anything larger is
// refused on its length before any cryptographic work.
const maxSealedBytes = v1.TemporalDefaultBlobLimitBytes

// zeroNonce is GCM's nonce for every content key. See the package doc for why a
// fixed nonce is sound here and nowhere else.
var zeroNonce = make([]byte, 12)

// Key is one key-encryption key, named by the id it is stamped under.
//
// The material is reachable only through a closure, never a field, so that no
// formatting verb, reflection, or accidental %#v can print it: fmt prints the
// fields of a value it reaches through an unexported field, and a closure's
// captured variables are not fields. The one thing a Key prints is its id.
type Key struct {
	id          string
	fingerprint string
	derive      func(salt []byte, info string) ([]byte, error)
}

// NewKey returns a key-encryption key named id over 32 bytes of material. The
// material is copied, so the caller may clear its own slice afterwards.
func NewKey(id string, material []byte) (Key, error) {
	if err := payloadcodec.ValidateKeyID(id); err != nil {
		return Key{}, fmt.Errorf("envelope: key id: %w", err)
	}
	if len(material) != KeyBytes {
		return Key{}, fmt.Errorf("envelope: key %q is %d bytes, and a key is exactly %d", id, len(material), KeyBytes)
	}

	held := bytes.Clone(material)

	fp, err := hkdf.Key(sha256.New, held, nil, fingerprintLabel, fingerprintBytes)
	if err != nil {
		return Key{}, fmt.Errorf("envelope: key %q: %w", id, err)
	}

	return Key{
		id:          id,
		fingerprint: hex.EncodeToString(fp),
		derive: func(salt []byte, info string) ([]byte, error) {
			return hkdf.Key(sha256.New, held, salt, info, KeyBytes)
		},
	}, nil
}

// ID is the key's id, as stamped on every payload sealed under it.
func (k Key) ID() string { return k.id }

// Fingerprint is a short one-way digest of the key material, domain-separated
// from every content key, for comparing configurations without revealing them:
// two workers whose fingerprints for one id differ hold different keys under
// that id, and each will refuse the other's payloads.
func (k Key) Fingerprint() string { return k.fingerprint }

// String prints the id and nothing else.
func (k Key) String() string { return "envelope.Key(" + k.id + ")" }

// GoString prints what String does, so %#v cannot reach the closure.
func (k Key) GoString() string { return k.String() }

// Format prints what String does for every verb.
func (k Key) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(k.String())) }

// LogValue is the key's id, so structured logging cannot reach the closure.
func (k Key) LogValue() slog.Value { return slog.StringValue(k.String()) }

// GenerateKey returns 32 fresh random bytes of key material, encoded as the text
// [ParseKey] reads: one line of standard, padded base64.
func GenerateKey() []byte {
	material := make([]byte, KeyBytes)
	// crypto/rand.Read never returns an error on supported platforms and
	// crashes the program irrecoverably if the source fails (Go 1.24+).
	_, _ = rand.Read(material)
	defer clear(material)

	out := make([]byte, base64.StdEncoding.EncodedLen(KeyBytes)+1)
	base64.StdEncoding.Encode(out, material)
	out[len(out)-1] = '\n'
	return out
}

// ParseKey reads key material in the text form [GenerateKey] writes and returns
// the key under id. Surrounding whitespace is ignored; anything that is not
// exactly 32 bytes of standard base64 is refused, and the refusal never quotes
// the text it was given.
func ParseKey(id string, text []byte) (Key, error) {
	trimmed := bytes.TrimSpace(text)
	material := make([]byte, base64.StdEncoding.DecodedLen(len(trimmed)))
	defer clear(material)

	n, err := base64.StdEncoding.Decode(material, trimmed)
	if err != nil {
		return Key{}, fmt.Errorf("envelope: key %q is not standard base64 (%d bytes of text); "+
			"generate one with `flow codec keygen`", id, len(trimmed))
	}
	return NewKey(id, material[:n])
}

// Options configures a [Codec].
type Options struct {
	// Keys is every key the codec may decrypt with. It must hold Current.
	Keys []Key

	// Current is the id of the key Encode seals new payloads under.
	Current string

	// Binding is authenticated into every payload and must be supplied, on
	// both sides, from trusted configuration: in Flowstate, the Temporal
	// namespace the client is dialed for. It is never read off a payload or a
	// request. See the package doc for what it does and does not bind.
	Binding string

	// AcceptUnencrypted lets Decode return a payload that carries no
	// encryption at all, unchanged. Off, such a payload is refused.
	//
	// It exists for one situation: a namespace whose history was written
	// before encryption was turned on, and which still holds runs that must
	// finish. Encode is unaffected; everything written is still sealed.
	AcceptUnencrypted bool
}

// Codec is the envelope codec. It implements [payloadcodec.Codec] and is safe
// for concurrent use.
//
// A codec built by [New] serves one binding: it seals under its current key and
// opens only payloads sealed under one of its own keys for that binding. A
// [Keyring]'s reader is the other shape, decode-only, holding every
// namespace's keys each with its own binding; see [Keyring.Reader].
type Codec struct {
	current           *ringEntry
	ring              map[string]ringEntry
	acceptUnencrypted bool
	envelopeSize      int
}

// ringEntry is a key together with the binding payloads sealed under it are
// authenticated to. The binding travels with the key rather than with the
// codec so that a reader holding several namespaces' keys still opens each
// payload against the namespace its key belongs to, and nothing on the payload
// gets a say in which that is.
type ringEntry struct {
	key     Key
	binding string
}

// New returns a codec from opts, or an error naming what is wrong with them.
func New(opts Options) (*Codec, error) {
	if err := validateBinding(opts.Binding); err != nil {
		return nil, err
	}
	if len(opts.Keys) == 0 {
		return nil, errors.New("envelope: no keys: a codec needs at least the key it encrypts with")
	}

	ring := make(map[string]ringEntry, len(opts.Keys))
	if err := addToRing(ring, opts.Binding, opts.Keys); err != nil {
		return nil, err
	}

	current, ok := ring[opts.Current]
	if !ok {
		return nil, fmt.Errorf("envelope: the current key %q is not among the configured keys", opts.Current)
	}

	return &Codec{
		current:           &current,
		ring:              ring,
		acceptUnencrypted: opts.AcceptUnencrypted,
		envelopeSize:      envelopeSizeFor(current.key.id),
	}, nil
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

// addToRing adds keys under binding, refusing an id already present under any
// binding: an id names exactly one key across everything a process holds, or
// a reader could not tell which key, and so which namespace, sealed a payload.
func addToRing(ring map[string]ringEntry, binding string, keys []Key) error {
	for _, k := range keys {
		if k.derive == nil {
			return errors.New("envelope: a zero Key; build keys with NewKey or ParseKey")
		}
		if _, dup := ring[k.id]; dup {
			return fmt.Errorf("envelope: key id %q is configured twice: an id names exactly one key, "+
				"or Decode could not tell which one sealed a payload", k.id)
		}
		ring[k.id] = ringEntry{key: k, binding: binding}
	}
	return nil
}

// Name implements [payloadcodec.Codec].
func (c *Codec) Name() string { return "envelope-v1" }

// CurrentKeyID implements [payloadcodec.Codec]. A reader, which never
// encodes, answers with the empty string.
func (c *Codec) CurrentKeyID() string {
	if c.current == nil {
		return ""
	}
	return c.current.key.id
}

// Binding is the context this codec authenticates into every payload it seals.
// A reader, which seals nothing, answers with the empty string.
func (c *Codec) Binding() string {
	if c.current == nil {
		return ""
	}
	return c.current.binding
}

// AcceptsUnencrypted reports whether Decode passes unencrypted payloads through.
func (c *Codec) AcceptsUnencrypted() bool { return c.acceptUnencrypted }

// Keys lists the ids and fingerprints this codec can decrypt with, current
// first, for status output. It carries no material.
func (c *Codec) Keys() []KeyStatus {
	out := make([]KeyStatus, 0, len(c.ring))
	for id, e := range c.ring {
		out = append(out, KeyStatus{
			ID:          id,
			Fingerprint: e.key.fingerprint,
			Binding:     e.binding,
			Current:     c.current != nil && id == c.current.key.id,
		})
	}
	// Map order is random; status output should not be.
	slices.SortFunc(out, func(a, b KeyStatus) int {
		if a.Current != b.Current {
			if a.Current {
				return -1
			}
			return 1
		}
		return cmp.Or(cmp.Compare(a.Binding, b.Binding), cmp.Compare(a.ID, b.ID))
	})
	return out
}

// KeyStatus describes one key a codec holds without revealing it.
type KeyStatus struct {
	ID          string
	Fingerprint string
	Binding     string
	Current     bool
}

// envelopeSizeFor is the encoded size of the two metadata entries a payload
// sealed under keyID carries: measured from a data-less payload of exactly the
// shape Encode builds rather than counted by hand, so it cannot drift from it.
func envelopeSizeFor(keyID string) int {
	return proto.Size(&commonpb.Payload{Metadata: metadataFor(keyID)})
}

func metadataFor(keyID string) map[string][]byte {
	return map[string][]byte{
		encodingMetadataKey:           []byte(Encoding),
		payloadcodec.KeyIDMetadataKey: []byte(keyID),
	}
}

// context is the length-prefixed encoding of the key id and the binding, shared
// by the derivation's info and the AEAD's associated data. Length prefixes make
// the encoding injective, so no two (id, binding) pairs produce the same bytes.
func context(label, keyID, binding string, salt []byte) []byte {
	b := make([]byte, 0, len(label)+1+2+len(keyID)+2+len(binding)+len(salt))
	b = append(b, label...)
	b = append(b, 0)
	b = binary.BigEndian.AppendUint16(b, uint16(len(keyID)))
	b = append(b, keyID...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(binding)))
	b = append(b, binding...)
	b = append(b, salt...)
	return b
}

// aead derives the content key for one payload and returns the AEAD over it.
func aead(k Key, binding string, salt []byte) (cipher.AEAD, error) {
	contentKey, err := k.derive(salt, string(context(contentKeyLabel, k.id, binding, nil)))
	if err != nil {
		return nil, err
	}
	defer clear(contentKey)

	block, err := aes.NewCipher(contentKey)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// ErrReaderCannotEncode is what a [Keyring.Reader] answers to Encode. A reader
// holds every namespace's keys, so it has no single namespace to seal for, and
// a client handed one by mistake fails loudly on its first write rather than
// sealing a tenant's payload under some other tenant's key.
var ErrReaderCannotEncode = errors.New("envelope: this codec is a keyring reader, which decodes for every " +
	"configured namespace and encodes for none; build clients with the codec for their own namespace")

// Encode implements [payloadcodec.Codec].
func (c *Codec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	if c.current == nil {
		return nil, ErrReaderCannotEncode
	}
	cur := c.current

	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		plaintext, err := proto.MarshalOptions{Deterministic: true}.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("envelope: marshaling a payload: %w", err)
		}
		// What Decode refuses on length is never written: a payload past the
		// blob limit is one Temporal would refuse anyway, and bounding it here
		// keeps the allocation below from sizing itself off an unbounded value.
		if len(plaintext) > maxSealedBytes-saltBytes-tagBytes {
			return nil, fmt.Errorf("envelope: a %d-byte payload seals past the %d-byte limit history holds",
				len(plaintext), maxSealedBytes)
		}

		data := make([]byte, saltBytes, saltBytes+len(plaintext)+tagBytes)
		_, _ = rand.Read(data)
		salt := data[:saltBytes]

		gcm, err := aead(cur.key, cur.binding, salt)
		if err != nil {
			return nil, fmt.Errorf("envelope: deriving a content key: %w", err)
		}
		data = gcm.Seal(data, zeroNonce, plaintext, context(aadLabel, cur.key.id, cur.binding, salt))
		clear(plaintext)

		out[i] = &commonpb.Payload{Metadata: metadataFor(cur.key.id), Data: data}
	}
	return out, nil
}

// dataField is Payload.data's field number, for the exact size declaration.
var dataField = (&commonpb.Payload{}).ProtoReflect().Descriptor().Fields().ByName("data").Number()

// MaxEncodedSize implements [payloadcodec.Codec], exactly: the salt, the
// plaintext payload, and the tag, framed as the data field, beside the two
// metadata entries. Deterministic marshaling produces exactly proto.Size bytes.
func (c *Codec) MaxEncodedSize(plain int) int {
	plain = max(plain, 0)
	sealed := saltBytes + plain + tagBytes
	return c.envelopeSize + protowire.SizeTag(dataField) + protowire.SizeBytes(sealed)
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

	// ErrUnknownKey is a payload sealed under a key this codec does not hold.
	ErrUnknownKey = errors.New("envelope: payload was sealed under a key this codec does not hold")

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
	if keyID == "" {
		return nil, fmt.Errorf("%w: it is marked encrypted and names no key", ErrMalformed)
	}
	// The id is outside input and the refusal below quotes it, so it is held
	// to the grammar first; ValidateKeyID's own errors never echo it.
	if err := payloadcodec.ValidateKeyID(keyID); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMalformed, err)
	}

	entry, ok := c.ring[keyID]
	if !ok {
		return nil, fmt.Errorf("%w: the key is %q. If that key was retired from the ring or destroyed, "+
			"history sealed under it cannot be read; otherwise this process was started without it", ErrUnknownKey, keyID)
	}

	data := p.GetData()
	if len(data) < saltBytes+tagBytes || len(data) > maxSealedBytes {
		return nil, fmt.Errorf("%w: %d bytes of sealed data, outside the %d to %d an envelope can hold",
			ErrMalformed, len(data), saltBytes+tagBytes, maxSealedBytes)
	}

	salt := data[:saltBytes]
	gcm, err := aead(entry.key, entry.binding, salt)
	if err != nil {
		return nil, fmt.Errorf("envelope: deriving a content key: %w", err)
	}

	plaintext, err := gcm.Open(nil, zeroNonce, data[saltBytes:], context(aadLabel, entry.key.id, entry.binding, salt))
	if err != nil {
		// Deliberately undifferentiated: which of wrong key material, wrong
		// namespace, or tampering is not something the ciphertext can say,
		// and an oracle that tried would be telling an attacker which edit
		// got further.
		return nil, fmt.Errorf("%w under key %q: either the key material differs from the key that sealed "+
			"it, it was sealed for a different namespace, or it was altered", ErrAuthentication, entry.key.id)
	}
	defer clear(plaintext)

	var decoded commonpb.Payload
	if err := proto.Unmarshal(plaintext, &decoded); err != nil {
		return nil, fmt.Errorf("%w: the authenticated plaintext is not a payload", ErrMalformed)
	}
	return &decoded, nil
}

var _ payloadcodec.Codec = (*Codec)(nil)
