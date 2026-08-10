// Package toycodec is a payload codec for tests, and for nothing else.
//
// It exists so a round-trip test can prove that a configured codec is actually
// on the path a payload takes, that history holds bytes the default converter
// cannot read, and that the worker reads them back. Proving that needs a codec
// whose output is unmistakably not the plaintext; it does not need a key
// custody story, which is [github.com/picatz/flowstate/issues/353] workstream
// A.1's real subject and is deliberately absent here.
//
// It is also the executable specification of [payloadcodec.Codec]'s key-id
// contract: it stamps the id of the key it encrypted with, selects the key to
// decrypt with by the id it reads, and refuses an id it does not hold with the
// error a shredded payload produces. A codec's implementation of that contract
// is not obvious from the prose alone, so one lives in the tree.
//
// What is missing, said plainly so nobody mistakes this for a starting point:
// the key is handed over as bytes rather than held by a KMS or derived per
// tenant, the ring holds exactly one key so there is nothing to rotate *to*,
// and nothing here is scoped to a tenant or a subject. A production codec is a
// plugin with custody unified with `flow keys` and the issuer material, not
// this.
package toycodec

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
)

// encodingMetadataKey is the metadata entry marking a payload this codec wrote.
//
// It is what makes Decode able to tolerate a payload written before the codec
// was turned on, which [payloadcodec.Codec] requires of every implementation:
// a deployment that enables encryption has history from before it did, and a
// codec that decrypts unconditionally would fail every one of those reads.
//
// It is the SDK's bare "encoding" name, deliberately, because that is the
// question it answers: what shape are these bytes. Which key wrote them is a
// different question and lives under [payloadcodec.KeyIDMetadataKey], which is
// namespaced because it is Flowstate's own.
const encodingMetadataKey = "encoding"

// encodingName is the value under [encodingMetadataKey].
const encodingName = "binary/flowstate-toy-aesgcm"

// keyIDDomain separates this derivation from any other use of the same key
// material, so that publishing an id says nothing about what else the key
// computes.
const keyIDDomain = "flowstate.toycodec.keyid.v1\x00"

// keyIDBytes is how much of the digest becomes the id: 8 bytes, 16 hex
// characters, well inside [payloadcodec.MaxKeyIDBytes] and far past any chance
// of two keys in one ring colliding.
const keyIDBytes = 8

// KeyID derives the public id of a key.
//
// It is a truncated SHA-256 over a domain-separated copy of the key, which is
// the shape the contract calls for: stable, so the id a payload carries in 2027
// still names the key that wrote it; one-way, so an id sitting in plaintext
// metadata beside its own ciphertext reveals nothing about the key; and derived
// from the key rather than assigned, so two workers handed the same key agree on
// the id without coordinating.
//
// A real codec would take the id from custody instead, because a KMS names its
// own keys. The property that has to survive that change is this one: the id is
// not the key, and cannot be walked back to it.
func KeyID(key []byte) string {
	sum := sha256.Sum256(append([]byte(keyIDDomain), key...))
	return hex.EncodeToString(sum[:keyIDBytes])
}

// ringKey is one key the codec can decrypt with.
//
// The AEAD is reached only through closures, and that is the point of the type.
// CLAUDE.md's containment rule is that fmt cannot call a method on a value it
// reaches through an unexported field, so it prints the fields instead: a
// cipher.AEAD held in a struct field is an expanded AES key schedule that %#v
// will happily render, and the first round key of that schedule is the key. A
// captured variable is not a field, and reflection cannot reach it.
type ringKey struct {
	seal      func(dst, nonce, plaintext []byte) []byte
	open      func(nonce, ciphertext []byte) ([]byte, error)
	nonceSize int
	overhead  int
}

func newRingKey(key []byte) (*ringKey, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("toycodec: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("toycodec: %w", err)
	}
	return &ringKey{
		seal:      func(dst, nonce, plaintext []byte) []byte { return aead.Seal(dst, nonce, plaintext, nil) },
		open:      func(nonce, ciphertext []byte) ([]byte, error) { return aead.Open(nil, nonce, ciphertext, nil) },
		nonceSize: aead.NonceSize(),
		overhead:  aead.Overhead(),
	}, nil
}

// Codec is an AES-256-GCM codec over the whole serialized payload.
//
// Encoding the entire payload, metadata included, rather than only its data
// is deliberate: a payload's metadata says which converter wrote it and, for a
// proto payload, the message's full name. "This run carried a
// flowstate.v1.RunState" is a small leak, but the seam is easier to reason
// about when nothing of the original crosses it.
//
// The plaintext payload's metadata therefore does not survive, which is exactly
// why the two entries an encoded payload carries have to be written fresh: the
// encoding mark, and the key id. They are the only things about a stored payload
// that are readable without a key, and they are the two things a reader must
// know before it can choose one.
type Codec struct {
	// currentID names the key Encode uses. Every payload written by this
	// process carries it.
	currentID string

	// ring is every key Decode will select from, by id. It holds one entry
	// here, and the lookup is still a lookup: an id that is not in it is
	// refused rather than tried against whatever key is current, which is the
	// difference between a shredded payload staying shredded and a rotated
	// deployment silently producing garbage.
	ring map[string]*ringKey

	// envelopeSize is the encoded cost of the two metadata entries, fixed at
	// construction because the id's length is.
	envelopeSize int
}

// New returns a codec over a 32 byte key, with a ring holding it alone.
func New(key []byte) (*Codec, error) {
	rk, err := newRingKey(key)
	if err != nil {
		return nil, err
	}

	id := KeyID(key)
	if err := payloadcodec.ValidateKeyID(id); err != nil {
		return nil, fmt.Errorf("toycodec: derived key id: %w", err)
	}

	return &Codec{
		currentID: id,
		ring:      map[string]*ringKey{id: rk},
		// Measured from an empty-data payload of exactly the shape Encode
		// builds, rather than counted out by hand, because a hand count is a
		// copy of the wire format that drifts the first time encodingName is
		// renamed or the id changes length. A payload with no data omits the
		// data field entirely, so this is the metadata and nothing else.
		envelopeSize: proto.Size(&commonpb.Payload{Metadata: metadataFor(id)}),
	}, nil
}

// metadataFor is the metadata every encoded payload carries, in one place, so
// that what Encode stamps and what the size declaration measures cannot drift
// apart.
func metadataFor(keyID string) map[string][]byte {
	return map[string][]byte{
		encodingMetadataKey:           []byte(encodingName),
		payloadcodec.KeyIDMetadataKey: []byte(keyID),
	}
}

// Name implements [payloadcodec.Codec].
func (c *Codec) Name() string { return "toy-aesgcm" }

// CurrentKeyID implements [payloadcodec.Codec].
func (c *Codec) CurrentKeyID() string { return c.currentID }

// Encode implements [payloadcodec.Codec].
func (c *Codec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	key := c.ring[c.currentID]
	if key == nil {
		// Unreachable through New, and refused rather than dereferenced: a
		// codec that cannot find its own current key must not fall through to
		// some other one.
		return nil, fmt.Errorf("toycodec: no key %q in the ring to encode with", c.currentID)
	}

	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		plaintext, err := proto.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("toycodec: marshaling a payload: %w", err)
		}

		nonce := make([]byte, key.nonceSize)
		if _, err := rand.Read(nonce); err != nil {
			return nil, fmt.Errorf("toycodec: nonce: %w", err)
		}

		out[i] = &commonpb.Payload{
			Metadata: metadataFor(c.currentID),
			Data:     key.seal(nonce, nonce, plaintext),
		}
	}
	return out, nil
}

// dataField is Payload.data's field number, taken from the descriptor because
// the framing around the ciphertext is a tag plus a length, and the tag's width
// depends on a number that belongs to the schema.
var dataField = (&commonpb.Payload{}).ProtoReflect().Descriptor().Fields().ByName("data").Number()

// MaxEncodedSize implements [payloadcodec.Codec], exactly rather than
// approximately.
//
// AES-GCM's output length is its input plus the tag, and this codec prefixes the
// nonce, so every term is known: the ciphertext is the marshaled payload plus a
// nonce and a tag, and the encoded payload is that ciphertext framed as bytes
// alongside the two metadata entries. proto.Marshal produces exactly proto.Size
// bytes, so the answer is reached rather than merely respected, which is what
// TestMaxEncodedSizeIsTheSizeEncodeProduces asserts payload by payload.
//
// The key id is inside this number, which is why it is bounded: an id is
// per-payload overhead exactly as the nonce and the tag are, and an id nobody
// bounded would be expansion nobody checked.
//
// Being exact is the point of declaring at all. A codec that padded this out
// would pass the startup check on nothing that a leaner declaration would have
// failed, but it would fail deployments that fit, and it would do so at startup
// where the operator has no way to see the slack.
func (c *Codec) MaxEncodedSize(plain int) int {
	if plain < 0 {
		plain = 0
	}
	key := c.ring[c.currentID]
	sealed := key.nonceSize + plain + key.overhead
	return c.envelopeSize + protowire.SizeTag(dataField) + protowire.SizeBytes(sealed)
}

// Decode implements [payloadcodec.Codec].
func (c *Codec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		if string(p.GetMetadata()[encodingMetadataKey]) != encodingName {
			// Not ours: a payload written before this codec was configured.
			out[i] = p
			continue
		}

		keyID := string(p.GetMetadata()[payloadcodec.KeyIDMetadataKey])
		if keyID == "" {
			return nil, fmt.Errorf(
				"toycodec: a payload marked %q carries no %q metadata, so it was not written by this "+
					"codec, which stamps a key id on everything it writes. Refusing rather than "+
					"guessing at a key: a payload claiming an origin it does not have is the one case "+
					"where trying the current key would turn a bad payload into a plausible value",
				encodingName, payloadcodec.KeyIDMetadataKey)
		}

		// The id came off a payload, which is input somebody else chose, and
		// the refusal below quotes it. Check it against the grammar before it
		// is used as a map key or shown to anyone.
		if err := payloadcodec.ValidateKeyID(keyID); err != nil {
			return nil, fmt.Errorf("toycodec: a payload names an unusable key id: %w", err)
		}

		key := c.ring[keyID]
		if key == nil {
			return nil, fmt.Errorf(
				"toycodec: this payload was encrypted under key %q, which this codec does not hold. "+
					"If that key was destroyed, this is what destroyed means: the plaintext is gone "+
					"and no key, backup, or repair can bring it back. Otherwise the worker was started "+
					"without the key in its ring",
				keyID)
		}

		data := p.GetData()
		if len(data) < key.nonceSize {
			return nil, fmt.Errorf("toycodec: payload too short to hold a nonce")
		}

		plaintext, err := key.open(data[:key.nonceSize], data[key.nonceSize:])
		if err != nil {
			return nil, fmt.Errorf("toycodec: opening a payload: %w", err)
		}

		var decoded commonpb.Payload
		if err := proto.Unmarshal(plaintext, &decoded); err != nil {
			return nil, fmt.Errorf("toycodec: unmarshaling a payload: %w", err)
		}
		out[i] = &decoded
	}
	return out, nil
}

// Assert the interface at compile time, which is the whole point of the
// dependency this package takes on payloadcodec.
var _ payloadcodec.Codec = (*Codec)(nil)
