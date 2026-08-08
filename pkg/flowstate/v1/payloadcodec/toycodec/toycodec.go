// Package toycodec is a payload codec for tests, and for nothing else.
//
// It exists so a round-trip test can prove that a configured codec is actually
// on the path a payload takes, that history holds bytes the default converter
// cannot read, and that the worker reads them back. Proving that needs a codec
// whose output is unmistakably not the plaintext; it does not need a key
// custody story, which is [github.com/picatz/flowstate/issues/353] workstream
// A.1's real subject and is deliberately absent here.
//
// What is missing, said plainly so nobody mistakes this for a starting point:
// the key is handed over as bytes rather than held by a KMS or derived per
// tenant, there is no key id in the payload and so no rotation and no
// crypto-shredding, and nothing here is scoped to a tenant or a subject. A
// production codec is a plugin with custody unified with `flow keys` and the
// issuer material, not this.
package toycodec

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
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
const encodingMetadataKey = "encoding"

// encodingName is the value under [encodingMetadataKey].
const encodingName = "binary/flowstate-toy-aesgcm"

// Codec is an AES-256-GCM codec over the whole serialized payload.
//
// Encoding the entire payload, metadata included, rather than only its data
// is deliberate: a payload's metadata says which converter wrote it and, for a
// proto payload, the message's full name. "This run carried a
// flowstate.v1.RunState" is a small leak, but the seam is easier to reason
// about when nothing of the original crosses it.
type Codec struct {
	aead cipher.AEAD
}

// New returns a codec over a 32 byte key.
func New(key []byte) (*Codec, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("toycodec: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("toycodec: %w", err)
	}
	return &Codec{aead: aead}, nil
}

// Name implements [payloadcodec.Codec].
func (c *Codec) Name() string { return "toy-aesgcm" }

// Encode implements [payloadcodec.Codec].
func (c *Codec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		plaintext, err := proto.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("toycodec: marshaling a payload: %w", err)
		}

		nonce := make([]byte, c.aead.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return nil, fmt.Errorf("toycodec: nonce: %w", err)
		}

		out[i] = &commonpb.Payload{
			Metadata: map[string][]byte{encodingMetadataKey: []byte(encodingName)},
			Data:     c.aead.Seal(nonce, nonce, plaintext, nil),
		}
	}
	return out, nil
}

// encodedEnvelopeSize is what an encoded payload costs before its data: the
// metadata entry this codec stamps, framed on the wire.
//
// Measured from an empty-data payload of exactly the shape [Codec.Encode]
// builds, rather than counted out by hand, because a hand count is a copy of the
// wire format that drifts the first time [encodingName] is renamed. A payload
// with no data omits the data field entirely, so this is the metadata and
// nothing else.
var encodedEnvelopeSize = proto.Size(&commonpb.Payload{
	Metadata: map[string][]byte{encodingMetadataKey: []byte(encodingName)},
})

// dataField is Payload.data's field number, taken from the descriptor for the
// same reason: the framing around the ciphertext is a tag plus a length, and
// the tag's width depends on a number that belongs to the schema.
var dataField = (&commonpb.Payload{}).ProtoReflect().Descriptor().Fields().ByName("data").Number()

// MaxEncodedSize implements [payloadcodec.Codec], exactly rather than
// approximately.
//
// AES-GCM's output length is its input plus the tag, and this codec prefixes the
// nonce, so every term is known: the ciphertext is the marshaled payload plus a
// nonce and a tag, and the encoded payload is that ciphertext framed as bytes
// alongside the encoding metadata. proto.Marshal produces exactly proto.Size
// bytes, so the answer is reached rather than merely respected, which is what
// TestMaxEncodedSizeIsTheSizeEncodeProduces asserts payload by payload.
//
// Being exact is the point of declaring at all. A codec that padded this out
// would pass the startup check on nothing that a leaner declaration would have
// failed, but it would fail deployments that fit, and it would do so at startup
// where the operator has no way to see the slack.
func (c *Codec) MaxEncodedSize(plain int) int {
	if plain < 0 {
		plain = 0
	}
	sealed := c.aead.NonceSize() + plain + c.aead.Overhead()
	return encodedEnvelopeSize + protowire.SizeTag(dataField) + protowire.SizeBytes(sealed)
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

		data := p.GetData()
		if len(data) < c.aead.NonceSize() {
			return nil, fmt.Errorf("toycodec: payload too short to hold a nonce")
		}

		plaintext, err := c.aead.Open(nil, data[:c.aead.NonceSize()], data[c.aead.NonceSize():], nil)
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
