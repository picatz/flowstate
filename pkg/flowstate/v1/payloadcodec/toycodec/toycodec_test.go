package toycodec_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/toycodec"
)

// testKey is 32 bytes of nothing in particular: what this codec computes is not
// what is under test, only how much of it there is.
var testKey = bytes.Repeat([]byte{0x2a}, 32)

func newCodec(t *testing.T) *toycodec.Codec {
	t.Helper()

	c, err := toycodec.New(testKey)
	require.NoError(t, err)
	return c
}

// payloadOf builds a payload of a given data length, in the shape the SDK's
// default converter produces: metadata saying which converter wrote it, and the
// bytes.
func payloadOf(n int) *commonpb.Payload {
	return &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/protobuf")},
		Data:     bytes.Repeat([]byte{0x5a}, n),
	}
}

// TestMaxEncodedSizeIsTheSizeEncodeProduces is the test the declaration exists
// for, and it asserts the bound is *reached*, not merely respected.
//
// An upper bound that is never approached is satisfied by any large enough
// number, and a codec that answered with a gibibyte would pass a test written
// only as "encoded <= declared" while refusing to start on every deployment.
// AES-GCM's output length is exactly its input plus the tag, and proto.Marshal
// produces exactly proto.Size bytes, so for this codec the honest declaration is
// an equality and that is what is checked.
//
// The sizes cross the varint width boundaries of the length prefix on the
// ciphertext, since that prefix is the one term in the arithmetic that is not
// constant.
func TestMaxEncodedSizeIsTheSizeEncodeProduces(t *testing.T) {
	t.Parallel()

	codec := newCodec(t)

	for _, n := range []int{0, 1, 63, 64, 127, 128, 16383, 16384, 1 << 20} {
		t.Run(fmt.Sprintf("data=%d", n), func(t *testing.T) {
			t.Parallel()

			in := payloadOf(n)

			out, err := codec.Encode([]*commonpb.Payload{in})
			require.NoError(t, err)
			require.Len(t, out, 1)

			declared := codec.MaxEncodedSize(proto.Size(in))
			actual := proto.Size(out[0])

			require.Equal(t, declared, actual,
				"the declaration and what Encode produced disagree; a declaration under the "+
					"truth is a run that wedges, and one over it is a deployment refused for nothing")

			// And the bytes really are on the wire in that quantity: proto.Size
			// is the same arithmetic the declaration does, so a marshal is what
			// makes this an observation rather than a restatement.
			marshaled, err := proto.Marshal(out[0])
			require.NoError(t, err)
			require.Len(t, marshaled, actual)
		})
	}
}

// TestMaxEncodedSizeIsMonotone pins the property [payloadcodec.Codec] requires
// and the startup check relies on: it is checked at one input, so a codec that
// expanded more at some smaller size would be checked at the wrong place.
func TestMaxEncodedSizeIsMonotone(t *testing.T) {
	t.Parallel()

	codec := newCodec(t)

	prev := codec.MaxEncodedSize(0)
	for n := 1; n <= 1<<16; n *= 2 {
		got := codec.MaxEncodedSize(n)
		require.Greater(t, got, prev, "MaxEncodedSize(%d) did not grow", n)
		require.GreaterOrEqual(t, got, n, "a bound below its own input is not a bound")
		prev = got
	}
}

// TestToyCodecFitsUnderTheBlobLimit is the whole point of the declaration,
// asserted at the size that actually decides it: an authentication tag and a
// nonce per payload is tens of bytes, so a real encrypting codec of this shape
// fits inside the reserve with room to spare, and Flowstate's own test codec had
// better be one of them.
func TestToyCodecFitsUnderTheBlobLimit(t *testing.T) {
	t.Parallel()

	codec := newCodec(t)

	require.NoError(t, payloadcodec.Config{Codec: codec}.Validate())

	expansion := codec.MaxEncodedSize(v1.MaxRunStateBytes) - v1.MaxRunStateBytes
	require.Positive(t, expansion, "an encrypting codec that expands by nothing is not encrypting")
	require.Less(t, expansion, v1.MaxCodecExpansionBytes,
		"the toy codec no longer fits the budget the startup check hands a codec")
	t.Logf("toy codec expands a maximal run state by %d bytes, of %d available",
		expansion, v1.MaxCodecExpansionBytes)
}

// otherKey is a second key, so that "the ring selected by id" can be told apart
// from "the ring held one key and used it".
var otherKey = bytes.Repeat([]byte{0x77}, 32)

// TestEncodeStampsTheCurrentKeyID is the contract's write half: a payload this
// codec wrote says which key wrote it, in plaintext metadata, because that is
// the only thing a reader has before it has chosen a key.
func TestEncodeStampsTheCurrentKeyID(t *testing.T) {
	t.Parallel()

	codec := newCodec(t)

	out, err := codec.Encode([]*commonpb.Payload{payloadOf(16)})
	require.NoError(t, err)
	require.Len(t, out, 1)

	metadata := out[0].GetMetadata()
	require.Equal(t, codec.CurrentKeyID(), string(metadata[payloadcodec.KeyIDMetadataKey]),
		"the payload does not name the key that encrypted it, so nothing can rotate off it or shred it")
	require.Equal(t, "binary/flowstate-toy-aesgcm", string(metadata["encoding"]))
	require.Len(t, metadata, 2,
		"an encoded payload carries the encoding mark and the key id, and nothing else readable "+
			"without a key")
}

// TestKeyIDIsDerivedFromTheKeyAndMeetsTheGrammar pins the three properties the
// contract asks of an id: it is stable, it differs between keys, and it can be
// spelled.
func TestKeyIDIsDerivedFromTheKeyAndMeetsTheGrammar(t *testing.T) {
	t.Parallel()

	id := toycodec.KeyID(testKey)

	require.NoError(t, payloadcodec.ValidateKeyID(id))
	require.Equal(t, id, toycodec.KeyID(testKey),
		"the id is not stable, so a payload written today names nothing tomorrow")
	require.NotEqual(t, id, toycodec.KeyID(otherKey),
		"two keys share an id, so a ring cannot tell them apart")

	require.Equal(t, id, newCodec(t).CurrentKeyID())
}

// TestDecodeSelectsTheKeyByTheIDThePayloadNames is the read half, written as the
// negative direction on purpose: what has to be true is not that a codec reads
// its own payload, which any codec does, but that a codec refuses one it has no
// key for instead of reaching for the key it happens to hold.
//
// That fallback is the bug this slice exists to make impossible. It turns a
// rotated deployment into garbled plaintext, and it is the difference between a
// shredded payload staying shredded and a shred that quietly did nothing.
func TestDecodeSelectsTheKeyByTheIDThePayloadNames(t *testing.T) {
	t.Parallel()

	writer := newCodec(t)

	reader, err := toycodec.New(otherKey)
	require.NoError(t, err)

	encoded, err := writer.Encode([]*commonpb.Payload{payloadOf(32)})
	require.NoError(t, err)

	// The codec that wrote it reads it.
	back, err := writer.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, payloadOf(32).GetData(), back[0].GetData())

	// The codec that did not, does not, and says so as a key it does not hold
	// rather than as bytes it could not open.
	_, err = reader.Decode(encoded)
	require.Error(t, err, "a codec decoded a payload written under a key it does not hold")

	msg := err.Error()
	require.Contains(t, msg, writer.CurrentKeyID(),
		"the refusal does not name the key, which is the one thing an operator needs to look it up")
	require.Contains(t, msg, "does not hold")
	require.Contains(t, msg, "this is what destroyed means",
		"the refusal reads as corruption; after a deliberate shred this is the message an operator "+
			"gets, and it must send them to the shred rather than to a bug hunt")
	require.NotContains(t, msg, "opening a payload",
		"the ring lookup fell through and the current key was tried anyway")
}

// TestAPayloadMarkedOursWithNoKeyIDIsRefused covers the one case that must not
// be waved through as pre-codec history. A payload carrying no mark of this
// codec is history from before it was configured, and is passed along; a payload
// carrying the mark but no id is claiming an origin it does not have.
func TestAPayloadMarkedOursWithNoKeyIDIsRefused(t *testing.T) {
	t.Parallel()

	codec := newCodec(t)

	// Tolerated: no mark at all.
	plain := payloadOf(8)
	out, err := codec.Decode([]*commonpb.Payload{plain})
	require.NoError(t, err, "a payload written before the codec was configured was refused")
	require.Equal(t, plain.GetData(), out[0].GetData())

	// Refused: marked, unattributed.
	forged := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/flowstate-toy-aesgcm")},
		Data:     bytes.Repeat([]byte{0x01}, 64),
	}
	_, err = codec.Decode([]*commonpb.Payload{forged})
	require.Error(t, err, "a payload claiming this codec's encoding but naming no key was decoded anyway")
	require.Contains(t, err.Error(), payloadcodec.KeyIDMetadataKey)
	require.Contains(t, err.Error(), "not written by this codec")
}

// TestAnUnusableKeyIDIsRefusedWithoutEchoingIt is the bounded-input direction.
// The id on a payload is chosen by whoever wrote the payload, and the refusal
// path quotes ids, so an id outside the grammar is refused before it is used as
// a map key or shown to anybody.
func TestAnUnusableKeyIDIsRefusedWithoutEchoingIt(t *testing.T) {
	t.Parallel()

	codec := newCodec(t)

	for name, id := range map[string]string{
		"far past the length bound": strings.Repeat("z", 4096),
		"a newline in the id":       "abc\ndef",
		"a control byte":            "abc\x00def",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := codec.Decode([]*commonpb.Payload{{
				Metadata: map[string][]byte{
					"encoding":                    []byte("binary/flowstate-toy-aesgcm"),
					payloadcodec.KeyIDMetadataKey: []byte(id),
				},
				Data: bytes.Repeat([]byte{0x01}, 64),
			}})

			require.Error(t, err, "an id outside the grammar was used to look up a key")
			require.NotContains(t, err.Error(), id,
				"the refusal echoed an id somebody else chose, which is how a log line comes to say "+
					"whatever the writer of a payload wanted it to say")
		})
	}
}

// TestKeyMaterialIsNeverInAPayloadOrItsRendering is the containment direction
// this slice owes, and it is owed precisely because the id is deliberately in
// the clear: something derived from the key now travels in plaintext beside the
// ciphertext, so what has to be proven is that what travels is the derivation
// and never the key.
//
// The shapes are CLAUDE.md's: %v, %+v, %#v and %s, on the value and on a slice
// of them, because a redacting String method does nothing for a value reached
// through an unexported field, and the key here is reached through several.
func TestKeyMaterialIsNeverInAPayloadOrItsRendering(t *testing.T) {
	t.Parallel()

	// A key that is its own tell in every encoding a formatter might choose.
	key := []byte("k3y-mat3rial-do-not-print-012345")
	require.Len(t, key, 32)

	codec, err := toycodec.New(key)
	require.NoError(t, err)

	encoded, err := codec.Encode([]*commonpb.Payload{payloadOf(128)})
	require.NoError(t, err)

	// On the wire first: the bytes the substrate would actually store.
	marshaled, err := proto.Marshal(encoded[0])
	require.NoError(t, err)
	require.NotContains(t, string(marshaled), string(key),
		"the key travelled with the ciphertext it encrypted")
	require.NotContains(t, codec.CurrentKeyID(), string(key))
	require.NotContains(t, hex.EncodeToString(key), codec.CurrentKeyID(),
		"the id is a slice of the key rather than a one-way function of it")

	type holder struct{ Payload *commonpb.Payload }

	subjects := map[string]any{
		"payload":              encoded[0],
		"payload slice":        encoded,
		"struct holding one":   holder{Payload: encoded[0]},
		"slice of those":       []holder{{Payload: encoded[0]}},
		"codec":                codec,
		"codec slice":          []*toycodec.Codec{codec},
		"struct holding codec": struct{ C *toycodec.Codec }{C: codec},
	}

	for label, subject := range subjects {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			// The verb is the variable under test: an operator's log line picks
			// one of these, and the claim is that every one of them is safe.
			rendered := fmt.Sprintf(verb, subject)
			assertNoKeyMaterial(t, fmt.Sprintf("%s under %s", label, verb), rendered, key)
		}
	}
}

// assertNoKeyMaterial looks for the key in every encoding a formatter plausibly
// renders bytes in: as text, as the decimal list %v gives a []byte, as the
// escaped form %q gives one, as Go syntax, and as hex.
//
// One search would prove nothing. fmt renders a []byte as decimals, so a raw
// substring search over a %v of a struct full of bytes cannot see the key even
// when it is sitting right there.
func assertNoKeyMaterial(t *testing.T, where, rendered string, key []byte) {
	t.Helper()

	needles := map[string]string{
		"as text":     string(key),
		"as decimals": strings.Trim(fmt.Sprintf("%v", key), "[]"),
		"escaped":     strings.Trim(fmt.Sprintf("%q", key), `"`),
		"as hex":      hex.EncodeToString(key),
		"as Go bytes": strings.TrimSuffix(strings.TrimPrefix(fmt.Sprintf("%#v", key), "[]byte{"), "}"),
	}

	for name, needle := range needles {
		require.NotContains(t, rendered, needle,
			"%s: the key appears %s. Key material reachable through a formatter is key material in "+
				"whatever log line printed it", where, name)
	}
}
