package payloadcodec_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
)

// The write encoding, and the property that makes it reversible (#911).
//
// Flowstate hands its converter nothing but proto messages, and the SDK's
// default composite registers ProtoJSONPayloadConverter ahead of the binary
// ProtoPayloadConverter — both match `proto.Message`, and
// CompositeDataConverter.ToPayload takes the first converter that returns a
// payload. So the SDK's registration order, not any decision here, made every
// RunState and every transcript ProtoJSON. #911 reordered that pair.
//
// The tests below are the pair the decision asked for. One says new payloads
// are binary. The rest say the reorder cost nothing on read: both encodings of
// one value decode, through the plain converter and through a codec-wrapped
// one, to values proto.Equal to the original. That property is what makes the
// flip a two-way door, and it holds only while *both* proto converters stay
// registered — which is the thing these tests are really guarding.

// oldEncoding is the converter flowstate wrote with before #911: the SDK
// default, ProtoJSON first. Standing in for history rather than mocking it —
// the bytes a pre-#911 worker put in history are exactly the bytes this
// produces.
func oldEncoding() converter.DataConverter { return converter.GetDefaultDataConverter() }

// sampleValue is a small transcript-shaped message: a map of named values, the
// shape the size argument in #911 is about.
func sampleValue() *v1.Node_Outputs {
	return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		v1.ValueOutput: v1.NewLiteral("ship it"),
		"attempts":     v1.NewLiteral(float64(3)),
		"approved":     v1.NewLiteral(true),
	}}
}

func encodingOf(tb testing.TB, payload *commonpb.Payload) string {
	tb.Helper()

	enc, ok := payload.GetMetadata()[converter.MetadataEncoding]
	require.Truef(tb, ok, "payload carries no %s metadata", converter.MetadataEncoding)

	return string(enc)
}

// TestNewPayloadsAreBinaryProtobuf is the flip itself, asserted on the bytes
// rather than on the converter list: a value serialized through the
// unconfigured path comes out stamped `binary/protobuf`, and the equivalent
// pre-#911 payload comes out `json/protobuf`, so the test would still fail if
// the reorder were reverted and the assertion left behind.
func TestNewPayloadsAreBinaryProtobuf(t *testing.T) {
	t.Parallel()

	value := sampleValue()

	newPayload, err := payloadcodec.Serializer().ToPayload(value)
	require.NoError(t, err)
	require.Equal(t, "binary/protobuf", encodingOf(t, newPayload),
		"payloads are still being written as ProtoJSON; #911 flipped the write side to binary protobuf")

	oldPayload, err := oldEncoding().ToPayload(value)
	require.NoError(t, err)
	require.Equal(t, "json/protobuf", encodingOf(t, oldPayload),
		"test premise broken: the SDK default no longer writes ProtoJSON, so this file is not comparing two encodings")

	// The reason the flip was worth making, on the shape that motivated it.
	// Not a ratio assertion — the 1.03x-1.32x range in size.go is measured
	// against real transcripts, not this one — just the direction.
	require.Less(t, len(newPayload.GetData()), len(oldPayload.GetData()),
		"binary protobuf did not come out smaller than ProtoJSON for a map-shaped transcript")
}

// TestDecodeBothEncodings is the decode-both round trip #911 required, in both
// directions, with the two results compared to each other.
//
// Backward: a payload written by the old converter — which is what every
// history recorded before this change holds — decodes through the new one.
// Forward: a payload written by the new converter decodes too. And the two
// decoded values are proto.Equal, which is the claim that actually matters:
// the encodings are two spellings of one value, not two values.
func TestDecodeBothEncodings(t *testing.T) {
	t.Parallel()

	original := sampleValue()

	oldPayload, err := oldEncoding().ToPayload(original)
	require.NoError(t, err)

	newPayload, err := payloadcodec.Serializer().ToPayload(original)
	require.NoError(t, err)

	var fromOld v1.Node_Outputs
	require.NoError(t, payloadcodec.Serializer().FromPayload(oldPayload, &fromOld),
		"a ProtoJSON payload written before #911 no longer decodes: the reorder became a replace, and every existing history is stranded")

	var fromNew v1.Node_Outputs
	require.NoError(t, payloadcodec.Serializer().FromPayload(newPayload, &fromNew))

	require.True(t, proto.Equal(original, &fromOld), "the old encoding decoded to a different value")
	require.True(t, proto.Equal(original, &fromNew), "the new encoding decoded to a different value")
	require.True(t, proto.Equal(&fromOld, &fromNew), "one value's two encodings decoded to different values")

	// The other direction of the same property, and the one a rollback needs:
	// a payload this build writes is readable by a worker still running the
	// SDK's order. Decode is metadata-driven on both sides, so the flip is
	// reversible per deployment rather than a fleet-wide cutover.
	var oldReadsNew v1.Node_Outputs
	require.NoError(t, oldEncoding().FromPayload(newPayload, &oldReadsNew),
		"a pre-#911 worker cannot read a payload this build writes, which would make the flip one-way")
	require.True(t, proto.Equal(original, &oldReadsNew))
}

// TestDecodeBothEncodingsThroughACodec repeats the round trip on the path a
// deployment with a payload codec actually has.
//
// Worth its own test because the codec sits *below* the serializer: history on
// such a deployment holds codec(protojson) for everything written before this
// change and codec(binary) after, and the encoding metadata the composite
// selects on is inside the ciphertext until the codec has run. If the two
// layers were ever composed the other way round, this is the test that would
// notice.
func TestDecodeBothEncodingsThroughACodec(t *testing.T) {
	t.Parallel()

	cfg := newToyConfig(t)
	original := sampleValue()

	// What that deployment's history holds from before #911: the same codec,
	// wrapping the SDK's converter order.
	oldPayload, err := converter.NewCodecDataConverter(oldEncoding(), cfg.Codec).ToPayload(original)
	require.NoError(t, err)

	newPayload, err := cfg.DataConverter().ToPayload(original)
	require.NoError(t, err)

	var fromOld, fromNew v1.Node_Outputs
	require.NoError(t, cfg.DataConverter().FromPayload(oldPayload, &fromOld),
		"an encrypted ProtoJSON payload written before #911 no longer decodes")
	require.NoError(t, cfg.DataConverter().FromPayload(newPayload, &fromNew))

	require.True(t, proto.Equal(original, &fromOld))
	require.True(t, proto.Equal(&fromOld, &fromNew), "one value's two encodings decoded to different values under a codec")
}

// TestTheSerializerKeepsEveryDefaultConverter guards the reorder against
// becoming a subtraction.
//
// The composite is keyed by encoding on decode, so a converter that is not
// registered is an encoding that cannot be read — and the ones flowstate does
// not itself write are exactly the ones nobody would notice losing until a
// history, a memo, or an SDK-internal payload failed to decode in production.
// Asserting the set, rather than only the two proto converters, is what makes
// this a check on the list instead of on the change.
func TestTheSerializerKeepsEveryDefaultConverter(t *testing.T) {
	t.Parallel()

	// Each case is written by the SDK's own default converter, so the set
	// cannot drift from the SDK it is derived from: whatever the default
	// composite writes, flowstate's must read.
	for _, tc := range []struct {
		name  string
		value any
		into  func() any
	}{
		{"nil", nil, func() any { return new([]byte) }},
		{"byte slice", []byte("bytes"), func() any { return new([]byte) }},
		{"proto", sampleValue(), func() any { return new(v1.Node_Outputs) }},
		{"json", map[string]int{"json": 1}, func() any { return new(map[string]int) }},
	} {
		payload, err := oldEncoding().ToPayload(tc.value)
		require.NoError(t, err)

		require.NoErrorf(t, payloadcodec.Serializer().FromPayload(payload, tc.into()),
			"flowstate's serializer cannot decode %s (%s), which the SDK default writes: the reorder dropped a converter",
			tc.name, encodingOf(t, payload))
	}
}
