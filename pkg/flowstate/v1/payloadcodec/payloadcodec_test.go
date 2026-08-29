package payloadcodec_test

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/toycodec"
)

// testKey is 32 bytes of nothing in particular. A test codec's key is not a
// secret; the thing being tested is where the codec sits, not what it computes.
var testKey = bytes.Repeat([]byte{0x2a}, 32)

func newToyConfig(t *testing.T) payloadcodec.Config {
	t.Helper()

	codec, err := toycodec.New(testKey)
	require.NoError(t, err)

	return payloadcodec.Config{Codec: codec}
}

// TestZeroConfigIsTheNullCodec pins the default. Every deployment that has
// configured nothing must get the serializer itself, not a codec converter
// wrapping an identity codec, which would be a different set of bytes for the
// same value.
//
// The serializer rather than converter.GetDefaultDataConverter() since #911:
// the composite is flowstate's own now, differing from the SDK's only in the
// order of the two proto converters. Pinning the identity here is what stops a
// codec converter quietly reappearing on the unconfigured path.
func TestZeroConfigIsTheNullCodec(t *testing.T) {
	t.Parallel()

	var cfg payloadcodec.Config

	require.False(t, cfg.Enabled())
	require.Equal(t, "none", cfg.Name())
	require.NoError(t, cfg.Validate())
	require.Equal(t, payloadcodec.Serializer(), cfg.DataConverter())
}

// TestCodecRoundTripsAValue is the ordinary direction: a value encoded through
// the codec converter comes back as itself.
func TestCodecRoundTripsAValue(t *testing.T) {
	t.Parallel()

	dc := newToyConfig(t).DataConverter()

	original := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		"approved": v1.NewLiteral(true),
		"note":     v1.NewLiteral("ship it"),
	}}

	payload, err := dc.ToPayload(original)
	require.NoError(t, err)

	var decoded v1.Node_Outputs
	require.NoError(t, dc.FromPayload(payload, &decoded))
	require.True(t, proto.Equal(original, &decoded))
}

// TestEncodedPayloadIsNotReadableWithoutTheCodec is the direction that actually
// says something, and the one CLAUDE.md's "test that A cannot reach B" rule
// asks for: a round trip through one converter proves the converter is
// self-consistent, not that anything was encrypted. What proves the seam is that
// the substrate's own default converter, which is what anyone reading history
// without the key has, can neither read the value nor find it in the bytes.
func TestEncodedPayloadIsNotReadableWithoutTheCodec(t *testing.T) {
	t.Parallel()

	secretish := "ship-it-to-acme-corp"

	payload, err := newToyConfig(t).DataConverter().ToPayload(&v1.Node_Outputs{
		NamedValues: map[string]*v1.Value{"note": v1.NewLiteral(secretish)},
	})
	require.NoError(t, err)

	require.NotContains(t, string(payload.GetData()), secretish,
		"the plaintext survived into the payload the substrate would store")

	var decoded v1.Node_Outputs
	require.Error(t, converter.GetDefaultDataConverter().FromPayload(payload, &decoded),
		"the default converter read a payload it has no key for")
}

// TestApplySetsBothConverters pins the pairing. A client with the codec data
// converter and the SDK's default failure converter is the fail-open shape:
// every payload encrypted, every error message in the clear.
func TestApplySetsBothConverters(t *testing.T) {
	t.Parallel()

	var opts client.Options
	newToyConfig(t).Apply(&opts)

	require.NotNil(t, opts.DataConverter)
	require.NotNil(t, opts.FailureConverter)
	require.NotEqual(t, converter.GetDefaultDataConverter(), opts.DataConverter)
}

// TestFailureEncodingFollowsTheCodec is the failure-path half of the seam.
//
// The SDK's default failure converter writes a failure's message and stack trace
// into history as plain strings (go.temporal.io/sdk@v1.47.0
// internal/failure_converter.go:41, EncodeCommonAttributes defaults to false),
// and an error message is where a rejected value usually ends up. So the
// question is not whether the option exists but whether it is on exactly when a
// codec is, without an operator having to know to ask.
func TestFailureEncodingFollowsTheCodec(t *testing.T) {
	t.Parallel()

	const message = "refused: card ending 4242 is not accepted"

	t.Run("encoded when a codec is configured", func(t *testing.T) {
		t.Parallel()

		cfg := newToyConfig(t)
		fc := cfg.FailureConverter()

		failure := fc.ErrorToFailure(errors.New(message))

		require.NotEqual(t, message, failure.GetMessage(),
			"the failure message went into history in the clear")
		require.Equal(t, "Encoded failure", failure.GetMessage())
		require.Empty(t, failure.GetStackTrace())

		encoded := failure.GetEncodedAttributes()
		require.NotNil(t, encoded, "nothing was encoded in the message's place")
		require.NotContains(t, string(encoded.GetData()), "4242",
			"the encoded attributes are not actually encrypted: the codec was not applied to them")

		// And a worker holding the key still gets a usable error, which is the
		// half that makes this safe to turn on by default alongside a codec.
		require.EqualError(t, fc.FailureToError(failure), message)
	})

	t.Run("untouched when no codec is configured", func(t *testing.T) {
		t.Parallel()

		var cfg payloadcodec.Config

		failure := cfg.FailureConverter().ErrorToFailure(errors.New(message))

		require.Equal(t, message, failure.GetMessage(),
			"a deployment with no codec had its failure behavior changed")
		require.Nil(t, failure.GetEncodedAttributes())
	})
}

// TestDecodeToleratesPayloadsItNeverWrote covers the migration case every
// deployment that turns a codec on will hit on its first read: history written
// before the codec existed.
func TestDecodeToleratesPayloadsItNeverWrote(t *testing.T) {
	t.Parallel()

	original := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"note": v1.NewLiteral("older")}}

	plain, err := converter.GetDefaultDataConverter().ToPayload(original)
	require.NoError(t, err)

	var decoded v1.Node_Outputs
	require.NoError(t, newToyConfig(t).DataConverter().FromPayload(plain, &decoded),
		"a codec refused a payload written before it was configured")
	require.True(t, proto.Equal(original, &decoded))
}

// expandingCodec is the null codec with a declaration bolted on, which is the
// only part of a codec the startup check reads.
//
// It deliberately does not expand anything when it encodes: the check is a check
// of what a codec *says about itself*, made before any payload exists, and a
// test that had to encode two mebibytes to ask the question would be testing a
// different one.
type expandingCodec struct {
	name     string
	declared func(plain int) int

	// keyID is what this codec claims to encrypt with. Empty means "a valid
	// one", so that a test about sizes is not also a test about ids: the
	// key-id tests set it, and the size tests below stay about the size check.
	keyID string
}

func (c expandingCodec) Name() string { return c.name }

func (c expandingCodec) CurrentKeyID() string {
	if c.keyID == "" {
		return "test-key-01"
	}
	return c.keyID
}

func (c expandingCodec) Encode(p []*commonpb.Payload) ([]*commonpb.Payload, error) { return p, nil }

func (c expandingCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) { return p, nil }

func (c expandingCodec) MaxEncodedSize(plain int) int { return c.declared(plain) }

// byFixedOverhead is the shape of every codec that encrypts one payload as one
// payload: a nonce, a tag, a key id, and nothing that grows with the plaintext.
func byFixedOverhead(name string, overhead int) payloadcodec.Codec {
	return expandingCodec{name: name, declared: func(plain int) int { return plain + overhead }}
}

// TestTheNullCodecFitsWithoutAnExemption pins that the default passes the check
// by arithmetic rather than by being skipped. A check every real deployment runs
// and the default does not is a check whose failure mode nobody meets until they
// configure something.
func TestTheNullCodecFitsWithoutAnExemption(t *testing.T) {
	t.Parallel()

	require.NoError(t, payloadcodec.Config{}.Validate())
	require.Equal(t, v1.MaxRunStateBytes, payloadcodec.Null().MaxEncodedSize(v1.MaxRunStateBytes),
		"the identity codec is no longer the identity on sizes")
}

// TestCodecExpansionIsCheckedAtItsExactBoundary decides the boundary rather than
// leaving it to whichever comparison somebody typed.
//
// The budget is spendable to the last byte: a codec whose worst case lands
// exactly on Temporal's limit produces a payload Temporal stores, so refusing it
// would be refusing a deployment that works. One byte further is the run that
// wedges, and it is refused at startup.
func TestCodecExpansionIsCheckedAtItsExactBoundary(t *testing.T) {
	t.Parallel()

	t.Run("exactly at the limit starts", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, payloadcodec.Config{
			Codec: byFixedOverhead("exact-fit", v1.MaxCodecExpansionBytes),
		}.Validate())
	})

	t.Run("one byte past the limit is refused", func(t *testing.T) {
		t.Parallel()

		err := payloadcodec.Config{
			Codec: byFixedOverhead("one-too-many", v1.MaxCodecExpansionBytes+1),
		}.Validate()

		require.Error(t, err, "a codec that overflows the blob limit by a byte was allowed to start")
		require.Contains(t, err.Error(), `"one-too-many"`, "the refusal does not name the codec")
		require.Contains(t, err.Error(), "which is 1 over",
			"the refusal does not say by how much, which is what tells an operator whether "+
				"this is a codec to trim or a codec to replace")
	})
}

// TestCodecThatCannotFitIsRefusedWithSomethingActionable is the negative
// direction the whole slice exists for: a codec whose ciphertext cannot fit
// where its plaintext did must stop the process, not the first run that reaches
// it.
//
// The advice is asserted too, because there is an obvious wrong answer here and
// an operator reading a size error will reach for it. Raising the cluster's blob
// limit does not help: [v1.MaxRunStateBytes] is compiled in, being a determinism
// input, so it does not move with the cluster's configuration.
func TestCodecThatCannotFitIsRefusedWithSomethingActionable(t *testing.T) {
	t.Parallel()

	// Base64 armour over the ciphertext: a third of two mebibytes, which no
	// reserve carved out of the blob limit could ever cover.
	armoured := expandingCodec{
		name:     "armoured-aesgcm",
		declared: func(plain int) int { return (plain+2)/3*4 + 64 },
	}

	err := payloadcodec.Config{Codec: armoured}.Validate()
	require.Error(t, err)

	msg := err.Error()
	require.Contains(t, msg, `"armoured-aesgcm"`, "the refusal does not name the codec")
	require.Contains(t, msg, strconv.Itoa(v1.TemporalDefaultBlobLimitBytes), "the limit is not stated")
	require.Contains(t, msg, strconv.Itoa(v1.MaxCodecExpansionBytes), "the budget is not stated")
	require.Contains(t, msg, strconv.Itoa(v1.MaxRunStateBytes), "the run state bound is not stated")
	require.Contains(t, msg, "leaner", "the refusal does not say what to do")
	require.Contains(t, msg, "Raising the cluster's blob limit is not the fix",
		"the refusal leaves an operator to reach for the wrong lever")
}

// TestADeclarationBelowItsInputIsRefused covers the answer that is not too big
// but is not an answer: a bound under its own input.
//
// Fail closed rather than clamp. The likeliest way to produce one is arithmetic
// that overflowed, and an implementation that has overflowed at two mebibytes is
// not one whose bound should be trusted at any other size.
func TestADeclarationBelowItsInputIsRefused(t *testing.T) {
	t.Parallel()

	err := payloadcodec.Config{
		Codec: expandingCodec{name: "shrinking", declared: func(plain int) int { return plain - 1 }},
	}.Validate()

	require.Error(t, err, "a codec declaring a bound below its own input was allowed to start")
	require.Contains(t, err.Error(), `"shrinking"`)
	require.Contains(t, err.Error(), "smaller than the input")

	// Overflow is the same shape and gets the same answer.
	require.Error(t, payloadcodec.Config{
		Codec: expandingCodec{name: "overflowing", declared: func(int) int { return math.MinInt }},
	}.Validate())
}

// TestValidateChecksTheSizeOfTheCarriedRunState pins *which* size the check asks
// about. It is the maximal run state plus the envelope the run state travels
// inside, because the codec is handed the payload rather than the message, and a
// codec that expands per byte expands the envelope too.
func TestValidateChecksTheSizeOfTheCarriedRunState(t *testing.T) {
	t.Parallel()

	var asked []int
	require.NoError(t, payloadcodec.Config{
		Codec: expandingCodec{name: "observer", declared: func(plain int) int {
			asked = append(asked, plain)
			return plain
		}},
	}.Validate())

	require.Equal(t, []int{v1.MaxRunStateBytes + v1.PayloadEnvelopeReserveBytes}, asked)
}
