package payloadcodec_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
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
// configured nothing must get exactly the payload path it had before this
// package existed, not a codec converter wrapping an identity codec, which
// would be a different set of bytes for the same value.
func TestZeroConfigIsTheNullCodec(t *testing.T) {
	t.Parallel()

	var cfg payloadcodec.Config

	require.False(t, cfg.Enabled())
	require.Equal(t, "none", cfg.Name())
	require.NoError(t, cfg.Validate())
	require.Equal(t, converter.GetDefaultDataConverter(), cfg.DataConverter())
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
