package temporalclient_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/toycodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
)

// TestOptionsCarryTheCodec pins the single construction point.
//
// [temporalclient.Config.Options] is the one function that turns this
// deployment's configuration into client options, and both things that dial go
// through it: [temporalclient.Dial], and [temporalclient.NewPool], which dials
// one client per mapped Temporal namespace. Asserting here rather than at each
// call site is the point: a codec applied at Dial would encrypt the fallback
// client's payloads and write every mapped tenant's in plaintext.
func TestOptionsCarryTheCodec(t *testing.T) {
	t.Parallel()

	toy, err := toycodec.New(bytes.Repeat([]byte{0x2a}, 32))
	require.NoError(t, err)

	opts, err := temporalclient.Config{Codec: payloadcodec.Config{Codec: toy}}.Options()
	require.NoError(t, err)

	require.NotNil(t, opts.DataConverter)
	require.NotEqual(t, converter.GetDefaultDataConverter(), opts.DataConverter,
		"the codec did not reach the client's data converter")

	// The pairing, not just the converter: a client encrypting payloads while
	// writing error messages into history in the clear is the fail-open shape
	// this slot exists to make unrepresentable.
	require.NotNil(t, opts.FailureConverter)

	failure := opts.FailureConverter.ErrorToFailure(errNotAcceptable)
	require.NotEqual(t, errNotAcceptable.Error(), failure.GetMessage(),
		"the failure converter left the message in the clear beside an encrypted payload path")
}

// TestUnconfiguredOptionsAreUnchanged is the other half: a deployment that has
// configured no codec must get the byte-for-byte payload path it had before the
// slot existed, including the SDK's own failure behavior.
func TestUnconfiguredOptionsAreUnchanged(t *testing.T) {
	t.Parallel()

	opts, err := temporalclient.Config{}.Options()
	require.NoError(t, err)

	require.Equal(t, converter.GetDefaultDataConverter(), opts.DataConverter)

	failure := opts.FailureConverter.ErrorToFailure(errNotAcceptable)
	require.Equal(t, errNotAcceptable.Error(), failure.GetMessage())
	require.Nil(t, failure.GetEncodedAttributes())
}

// errNotAcceptable is an error whose message is the kind of thing that ends up
// in a failure: a rejected value, quoted back.
var errNotAcceptable = errTest("refused: card ending 4242 is not accepted")

type errTest string

func (e errTest) Error() string { return string(e) }
