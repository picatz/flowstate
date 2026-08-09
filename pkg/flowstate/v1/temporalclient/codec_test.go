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

// A codec whose ciphertext cannot fit inside the blob limit never becomes
// client options: the same declaration [payloadcodec.Config.Validate] checks
// has to be checked on this path too, because this is the path `flow worker`
// and `flow server` build their clients through. Only the CLI's own resolution
// refusing it would leave a worker constructed some other way to start, write
// history, and wedge a run at its first Continue-As-New.
func TestOptionsRefuseACodecThatCannotFit(t *testing.T) {
	t.Parallel()

	_, err := temporalclient.Config{Codec: payloadcodec.Config{Codec: overExpandingCodec{}}}.Options()
	require.Error(t, err,
		"a codec that expands past the blob limit was turned into client options; a worker built "+
			"from them would wedge the first run that reaches a Continue-As-New")
	require.Contains(t, err.Error(), "over-expanding",
		"the refusal does not name the codec, so an operator cannot tell which configuration to fix")
}

// overExpandingCodec declares more expansion than any reserve can cover. Only
// the declaration matters to the test: Encode and Decode are the identity, and
// are never reached, because the refusal happens before a payload exists.
type overExpandingCodec struct{ payloadcodec.Codec }

func (overExpandingCodec) Name() string { return "over-expanding" }

// Declared rather than inherited from the embedded nil interface: the startup
// check asks for the key id before it asks for the size, so a codec that
// answered this by dereferencing nothing would panic in place of being refused.
func (overExpandingCodec) CurrentKeyID() string { return "over-expanding-key" }

func (overExpandingCodec) MaxEncodedSize(plain int) int { return plain + plain/2 }
