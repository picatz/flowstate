package toycodec_test

import (
	"bytes"
	"fmt"
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
