package envelope_test

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"golang.org/x/crypto/chacha20poly1305"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
)

// The construction, implemented a second time from the package doc alone and
// with fixed randomness, so the format is pinned by something other than the
// code under test: a change to a label, an encoding, the order of the AAD, or
// the header's framing fails here even when Encode and Decode change in step.

func prefixed(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func specInfo(label string, suite uint32, keyID, binding string) string {
	b := append([]byte(label), 0)
	b = binary.BigEndian.AppendUint32(b, suite)
	b = prefixed(b, keyID)
	return string(prefixed(b, binding))
}

func specDerive(t testing.TB, dataKey, salt []byte, suite uint32, keyID, binding string) (ck, c []byte) {
	t.Helper()
	prk, err := hkdf.Extract(sha256.New, dataKey, salt)
	require.NoError(t, err)
	ck, err = hkdf.Expand(sha256.New, prk, specInfo(envelope.ContentKeyLabel, suite, keyID, binding), 32)
	require.NoError(t, err)
	c, err = hkdf.Expand(sha256.New, prk, specInfo(envelope.CommitmentLabel, suite, keyID, binding), 32)
	require.NoError(t, err)
	return ck, c
}

// TestTheDerivationIsPinned: fixed inputs, fixed outputs. Changing either
// constant is a format change, which no history can survive.
func TestTheDerivationIsPinned(t *testing.T) {
	t.Parallel()

	ck, c := specDerive(t, bytes.Repeat([]byte{0x01}, 32), bytes.Repeat([]byte{0x02}, 32), 1, "k1", "ns")
	require.Equal(t, pinnedContentKey, hex.EncodeToString(ck))
	require.Equal(t, pinnedCommitment, hex.EncodeToString(c))
}

const (
	pinnedContentKey = "8cfec912a9ba1f4b9c3d9779398c2f9136485d9f84f2ecf58c639db1b3e24d29"
	pinnedCommitment = "698b135d1eacd869b1b6276ec569f56e8f36fc584cab332b358adbbd850e263b"
)

// TestAnIndependentSealerIsReadByDecode seals with the spec's own steps and a
// fixed salt and nonce, for each suite, and requires the production codec to
// open it.
func TestAnIndependentSealerIsReadByDecode(t *testing.T) {
	t.Parallel()

	key := localKey(t, 7)
	opts := envelope.Options{Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: key}},
		DecryptSuites: []envelope.Suite{envelope.SuiteAES256GCM, envelope.SuiteXChaCha20Poly1305}}
	c := newCodec(t, opts)

	for _, suite := range []uint32{uint32(envelope.SuiteAES256GCM), uint32(envelope.SuiteXChaCha20Poly1305)} {
		dataKey := bytes.Repeat([]byte{0x33}, 32)
		wrapped, err := key.Wrap(t.Context(), dataKey, keyprovider.Context{Namespace: "ns", KeyID: "k1", Suite: suite})
		require.NoError(t, err)

		salt := bytes.Repeat([]byte{0x44}, 32)
		ck, commitment := specDerive(t, dataKey, salt, suite, "k1", "ns")
		h, err := proto.MarshalOptions{Deterministic: true}.Marshal(&v1.PayloadEnvelopeHeader{
			Suite: suite, KeyId: "k1", Salt: salt, Commitment: commitment, WrappedKey: wrapped.Bytes,
		})
		require.NoError(t, err)

		framed := append([]byte(envelope.Magic), binary.AppendUvarint(nil, uint64(len(h)))...)
		framed = append(framed, h...)
		aad := prefixed(append([]byte(envelope.AADLabel), 0), "ns")
		aad = append(aad, framed...)

		plaintext, err := proto.MarshalOptions{Deterministic: true}.Marshal(plainPayload(marker))
		require.NoError(t, err)

		var body []byte
		switch envelope.Suite(suite) {
		case envelope.SuiteAES256GCM:
			block, err := aes.NewCipher(ck)
			require.NoError(t, err)
			gcm, err := cipher.NewGCM(block)
			require.NoError(t, err)
			nonce := bytes.Repeat([]byte{0x55}, 12)
			body = gcm.Seal(append([]byte(nil), nonce...), nonce, plaintext, aad)
		case envelope.SuiteXChaCha20Poly1305:
			x, err := chacha20poly1305.NewX(ck)
			require.NoError(t, err)
			nonce := bytes.Repeat([]byte{0x66}, 24)
			body = x.Seal(append([]byte(nil), nonce...), nonce, plaintext, aad)
		}

		sealed := &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte(envelope.Encoding), payloadcodec.KeyIDMetadataKey: []byte("k1")},
			Data:     append(framed, body...),
		}
		out, err := c.Decode([]*commonpb.Payload{sealed})
		require.NoError(t, err, "suite %d", suite)
		require.True(t, proto.Equal(plainPayload(marker), out[0]))
	}
}
