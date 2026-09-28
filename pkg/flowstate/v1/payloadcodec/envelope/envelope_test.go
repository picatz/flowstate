package envelope_test

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
)

// A plaintext marker no ciphertext can contain by accident, so "the bytes do
// not appear" is a meaningful assertion.
const marker = "synthetic-sensitive-7f3a9c"

func testKey(t testing.TB, id string, fill byte) envelope.Key {
	t.Helper()
	k, err := envelope.NewKey(id, bytes.Repeat([]byte{fill}, envelope.KeyBytes))
	require.NoError(t, err)
	return k
}

func newCodec(t testing.TB, opts envelope.Options) *envelope.Codec {
	t.Helper()
	c, err := envelope.New(opts)
	require.NoError(t, err)
	return c
}

func plainPayload(data string) *commonpb.Payload {
	return &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("json/plain"), "messageType": []byte("flowstate.v1.Secretish")},
		Data:     []byte(data),
	}
}

func TestRoundTripHidesPayloadAndMetadata(t *testing.T) {
	t.Parallel()

	c := newCodec(t, envelope.Options{Keys: []envelope.Key{testKey(t, "k1", 1)}, Current: "k1", Binding: "tenant-a"})

	in := plainPayload(`{"token":"` + marker + `"}`)
	enc, err := c.Encode([]*commonpb.Payload{in})
	require.NoError(t, err)
	require.Len(t, enc, 1)

	wire, err := proto.Marshal(enc[0])
	require.NoError(t, err)
	require.NotContains(t, string(wire), marker, "plaintext data survived encoding")
	require.NotContains(t, string(wire), "flowstate.v1.Secretish", "plaintext metadata survived encoding")
	require.Equal(t, envelope.Encoding, string(enc[0].GetMetadata()["encoding"]))
	require.Equal(t, "k1", string(enc[0].GetMetadata()[payloadcodec.KeyIDMetadataKey]))
	require.Len(t, enc[0].GetMetadata(), 2, "an envelope carries exactly the encoding and the key id in the clear")

	dec, err := c.Decode(enc)
	require.NoError(t, err)
	require.True(t, proto.Equal(in, dec[0]))

	// Encode must not mutate its argument.
	require.Equal(t, `{"token":"`+marker+`"}`, string(in.GetData()))
}

func TestEachEncodeIsFresh(t *testing.T) {
	t.Parallel()

	c := newCodec(t, envelope.Options{Keys: []envelope.Key{testKey(t, "k1", 1)}, Current: "k1", Binding: "ns"})
	a, err := c.Encode([]*commonpb.Payload{plainPayload("same")})
	require.NoError(t, err)
	b, err := c.Encode([]*commonpb.Payload{plainPayload("same")})
	require.NoError(t, err)
	require.NotEqual(t, a[0].GetData(), b[0].GetData(), "two encodings of one payload produced identical ciphertext")
}

func TestMaxEncodedSizeIsExact(t *testing.T) {
	t.Parallel()

	c := newCodec(t, envelope.Options{Keys: []envelope.Key{testKey(t, "a-long-key-id-2026-09", 1)}, Current: "a-long-key-id-2026-09", Binding: "ns"})
	for _, n := range []int{0, 1, 127, 128, 16383, 16384, 1 << 20} {
		p := &commonpb.Payload{Data: bytes.Repeat([]byte{'x'}, n)}
		enc, err := c.Encode([]*commonpb.Payload{p})
		require.NoError(t, err)
		require.Equal(t, c.MaxEncodedSize(proto.Size(p)), proto.Size(enc[0]), "declared size drifted from produced size at %d", n)
	}
}

// The codec has to pass the same startup check every codec does: its
// expansion of a maximal run state fits under Temporal's blob limit.
func TestPassesTheStartupCheck(t *testing.T) {
	t.Parallel()

	c := newCodec(t, envelope.Options{
		Keys:    []envelope.Key{testKey(t, strings.Repeat("k", payloadcodec.MaxKeyIDBytes), 1)},
		Current: strings.Repeat("k", payloadcodec.MaxKeyIDBytes),
		Binding: strings.Repeat("n", envelope.MaxBindingBytes),
	})
	require.NoError(t, payloadcodec.Config{Codec: c}.Validate())
	require.LessOrEqual(t, c.MaxEncodedSize(v1.MaxRunStateBytes)-v1.MaxRunStateBytes, v1.MaxCodecExpansionBytes)
}

func TestRotationReadsOldAndWritesNew(t *testing.T) {
	t.Parallel()

	old := testKey(t, "2026-06", 1)
	next := testKey(t, "2026-09", 2)

	before := newCodec(t, envelope.Options{Keys: []envelope.Key{old}, Current: "2026-06", Binding: "ns"})
	oldPayloads, err := before.Encode([]*commonpb.Payload{plainPayload("written before rotation")})
	require.NoError(t, err)

	after := newCodec(t, envelope.Options{Keys: []envelope.Key{next, old}, Current: "2026-09", Binding: "ns"})
	newPayloads, err := after.Encode([]*commonpb.Payload{plainPayload("written after rotation")})
	require.NoError(t, err)
	require.Equal(t, "2026-09", string(newPayloads[0].GetMetadata()[payloadcodec.KeyIDMetadataKey]))

	got, err := after.Decode(append(oldPayloads, newPayloads...))
	require.NoError(t, err)
	require.Equal(t, "written before rotation", string(got[0].GetData()))
	require.Equal(t, "written after rotation", string(got[1].GetData()))

	// A worker that has not yet been given the new key refuses the new
	// history by name, rather than trying its current key.
	_, err = before.Decode(newPayloads)
	require.ErrorIs(t, err, envelope.ErrUnknownKey)
	require.Contains(t, err.Error(), `"2026-09"`)

	// And once the old key is retired, its history is unreadable: this is
	// what destroying a key means.
	retired := newCodec(t, envelope.Options{Keys: []envelope.Key{next}, Current: "2026-09", Binding: "ns"})
	_, err = retired.Decode(oldPayloads)
	require.ErrorIs(t, err, envelope.ErrUnknownKey)
}

func TestCrossNamespaceSpliceFailsEvenUnderTheSameKey(t *testing.T) {
	t.Parallel()

	shared := testKey(t, "shared", 7)
	a := newCodec(t, envelope.Options{Keys: []envelope.Key{shared}, Current: "shared", Binding: "tenant-a"})
	b := newCodec(t, envelope.Options{Keys: []envelope.Key{shared}, Current: "shared", Binding: "tenant-b"})

	sealed, err := a.Encode([]*commonpb.Payload{plainPayload(marker)})
	require.NoError(t, err)

	_, err = b.Decode(sealed)
	require.ErrorIs(t, err, envelope.ErrAuthentication)
	require.NotContains(t, err.Error(), marker)
}

func TestSameIDDifferentMaterialFailsAuthentication(t *testing.T) {
	t.Parallel()

	a := newCodec(t, envelope.Options{Keys: []envelope.Key{testKey(t, "k", 1)}, Current: "k", Binding: "ns"})
	b := newCodec(t, envelope.Options{Keys: []envelope.Key{testKey(t, "k", 2)}, Current: "k", Binding: "ns"})

	sealed, err := a.Encode([]*commonpb.Payload{plainPayload("x")})
	require.NoError(t, err)
	_, err = b.Decode(sealed)
	require.ErrorIs(t, err, envelope.ErrAuthentication)

	require.NotEqual(t, a.Keys()[0].Fingerprint, b.Keys()[0].Fingerprint,
		"fingerprints must tell two different keys under one id apart")
}

// Every byte of the sealed data, and the authenticated metadata, is covered:
// flipping any one of them is refused, never decoded into something else.
func TestTamperingIsRefused(t *testing.T) {
	t.Parallel()

	c := newCodec(t, envelope.Options{Keys: []envelope.Key{testKey(t, "k1", 1), testKey(t, "k2", 1)}, Current: "k1", Binding: "ns"})
	sealed, err := c.Encode([]*commonpb.Payload{plainPayload("tamper me")})
	require.NoError(t, err)

	data := sealed[0].GetData()
	for i := range data {
		mutated := proto.Clone(sealed[0]).(*commonpb.Payload)
		mutated.Data[i] ^= 0x01
		_, err := c.Decode([]*commonpb.Payload{mutated})
		require.ErrorIs(t, err, envelope.ErrAuthentication, "flipping byte %d was not refused", i)
	}

	// Relabeling onto another key the codec holds, even one with identical
	// material, fails: the key id is in the derivation and the AAD.
	relabeled := proto.Clone(sealed[0]).(*commonpb.Payload)
	relabeled.Metadata[payloadcodec.KeyIDMetadataKey] = []byte("k2")
	_, err = c.Decode([]*commonpb.Payload{relabeled})
	require.ErrorIs(t, err, envelope.ErrAuthentication)

	// Truncation and extension.
	short := proto.Clone(sealed[0]).(*commonpb.Payload)
	short.Data = short.Data[:len(short.Data)-1]
	_, err = c.Decode([]*commonpb.Payload{short})
	require.ErrorIs(t, err, envelope.ErrAuthentication)

	long := proto.Clone(sealed[0]).(*commonpb.Payload)
	long.Data = append(long.Data, 0)
	_, err = c.Decode([]*commonpb.Payload{long})
	require.ErrorIs(t, err, envelope.ErrAuthentication)
}

func TestUnencryptedPayloadsAreRefusedUnlessAccepted(t *testing.T) {
	t.Parallel()

	plain := plainPayload(marker)

	strict := newCodec(t, envelope.Options{Keys: []envelope.Key{testKey(t, "k", 1)}, Current: "k", Binding: "ns"})
	_, err := strict.Decode([]*commonpb.Payload{plain})
	require.ErrorIs(t, err, envelope.ErrUnencrypted)
	require.NotContains(t, err.Error(), marker)

	// A payload with no metadata at all is unencrypted too, not a pass.
	_, err = strict.Decode([]*commonpb.Payload{{Data: []byte(marker)}})
	require.ErrorIs(t, err, envelope.ErrUnencrypted)

	migrating := newCodec(t, envelope.Options{Keys: []envelope.Key{testKey(t, "k", 1)}, Current: "k", Binding: "ns", AcceptUnencrypted: true})
	got, err := migrating.Decode([]*commonpb.Payload{plain})
	require.NoError(t, err)
	require.True(t, proto.Equal(plain, got[0]))

	// Accepting unencrypted history never weakens the write side.
	enc, err := migrating.Encode([]*commonpb.Payload{plain})
	require.NoError(t, err)
	require.Equal(t, envelope.Encoding, string(enc[0].GetMetadata()["encoding"]))
}

// A payload that claims to be this envelope must never fall through to the
// unencrypted path, whatever else is wrong with it: that is the downgrade.
func TestAMarkedPayloadNeverDowngrades(t *testing.T) {
	t.Parallel()

	c := newCodec(t, envelope.Options{Keys: []envelope.Key{testKey(t, "k", 1)}, Current: "k", Binding: "ns", AcceptUnencrypted: true})

	cases := map[string]*commonpb.Payload{
		"future version": {Metadata: map[string][]byte{"encoding": []byte("binary/flowstate-envelope-v2"), payloadcodec.KeyIDMetadataKey: []byte("k")}, Data: []byte(marker)},
		"no key id":      {Metadata: map[string][]byte{"encoding": []byte(envelope.Encoding)}, Data: []byte(marker)},
		"bad key id":     {Metadata: map[string][]byte{"encoding": []byte(envelope.Encoding), payloadcodec.KeyIDMetadataKey: []byte("k/../x")}, Data: []byte(marker)},
		"unknown key":    {Metadata: map[string][]byte{"encoding": []byte(envelope.Encoding), payloadcodec.KeyIDMetadataKey: []byte("other")}, Data: []byte(marker)},
		"too short":      {Metadata: map[string][]byte{"encoding": []byte(envelope.Encoding), payloadcodec.KeyIDMetadataKey: []byte("k")}, Data: []byte("x")},
		"too long":       {Metadata: map[string][]byte{"encoding": []byte(envelope.Encoding), payloadcodec.KeyIDMetadataKey: []byte("k")}, Data: make([]byte, v1.TemporalDefaultBlobLimitBytes+1)},
		"garbage":        {Metadata: map[string][]byte{"encoding": []byte(envelope.Encoding), payloadcodec.KeyIDMetadataKey: []byte("k")}, Data: bytes.Repeat([]byte(marker), 4)},
		// The encoding rewritten to a plaintext one, the key id left: still
		// a sealed payload, and still refused.
		"encoding rewritten": {Metadata: map[string][]byte{"encoding": []byte("json/plain"), payloadcodec.KeyIDMetadataKey: []byte("k")}, Data: []byte(marker)},
		"encoding removed":   {Metadata: map[string][]byte{payloadcodec.KeyIDMetadataKey: []byte("k")}, Data: []byte(marker)},
	}
	for name, p := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := c.Decode([]*commonpb.Payload{p})
			require.Error(t, err)
			require.NotContains(t, err.Error(), marker, "a refusal echoed payload bytes")
			for _, sentinel := range []error{envelope.ErrUnknownVersion, envelope.ErrMalformed, envelope.ErrUnknownKey, envelope.ErrAuthentication} {
				if errors.Is(err, sentinel) {
					return
				}
			}
			t.Fatalf("refusal is not classified: %v", err)
		})
	}
}

// TestARewrittenEncodingDoesNotDowngradeASealedPayload is the tamper the
// migration setting would otherwise admit: a real envelope, its encoding
// edited to a plaintext one, handed to a codec that accepts unencrypted
// history.
func TestARewrittenEncodingDoesNotDowngradeASealedPayload(t *testing.T) {
	t.Parallel()

	c := newCodec(t, envelope.Options{Keys: []envelope.Key{testKey(t, "k", 1)}, Current: "k", Binding: "ns", AcceptUnencrypted: true})
	sealed, err := c.Encode([]*commonpb.Payload{plainPayload(marker)})
	require.NoError(t, err)

	tampered := proto.Clone(sealed[0]).(*commonpb.Payload)
	tampered.Metadata["encoding"] = []byte("json/plain")
	_, err = c.Decode([]*commonpb.Payload{tampered})
	require.ErrorIs(t, err, envelope.ErrMalformed)
}

func TestOneBadPayloadFailsTheBatch(t *testing.T) {
	t.Parallel()

	c := newCodec(t, envelope.Options{Keys: []envelope.Key{testKey(t, "k", 1)}, Current: "k", Binding: "ns"})
	good, err := c.Encode([]*commonpb.Payload{plainPayload("fine")})
	require.NoError(t, err)
	out, err := c.Decode([]*commonpb.Payload{good[0], plainPayload("smuggled")})
	require.Error(t, err)
	require.Nil(t, out, "a partially decoded batch was returned")
}

func TestOptionsAreValidated(t *testing.T) {
	t.Parallel()

	k := testKey(t, "k", 1)
	for name, opts := range map[string]envelope.Options{
		"no binding":      {Keys: []envelope.Key{k}, Current: "k"},
		"long binding":    {Keys: []envelope.Key{k}, Current: "k", Binding: strings.Repeat("n", envelope.MaxBindingBytes+1)},
		"no keys":         {Current: "k", Binding: "ns"},
		"missing current": {Keys: []envelope.Key{k}, Current: "other", Binding: "ns"},
		"duplicate id":    {Keys: []envelope.Key{k, testKey(t, "k", 2)}, Current: "k", Binding: "ns"},
		"zero key":        {Keys: []envelope.Key{{}}, Current: "", Binding: "ns"},
	} {
		_, err := envelope.New(opts)
		require.Error(t, err, name)
	}

	_, err := envelope.NewKey("k", make([]byte, 16))
	require.Error(t, err, "a 128-bit key was accepted")
	_, err = envelope.NewKey("bad id", make([]byte, envelope.KeyBytes))
	require.Error(t, err)
}

func TestParseKeyRoundTripsGenerateKeyAndNeverEchoes(t *testing.T) {
	t.Parallel()

	text := envelope.GenerateKey()
	k, err := envelope.ParseKey("gen", text)
	require.NoError(t, err)
	require.Equal(t, "gen", k.ID())
	require.NotEqual(t, envelope.GenerateKey(), text, "two generated keys were equal")

	// Whitespace around the line is fine.
	_, err = envelope.ParseKey("gen", append([]byte("  \n"), text...))
	require.NoError(t, err)

	for _, bad := range []string{"", "not base64 " + marker, "c2hvcnQ="} {
		_, err := envelope.ParseKey("gen", []byte(bad))
		require.Error(t, err)
		require.NotContains(t, err.Error(), marker)
		require.NotContains(t, err.Error(), "c2hvcnQ")
	}
}

// Key material must not escape through any formatting or logging path. The
// material here is a recognizable pattern so any rendering of it is caught.
func TestKeyMaterialDoesNotFormat(t *testing.T) {
	t.Parallel()

	material := bytes.Repeat([]byte{0xAB}, envelope.KeyBytes)
	k, err := envelope.NewKey("visible-id", material)
	require.NoError(t, err)
	c := newCodec(t, envelope.Options{Keys: []envelope.Key{k}, Current: "visible-id", Binding: "ns"})

	var logged bytes.Buffer
	slog.New(slog.NewTextHandler(&logged, nil)).Info("key", "k", k, "codec", c)

	type holder struct{ K envelope.Key }
	renders := []string{
		fmt.Sprintf("%v %+v %#v %s %x %q", k, k, k, k, k, k),
		fmt.Sprintf("%v %+v %#v", holder{k}, []envelope.Key{k}, &holder{k}),
		fmt.Sprintf("%v %+v %#v", c, c, *c),
		logged.String(),
	}
	for _, r := range renders {
		lower := strings.ToLower(r)
		require.NotContains(t, lower, "abab", "key material rendered: %s", r)
		require.NotContains(t, lower, "171, 171", "key material rendered as a byte list: %s", r)
		require.NotContains(t, r, "q6ur", "key material rendered as base64: %s", r)
	}
}

func FuzzDecodeNeverPanicsOrEchoes(f *testing.F) {
	c := newCodec(f, envelope.Options{Keys: []envelope.Key{testKey(f, "k", 1)}, Current: "k", Binding: "ns"})
	sealed, err := c.Encode([]*commonpb.Payload{plainPayload("seed")})
	require.NoError(f, err)
	f.Add([]byte(envelope.Encoding), []byte("k"), sealed[0].GetData())
	f.Add([]byte("json/plain"), []byte(""), []byte("{}"))
	f.Add([]byte("binary/flowstate-envelope-v9"), []byte("k"), []byte{})

	f.Fuzz(func(t *testing.T, encoding, keyID, data []byte) {
		p := &commonpb.Payload{Metadata: map[string][]byte{"encoding": encoding, payloadcodec.KeyIDMetadataKey: keyID}, Data: data}
		out, err := c.Decode([]*commonpb.Payload{p})
		if err != nil {
			// The key id is quoted on purpose, once it passes the grammar, so
			// it is removed before looking for the data: data that happens to
			// spell part of it is not an echo.
			text := strings.ReplaceAll(err.Error(), strconv.Quote(string(keyID)), "")
			if len(data) >= 8 && strings.Contains(text, string(data)) {
				t.Fatalf("refusal echoed payload data")
			}
			return
		}
		// The only success without authentication is refused in strict mode.
		require.Equal(t, envelope.Encoding, string(encoding), "an unencrypted payload decoded in strict mode")
		require.Len(t, out, 1)
	})
}
