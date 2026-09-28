package envelope_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/hpke"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/local"
)

const marker = "synthetic-plaintext-7c41"

func localKey(t testing.TB, fill byte) *local.Key {
	t.Helper()
	k, err := local.NewKey(bytes.Repeat([]byte{fill}, local.KeyBytes))
	require.NoError(t, err)
	return k
}

func newCodec(t testing.TB, opts envelope.Options) *envelope.Codec {
	t.Helper()
	c, err := envelope.New(t.Context(), opts)
	require.NoError(t, err)
	return c
}

func oneKey(t testing.TB, binding string) envelope.Options {
	return envelope.Options{Binding: binding, Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: localKey(t, 1)}}}
}

func plainPayload(data string) *commonpb.Payload {
	return &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("json/plain"), "messageType": []byte("secret.Type")},
		Data:     []byte(data),
	}
}

func seal(t testing.TB, c *envelope.Codec, data string) *commonpb.Payload {
	t.Helper()
	out, err := c.Encode([]*commonpb.Payload{plainPayload(data)})
	require.NoError(t, err)
	return out[0]
}

func header(t testing.TB, p *commonpb.Payload) *v1.PayloadEnvelopeHeader {
	t.Helper()
	data := p.GetData()
	require.True(t, bytes.HasPrefix(data, []byte(envelope.Magic)))
	n, read := uvarint(data[len(envelope.Magic):])
	var h v1.PayloadEnvelopeHeader
	require.NoError(t, proto.Unmarshal(data[len(envelope.Magic)+read:len(envelope.Magic)+read+int(n)], &h))
	return &h
}

func uvarint(b []byte) (uint64, int) {
	var x uint64
	for i, c := range b {
		x |= uint64(c&0x7f) << (7 * i)
		if c < 0x80 {
			return x, i + 1
		}
	}
	return 0, 0
}

func TestRoundTripHidesPayloadAndMetadata(t *testing.T) {
	t.Parallel()

	for _, suite := range []envelope.Suite{envelope.SuiteAES256GCM, envelope.SuiteXChaCha20Poly1305} {
		opts := oneKey(t, "ns")
		opts.Suite = suite
		c := newCodec(t, opts)

		sealed := seal(t, c, marker)
		require.Equal(t, envelope.Encoding, string(sealed.GetMetadata()["encoding"]))
		require.Equal(t, "k1", string(sealed.GetMetadata()[payloadcodec.KeyIDMetadataKey]))
		require.Len(t, sealed.GetMetadata(), 2, "only the encoding and the key id may be in the clear")
		require.NotContains(t, string(sealed.GetData()), marker)
		require.NotContains(t, string(sealed.GetData()), "secret.Type", "the original metadata must be sealed too")
		require.Equal(t, uint32(suite), header(t, sealed).GetSuite())

		out, err := c.Decode([]*commonpb.Payload{sealed})
		require.NoError(t, err)
		require.True(t, proto.Equal(plainPayload(marker), out[0]), suite.String())
	}
}

// TestADataKeyIsReusedWithinItsWindow is envelope encryption's point: one
// wrap serves many payloads, each still sealed under its own content key.
func TestADataKeyIsReusedWithinItsWindow(t *testing.T) {
	t.Parallel()

	counting := &countingKey{Key: localKey(t, 1)}
	c := newCodec(t, envelope.Options{Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: counting}}})

	a, b := seal(t, c, marker), seal(t, c, marker)
	require.NotEqual(t, a.GetData(), b.GetData(), "two encodes of one payload must differ")
	require.Equal(t, header(t, a).GetWrappedKey(), header(t, b).GetWrappedKey(), "one data key serves the window")
	require.NotEqual(t, header(t, a).GetSalt(), header(t, b).GetSalt(), "each payload has its own content key")
	require.EqualValues(t, 1, counting.wraps.Load(), "one wrap at startup, none per payload")
	_, err := c.Decode([]*commonpb.Payload{a, b})
	require.NoError(t, err)
	require.Zero(t, counting.unwraps.Load(), "a process reads what it wrote without asking the provider")
}

func TestADataKeyRollsOverAtEachBound(t *testing.T) {
	t.Parallel()

	var clock atomic.Int64
	clock.Store(time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()) }

	for name, tc := range map[string]struct {
		policy  *v1.PayloadDataKeyPolicy
		advance func()
	}{
		"age":      {policy: &v1.PayloadDataKeyPolicy{MaxAge: durationpb.New(time.Minute)}, advance: func() { clock.Add(int64(time.Minute)) }},
		"messages": {policy: &v1.PayloadDataKeyPolicy{MaxMessages: 2}, advance: func() {}},
		"bytes":    {policy: &v1.PayloadDataKeyPolicy{MaxBytes: 200}, advance: func() {}},
	} {
		opts := oneKey(t, "ns")
		opts.DataKey = tc.policy
		envelope.SetClock(&opts, now)
		c := newCodec(t, opts)

		first := header(t, seal(t, c, marker)).GetWrappedKey()
		require.Equal(t, first, header(t, seal(t, c, "x")).GetWrappedKey(), name)
		tc.advance()
		third := seal(t, c, strings.Repeat("y", 200))
		require.NotEqual(t, first, header(t, third).GetWrappedKey(), "%s: the data key did not roll over", name)
		_, err := c.Decode([]*commonpb.Payload{third})
		require.NoError(t, err, name)
	}
}

// TestAnUnreachableProviderStopsWritesNotReads: while the provider is down,
// payloads under the data key still in its window are written and read from
// the cache; past the window, writes stop (nothing is written unsealed) unless
// a stale grace keeps the old key sealing, and then this process can still
// read what it wrote in the grace. A failed rollover is not retried on every
// encode.
func TestAnUnreachableProviderStopsWritesNotReads(t *testing.T) {
	t.Parallel()

	for _, grace := range []time.Duration{0, 30 * time.Second} {
		var clock atomic.Int64
		clock.Store(time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC).UnixNano())
		now := func() time.Time { return time.Unix(0, clock.Load()) }

		flaky := &flakyKey{Key: localKey(t, 1)}
		opts := envelope.Options{Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: flaky}},
			DataKey: &v1.PayloadDataKeyPolicy{MaxAge: durationpb.New(time.Minute), StaleGrace: durationpb.New(grace)}}
		envelope.SetClock(&opts, now)
		c := newCodec(t, opts)
		before := seal(t, c, marker)

		// Down, inside the window: the cached data key serves both ways.
		flaky.down.Store(true)
		clock.Add(int64(30 * time.Second))
		inWindow := seal(t, c, marker)
		_, err := c.Decode([]*commonpb.Payload{before, inWindow})
		require.NoError(t, err, "grace %s: a cached data key reads while the provider is down", grace)

		// Past the window.
		clock.Add(int64(31 * time.Second))
		wrapsBefore := flaky.wraps.Load()
		_, err = c.Encode([]*commonpb.Payload{plainPayload(marker)})
		if grace == 0 {
			require.ErrorIs(t, err, envelope.ErrProviderUnavailable)
			_, err = c.Decode([]*commonpb.Payload{before})
			require.ErrorIs(t, err, envelope.ErrProviderUnavailable,
				"past the window, an uncached data key needs the provider")
		} else {
			require.NoError(t, err, "within the grace, the old data key keeps sealing")
			inGrace := seal(t, c, marker)
			_, err = c.Decode([]*commonpb.Payload{before, inWindow, inGrace})
			require.NoError(t, err, "this process reads what it wrote in the grace")

			clock.Add(int64(grace))
			_, err = c.Encode([]*commonpb.Payload{plainPayload(marker)})
			require.ErrorIs(t, err, envelope.ErrProviderUnavailable, "past the grace, sealing stops")
		}
		for range 10 {
			_, _ = c.Encode([]*commonpb.Payload{plainPayload(marker)})
		}
		require.LessOrEqual(t, flaky.wraps.Load()-wrapsBefore, int64(2),
			"grace %s: every encode asked the unreachable provider again", grace)

		// Back up: the next attempt after the backoff succeeds.
		flaky.down.Store(false)
		clock.Add(int64(3 * time.Second))
		_ = seal(t, c, marker)
	}
}

func TestStartupFailsClosedWhenTheProviderCannotWrap(t *testing.T) {
	t.Parallel()

	flaky := &flakyKey{Key: localKey(t, 1)}
	flaky.down.Store(true)
	_, err := envelope.New(t.Context(), envelope.Options{Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: flaky}}})
	require.ErrorIs(t, err, envelope.ErrProviderUnavailable)
}

// TestConcurrentReadsOfOneDataKeyAskTheProviderOnce: a page of history under
// one data key costs one unwrap, however many goroutines decode it.
func TestConcurrentReadsOfOneDataKeyAskTheProviderOnce(t *testing.T) {
	t.Parallel()

	// In a bubble, so "every reader has arrived while the first unwrap is
	// in flight" is a state the test waits for rather than a race it hopes
	// to win: the provider holds its unwrap until every goroutine is blocked.
	synctest.Test(t, func(t *testing.T) {
		key := localKey(t, 1)
		writer := newCodec(t, envelope.Options{Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: key}}})
		sealed := seal(t, writer, marker)

		counting := &countingKey{Key: key, gate: make(chan struct{})}
		reader := newCodec(t, envelope.Options{Binding: "ns", Keys: []envelope.Recipient{{ID: "k1", Key: counting}}})

		var wg sync.WaitGroup
		for range 64 {
			wg.Go(func() {
				_, err := reader.Decode([]*commonpb.Payload{sealed})
				require.NoError(t, err)
			})
		}
		synctest.Wait()
		close(counting.gate)
		wg.Wait()
		require.EqualValues(t, 1, counting.unwraps.Load())
	})
}

// TestADefinitiveRefusalIsRememberedBriefly: a revoked key is asked once, not
// once per payload; an unavailable one is asked every time.
func TestADefinitiveRefusalIsRememberedBriefly(t *testing.T) {
	t.Parallel()

	key := localKey(t, 1)
	sealed := seal(t, newCodec(t, envelope.Options{Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: key}}}), marker)

	for _, tc := range []struct {
		refusal error
		want    error
		asks    int64
	}{
		{keyprovider.ErrDenied, envelope.ErrKeyDenied, 1},
		{keyprovider.ErrUnavailable, envelope.ErrProviderUnavailable, 3},
	} {
		refusing := &countingKey{Key: key, refuse: tc.refusal}
		reader := newCodec(t, envelope.Options{Binding: "ns", Keys: []envelope.Recipient{{ID: "k1", Key: refusing}}})
		for range 3 {
			_, err := reader.Decode([]*commonpb.Payload{sealed})
			require.ErrorIs(t, err, tc.want)
		}
		require.Equal(t, tc.asks, refusing.unwraps.Load(), tc.refusal.Error())
	}
}

func TestMaxEncodedSizeBoundsEveryPayload(t *testing.T) {
	t.Parallel()

	for _, suite := range []envelope.Suite{envelope.SuiteAES256GCM, envelope.SuiteXChaCha20Poly1305} {
		opts := oneKey(t, "ns")
		opts.Suite = suite
		opts.Escrow = []envelope.Recipient{{ID: "vault-escrow", Key: hpkeRecipient(t, false)}}
		c := newCodec(t, opts)
		for _, size := range []int{0, 1, 100, 64 << 10} {
			sealed := seal(t, c, strings.Repeat("a", size))
			plain := proto.Size(plainPayload(strings.Repeat("a", size)))
			got, bound := proto.Size(sealed), c.MaxEncodedSize(plain)
			require.LessOrEqual(t, got, bound, "%s: %d-byte payload", suite, size)
			require.Less(t, bound-got, 16, "%s: the bound is loose by %d bytes", suite, bound-got)
		}
	}
}

func TestPassesTheStartupCheck(t *testing.T) {
	t.Parallel()

	opts := oneKey(t, "ns")
	opts.Escrow = []envelope.Recipient{{ID: "e1", Key: hpkeRecipient(t, false)}, {ID: "e2", Key: hpkeRecipient(t, false)}}
	require.NoError(t, payloadcodec.Config{Codec: newCodec(t, opts)}.Validate())
}

func TestRotationReadsOldAndWritesNew(t *testing.T) {
	t.Parallel()

	old, next := localKey(t, 1), localKey(t, 2)
	before := newCodec(t, envelope.Options{Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: old}}})
	sealedOld := seal(t, before, marker)

	after := newCodec(t, envelope.Options{Binding: "ns", Current: "k2",
		Keys: []envelope.Recipient{{ID: "k1", Key: old}, {ID: "k2", Key: next}}})
	sealedNew := seal(t, after, marker)
	require.Equal(t, "k2", header(t, sealedNew).GetKeyId())
	_, err := after.Decode([]*commonpb.Payload{sealedOld, sealedNew})
	require.NoError(t, err)

	retired := newCodec(t, envelope.Options{Binding: "ns", Current: "k2", Keys: []envelope.Recipient{{ID: "k2", Key: next}}})
	_, err = retired.Decode([]*commonpb.Payload{sealedOld})
	require.ErrorIs(t, err, envelope.ErrUnknownKey, "retiring a key is crypto-erasure of what it sealed")
}

func TestCrossNamespaceSpliceFailsEvenUnderTheSameKey(t *testing.T) {
	t.Parallel()

	key := localKey(t, 1)
	a := newCodec(t, envelope.Options{Binding: "tenant-a", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: key}}})
	b := newCodec(t, envelope.Options{Binding: "tenant-b", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: key}}})

	_, err := b.Decode([]*commonpb.Payload{seal(t, a, marker)})
	require.ErrorIs(t, err, envelope.ErrAuthentication)
}

func TestSameIDDifferentMaterialFailsAuthentication(t *testing.T) {
	t.Parallel()

	a := newCodec(t, envelope.Options{Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: localKey(t, 1)}}})
	b := newCodec(t, envelope.Options{Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: localKey(t, 2)}}})
	_, err := b.Decode([]*commonpb.Payload{seal(t, a, marker)})
	require.ErrorIs(t, err, envelope.ErrAuthentication)
}

// TestEveryByteIsAuthenticated flips each byte of a sealed payload's data in
// turn. Every flip must be refused; none may decode, whatever it hits:
// magic, header length, any header field, the wrapped key, the commitment,
// the nonce, the ciphertext or the tag.
func TestEveryByteIsAuthenticated(t *testing.T) {
	t.Parallel()

	for _, suite := range []envelope.Suite{envelope.SuiteAES256GCM, envelope.SuiteXChaCha20Poly1305} {
		opts := oneKey(t, "ns")
		opts.Suite = suite
		opts.DecryptSuites = []envelope.Suite{envelope.SuiteAES256GCM, envelope.SuiteXChaCha20Poly1305}
		c := newCodec(t, opts)
		sealed := seal(t, c, marker)

		for i := range sealed.GetData() {
			tampered := proto.Clone(sealed).(*commonpb.Payload)
			tampered.Data[i] ^= 0x01
			_, err := c.Decode([]*commonpb.Payload{tampered})
			require.Error(t, err, "%s: flipping byte %d decoded", suite, i)
			require.True(t, errors.Is(err, envelope.ErrAuthentication) || errors.Is(err, envelope.ErrMalformed) ||
				errors.Is(err, envelope.ErrSuiteRefused) || errors.Is(err, envelope.ErrUnknownKey),
				"%s: byte %d: %v", suite, i, err)
		}
	}
}

// TestTheCommitmentIsCheckedBeforeTheAEAD: a payload whose commitment was
// replaced, with everything else intact and re-framed consistently, is
// refused even though its AEAD would still verify; the commitment is not an
// ornament the AEAD happens to cover.
func TestTheCommitmentIsCheckedBeforeTheAEAD(t *testing.T) {
	t.Parallel()

	c := newCodec(t, oneKey(t, "ns"))
	sealed := seal(t, c, marker)
	h := header(t, sealed)
	h.Commitment[0] ^= 0xff
	_, err := c.Decode([]*commonpb.Payload{reframe(t, sealed, h)})
	require.ErrorIs(t, err, envelope.ErrAuthentication)
}

// reframe rebuilds a payload with header h and the original body.
func reframe(t testing.TB, p *commonpb.Payload, h *v1.PayloadEnvelopeHeader) *commonpb.Payload {
	t.Helper()
	data := p.GetData()
	n, read := uvarint(data[len(envelope.Magic):])
	body := data[len(envelope.Magic)+read+int(n):]
	hb, err := proto.MarshalOptions{Deterministic: true}.Marshal(h)
	require.NoError(t, err)
	out := append([]byte(envelope.Magic), appendUvarint(nil, uint64(len(hb)))...)
	out = append(append(out, hb...), body...)
	q := proto.Clone(p).(*commonpb.Payload)
	q.Data = out
	return q
}

func appendUvarint(b []byte, x uint64) []byte {
	for x >= 0x80 {
		b = append(b, byte(x)|0x80)
		x >>= 7
	}
	return append(b, byte(x))
}

func TestASuiteTheNamespaceDoesNotAcceptIsRefused(t *testing.T) {
	t.Parallel()

	opts := oneKey(t, "ns")
	opts.Suite = envelope.SuiteXChaCha20Poly1305
	writer := newCodec(t, opts)

	strict := oneKey(t, "ns")
	strict.DecryptSuites = []envelope.Suite{envelope.SuiteAES256GCM}
	_, err := newCodec(t, strict).Decode([]*commonpb.Payload{seal(t, writer, marker)})
	require.ErrorIs(t, err, envelope.ErrSuiteRefused)
}

func TestTheHeaderAndMetadataMustNameOneKey(t *testing.T) {
	t.Parallel()

	key := localKey(t, 1)
	c := newCodec(t, envelope.Options{Binding: "ns", Current: "k1",
		Keys: []envelope.Recipient{{ID: "k1", Key: key}, {ID: "k2", Key: localKey(t, 2)}}})
	sealed := seal(t, c, marker)
	sealed.Metadata[payloadcodec.KeyIDMetadataKey] = []byte("k2")
	_, err := c.Decode([]*commonpb.Payload{sealed})
	require.ErrorIs(t, err, envelope.ErrMalformed)
}

// TestEscrowRecoversWhatThePrimaryKeyCannot is the recovery journey: a worker
// holding only an HPKE recipient's public key wraps every data key to it and
// can unwrap nothing with it; a recovery process holding the private key and
// none of the primary keys reads the history.
func TestEscrowRecoversWhatThePrimaryKeyCannot(t *testing.T) {
	t.Parallel()

	private, public, err := hpke.Generate(0)
	require.NoError(t, err)
	publicOnly, err := hpke.Parse(public, nil)
	require.NoError(t, err)
	withPrivate, err := hpke.Parse(public, private)
	require.NoError(t, err)

	primary := localKey(t, 1)
	worker := newCodec(t, envelope.Options{Binding: "ns", Current: "k1",
		Keys: []envelope.Recipient{{ID: "k1", Key: primary}}, Escrow: []envelope.Recipient{{ID: "break-glass", Key: publicOnly}}})
	sealed := seal(t, worker, marker)
	require.Len(t, header(t, sealed).GetEscrow(), 1)
	require.NotContains(t, string(sealed.GetData()), marker)

	// A worker whose primary key is gone cannot read through the public half.
	lost := newCodec(t, envelope.Options{Binding: "ns", Escrow: []envelope.Recipient{{ID: "break-glass", Key: publicOnly}}})
	_, err = lost.Decode([]*commonpb.Payload{sealed})
	require.ErrorIs(t, err, envelope.ErrUnknownKey)

	recovery := newCodec(t, envelope.Options{Binding: "ns", Escrow: []envelope.Recipient{{ID: "break-glass", Key: withPrivate}}})
	out, err := recovery.Decode([]*commonpb.Payload{sealed})
	require.NoError(t, err)
	require.True(t, proto.Equal(plainPayload(marker), out[0]))

	// Recovery is bound to the namespace like everything else.
	elsewhere := newCodec(t, envelope.Options{Binding: "other", Escrow: []envelope.Recipient{{ID: "break-glass", Key: withPrivate}}})
	_, err = elsewhere.Decode([]*commonpb.Payload{sealed})
	require.Error(t, err)

	// And a decode-only codec refuses to write.
	_, err = recovery.Encode([]*commonpb.Payload{plainPayload(marker)})
	require.ErrorIs(t, err, envelope.ErrReaderCannotEncode)
}

// TestEscrowCannotVouchForAWriter: anyone holding an escrow public key can
// wrap a data key to it, so a payload opened through escrow proves nothing
// about who sealed it. A codec that writes, whose reads drive workflows,
// therefore never reads through escrow, even holding the private key; and an
// HPKE key cannot be a namespace's own key at all.
func TestEscrowCannotVouchForAWriter(t *testing.T) {
	t.Parallel()

	withPrivate := hpkeRecipient(t, true)
	lostPrimary := newCodec(t, envelope.Options{Binding: "ns", Current: "k1",
		Keys: []envelope.Recipient{{ID: "k1", Key: localKey(t, 1)}}, Escrow: []envelope.Recipient{{ID: "break-glass", Key: withPrivate}}})

	// A payload sealed by a writer whose primary key this codec does not hold.
	elsewhere := newCodec(t, envelope.Options{Binding: "ns", Current: "k9",
		Keys: []envelope.Recipient{{ID: "k9", Key: localKey(t, 9)}}, Escrow: []envelope.Recipient{{ID: "break-glass", Key: withPrivate}}})
	_, err := lostPrimary.Decode([]*commonpb.Payload{seal(t, elsewhere, marker)})
	require.ErrorIs(t, err, envelope.ErrUnknownKey, "a writing codec read through escrow")

	_, err = envelope.New(t.Context(), envelope.Options{Binding: "ns", Current: "h1",
		Keys: []envelope.Recipient{{ID: "h1", Key: withPrivate}}})
	require.ErrorContains(t, err, "escrow key", "an HPKE key was accepted as a namespace's own key")
}

// TestAHeaderMustBeCanonical: a header that parses to the same fields from
// different bytes, here with a field repeated, is refused before any key is
// used, so two readers can never disagree about which bytes they checked.
func TestAHeaderMustBeCanonical(t *testing.T) {
	t.Parallel()

	c := newCodec(t, oneKey(t, "ns"))
	sealed := seal(t, c, marker)
	data := sealed.GetData()
	n, read := uvarint(data[len(envelope.Magic):])
	headerEnd := len(envelope.Magic) + read + int(n)
	h := header(t, sealed)

	// Field 2 (key_id), repeated with the same value: proto's last-wins parse
	// yields the same header from longer bytes.
	extra := append([]byte{0x12, byte(len(h.GetKeyId()))}, h.GetKeyId()...)
	hb := append(append([]byte(nil), data[len(envelope.Magic)+read:headerEnd]...), extra...)
	out := append([]byte(envelope.Magic), appendUvarint(nil, uint64(len(hb)))...)
	out = append(append(out, hb...), data[headerEnd:]...)
	tampered := proto.Clone(sealed).(*commonpb.Payload)
	tampered.Data = out

	_, err := c.Decode([]*commonpb.Payload{tampered})
	require.ErrorIs(t, err, envelope.ErrMalformed)
	require.ErrorContains(t, err, "canonical")
}

func TestUnencryptedPayloadsAreRefusedUnlessAccepted(t *testing.T) {
	t.Parallel()

	strict := newCodec(t, oneKey(t, "ns"))
	_, err := strict.Decode([]*commonpb.Payload{plainPayload(marker)})
	require.ErrorIs(t, err, envelope.ErrUnencrypted)

	opts := oneKey(t, "ns")
	opts.AcceptUnencrypted = true
	lenient := newCodec(t, opts)
	out, err := lenient.Decode([]*commonpb.Payload{plainPayload(marker)})
	require.NoError(t, err)
	require.Equal(t, marker, string(out[0].GetData()))

	sealed, err := lenient.Encode([]*commonpb.Payload{plainPayload(marker)})
	require.NoError(t, err)
	require.NotContains(t, string(sealed[0].GetData()), marker, "accepting unencrypted never affects what is written")
}

func TestAMarkedPayloadNeverDowngrades(t *testing.T) {
	t.Parallel()

	opts := oneKey(t, "ns")
	opts.AcceptUnencrypted = true
	c := newCodec(t, opts)
	sealed := seal(t, c, marker)

	newer := proto.Clone(sealed).(*commonpb.Payload)
	newer.Metadata["encoding"] = []byte("binary/flowstate-envelope-v9")
	_, err := c.Decode([]*commonpb.Payload{newer})
	require.ErrorIs(t, err, envelope.ErrUnknownVersion)

	relabeled := proto.Clone(sealed).(*commonpb.Payload)
	relabeled.Metadata["encoding"] = []byte("json/plain")
	_, err = c.Decode([]*commonpb.Payload{relabeled})
	require.ErrorIs(t, err, envelope.ErrMalformed, "a stamped payload relabeled as plaintext must not pass through")
}

func TestOneBadPayloadFailsTheBatch(t *testing.T) {
	t.Parallel()

	c := newCodec(t, oneKey(t, "ns"))
	good := seal(t, c, marker)
	bad := proto.Clone(good).(*commonpb.Payload)
	bad.Data[len(bad.Data)-1] ^= 1
	out, err := c.Decode([]*commonpb.Payload{good, bad})
	require.Error(t, err)
	require.Nil(t, out)
}

func TestOptionsAreValidated(t *testing.T) {
	t.Parallel()

	key := localKey(t, 1)
	for name, opts := range map[string]envelope.Options{
		"no binding":          {Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: key}}},
		"long binding":        {Binding: strings.Repeat("n", 256), Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: key}}},
		"no keys":             {Binding: "ns"},
		"current not listed":  {Binding: "ns", Current: "k2", Keys: []envelope.Recipient{{ID: "k1", Key: key}}},
		"duplicate id":        {Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: key}, {ID: "k1", Key: localKey(t, 2)}}},
		"escrow reuses an id": {Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: key}}, Escrow: []envelope.Recipient{{ID: "k1", Key: key}}},
		"bad id":              {Binding: "ns", Current: "k 1", Keys: []envelope.Recipient{{ID: "k 1", Key: key}}},
		"nil key":             {Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1"}}},
		"unknown suite":       {Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: key}}, Suite: 3},
		"encrypt suite not decryptable": {Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: key}},
			Suite: envelope.SuiteXChaCha20Poly1305, DecryptSuites: []envelope.Suite{envelope.SuiteAES256GCM}},
		"too many escrow": {Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: key}}, Escrow: []envelope.Recipient{
			{ID: "e1", Key: localKey(t, 3)}, {ID: "e2", Key: localKey(t, 4)}, {ID: "e3", Key: localKey(t, 5)}, {ID: "e4", Key: localKey(t, 6)}}},
		"current cannot wrap": {Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: &describingKey{Key: key, info: keyprovider.KeyInfo{Kind: "local", MaxWrappedBytes: 60, CanUnwrap: true}}}}},
		"oversized wrap":      {Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: &describingKey{Key: key, info: keyprovider.KeyInfo{Kind: "local", MaxWrappedBytes: keyprovider.MaxWrappedBytes + 1, CanWrap: true}}}}},
	} {
		_, err := envelope.New(t.Context(), opts)
		require.Error(t, err, name)
	}
}

// TestKeysDoNotFormat: no formatting verb or structured log reaches a key's
// material or a private key.
func TestKeysDoNotFormat(t *testing.T) {
	t.Parallel()

	material := bytes.Repeat([]byte{0x5a}, local.KeyBytes)
	lk, err := local.NewKey(material)
	require.NoError(t, err)
	private, public, err := hpke.Generate(0)
	require.NoError(t, err)
	hk, err := hpke.Parse(public, private)
	require.NoError(t, err)

	for _, k := range []any{lk, hk} {
		var logged bytes.Buffer
		slog.New(slog.NewJSONHandler(&logged, nil)).Info("key", "key", k)
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%x", "%q"} {
			out := fmt.Sprintf(verb, k) + logged.String()
			require.NotContains(t, out, string(material))
			require.NotContains(t, out, fmt.Sprintf("%x", material))
			require.NotContains(t, out, strings.TrimSpace(strings.SplitN(string(private), ":", 3)[2]))
		}
	}
}

func hpkeRecipient(t testing.TB, withPrivate bool) *hpke.Key {
	t.Helper()
	private, public, err := hpke.Generate(0)
	require.NoError(t, err)
	if !withPrivate {
		private = nil
	}
	k, err := hpke.Parse(public, private)
	require.NoError(t, err)
	return k
}

// countingKey counts provider calls, optionally holding unwraps until gate
// is closed, or refusing them.
type countingKey struct {
	keyprovider.Key
	wraps, unwraps atomic.Int64
	gate           chan struct{}
	refuse         error
}

func (k *countingKey) Wrap(ctx context.Context, dk []byte, ectx keyprovider.Context) (keyprovider.Wrapped, error) {
	k.wraps.Add(1)
	return k.Key.Wrap(ctx, dk, ectx)
}

func (k *countingKey) Unwrap(ctx context.Context, w keyprovider.Wrapped, ectx keyprovider.Context) ([]byte, error) {
	k.unwraps.Add(1)
	if k.gate != nil {
		<-k.gate
	}
	if k.refuse != nil {
		return nil, k.refuse
	}
	return k.Key.Unwrap(ctx, w, ectx)
}

// flakyKey is a provider that can be taken down, for wraps and unwraps alike.
type flakyKey struct {
	keyprovider.Key
	down  atomic.Bool
	wraps atomic.Int64
}

func (k *flakyKey) Wrap(ctx context.Context, dk []byte, ectx keyprovider.Context) (keyprovider.Wrapped, error) {
	k.wraps.Add(1)
	if k.down.Load() {
		return keyprovider.Wrapped{}, keyprovider.ErrUnavailable
	}
	return k.Key.Wrap(ctx, dk, ectx)
}

func (k *flakyKey) Unwrap(ctx context.Context, w keyprovider.Wrapped, ectx keyprovider.Context) ([]byte, error) {
	if k.down.Load() {
		return nil, keyprovider.ErrUnavailable
	}
	return k.Key.Unwrap(ctx, w, ectx)
}

// describingKey overrides what a key says about itself.
type describingKey struct {
	keyprovider.Key
	info keyprovider.KeyInfo
}

func (k *describingKey) Describe(context.Context) (keyprovider.KeyInfo, error) { return k.info, nil }

// FuzzDecodeNeverPanicsOrEchoes feeds Decode arbitrary data under a valid
// key id, including mutations of a real sealed payload, and requires that it
// never panics and that no refusal quotes the bytes it was given.
func FuzzDecodeNeverPanicsOrEchoes(f *testing.F) {
	private, public, err := hpke.Generate(0)
	require.NoError(f, err)
	recipient, err := hpke.Parse(public, private)
	require.NoError(f, err)
	c, err := envelope.New(context.Background(), envelope.Options{
		Binding: "ns", Current: "k1", Keys: []envelope.Recipient{{ID: "k1", Key: localKey(f, 1)}},
		Escrow:        []envelope.Recipient{{ID: "e1", Key: recipient}},
		DecryptSuites: []envelope.Suite{envelope.SuiteAES256GCM, envelope.SuiteXChaCha20Poly1305},
	})
	require.NoError(f, err)
	sealed, err := c.Encode([]*commonpb.Payload{plainPayload(marker)})
	require.NoError(f, err)

	f.Add(sealed[0].GetData())
	f.Add([]byte(envelope.Magic))
	f.Add([]byte(envelope.Magic + "\xff\xff\xff\xff\x0f"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		p := &commonpb.Payload{Metadata: map[string][]byte{
			"encoding": []byte(envelope.Encoding), payloadcodec.KeyIDMetadataKey: []byte("k1"),
		}, Data: data}
		_, err := c.Decode([]*commonpb.Payload{p})
		if err == nil {
			return
		}
		msg := strings.ReplaceAll(err.Error(), `"k1"`, "")
		if len(data) >= 8 {
			require.NotContains(t, msg, string(data[len(data)-8:]))
		}
	})
}
