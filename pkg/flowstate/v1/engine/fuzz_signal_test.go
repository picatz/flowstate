package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// FuzzSignalDeliveryDecode fuzzes the bytes a waiting workflow decodes a signal
// out of: an arbitrary [commonpb.Payload] — its encoding metadata as well as
// its data — handed to the converter a workflow's signal channel actually
// reads through, [signalDeliveryCompatConverter].
//
// This is picatz/flowstate#403's item 4. A signal is the API surface an
// outside party pokes, and the payload half of it is theirs by definition:
// [FlowstateServer.Signal]'s own comment calls it "what the sender sent,
// unchanged and untrusted". The bytes reaching *this* decode have been through
// the server's door, which is why the bound below is the door's own
// [v1.MaxSignalPayloadBytes] — but the door checks a size, not a shape, and
// everything a shape can do wrong happens after it.
//
// # Why the compat converter and not [proto.Unmarshal]
//
// Because the decode a worker performs is not one decode. `signal_compat.go`
// tries [v1.SignalDelivery] first and falls back to a bare [v1.Node_Outputs],
// which was the wire shape before #194, and the whole design rests on a claim
// about *bytes*: that the two shapes share no field name, so a well-formed,
// non-empty encoding of one can never decode as the other, and trying both is
// therefore a proof rather than a guess. `TestSignalCompatDiscriminationIsSound`
// asserts that on two hand-built messages. This asserts it on whatever the
// fuzzer can build, which is the difference between a claim checked at two
// points and a claim checked over a space.
//
// # The properties
//
//   - **Bounded.** Data is capped at [v1.MaxSignalPayloadBytes], the bound the
//     server applies before a signal is ever forwarded. Bytes are the resource
//     the sender controls, so bytes are what this target bounds, at the number
//     the real path bounds them at.
//   - **Decode-then-encode is stable.** A delivery that decodes, re-encoded by
//     the same converter and decoded again, is the same message. An unstable
//     round trip here is a signal that means one thing on the worker that
//     received it and another after a Continue-As-New carried it — the seam
//     [PendingSignal] exists for.
//   - **Attestation is never invented.** The fallback branch must leave
//     `sender` nil. A legacy payload carries no attestation at all, and
//     `signalSenderValue` renders an empty-but-present [v1.SignalSender]
//     identically to a real one — so a fabricated empty sender would read to a
//     workflow's `${gate.sender}` as an attested-but-anonymous caller. Bytes
//     must not be able to manufacture that.
//   - **Discrimination stays sound.** Whenever both readings decode, they have
//     to agree, which is exactly the "empty payload" case the compat code
//     documents as harmlessly ambiguous. A pair of bytes that decodes as two
//     *different* non-empty meanings is the failure the fallback is not allowed
//     to have.
//   - **The Value tree survives its own encodings.** The payload is a map of
//     [v1.Value], the recursive type #334's depth bounds live on, and it
//     crosses this seam twice — protobuf-JSON on the wire, binary proto
//     wherever size is measured. Both round trips are asserted, and
//     [proto.Size] is asserted to agree with what marshaling actually
//     produces, because that is the number [v1.CheckSignalPayloadSize] admits a
//     payload on.
//
// No panic and no hang are the usual pair. A refusal is an ordinary answer:
// most byte sequences are not a signal, and there is no oracle here for which
// ones should be.
func FuzzSignalDeliveryDecode(f *testing.F) {
	for _, seed := range signalFuzzSeeds(f) {
		f.Add(seed.encoding, seed.messageType, seed.data)
	}

	f.Fuzz(func(t *testing.T, encoding, messageType string, data []byte) {
		if len(data) > v1.MaxSignalPayloadBytes {
			t.Skip("past the payload bound the server's door applies before forwarding")
		}

		payload := &commonpb.Payload{
			Metadata: map[string][]byte{
				converter.MetadataEncoding:    []byte(encoding),
				converter.MetadataMessageType: []byte(messageType),
			},
			Data: data,
		}

		compat := newCompatConverter()

		var delivery v1.SignalDelivery
		if err := compat.FromPayload(payload, &delivery); err != nil {
			// Neither shape decoded, which is what the workflow treats as a
			// corrupted signal. Nothing further to assert.
			return
		}

		// Which branch fired, established the same way the converter does
		// rather than inferred from the result: the direct decode is tried
		// first, so if it succeeds the fallback never ran.
		dc := converter.GetDefaultDataConverter()
		var direct v1.SignalDelivery
		directOK := dc.FromPayload(payload, &direct) == nil

		var legacy v1.Node_Outputs
		legacyOK := dc.FromPayload(payload, &legacy) == nil

		if !directOK {
			require.True(t, legacyOK,
				"the compat converter returned a delivery neither shape decodes")
			require.Nil(t, delivery.GetSender(),
				"the legacy fallback produced a sender out of bytes that carry no attestation; "+
					"an empty-but-present sender is rendered exactly like a real one")
			require.True(t, proto.Equal(delivery.GetPayload(), &legacy),
				"the fallback's payload is not the legacy message it decoded")
		}

		if directOK && legacyOK {
			// The one ambiguous encoding the design admits: both readings
			// succeed only when neither carries anything, and then they mean
			// the same thing. Anything else here is two meanings for one set
			// of bytes.
			require.Empty(t, legacy.GetNamedValues(),
				"a non-empty legacy payload also decoded as the current shape; the two "+
					"wire forms can be confused for one another")
			require.Empty(t, direct.GetPayload().GetNamedValues(),
				"a non-empty delivery also decoded as the legacy shape; the two wire forms "+
					"can be confused for one another")
			require.Nil(t, direct.GetSender(),
				"a delivery carrying a sender also decoded as the legacy shape")
		}

		// Decode, re-encode, decode: what a worker read has to be what a worker
		// reads again after the value has been carried.
		reencoded, err := dc.ToPayload(&delivery)
		require.NoError(t, err, "a decoded delivery would not re-encode")

		var again v1.SignalDelivery
		require.NoError(t, compat.FromPayload(reencoded, &again),
			"a re-encoded delivery would not decode again")
		require.True(t, proto.Equal(&delivery, &again),
			"a delivery changed meaning across an encode/decode round trip:\nfirst:  %v\nsecond: %v",
			&delivery, &again)

		requireValueTreeRoundTrips(t, delivery.GetPayload())
	})
}

// requireValueTreeRoundTrips checks the payload's [v1.Value] tree against the
// two encodings it crosses this seam in.
//
// Binary proto is what [v1.CheckSignalPayloadSize] measures and what
// [PendingSignal] is carried inside; protobuf-JSON is what Temporal's default
// converter writes into history. A tree that survives one and not the other is
// a signal whose meaning depends on which side of the seam it is read from.
func requireValueTreeRoundTrips(t *testing.T, payload *v1.Node_Outputs) {
	t.Helper()

	if payload == nil {
		return
	}

	binary, err := proto.Marshal(payload)
	require.NoError(t, err, "a decoded payload would not marshal to binary proto")
	require.Equal(t, proto.Size(payload), len(binary),
		"proto.Size disagreed with what marshaling produced; it is the number the "+
			"server's payload bound admits a signal on")

	var fromBinary v1.Node_Outputs
	require.NoError(t, proto.Unmarshal(binary, &fromBinary),
		"a payload this package marshaled would not unmarshal")
	require.True(t, proto.Equal(payload, &fromBinary),
		"a payload changed across a binary proto round trip")

	asJSON, err := protojson.Marshal(payload)
	require.NoError(t, err, "a decoded payload would not marshal to protobuf-JSON")

	var fromJSON v1.Node_Outputs
	require.NoError(t, protojson.Unmarshal(asJSON, &fromJSON),
		"a payload this package marshaled to protobuf-JSON would not unmarshal")
	require.True(t, proto.Equal(payload, &fromJSON),
		"a payload changed across a protobuf-JSON round trip")
}

// A signalFuzzSeed is one Temporal payload, in the three parts a fuzz corpus
// can carry.
type signalFuzzSeed struct {
	encoding    string
	messageType string
	data        []byte
}

// signalFuzzSeeds are real payloads, encoded exactly as a server encodes one.
//
// Built through the default data converter rather than written as literals, so
// the corpus carries the metadata a genuine signal carries and the fuzzer
// starts from inputs that reach the decode rather than ones the converter
// refuses on the encoding alone. The messages themselves are the ones this
// package's own signal tests use — an approval with a sender, the legacy bare
// outputs, the empty payload both shapes accept — joined by the shapes a hand
// written test does not think to build: a deep [v1.Value] tree, a wide one, and
// a sender with no payload.
func signalFuzzSeeds(f *testing.F) []signalFuzzSeed {
	f.Helper()

	dc := converter.GetDefaultDataConverter()

	seeds := []signalFuzzSeed{
		// Not a proto payload at all: the encodings the converter dispatches on
		// that are not this one, plus an encoding nothing serves. The metadata
		// is as much the sender's choice as the data is.
		{"json/plain", "", []byte(`{"payload":{"namedValues":{}}}`)},
		{"binary/plain", "", []byte{0x00, 0x01, 0x02}},
		{"", "", nil},
		{"nonsense/encoding", "flowstate.v1.SignalDelivery", []byte(`{}`)},
	}

	for _, message := range []proto.Message{
		// The current shape, attested, which is what every up-to-date server
		// sends.
		&v1.SignalDelivery{
			Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"approved": v1.NewLiteral(true),
				"by":       v1.NewLiteral("someone@example.com"),
			}},
			Sender: &v1.SignalSender{
				Identity: &v1.WorkloadIdentity{Subject: "real-caller@example.com", Namespace: "team-a"},
			},
		},
		// A sender with nothing attached, which is the narrowest message that
		// still has to fail the legacy decode.
		&v1.SignalDelivery{Sender: &v1.SignalSender{
			Identity: &v1.WorkloadIdentity{Subject: "someone@example.com"},
		}},
		// The legacy shape, which is what an old server sends and what sits in
		// histories written before #194.
		&v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
		// The empty payload, the one encoding both shapes accept.
		&v1.Node_Outputs{},
		&v1.SignalDelivery{},
		// A deep Value tree and a wide one: depth and breadth are different
		// resources and a bound on one does nothing about the other. Kept well
		// under the payload bound, so what the fuzzer explores from here is the
		// shape rather than the size.
		&v1.SignalDelivery{Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			"deep": deepSignalValue(64),
		}}},
		&v1.SignalDelivery{Payload: wideSignalOutputs(256)},
	} {
		data, err := dc.ToPayload(message)
		require.NoError(f, err)
		seeds = append(seeds, signalFuzzSeed{
			encoding:    string(data.GetMetadata()[converter.MetadataEncoding]),
			messageType: string(data.GetMetadata()[converter.MetadataMessageType]),
			data:        data.GetData(),
		})
	}

	return seeds
}

// deepSignalValue builds a Value nested depth levels deep, one list per level.
func deepSignalValue(depth int) *v1.Value {
	value := v1.NewLiteral("bottom")
	for range depth {
		value = v1.NewLiteralList(value)
	}
	return value
}

// wideSignalOutputs builds a payload with n distinct keys — breadth rather than
// nesting, the direction a depth bound cannot see.
func wideSignalOutputs(n int) *v1.Node_Outputs {
	outputs := &v1.Node_Outputs{NamedValues: make(map[string]*v1.Value, n)}
	for i := range n {
		outputs.NamedValues[string(rune('a'+i%26))+string(rune('a'+i/26))] = v1.NewLiteral(int64(i))
	}
	return outputs
}
