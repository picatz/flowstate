package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `Workflow.inputs` was field 4, a `map<string, Value>` meant as initial data for a
// workflow. Nothing ever wrote it and nothing ever read it: no Flowfile key compiled
// to it, no RPC carried a value for it, no evaluation bound a name from it. The
// parser answers `inputs:` at that level with "unknown key" and lists the keys that
// do exist.
//
// It is reserved rather than merely deleted, and by *name* as well as by number,
// which is the part worth a test.

// TestTheDeadInputsFieldStaysGone is the whole claim, asked of the descriptor.
//
// A test that only checked the Go struct would pass the moment somebody regenerated
// from a schema that had quietly grown the field back — the descriptor is what
// actually decides what a message can hold.
func TestTheDeadInputsFieldStaysGone(t *testing.T) {
	t.Parallel()

	fields := (&v1.Workflow{}).ProtoReflect().Descriptor().Fields()

	assert.Nil(t, fields.ByName("inputs"),
		"Workflow grew an `inputs` field back")
	assert.Nil(t, fields.ByNumber(4),
		"field 4 was reused, and a stored specification carrying the old one would be read as the new")
}

// TestTheNameIsReservedAndNotOnlyTheNumber is the reservation that a Protobuf habit
// would leave out, and the one this system actually needs.
//
// The usual advice is to reserve the number, because that is what the binary wire
// format addresses a field by. This system does not store the binary wire format.
// There is no custom data converter, so Temporal's default composite converter
// reaches ProtoJSON before Proto, and every specification in a workflow's history is
// stored as JSON with field *names*.
//
// So the name is the wire identity here. A future `inputs` of a different shape —
// Phase 2's parameters are typed and declared, not a bare map — would take a
// different number and still be read out of an old history by name, silently
// meaning something else. Reserving the number alone would not stop it; the
// compiler refusing the name is what does.
func TestTheNameIsReservedAndNotOnlyTheNumber(t *testing.T) {
	t.Parallel()

	desc := (&v1.Workflow{}).ProtoReflect().Descriptor()

	assert.True(t, reservesName(desc, "inputs"),
		"`inputs` is free for a later field to take, and history stores field names")
	assert.True(t, reservesNumber(desc, 4),
		"field number 4 is free for a later field to take")
}

// TestNothingElseLostItsNumber is the direction a reservation edit gets wrong.
//
// Reserving a range instead of a number, or reserving the wrong one, takes a live
// field out of the schema — and the failure is not a compile error but a field that
// silently stops being written. These are the neighbours, and every one of them is
// read on a path that a test elsewhere would notice only indirectly.
func TestNothingElseLostItsNumber(t *testing.T) {
	t.Parallel()

	fields := (&v1.Workflow{}).ProtoReflect().Descriptor().Fields()

	for name, number := range map[string]int32{
		"name":   1,
		"steps":  3,
		"labels": 5,
	} {
		field := fields.ByName(protoreflect.Name(name))
		require.NotNil(t, field, "Workflow lost its %q field", name)
		assert.Equal(t, number, int32(field.Number()),
			"%q changed number, so a stored specification no longer decodes to it", name)
	}
}

// TestADeletedFieldCostsAHistoryThatCarriesIt is the price of every deletion in
// this schema, measured rather than assumed.
//
// Temporal's ProtoJSON converter is built with `protojson.UnmarshalOptions{}`, so
// `DiscardUnknown` is false and a stored specification carrying a field that no
// longer exists does not decode at all — the run fails at the worker rather than
// ignoring a value nothing reads. Reserving the name and number prevents the field
// coming back meaning something else; it does not make an old payload readable, and
// nothing does short of keeping the field.
//
// Written down as a test because the comment in the schema is where somebody
// reading the deletion looks, and this is where somebody *proposing the next one*
// finds out what it costs. It is a fact about the converter rather than about
// `inputs`, so it is stated with a field name that was never real: if this ever
// stops failing, the converter's options changed underneath every deletion here.
func TestADeletedFieldCostsAHistoryThatCarriesIt(t *testing.T) {
	t.Parallel()

	stored := []byte(`{"workflow":{"name":"t","steps":[{"id":"a","task":{"name":"log"}}],` +
		`"somethingDeleted":{"who":"world"}},"stepsBudget":100}`)

	converter := converter.NewProtoJSONPayloadConverter()

	payload, err := converter.ToPayload(&v1.RunState{})
	require.NoError(t, err)
	payload.Data = stored

	err = converter.FromPayload(payload, &v1.RunState{})
	require.Error(t, err,
		"a deleted field in a stored specification decoded, so this schema's deletions "+
			"are cheaper than they are documented to be")
	assert.Contains(t, err.Error(), "unknown field",
		"the decode failed for some reason other than the deleted field")
}

func reservesName(desc protoreflect.MessageDescriptor, name protoreflect.Name) bool {
	names := desc.ReservedNames()
	for i := range names.Len() {
		if names.Get(i) == name {
			return true
		}
	}

	return false
}

func reservesNumber(desc protoreflect.MessageDescriptor, number protoreflect.FieldNumber) bool {
	ranges := desc.ReservedRanges()
	for i := range ranges.Len() {
		if r := ranges.Get(i); number >= r[0] && number < r[1] {
			return true
		}
	}

	return false
}
