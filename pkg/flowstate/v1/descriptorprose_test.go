package flowstatev1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// The comments a plugin author writes travel in the descriptor bytes the
// manifest already carried (#723). These tests are about the three answers that
// mechanism can give — attach, decline, and leave alone — because two of them are
// silence, and silence is what a test has to make visible.

// proseFileName is the path both halves of a graft have to agree on: prose is
// keyed by the file it describes, and a set describing some other file has
// nothing to say about this one.
const proseFileName = "prose/v1/prose.proto"

// proseFile is a plugin's schema as its compiled-in descriptor has it: shape,
// and no comments, the way protoc leaves what a .pb.go embeds.
func proseFile(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	file, err := protodesc.NewFile(proseFileProto(), nil)
	require.NoError(t, err)

	return file.Messages().Get(0)
}

// proseFileProto is that same file as a descriptor proto, so a test can vary one
// declaration of it and see what the graft decides.
func proseFileProto() *descriptorpb.FileDescriptorProto {
	return &descriptorpb.FileDescriptorProto{
		Name:    proto.String(proseFileName),
		Package: proto.String("prose.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Inputs"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name:   proto.String("name"),
					Number: proto.Int32(1),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
				{
					Name:   proto.String("greeting"),
					Number: proto.Int32(2),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
			},
		}},
	}
}

// proseSet is what `buf build --exclude-imports` writes: the same file, with the
// author's comments kept.
func proseSet(t *testing.T, mutate func(*descriptorpb.FileDescriptorProto)) []byte {
	t.Helper()

	file := proseFileProto()
	file.SourceCodeInfo = &descriptorpb.SourceCodeInfo{
		Location: []*descriptorpb.SourceCodeInfo_Location{
			{
				Path:            []int32{4, 0, 2, 0},
				Span:            []int32{1, 0, 1},
				LeadingComments: proto.String(" Name is who to greet.\n"),
			},
			{
				Path:            []int32{4, 0, 2, 1},
				Span:            []int32{2, 0, 1},
				LeadingComments: proto.String(" Greeting overrides the default.\n"),
			},
		},
	}
	if mutate != nil {
		mutate(file)
	}

	raw, err := proto.Marshal(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{file}})
	require.NoError(t, err)

	return raw
}

// commentsOf reconstructs a serialized descriptor the way a host does and reads
// back what each field is documented as, which is the only question any of this
// is asked.
func commentsOf(t *testing.T, raw []byte) map[string]string {
	t.Helper()

	var fdp descriptorpb.FileDescriptorProto
	require.NoError(t, proto.Unmarshal(raw, &fdp))

	file, err := protodesc.NewFile(&fdp, nil)
	require.NoError(t, err)

	fields := file.Messages().Get(0).Fields()
	out := make(map[string]string, fields.Len())
	for i := range fields.Len() {
		fd := fields.Get(i)
		out[string(fd.Name())] = file.SourceLocations().ByDescriptor(fd).LeadingComments
	}

	return out
}

// TestMessageDescriptorBytesCarriesTheProseItIsGiven is the whole of #723 in one
// assertion: the bytes were always able to hold comments, and what was missing
// was any comment to put in them.
func TestMessageDescriptorBytesCarriesTheProseItIsGiven(t *testing.T) {
	t.Parallel()

	md := proseFile(t)

	prose, err := ParseDescriptorProse(proseSet(t, nil))
	require.NoError(t, err)
	require.NotNil(t, prose)

	raw, name, err := MessageDescriptorBytesWithProse(md, prose)
	require.NoError(t, err)
	assert.Equal(t, "prose.v1.Inputs", name)

	assert.Equal(t, map[string]string{
		"name":     " Name is who to greet.\n",
		"greeting": " Greeting overrides the default.\n",
	}, commentsOf(t, raw))
}

// TestMessageDescriptorBytesWithoutProseIsUnchanged pins the fallback, which is
// the compatibility promise: a plugin that ships no descriptor set sends exactly
// the bytes it sent before this existed.
func TestMessageDescriptorBytesWithoutProseIsUnchanged(t *testing.T) {
	t.Parallel()

	md := proseFile(t)

	before, _, err := MessageDescriptorBytes(md)
	require.NoError(t, err)

	after, _, err := MessageDescriptorBytesWithProse(md, nil)
	require.NoError(t, err)

	assert.Equal(t, before, after, "a nil prose must not change a single byte")
	assert.Equal(t, map[string]string{"name": "", "greeting": ""}, commentsOf(t, before),
		"the compiled-in descriptor has no comments, and none may be invented")
}

// TestProseFromADriftedSchemaIsDropped is the direction worth the code that makes
// it possible.
//
// A SourceCodeInfo location addresses a declaration by index, so prose built from
// an older .proto does not fail to apply — it applies to whatever now sits at that
// index. Here the fields have been reordered, which would attribute each field's
// sentence to the other one. Silence is the right answer, and the only way to see
// it is to ask.
func TestProseFromADriftedSchemaIsDropped(t *testing.T) {
	t.Parallel()

	md := proseFile(t)

	reordered, err := ParseDescriptorProse(proseSet(t, func(file *descriptorpb.FileDescriptorProto) {
		fields := file.MessageType[0].Field
		fields[0], fields[1] = fields[1], fields[0]
	}))
	require.NoError(t, err)

	raw, _, err := MessageDescriptorBytesWithProse(md, reordered)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"name": "", "greeting": ""}, commentsOf(t, raw),
		"a sentence attached to the wrong field is worse than no sentence")

	renamed, err := ParseDescriptorProse(proseSet(t, func(file *descriptorpb.FileDescriptorProto) {
		file.MessageType[0].Field[1].Name = proto.String("salutation")
	}))
	require.NoError(t, err)

	raw, _, err = MessageDescriptorBytesWithProse(md, renamed)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"name": "", "greeting": ""}, commentsOf(t, raw))
}

// TestProseForAnotherFileIsDropped is the same fail-closed answer for a set that
// is perfectly valid and simply about something else.
func TestProseForAnotherFileIsDropped(t *testing.T) {
	t.Parallel()

	md := proseFile(t)

	elsewhere, err := ParseDescriptorProse(proseSet(t, func(file *descriptorpb.FileDescriptorProto) {
		file.Name = proto.String("elsewhere/v1/elsewhere.proto")
	}))
	require.NoError(t, err)

	raw, _, err := MessageDescriptorBytesWithProse(md, elsewhere)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"name": "", "greeting": ""}, commentsOf(t, raw))
}

// TestProseAlreadyOnADescriptorSurvivesReserialization is the second hop: a
// plugin's descriptor arrives carrying comments, is reconstructed by a host, and
// is written out again into a catalog document (#854). The caller writing that
// document has no prose of its own to attach, and must not strip what the first
// hop delivered.
func TestProseAlreadyOnADescriptorSurvivesReserialization(t *testing.T) {
	t.Parallel()

	prose, err := ParseDescriptorProse(proseSet(t, nil))
	require.NoError(t, err)

	first, _, err := MessageDescriptorBytesWithProse(proseFile(t), prose)
	require.NoError(t, err)

	var fdp descriptorpb.FileDescriptorProto
	require.NoError(t, proto.Unmarshal(first, &fdp))
	reconstructed, err := protodesc.NewFile(&fdp, nil)
	require.NoError(t, err)

	second, _, err := MessageDescriptorBytes(reconstructed.Messages().Get(0))
	require.NoError(t, err)

	assert.Equal(t, map[string]string{
		"name":     " Name is who to greet.\n",
		"greeting": " Greeting overrides the default.\n",
	}, commentsOf(t, second))
}

// TestParseDescriptorProseRefusesWhatIsNotADescriptorSet covers the one shape
// that is an error rather than silence: an author's build handing over something
// that is not a descriptor set at all is worth hearing about once, at startup,
// rather than never.
func TestParseDescriptorProseRefusesWhatIsNotADescriptorSet(t *testing.T) {
	t.Parallel()

	empty, err := ParseDescriptorProse(nil)
	require.NoError(t, err, "an artifact a build left empty degrades to the fallback")
	assert.Nil(t, empty)

	// A length-delimited field claiming more bytes than follow it: protobuf's
	// wire format is permissive, and this is one of the few shapes it genuinely
	// refuses.
	_, err = ParseDescriptorProse([]byte{0x0a, 0x7f})
	require.Error(t, err)

	// And the shape it does not refuse, which is why the file count is checked
	// separately: bytes carrying nothing this type recognises parse cleanly into
	// an empty set, so "it unmarshaled" is not the same question as "it is a
	// descriptor set".
	_, err = ParseDescriptorProse([]byte{0x78, 0x01})
	require.ErrorContains(t, err, "no files")
}
