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

// proseFileName is the path comments must be attributed to for them to attach:
// comments recorded for some other file have nothing to say about this one.
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

// proseComments is what a plugin's generated .doc.pb.go registers for
// proseFile: its author's comments, by full name, attributed to path.
func proseComments(path string) CommentLookup {
	comments := map[protoreflect.FullName]string{
		"prose.v1.Inputs.name":     " Name is who to greet.\n",
		"prose.v1.Inputs.greeting": " Greeting overrides the default.\n",
	}
	return func(name protoreflect.FullName) (string, string, bool) {
		comment, ok := comments[name]
		return comment, path, ok
	}
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

// TestMessageDescriptorBytesCarriesTheCommentsItIsGiven is the whole of #723 in
// one assertion: the bytes were always able to hold comments, and what was
// missing was any comment to put in them.
func TestMessageDescriptorBytesCarriesTheCommentsItIsGiven(t *testing.T) {
	t.Parallel()

	raw, name, err := MessageDescriptorBytesWithComments(proseFile(t), proseComments(proseFileName))
	require.NoError(t, err)
	assert.Equal(t, "prose.v1.Inputs", name)

	assert.Equal(t, map[string]string{
		"name":     " Name is who to greet.\n",
		"greeting": " Greeting overrides the default.\n",
	}, commentsOf(t, raw))
}

// TestMessageDescriptorBytesWithoutCommentsIsUnchanged pins the fallback, which
// is the compatibility promise: a plugin that generated no comments sends
// exactly the bytes it sent before this existed.
func TestMessageDescriptorBytesWithoutCommentsIsUnchanged(t *testing.T) {
	t.Parallel()

	md := proseFile(t)

	before, _, err := MessageDescriptorBytes(md)
	require.NoError(t, err)

	after, _, err := MessageDescriptorBytesWithComments(md, nil)
	require.NoError(t, err)

	assert.Equal(t, before, after, "nil comments must not change a single byte")
	assert.Equal(t, map[string]string{"name": "", "greeting": ""}, commentsOf(t, before),
		"the compiled-in descriptor has no comments, and none may be invented")
}

// TestCommentsAttachByNameNotPosition is why comments are looked up rather than
// grafted from another description of the file: a SourceCodeInfo location
// addresses a declaration by index, so comments copied from a schema whose
// fields have since moved would describe the wrong fields. Looked up by name,
// each lands on its own field whatever order the fields are declared in.
func TestCommentsAttachByNameNotPosition(t *testing.T) {
	t.Parallel()

	reordered := proseFileProto()
	fields := reordered.MessageType[0].Field
	fields[0], fields[1] = fields[1], fields[0]
	file, err := protodesc.NewFile(reordered, nil)
	require.NoError(t, err)

	raw, _, err := MessageDescriptorBytesWithComments(file.Messages().Get(0), proseComments(proseFileName))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"name":     " Name is who to greet.\n",
		"greeting": " Greeting overrides the default.\n",
	}, commentsOf(t, raw))
}

// TestCommentsForAnotherFileAreDropped is the fail-closed answer for comments
// attributed to some other file that happens to declare the same names.
func TestCommentsForAnotherFileAreDropped(t *testing.T) {
	t.Parallel()

	raw, _, err := MessageDescriptorBytesWithComments(proseFile(t), proseComments("elsewhere/v1/elsewhere.proto"))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"name": "", "greeting": ""}, commentsOf(t, raw))
}

// TestCommentsAlreadyOnADescriptorSurviveReserialization is the second hop: a
// plugin's descriptor arrives carrying comments, is reconstructed by a host,
// and is written out again into a catalog document (#854). The caller writing
// that document has no comments of its own to attach, and must not strip what
// the first hop delivered.
func TestCommentsAlreadyOnADescriptorSurviveReserialization(t *testing.T) {
	t.Parallel()

	first, _, err := MessageDescriptorBytesWithComments(proseFile(t), proseComments(proseFileName))
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

// TestTheDescriptorBoundIsTheBoundAHostApplies is the agreement the constants
// exist for: a descriptor the plugin SDK accepts at startup is one a host with
// default configuration accepts, so an author cannot be refused at a host for a
// size their own build called fine.
func TestTheDescriptorBoundIsTheBoundAHostApplies(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 1<<20, DefaultMaxDescriptorBytes)
	assert.Equal(t, 256, DefaultMaxDescriptorFiles)
}
