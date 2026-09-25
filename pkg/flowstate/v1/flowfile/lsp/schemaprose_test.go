package lsp

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/protodoc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// The prose about a schema-shaped field is the schema's, and these tests are what
// keeps that true rather than merely arranged once.
//
// Presence is the weaker claim and the one that lets the defect back in: hover
// asserting that it says *something* about `url:` passes just as well when
// somebody writes a second sentence here beside the field's own, which is the
// state slice 3 of #424 exists to leave behind. So what is asserted is
// provenance: the text hover renders contains the comment protodoc read out of
// the schema, byte for byte. A sentence rewritten in Go fails these even while it
// reads perfectly.

// TestEveryDocumentedTaskFieldReachesHover walks every input and output of every
// registered task, not the two fields a case would have picked.
//
// The traversal is the test. A single case proves one field is wired up and says
// nothing about the next task somebody registers, and the failure this guards
// against is exactly a field whose prose stops arriving: `flow tasks` would still
// print its bounds, the signature would still show its type, and the sentence
// saying what it is for would silently be gone.
func TestEveryDocumentedTaskFieldReachesHover(t *testing.T) {
	t.Parallel()

	checked := 0
	for _, def := range v1.DefaultRegistry().All() {
		for _, md := range []protoreflect.MessageDescriptor{def.Inputs, def.Outputs} {
			if md == nil {
				continue
			}
			fields := md.Fields()
			for i := range fields.Len() {
				fd := fields.Get(i)
				comment, ok := protodoc.Comment(fd.FullName())
				if !ok {
					// A task whose message this build's schema does not describe:
					// a plugin's, an embedder's. Nothing to inherit, which
					// TestATaskWithNoSchemaProseStillHovers covers.
					continue
				}
				checked++
				assert.Contains(t, inputDoc(def, string(fd.Name()), fd), comment,
					"hover for %s does not carry the schema's own comment", fd.FullName())
			}
		}
	}

	// A walk that reached nothing satisfies every assertion inside it.
	if checked < 20 {
		t.Errorf("only %d task fields carried schema prose, which is fewer than the built-in tasks declare; the walk is broken, not the schema", checked)
	}
}

// TestInputHoverBeginsWithTheSchemasSentence pins the shape a reader meets, not
// only that the words are somewhere in the popup.
//
// The heading is the surface's (the name, the type, whether it is required), and
// the paragraph naming the task is the position's; what the field *is* comes
// straight after, and it is the schema's. Asserting the order is what stops the
// schema's sentence from being demoted below a Go-written summary of it later.
func TestInputHoverBeginsWithTheSchemasSentence(t *testing.T) {
	t.Parallel()

	def, ok := v1.LookupTask("http")
	require.True(t, ok)

	for _, name := range []string{"url", "method"} {
		t.Run(name, func(t *testing.T) {
			fd := findField(def.Inputs, name)
			require.NotNil(t, fd)

			comment, ok := protodoc.Comment(fd.FullName())
			require.True(t, ok, "the schema must document %s; the presence pin in protodoc says so too", fd.FullName())

			got := inputDoc(def, name, fd)
			assert.Contains(t, got, "\n\n"+comment,
				"the schema's comment must appear as its own paragraph")
			assert.Contains(t, got, "Input of the `http` task.",
				"the position's own sentence stays: no message can say which task's input this is")
		})
	}
}

// TestOutputHoverCarriesTheSchemasProse is the same claim for the other
// direction, through the server rather than through the renderer.
//
// End to end because the two hovers reach a field by different paths: an input
// key is looked up on the task's input message, an output through a reference's
// step. A renderer test would pass with the output path unwired.
func TestOutputHoverCarriesTheSchemasProse(t *testing.T) {
	t.Parallel()

	const src = `name: outputprose
steps:
  - id: web
    http:
      url: https://example.com
  - id: shout
    log:
      message: ${steps.web.status_code}
edition: v2026.3
`
	comment, ok := protodoc.Comment("flowstate.v1.Task.HTTP.Outputs.status_code")
	require.True(t, ok)

	c := newClient(t)
	c.initialize()
	const uri = "file:///outputprose.yaml"
	c.open(uri, src)

	pos := positionOf(t, src, "${steps.web.status_code}", len("${steps.web."))
	got := c.hover(uri, pos.Line, pos.Character)
	require.NotNil(t, got)
	assert.Contains(t, hoverText(got), comment)
}

// TestWaitResultProseComesFromTheSchema covers the two of the wait's three result
// names the schema declares, and the one it does not.
//
// The third is the interesting assertion. `timed_out` is written in Go on
// purpose, because no symbol in the schema describes it, and a test that only
// checked the other two would let that override quietly spread to them.
func TestWaitResultProseComesFromTheSchema(t *testing.T) {
	t.Parallel()

	for name, symbol := range map[string]protoreflect.FullName{
		v1.PayloadOutput: signalPayloadField,
		v1.SenderOutput:  signalSenderField,
	} {
		t.Run(name, func(t *testing.T) {
			comment, ok := protodoc.Comment(symbol)
			require.True(t, ok, "the schema must document %s", symbol)

			sentence := protodoc.FirstSentence(comment)
			require.NotEmpty(t, sentence)
			assert.Contains(t, waitResultDoc(name), "\n\n"+sentence,
				"the wait's %s is described by the schema, and hover must quote it", name)
		})
	}

	t.Run(v1.TimedOutOutput, func(t *testing.T) {
		_, ok := protodoc.Comment("flowstate.v1.SignalDelivery.timed_out")
		require.False(t, ok, "a symbol for timed_out now exists; read it here and delete the override")
		assert.Contains(t, waitResultDoc(v1.TimedOutOutput), "nobody answered in time")
	})
}

// TestWaitResultCompletionAndHoverAgree is the rule the `now` documentation is
// shared for, applied to the three names a shaping binds: a menu and the hover
// over what the menu inserted are one keystroke apart, and two accounts of one
// name is what an author notices first.
func TestWaitResultCompletionAndHoverAgree(t *testing.T) {
	t.Parallel()

	candidates := waitResultCandidates([]string{"steps", "wait_for_signal", "outputs"})
	require.Len(t, candidates, 3)

	for _, candidate := range candidates {
		assert.Equal(t, waitResultDoc(candidate.Name), candidate.Docs,
			"completion and hover must render one text for %s", candidate.Name)
	}
}

// standInInputs is the shape a plugin's descriptor arrives in: a message this
// build's schema has never heard of, reconstructed from bytes rather than
// borrowed from the registry.
//
// Built here rather than borrowed deliberately — every message this repository
// declares is in protodoc's artifact, so borrowing one would quietly test the
// documented path again. The comment is a parameter because the two tests below
// are the two answers a plugin can give: one that shipped a descriptor set built
// with source info retained, and one that shipped none.
func standInInputs(t *testing.T, comment string) protoreflect.MessageDescriptor {
	t.Helper()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("standin/v1/standin.proto"),
		Package: proto.String("standin.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Inputs"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:   proto.String("greeting"),
				Number: proto.Int32(1),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			}},
		}},
	}

	if comment != "" {
		// The path addresses the declaration the comment belongs to the way a
		// .proto compiler records it: message_type (field 4) 0, field (field 2)
		// 0. Spelled out rather than borrowed from a helper, because a location
		// addressing the wrong declaration is the failure this whole mechanism
		// has to avoid, and a test that cannot express one cannot show it is
		// avoided.
		fdp.SourceCodeInfo = &descriptorpb.SourceCodeInfo{
			Location: []*descriptorpb.SourceCodeInfo_Location{{
				Path:            []int32{4, 0, 2, 0},
				Span:            []int32{1, 0, 1},
				LeadingComments: proto.String(" " + comment + "\n"),
			}},
		}
	}

	file, err := protodesc.NewFile(fdp, nil)
	require.NoError(t, err)

	return file.Messages().Get(0)
}

// TestAPluginsOwnProseReachesHover is the direction #723 opened: a plugin that
// ships a descriptor set built with source info retained documents its inputs
// exactly the way a built-in task does.
//
// The comment is the plugin author's, from their own .proto, and nothing in this
// build's schema describes the field — so this is the branch [fieldDoc] takes
// before it asks protodoc's artifact, and the one that had nothing to find for
// as long as the SDK sent the compiled-in descriptor protoc had stripped.
func TestAPluginsOwnProseReachesHover(t *testing.T) {
	t.Parallel()

	const comment = "Greeting overrides the default \"Hello\"."

	md := standInInputs(t, comment)
	fd := findField(md, "greeting")
	require.NotNil(t, fd)

	_, known := protodoc.Comment(fd.FullName())
	require.False(t, known,
		"this build's schema now describes %s, so nothing below distinguishes the plugin's own prose from ours", fd.FullName())

	assert.Equal(t, comment, fieldDoc(fd),
		"the sentence hover renders is the plugin author's, byte for byte")

	got := inputDoc(v1.TaskDef{Name: "stand_in", Inputs: md}, "greeting", fd)
	assert.Contains(t, got, "**`greeting`** · `string`")
	assert.Contains(t, got, comment)
}

// TestATaskWithNoSchemaProseStillHovers is the fail-closed half, and since #723
// it pins a documented fallback rather than the only behaviour there is: a plugin
// shipping no descriptor set — every plugin built before [sdk.Plugin.SchemaProse]
// existed, and every one that declines it since — has no sentence to inherit, and
// hover must render one paragraph fewer rather than a placeholder or a panic.
func TestATaskWithNoSchemaProseStillHovers(t *testing.T) {
	t.Parallel()

	assert.Empty(t, fieldDoc(nil))

	md := standInInputs(t, "")
	fd := findField(md, "greeting")
	require.NotNil(t, fd)
	assert.Empty(t, fieldDoc(fd), "nothing describes this field, and an invented sentence would be worse than none")

	got := inputDoc(v1.TaskDef{Name: "stand_in", Inputs: md}, "greeting", fd)
	assert.Contains(t, got, "**`greeting`** · `string`")
	assert.Contains(t, got, "Input of the `stand_in` task.")
	assert.NotContains(t, got, "(undocumented)")
}
