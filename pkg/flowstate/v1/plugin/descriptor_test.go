package plugin

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// widgetFile is a descriptor for a message this binary has never seen.
//
// It is built by hand rather than taken from a generated package on purpose: a
// message the test binary imports is already in the global registry, so
// reconstructing it would prove nothing. This one exists only as bytes, which is
// exactly the position the host is in with a real plugin's schema.
func widgetFile() *descriptorpb.FileDescriptorProto {
	return &descriptorpb.FileDescriptorProto{
		Name:    proto.String("plugintest/v1/widget.proto"),
		Package: proto.String("plugintest.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Widget"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name:   proto.String("name"),
					Number: proto.Int32(1),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				},
				{
					Name:   proto.String("count"),
					Number: proto.Int32(2),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
				},
			},
		}},
	}
}

// mustMarshal serializes a message or fails the test.
func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	raw, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshaling: %v", err)
	}
	return raw
}

// TestMessageDescriptorReconstruction covers every shape a task manifest's
// descriptors can arrive in, and every way they can be wrong.
//
// This is what makes a plugin task indistinguishable from a built-in one, so the
// failures matter as much as the successes: a descriptor that half-resolves
// would give the engine a shape it would then validate workflows against.
func TestMessageDescriptorReconstruction(t *testing.T) {
	t.Parallel()

	cfg := Config{}.withDefaults()

	// A file importing something the engine has, to check that imports resolve
	// against the engine's own registry rather than having to be shipped.
	importsWellKnown := widgetFile()
	importsWellKnown.Name = proto.String("plugintest/v1/stamped.proto")
	importsWellKnown.Dependency = []string{"google/protobuf/timestamp.proto"}
	importsWellKnown.MessageType[0].Name = proto.String("Stamped")
	importsWellKnown.MessageType[0].Field = append(importsWellKnown.MessageType[0].Field,
		&descriptorpb.FieldDescriptorProto{
			Name:     proto.String("at"),
			Number:   proto.Int32(3),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
			TypeName: proto.String(".google.protobuf.Timestamp"),
		})

	missingImport := widgetFile()
	missingImport.Dependency = []string{"nobody/has/this.proto"}

	cyclicA := widgetFile()
	cyclicA.Name = proto.String("cycle/a.proto")
	cyclicA.Dependency = []string{"cycle/b.proto"}
	cyclicB := widgetFile()
	cyclicB.Name = proto.String("cycle/b.proto")
	cyclicB.Package = proto.String("plugintest.b")
	cyclicB.Dependency = []string{"cycle/a.proto"}

	unnamed := widgetFile()
	unnamed.Name = nil

	tests := []struct {
		name        string
		raw         []byte
		message     string
		wantFields  []string
		wantErr     bool
		wantMessage string
	}{
		{
			name:       "a single FileDescriptorProto",
			raw:        mustMarshal(t, widgetFile()),
			message:    "plugintest.v1.Widget",
			wantFields: []string{"name", "count"},
		},
		{
			name: "a FileDescriptorSet",
			raw: mustMarshal(t, &descriptorpb.FileDescriptorSet{
				File: []*descriptorpb.FileDescriptorProto{widgetFile()},
			}),
			message:    "plugintest.v1.Widget",
			wantFields: []string{"name", "count"},
		},
		{
			name: "imports the engine already has",
			raw: mustMarshal(t, &descriptorpb.FileDescriptorSet{
				File: []*descriptorpb.FileDescriptorProto{importsWellKnown},
			}),
			message:    "plugintest.v1.Stamped",
			wantFields: []string{"name", "count", "at"},
		},
		{
			// No descriptor at all: a plugin reusing a type the engine has.
			name:       "a message the engine already knows",
			message:    "flowstate.v1.Task.Log.Inputs",
			wantFields: []string{"message", "level", "fields"},
		},
		{
			name:        "a message the engine does not know, with no descriptor",
			message:     "nobody.knows.This",
			wantErr:     true,
			wantMessage: "this engine does not know that message",
		},
		{
			name:        "bytes that are not a descriptor",
			raw:         []byte("this is not a descriptor"),
			message:     "plugintest.v1.Widget",
			wantErr:     true,
			wantMessage: "could not reconstruct",
		},
		{
			name:        "a descriptor that does not define the named message",
			raw:         mustMarshal(t, widgetFile()),
			message:     "plugintest.v1.Missing",
			wantErr:     true,
			wantMessage: "does not define",
		},
		{
			name:        "a descriptor with no path",
			raw:         mustMarshal(t, unnamed),
			message:     "plugintest.v1.Widget",
			wantErr:     true,
			wantMessage: "could not reconstruct",
		},
		{
			name:        "an import nobody has",
			raw:         mustMarshal(t, missingImport),
			message:     "plugintest.v1.Widget",
			wantErr:     true,
			wantMessage: "neither included in the descriptor nor known to this engine",
		},
		{
			name: "imports that form a cycle",
			raw: mustMarshal(t, &descriptorpb.FileDescriptorSet{
				File: []*descriptorpb.FileDescriptorProto{cyclicA, cyclicB},
			}),
			message:     "plugintest.v1.Widget",
			wantErr:     true,
			wantMessage: "cycle",
		},
		{
			name:        "a descriptor with no message named",
			raw:         mustMarshal(t, widgetFile()),
			message:     "",
			wantErr:     true,
			wantMessage: "names no message within it",
		},
		{
			name:        "a message name that is not a name",
			raw:         mustMarshal(t, widgetFile()),
			message:     "not a valid name!",
			wantErr:     true,
			wantMessage: "is not a valid message name",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			descriptor, err := messageDescriptor(test.raw, test.message, cfg)

			if test.wantErr {
				if err == nil {
					t.Fatalf("reconstruction succeeded, want a refusal")
				}
				if !errors.Is(err, ErrDescriptor) {
					t.Errorf("error = %v, want one wrapping %v", err, ErrDescriptor)
				}
				if !strings.Contains(err.Error(), test.wantMessage) {
					t.Errorf("error = %q, want it to mention %q", err.Error(), test.wantMessage)
				}
				return
			}

			if err != nil {
				t.Fatalf("reconstruction: %v", err)
			}
			if descriptor == nil {
				t.Fatal("reconstruction returned no descriptor and no error")
			}
			if got := string(descriptor.FullName()); got != test.message {
				t.Errorf("reconstructed %q, want %q", got, test.message)
			}

			// The fields are the point: they are what the engine validates a
			// workflow's inputs against and what an editor completes.
			fields := descriptor.Fields()
			var got []string
			for i := range fields.Len() {
				got = append(got, string(fields.Get(i).Name()))
			}
			if strings.Join(got, ",") != strings.Join(test.wantFields, ",") {
				t.Errorf("fields = %v, want %v", got, test.wantFields)
			}
		})
	}
}

// TestMessageDescriptorNoSchema checks that a task declaring no schema for a side
// is accepted, which [flowstatev1.TaskDef] permits.
func TestMessageDescriptorNoSchema(t *testing.T) {
	t.Parallel()

	descriptor, err := messageDescriptor(nil, "", Config{}.withDefaults())
	if err != nil {
		t.Fatalf("messageDescriptor: %v", err)
	}
	if descriptor != nil {
		t.Errorf("descriptor = %v, want nil for a task that declares no schema", descriptor)
	}
}

// TestDescriptorBoundsAreEnforced checks the bounds on descriptors, which are
// attacker-chosen input the host parses and links.
func TestDescriptorBoundsAreEnforced(t *testing.T) {
	t.Parallel()

	t.Run("size", func(t *testing.T) {
		t.Parallel()

		cfg := Config{MaxDescriptorBytes: 32}.withDefaults()

		_, err := messageDescriptor(mustMarshal(t, widgetFile()), "plugintest.v1.Widget", cfg)
		if err == nil {
			t.Fatal("an oversized descriptor was accepted")
		}
		if !strings.Contains(err.Error(), "over the 32 byte limit") {
			t.Errorf("error = %v, want one about the size limit", err)
		}
	})

	t.Run("file count", func(t *testing.T) {
		t.Parallel()

		// Breadth, which a depth bound would not catch.
		set := &descriptorpb.FileDescriptorSet{}
		for i := range 20 {
			file := widgetFile()
			file.Name = proto.String(string(rune('a'+i)) + "/widget.proto")
			file.Package = proto.String("plugintest.p" + string(rune('a'+i)))
			set.File = append(set.File, file)
		}

		cfg := Config{MaxDescriptorFiles: 4}.withDefaults()

		_, err := messageDescriptor(mustMarshal(t, set), "plugintest.pa.Widget", cfg)
		if err == nil {
			t.Fatal("a descriptor with too many files was accepted")
		}
		if !strings.Contains(err.Error(), "over the 4 file limit") {
			t.Errorf("error = %v, want one about the file limit", err)
		}
	})
}

// TestPluginCannotRedefineEngineTypes checks that a plugin shipping its own copy
// of a file the engine has does not get to replace the engine's definition.
//
// It matters because the reconstructed descriptor is what the engine validates
// workflows against. A plugin able to redefine flowstate's own types would be
// reaching outside its process through the one channel that is supposed to be
// inert data.
func TestPluginCannotRedefineEngineTypes(t *testing.T) {
	t.Parallel()

	// A file claiming a path the engine already has, defining something else
	// entirely under it.
	impostor := widgetFile()
	impostor.Name = proto.String("flowstate/v1/value.proto")
	impostor.Package = proto.String("flowstate.v1")
	impostor.MessageType[0].Name = proto.String("Value")

	raw := mustMarshal(t, &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{impostor},
	})

	descriptor, err := messageDescriptor(raw, "flowstate.v1.Value", Config{}.withDefaults())
	if err != nil {
		// Refusing outright is a fine outcome too; what must not happen is the
		// plugin's definition winning.
		return
	}

	// The engine's own Value has a oneof and no "name" field; the impostor's has
	// exactly the fields defined above.
	if fields := descriptor.Fields(); fields.ByName("name") != nil && fields.ByName("count") != nil {
		t.Error("a plugin's file replaced the engine's definition of flowstate.v1.Value")
	}
}
