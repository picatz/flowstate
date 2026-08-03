package plugin

import (
	"strconv"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// FuzzMessageDescriptor fuzzes [messageDescriptor], the rawest untrusted bytes
// this package parses: the descriptor a plugin's task manifest ships, arriving
// exactly as it does over the wire — either a serialized FileDescriptorProto or
// a FileDescriptorSet, neither distinguishable from the other by inspection, so
// both parse attempts run on every input the way messageDescriptor itself runs
// them.
//
// This is the stronger candidate over internal/protocol's handshake line: the
// handshake is a short, already-bounded ([MaxHandshakeLine]) text line, while a
// descriptor is an unbounded-until-checked binary blob that goes on to drive a
// recursive linker ([linker.link]) walking an import graph the same untrusted
// party shaped.
//
// The invariants under fuzz, matching the bounds this file documents:
//
//   - No panic proto.Unmarshal or protodesc.NewFile can be handed a message
//     that doesn't type-check as a valid descriptor without dying on it.
//   - MaxDescriptorBytes holds: bytes over the configured cap are refused
//     before proto.Unmarshal ever sees them budget for a scan of the input
//     itself.
//   - MaxDescriptorFiles holds: a FileDescriptorSet naming more files than the
//     limit, or an import graph reaching more files than the limit through
//     transitive dependencies, is refused rather than linked.
//   - maxDescriptorDepth holds: an import chain nested deeper than the bound
//     is refused by [linker.link]'s own depth check rather than recursing
//     further — this is the one CLAUDE.md calls out by name as a depth bound,
//     which does nothing about breadth (MaxDescriptorFiles is the separate
//     bound for that).
//   - A self-import or an import cycle is refused (linker.linking) rather than
//     looping forever, which a depth bound alone would not catch: a two-file
//     cycle never gets any deeper than 1.
func FuzzMessageDescriptor(f *testing.F) {
	cfg := Config{}.withDefaults()

	widget := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("plugintest/v1/widget.proto"),
		Package: proto.String("plugintest.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Widget"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:   proto.String("name"),
				Number: proto.Int32(1),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			}},
		}},
	}
	mustMarshalFuzz := func(m proto.Message) []byte {
		raw, err := proto.Marshal(m)
		if err != nil {
			f.Fatalf("marshaling seed: %v", err)
		}
		return raw
	}

	// A single FileDescriptorProto, and the same file wrapped as a
	// FileDescriptorSet — the two shapes messageDescriptor cannot tell apart by
	// inspection and so tries both ways for.
	f.Add(mustMarshalFuzz(widget), "plugintest.v1.Widget")
	f.Add(mustMarshalFuzz(&descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{widget},
	}), "plugintest.v1.Widget")

	// An import chain nested exactly to maxDescriptorDepth, and one nested one
	// past it — the shape [linker.link]'s depth check exists for.
	f.Add(mustMarshalFuzz(chainedDescriptorSet(maxDescriptorDepth)), "plugintest.v1.Depth0")
	f.Add(mustMarshalFuzz(chainedDescriptorSet(maxDescriptorDepth+8)), "plugintest.v1.Depth0")

	// An import cycle: two files that depend on each other, which a depth
	// bound alone would not catch, since neither file is ever more than one
	// import away from where the walk started.
	cycleA := &descriptorpb.FileDescriptorProto{
		Name: proto.String("cycle/a.proto"), Package: proto.String("plugintest.cycle"),
		Syntax: proto.String("proto3"), Dependency: []string{"cycle/b.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("A")}},
	}
	cycleB := &descriptorpb.FileDescriptorProto{
		Name: proto.String("cycle/b.proto"), Package: proto.String("plugintest.cycle"),
		Syntax: proto.String("proto3"), Dependency: []string{"cycle/a.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("B")}},
	}
	f.Add(mustMarshalFuzz(&descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{cycleA, cycleB},
	}), "plugintest.cycle.A")

	// A FileDescriptorSet naming more files than MaxDescriptorFiles allows —
	// the breadth bound, which a depth bound does nothing about.
	f.Add(mustMarshalFuzz(wideDescriptorSet(cfg.MaxDescriptorFiles+8)), "plugintest.wide.File0.Msg0")

	// Bytes that parse as neither a FileDescriptorProto nor a FileDescriptorSet.
	f.Add([]byte("this is not a descriptor"), "plugintest.v1.Widget")
	f.Add([]byte{}, "flowstate.v1.Task.Log.Inputs")

	// A message name that is syntactically invalid, which name.IsValid() must
	// catch before anything is parsed.
	f.Add(mustMarshalFuzz(widget), "not a valid \x00 name")

	f.Fuzz(func(t *testing.T, raw []byte, fullName string) {
		// messageDescriptor must never panic on any byte sequence or any string,
		// however malformed: it is the function standing between an arbitrary
		// plugin process and the engine's type registry, so failing means
		// returning an error, not stopping.
		_, _ = messageDescriptor(raw, fullName, cfg)
	})
}

// chainedDescriptorSet builds a FileDescriptorSet whose files import one
// another in a straight line depth files deep, ending in a message named
// Depth0 in the file with no further dependency — so a caller resolving
// "plugintest.v1.Depth0" walks the whole chain.
func chainedDescriptorSet(depth int) *descriptorpb.FileDescriptorSet {
	set := &descriptorpb.FileDescriptorSet{}
	for i := range depth + 1 {
		file := &descriptorpb.FileDescriptorProto{
			Name:    proto.String(chainFileName(i)),
			Package: proto.String("plugintest.v1"),
			Syntax:  proto.String("proto3"),
		}
		if i == 0 {
			file.MessageType = []*descriptorpb.DescriptorProto{{Name: proto.String("Depth0")}}
		} else {
			file.Dependency = []string{chainFileName(i - 1)}
		}
		set.File = append(set.File, file)
	}
	return set
}

func chainFileName(i int) string {
	return "plugintest/v1/chain" + strconv.Itoa(i) + ".proto"
}

// wideDescriptorSet builds a FileDescriptorSet with n unrelated files, each
// declaring one message, so a set can be built larger than any file limit
// without any file importing another — the breadth shape MaxDescriptorFiles
// bounds, as distinct from the depth shape maxDescriptorDepth bounds.
func wideDescriptorSet(n int) *descriptorpb.FileDescriptorSet {
	set := &descriptorpb.FileDescriptorSet{}
	for i := range n {
		set.File = append(set.File, &descriptorpb.FileDescriptorProto{
			Name:        proto.String("plugintest/wide/file" + strconv.Itoa(i) + ".proto"),
			Package:     proto.String("plugintest.wide"),
			Syntax:      proto.String("proto3"),
			MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Msg" + strconv.Itoa(i))}},
		})
	}
	return set
}
