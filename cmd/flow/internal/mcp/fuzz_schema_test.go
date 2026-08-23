package mcp

import (
	"fmt"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// FuzzSchemaProjection fuzzes [SchemaForMessage] over arbitrary descriptor
// bytes: a FileDescriptorSet in, every message it defines projected to JSON
// Schema out.
//
// Why fuzz a projection whose callers are trusted. Every descriptor reaching
// [SchemaForMessage] today is this binary's own — [WorkflowServiceMethods]
// walks the compiled-in service descriptor — so on today's wiring the input is
// not attacker-chosen. Two things make the target worth its 30 seconds anyway.
//
// The first is that "trusted" is a fact about the caller, not about the
// function, and the neighbouring surface already admits third-party
// descriptors: a plugin's task manifest ships a serialized descriptor that
// [plugin.TaskDefsFromCatalog] parses and links under explicit bounds (#854),
// and the whole point of deriving tool schemas rather than writing them is that
// a task's schema can be derived the same way a method's is. That is one call
// site away, and the bound it would need has to exist before it, not after.
//
// The second is that the bound was missing, and this target is what a fuzzer
// would have found. [maxSchemaNodes]' comment has the measurements: a
// sub-kilobyte acyclic descriptor quadrupled the projection at every level it
// grew by, because the cycle cut is per-path and a DAG re-expands shared
// messages once per path that reaches them.
//
// The invariants under fuzz:
//
//   - No panic. protodesc links a descriptor this function then walks by
//     reflection — map entries, groups, placeholder types for unresolved
//     references, oneofs, extensions — and a walk that type-asserts its way
//     through that is one unexpected shape from dying.
//   - [maxSchemaNodes] holds. The schema returned is counted, so the assertion
//     is over the artifact a client would receive rather than over the counter
//     that produced it — a bound that only its own accounting believes in is
//     not evidence.
//   - Determinism. The same descriptor projects to the same schema, twice in a
//     row. protoreflect ranges some collections in map order, so a projection
//     that reached for one would be a tool list that changes between two
//     servers built from one binary.
//
// Inputs over [v1.DefaultMaxDescriptorBytes] are skipped rather than
// projected: that is the size a host admits a descriptor at, so a larger one is
// not reachable input, and fuzzing it would spend the budget in protodesc
// rather than here.
func FuzzSchemaProjection(f *testing.F) {
	for _, seed := range schemaFuzzSeeds(f) {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > v1.DefaultMaxDescriptorBytes {
			t.Skip("larger than a host admits a descriptor at")
		}

		var set descriptorpb.FileDescriptorSet
		if err := proto.Unmarshal(raw, &set); err != nil {
			return
		}

		files, err := protodesc.NewFiles(&set)
		if err != nil {
			return
		}

		spent := 0
		files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
			messages := fd.Messages()
			for i := 0; i < messages.Len(); i++ {
				// One execution's own bound, measured in the same unit the
				// projection is. Each message may legitimately project up to
				// [maxSchemaNodes] objects, so a descriptor defining many of
				// them is that much map building per execution, and an
				// execution that takes a visible fraction of a second is a
				// minute of fuzzing spent on a handful of inputs. The property
				// under test is per-message, so the messages one input does not
				// reach cost nothing a later input cannot buy.
				if spent >= maxSchemaNodes/4 {
					return false
				}

				md := messages.Get(i)
				schema := SchemaForMessage(md)

				nodes := countSchemaNodes(schema)
				if nodes > maxSchemaNodes {
					t.Fatalf("%s projected to %d schema nodes, over the %d bound",
						md.FullName(), nodes, maxSchemaNodes)
				}
				spent += 2 * nodes // the projection, and the repeat below

				if again := SchemaForMessage(md); !reflect.DeepEqual(schema, again) {
					t.Fatalf("%s projected differently on a second call", md.FullName())
				}
			}
			return true
		})
	})
}

// countSchemaNodes counts the object schemas in a rendered schema — the node
// kind [maxSchemaNodes] budgets. It walks what a client would receive, which is
// the point: the assertion must not be able to agree with a broken budget by
// asking the budget.
func countSchemaNodes(schema map[string]any) int {
	n := 1
	for _, v := range schema {
		switch value := v.(type) {
		case map[string]any:
			n += countSchemaNodes(value)
		case []any:
			for _, item := range value {
				if nested, ok := item.(map[string]any); ok {
					n += countSchemaNodes(nested)
				}
			}
		}
	}
	return n
}

// schemaFuzzSeeds is the corpus this target starts from: the real descriptors
// the surface projects today, the shape that motivated the bound, and the
// shapes a projection has historically had to survive.
func schemaFuzzSeeds(f *testing.F) [][]byte {
	f.Helper()

	marshal := func(set *descriptorpb.FileDescriptorSet) []byte {
		raw, err := proto.Marshal(set)
		if err != nil {
			f.Fatalf("marshaling seed: %v", err)
		}
		return raw
	}

	var seeds [][]byte

	// Every file the advertised tools are actually derived from, each with its
	// transitive imports so that it links — the fuzzer's best starting point is
	// the input the code was written for.
	seen := map[string]bool{}
	set := &descriptorpb.FileDescriptorSet{}
	for _, method := range WorkflowServiceMethods() {
		appendFileWithImports(set, method.Input.ParentFile(), seen)
	}
	seeds = append(seeds, marshal(set))

	// The explosion the bound exists for: an acyclic chain of messages, each
	// with several fields of the next one's type. No cycle, so the cycle cut
	// never fires; breadth^depth schema objects, so the node bound does.
	//
	// Deliberately a *small* one — 3^6, nowhere near [maxSchemaNodes]. A seed
	// that already projects at the bound makes every mutation of it an
	// expensive execution, and the fuzzer spends its minute on a handful of
	// them; this gives it the shape to grow toward instead of starting at the
	// wall. The at-the-bound case is covered where a fixed input belongs
	// anyway: [TestAnAcyclicDescriptorCannotExplodeTheProjection] asserts it
	// and [BenchmarkSchemaForMessageAtTheNodeBound] prices it.
	seeds = append(seeds, marshal(dagDescriptorSet(6, 3)))

	// A genuine cycle — a message holding itself — which is the shape the cut
	// was written for and must keep handling with the budget in the way.
	seeds = append(seeds, marshal(&descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{{
			Name:    proto.String("fuzzschema/v1/self.proto"),
			Package: proto.String("fuzzschema.v1"),
			Syntax:  proto.String("proto3"),
			MessageType: []*descriptorpb.DescriptorProto{{
				Name: proto.String("Self"),
				Field: []*descriptorpb.FieldDescriptorProto{{
					Name:     proto.String("child"),
					Number:   proto.Int32(1),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					TypeName: proto.String(".fuzzschema.v1.Self"),
				}, {
					Name:     proto.String("children"),
					Number:   proto.Int32(2),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					TypeName: proto.String(".fuzzschema.v1.Self"),
				}},
			}},
		}},
	}))

	// Bytes that are not a descriptor at all, so the fuzzer starts with the
	// early-return path as well as the projecting one.
	seeds = append(seeds, []byte{}, []byte("not a descriptor"))

	return seeds
}

// appendFileWithImports adds a file and everything it imports, transitively, to
// a set — what protodesc.NewFiles needs to link it back.
func appendFileWithImports(set *descriptorpb.FileDescriptorSet, fd protoreflect.FileDescriptor, seen map[string]bool) {
	if seen[fd.Path()] {
		return
	}
	seen[fd.Path()] = true

	imports := fd.Imports()
	for i := 0; i < imports.Len(); i++ {
		appendFileWithImports(set, imports.Get(i).FileDescriptor, seen)
	}

	set.File = append(set.File, protodesc.ToFileDescriptorProto(fd))
}

// dagDescriptorSet builds an acyclic descriptor whose type graph is a DAG:
// depth+1 messages, each of the first depth of them holding breadth fields of
// the next message's type. Nothing in it is cyclic, and it projects to
// breadth^depth schema objects unless something bounds the total.
func dagDescriptorSet(depth, breadth int) *descriptorpb.FileDescriptorSet {
	file := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("fuzzschema/v1/dag.proto"),
		Package: proto.String("fuzzschema.v1"),
		Syntax:  proto.String("proto3"),
	}

	for i := 0; i <= depth; i++ {
		message := &descriptorpb.DescriptorProto{Name: proto.String(fmt.Sprintf("M%d", i))}
		if i < depth {
			for j := 0; j < breadth; j++ {
				message.Field = append(message.Field, &descriptorpb.FieldDescriptorProto{
					Name:     proto.String(fmt.Sprintf("f%d", j)),
					Number:   proto.Int32(int32(j + 1)),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					TypeName: proto.String(fmt.Sprintf(".fuzzschema.v1.M%d", i+1)),
				})
			}
		}
		file.MessageType = append(file.MessageType, message)
	}

	return &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{file}}
}
