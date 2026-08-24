package mcp

import (
	"testing"

	"google.golang.org/protobuf/reflect/protodesc"
)

// BenchmarkSchemaForMessage measures the projection every MCP server build
// performs: one JSON Schema per RPC, produced at registration time, so a client
// connecting to `flow mcp` waits on the whole set.
//
// The largest advertised request is the case that matters — SignalWithStart, at
// roughly five thousand schema objects — so it is benchmarked beside the
// smallest to show the range rather than an average of a list.
//
// See the note in pkg/flowstate/v1/celeval_bench_test.go for why this is not
// wired into CI.
func BenchmarkSchemaForMessage(b *testing.B) {
	for _, method := range WorkflowServiceMethods() {
		if method.Name != "SignalWithStart" && method.Name != "Get" {
			continue
		}

		b.Run(method.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				SchemaForMessage(method.Input)
			}
		})
	}
}

// BenchmarkSchemaForMessageAtTheNodeBound measures the hostile end: a
// descriptor whose acyclic type graph re-expands until [maxSchemaNodes] stops
// it, which is the worst case the bound admits.
//
// This is the number that says what the bound actually costs. Before it
// existed, this descriptor — under 1.2 KiB — did not finish projecting in three
// minutes; the point of measuring the bounded version is that "bounded" is a
// claim about time and memory, and a bound nobody has timed is a bound nobody
// knows the price of.
func BenchmarkSchemaForMessageAtTheNodeBound(b *testing.B) {
	files, err := protodesc.NewFiles(dagDescriptorSet(12, 4))
	if err != nil {
		b.Fatalf("linking the descriptor: %v", err)
	}

	fd, err := files.FindFileByPath("fuzzschema/v1/dag.proto")
	if err != nil {
		b.Fatalf("finding the file: %v", err)
	}

	md := fd.Messages().Get(0)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		SchemaForMessage(md)
	}
}
