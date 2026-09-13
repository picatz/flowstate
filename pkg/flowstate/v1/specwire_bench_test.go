package flowstatev1_test

import (
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/internal/testkit"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The benchmarks here measure the compiled specification on the wire, which is
// the half of the cost the authoring benchmarks in
// pkg/flowstate/v1/flowfile/flowfile_bench_test.go do not reach. Those time a
// person editing a Flowfile; these time what the machine does with the result:
// a submitted specification is decoded at admission, weighed against
// [v1.MaxSpecBytes] before it is accepted, and re-encoded into Temporal history
// as the run's argument when it starts.
//
// The distinction that makes these worth having separately is that the
// specification crosses a trust boundary the Flowfile never does. A Flowfile is
// read from the author's own disk; a specification arrives over the RPC path
// from whoever holds a token, so the decode and the size check are work spent
// on bytes another party controls (invariant 5).
//
// See the note in pkg/flowstate/v1/celeval_bench_test.go for why none of these
// are wired into CI.

// benchSpec is the source every benchmark below runs against: the compiled form
// of `examples/deployment-reconciler`, which at the time of writing is the
// largest specification the example corpus compiles to — 10,650 bytes of 77
// examples, against the 1 MiB [v1.MaxSpecBytes] ceiling.
//
// Chosen by compiled size rather than by length, because the two disagree:
// `examples/approval-escalation` is the longest Flowfile at 313 lines and
// compiles to 7,197 bytes, while this one is 139 lines and compiles to 10,650.
// Compiled size follows resolved structure — expressions, bindings, per-step
// policy — rather than lines written, so picking by the number this benchmark
// actually measures is the only way the worst case is the one being timed.
func benchSpec(b *testing.B) (*v1.Workflow, []byte) {
	b.Helper()

	wf, _, err := flowfile.ParseFile(
		filepath.Join(testkit.RepoRoot(b), "examples", "deployment-reconciler", "workflow.yaml"))
	if err != nil {
		b.Fatalf("compiling the example: %v", err)
	}

	raw, err := proto.Marshal(wf)
	if err != nil {
		b.Fatalf("encoding the compiled example: %v", err)
	}

	return wf, raw
}

// BenchmarkSpecUnmarshal measures decoding a submitted specification: the work
// `FlowstateServer` does before it has anything to validate, and the work a
// worker repeats whenever a specification is carried back out of history.
//
// Bytes per operation are reported against the wire size so the ratio is
// visible rather than the total alone. It is not close to one: a specification
// costs roughly ten times its wire size in decoded heap, because nearly every
// field on the way down is a pointer to another message.
func BenchmarkSpecUnmarshal(b *testing.B) {
	_, raw := benchSpec(b)

	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		var wf v1.Workflow
		if err := proto.Unmarshal(raw, &wf); err != nil {
			b.Fatalf("decoding: %v", err)
		}
	}
}

// BenchmarkSpecCheckSize is [v1.CheckSpecSize] on an already-decoded
// specification, timed apart from the decode so the size check's own cost is a
// number rather than a share of a larger one.
//
// The check is one `proto.Size`, which reads as free and is not: it walks every
// field of the whole message to total them, so admission traverses the
// specification once more after decoding it. That is the cost of weighing the
// message rather than the request body, which is the thing worth weighing —
// what bounds a run is what the specification compiles to, not how many bytes
// the sender spent saying it.
func BenchmarkSpecCheckSize(b *testing.B) {
	wf, _ := benchSpec(b)

	b.ReportAllocs()
	for b.Loop() {
		if err := v1.CheckSpecSize(wf); err != nil {
			b.Fatalf("checking the size: %v", err)
		}
	}
}

// BenchmarkSpecMarshal measures the encode direction: what it costs to put a
// specification into Temporal history, which every run pays once as
// [v1.RunState.Workflow] on the way into `engine.Run` and pays again on each
// Continue-As-New that re-carries the state.
//
// Deliberately not the memo-freeze path, which encodes a different and much
// smaller object. [server.signalPolicyMemoEntry] and its neighbour marshal
// `&v1.Workflow{Signals: resolved}` and `&v1.Workflow{Debug: resolved}` — one
// policy stanza, not a specification — so this number says nothing about them,
// and a reading of it that reaches for the memo is reaching for the wrong
// object.
func BenchmarkSpecMarshal(b *testing.B) {
	wf, raw := benchSpec(b)

	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := proto.Marshal(wf); err != nil {
			b.Fatalf("encoding: %v", err)
		}
	}
}
