package flowfile

import (
	"os"
	"path/filepath"
	"testing"
)

// The benchmarks here measure the two authoring paths a person waits on: the
// one an editor runs on every keystroke (parse, then validate) and the one
// `flow fmt` runs over a whole directory (marshal, then format). Both are timed
// against a real example rather than a constructed document, for the reason
// this repository already applies to its corpus tests — a synthetic workflow
// measures whatever its author happened to write, and drifts away from what the
// tool is actually run on.
//
// See the note in pkg/flowstate/v1/celeval_bench_test.go for why none of these
// are wired into CI.

// benchWorkflow is the source every benchmark below runs against: the largest
// example in the repository at the time of writing (366 lines), which is the
// honest end of the range for a hand-written Flowfile.
func benchWorkflow(b *testing.B) []byte {
	b.Helper()

	source, err := os.ReadFile(filepath.Join(repoRoot(), "examples", "approval-gate", "workflow.yaml"))
	if err != nil {
		b.Fatalf("reading the example: %v", err)
	}

	return source
}

// BenchmarkParse measures reading a Flowfile into a workflow: the YAML parse,
// the schema mapping and the position table the diagnostics later point at.
func BenchmarkParse(b *testing.B) {
	source := benchWorkflow(b)

	b.ReportAllocs()
	b.SetBytes(int64(len(source)))
	b.ResetTimer()
	for range b.N {
		if _, _, err := Parse(source); err != nil {
			b.Fatalf("parsing: %v", err)
		}
	}
}

// BenchmarkParseAndValidate measures what an editor actually runs.
//
// The language server re-reads a document on every change and answers with
// diagnostics, so the latency a person feels is this pair and not either half:
// [Parse] followed by [Validate], which compiles every expression in the file
// against the profile environment. Timing them together is the point — a
// regression in either one lands in the same place.
func BenchmarkParseAndValidate(b *testing.B) {
	source := benchWorkflow(b)

	b.ReportAllocs()
	b.SetBytes(int64(len(source)))
	b.ResetTimer()
	for range b.N {
		wf, _, err := Parse(source)
		if err != nil {
			b.Fatalf("parsing: %v", err)
		}
		Validate(wf)
	}
}

// BenchmarkMarshal is the measurement #889's stated cost was missing.
//
// [textToYAML] writes every scalar by trying candidate styles and verifying
// each against the emitter rather than trusting it, which marshal.go records as
// "three round trips through the emitter per scalar rather than one, on a path
// `flow fmt` runs over a whole directory". That cost is written down in prose
// and was nowhere in a number. This is the number: bytes per operation are
// reported too, so the per-scalar claim can be compared against document size
// rather than taken on faith.
func BenchmarkMarshal(b *testing.B) {
	source := benchWorkflow(b)

	wf, _, err := Parse(source)
	if err != nil {
		b.Fatalf("parsing: %v", err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(source)))
	b.ResetTimer()
	for range b.N {
		if _, err := Marshal(wf); err != nil {
			b.Fatalf("marshaling: %v", err)
		}
	}
}

// BenchmarkFormat measures `flow fmt`'s whole per-file operation — marshal plus
// the comment-preserving merge back onto the original document — so the extra
// that [Format] does over [Marshal] is visible as the difference between two
// numbers rather than folded into one.
func BenchmarkFormat(b *testing.B) {
	source := benchWorkflow(b)

	wf, _, err := Parse(source)
	if err != nil {
		b.Fatalf("parsing: %v", err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(source)))
	b.ResetTimer()
	for range b.N {
		if _, err := Format(source, wf); err != nil {
			b.Fatalf("formatting: %v", err)
		}
	}
}
