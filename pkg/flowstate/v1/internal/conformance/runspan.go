package conformance

import (
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// The shared case for #523's gap 4: a run's trace is one tree, rooted in a
// single span that covers the whole run.
//
// # Why the root's name is a parameter and the shape is not
//
// The two drivers name the root differently, on purpose. Locally it is
// `flowstate.run/<workflow>`, opened by [v1.StartRunSpan] at the submit
// boundary, because nothing else in a one-process run opens anything above the
// tasks. Durably it is Temporal's own `RunWorkflow:Run`, because the substrate
// already opens a span at exactly that seam and workflow code may not open a
// second one — a span minted in workflow code is minted again on every replay.
// [v1.StartRunSpan]'s doc carries the whole argument.
//
// What must not differ is the shape, because the shape is what an author is
// actually told by a rehearsal: one root, covering the run, with every task
// execution underneath it. A driver that opened a run span and parented nothing
// under it, or that left a task span rooted beside the run, would satisfy any
// assertion about a span merely being present and would hand an operator a
// trace that reads as two unrelated things happening at once.
//
// So this asserts the tree and takes the name, rather than asserting a name and
// assuming the tree.

// AssertRunIsOneTree holds a recorded run to the shape both drivers share: one
// root span, named wantRoot, with every task span a descendant of it and in its
// trace.
func AssertRunIsOneTree(tb testing.TB, recorder *tracetest.SpanRecorder, wantRoot string) {
	tb.Helper()

	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())

	byID := make(map[trace.SpanID]tracetest.SpanStub, len(stubs))
	for _, stub := range stubs {
		byID[stub.SpanContext.SpanID()] = stub
	}

	// Found rather than assumed: a driver whose root was renamed should fail
	// here saying what it recorded, not silently match nothing below.
	var (
		root  tracetest.SpanStub
		found bool
	)
	for _, stub := range stubs {
		if stub.Parent.IsValid() {
			continue
		}
		if found {
			tb.Fatalf("the run recorded more than one root span, so its trace is not one tree: %v", spanNames(recorder))
		}
		root, found = stub, true
	}
	if !found {
		tb.Fatalf("the run recorded no root span: %v", spanNames(recorder))
	}
	if root.Name != wantRoot {
		tb.Fatalf("the root of the run's trace is %q, want %q — every span recorded: %v",
			root.Name, wantRoot, spanNames(recorder))
	}

	// And every task span is *under* it. Checked for all of them rather than for
	// the first one found, because a driver that parented one correctly and left
	// the rest at the root would pass a single-span check.
	var tasks int
	for _, stub := range stubs {
		if !strings.HasPrefix(stub.Name, taskSpanPrefix) {
			continue
		}
		tasks++

		if stub.SpanContext.TraceID() != root.SpanContext.TraceID() {
			tb.Fatalf("%s is in a different trace from the run that opened it", stub.Name)
		}
		if !descendsFrom(byID, stub, root) {
			tb.Fatalf("%s is not underneath %s, so the run's trace is a forest: %v",
				stub.Name, root.Name, spanNames(recorder))
		}
	}

	// The bound reached, not merely not exceeded: a run whose tasks stopped
	// running satisfies every assertion above by vacuum.
	if want := len(ExpectedTaskSpans()); tasks != want {
		tb.Fatalf("the run opened %d task spans under its root, want %d: %v", tasks, want, spanNames(recorder))
	}
}

// descendsFrom reports whether stub sits anywhere beneath root.
//
// Walked rather than compared one level, because the durable driver has the
// substrate's activity span in between and the local driver has nothing — the
// same reason [recordedTaskSpans] reduces to the nearest task-span ancestor.
func descendsFrom(byID map[trace.SpanID]tracetest.SpanStub, stub, root tracetest.SpanStub) bool {
	for parent := stub.Parent; parent.IsValid(); {
		if parent.SpanID() == root.SpanContext.SpanID() {
			return true
		}

		above, ok := byID[parent.SpanID()]
		if !ok {
			return false
		}
		parent = above.Parent
	}

	return false
}
