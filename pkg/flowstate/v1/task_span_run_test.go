package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestRunWorkflowTaskSpans runs [conformance.AssertTaskSpans] against the local
// driver. The same case runs against the durable driver in the engine package
// (TestRunWorkflowTaskSpans there) — two verified callers, which is what
// invariant 3 asks a shared case to have.
//
// This is the test that fails without #523's gap-3 fix: before it, the local
// driver opened no `flowstate.*` span at all, so the tree here was empty while
// the identical assertion in the engine package passed.
//
// No t.Parallel: [conformance.RecordSpans] swaps the global tracer provider, the
// posture every other process-wide-state test in this package takes.
func TestRunWorkflowTaskSpans(t *testing.T) {
	recorder := conformance.RecordSpans(t)

	out, err := v1.Run(t.Context(), conformance.TaskSpanWorkflow())

	conformance.AssertTaskSpans(t, recorder, out, err)
}

// TestRunWorkflowOpensNoSpansWithoutATracerProvider is the zero-config half.
//
// The local driver now opens spans, and the promise that a process which
// configured no telemetry stays silent has to survive that. `engine` has the
// same test for the durable driver (TestNoSpansWithoutATracerProvider); this is
// the driver that just gained the code, so it needs its own.
//
// Asserted the way the engine's own does: the recorder is installed only
// *after* the run, so anything the run minted went to the global no-op provider
// and left nothing behind — which is what the not-recording guard in
// [v1.StartTaskSpan] makes cheap as well as silent.
func TestRunWorkflowOpensNoSpansWithoutATracerProvider(t *testing.T) {
	if _, err := v1.Run(t.Context(), conformance.TaskSpanWorkflow()); err != nil {
		t.Fatalf("the run failed: %v", err)
	}

	recorder := conformance.RecordSpans(t)

	if ended := recorder.Ended(); len(ended) != 0 {
		t.Fatalf("a run with no tracer provider configured recorded %d spans", len(ended))
	}
}
