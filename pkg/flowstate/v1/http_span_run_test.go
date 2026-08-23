package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestRunWorkflowHTTPSpan runs [conformance.AssertHTTPSpan] against the local
// driver. The same case runs against the durable driver in the engine package
// (TestRunWorkflowHTTPSpan there) — two verified callers, which is what
// invariant 3 asks a shared case to have.
//
// What it can see that netpolicy's own tests cannot: the span is opened on the
// context the task hands its client, and the two drivers build that context by
// different routes. A local run reaches the task function directly; a durable
// one arrives inside an activity, under whatever context Temporal's interceptor
// left there. Losing it on one side is exactly the kind of asymmetry #523's
// gap 3 already describes for the spans this slice does not add.
//
// No t.Parallel: [conformance.NewTracedHTTPServer] swaps the process-wide http
// task registration and [conformance.RecordSpans] the global tracer provider,
// the posture every other registry-swapping test in this package takes.
func TestRunWorkflowHTTPSpan(t *testing.T) {
	recorder := conformance.RecordSpans(t)
	server := conformance.NewTracedHTTPServer(t)

	out, err := v1.Run(t.Context(), conformance.HTTPSpanWorkflow(server.URL))

	conformance.AssertHTTPSpan(t, server, recorder, out, err)
}
