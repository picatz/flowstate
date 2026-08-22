package flowstatev1_test

import (
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestLocalRunIsOneTree is #523's gap 4 for the local driver: a rehearsal's
// trace is one tree rooted at the run, not a forest of task spans with nothing
// above them.
//
// The durable half of this claim is `engine.TestTaskSpanParentsUnderTemporalActivitySpan`,
// which makes the identical assertion through the same shared function with
// `RunWorkflow:Run` as the root — the name differing on purpose, the shape not.
// [v1.StartRunSpan]'s doc carries the reasoning.
//
// No t.Parallel: [conformance.RecordSpans] swaps the global tracer provider.
func TestLocalRunIsOneTree(t *testing.T) {
	recorder := conformance.RecordSpans(t)

	workflow := conformance.TaskSpanWorkflow()

	out, err := v1.Run(t.Context(), workflow)

	// The task spans first, so that a failure here says "the run went wrong"
	// rather than "the tree is shaped wrongly".
	conformance.AssertTaskSpans(t, recorder, out, err)

	conformance.AssertRunIsOneTree(t, recorder, v1.RunSpanName(workflow.GetName()))
}

// TestLocalRunSpanRecordsNoValues holds the run span to the same rule every
// other span in this repository is held to: names and classifications only.
//
// The workflow it runs hides [conformance.TaskSpanSecret] in a task input, and
// the assertion is over the rendered spans rather than over the attribute this
// code happens to write — the containment shape CLAUDE.md names, because `fmt`
// reaching a value through an unexported field prints the fields instead of
// calling any accessor.
func TestLocalRunSpanRecordsNoValues(t *testing.T) {
	recorder := conformance.RecordSpans(t)

	workflow := conformance.TaskSpanWorkflow()
	if _, err := v1.Run(t.Context(), workflow); err != nil {
		t.Fatalf("the run failed: %v", err)
	}

	want := v1.RunSpanName(workflow.GetName())

	var found bool
	for _, stub := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		if stub.Name != want {
			continue
		}
		found = true

		for _, attr := range stub.Attributes {
			if string(attr.Key) != v1.SpanAttributeWorkflowName {
				t.Fatalf("the run span carries %q, which is not in the vocabulary pkg/flowstate/v1/runspan.go names",
					attr.Key)
			}
			if got := attr.Value.AsString(); got != workflow.GetName() {
				t.Fatalf("the run span names workflow %q, want %q", got, workflow.GetName())
			}
		}
	}
	if !found {
		t.Fatalf("the run opened no %s span", want)
	}

	for _, rendered := range renderedSpanShapes(recorder) {
		if strings.Contains(rendered, conformance.TaskSpanSecret) {
			t.Fatal("an input value reached a span, which is exported to a collector")
		}
	}
}

// renderedSpanShapes renders what was recorded through the %v family, over the
// batch, each span, and a struct holding one.
//
// The shape CLAUDE.md's invariant 7 names: `fmt` cannot call a method on a value
// it reaches through an unexported field, so a value that a direct render hides
// can still surface from inside a wrapper.
func renderedSpanShapes(recorder *tracetest.SpanRecorder) []string {
	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())

	type wrapper struct {
		one   tracetest.SpanStub
		batch []tracetest.SpanStub
	}

	rendered := []string{
		fmt.Sprintf("%v", stubs), fmt.Sprintf("%+v", stubs), fmt.Sprintf("%#v", stubs),
	}
	for _, stub := range stubs {
		w := wrapper{one: stub, batch: stubs}
		rendered = append(rendered,
			fmt.Sprintf("%v", stub), fmt.Sprintf("%+v", stub), fmt.Sprintf("%#v", stub),
			fmt.Sprintf("%v", w), fmt.Sprintf("%+v", w), fmt.Sprintf("%#v", w))
	}

	return rendered
}

// TestLocalRunSpanRecordsARefusal is the reason the span is opened at the submit
// boundary rather than around the evaluator.
//
// A submission this driver refuses — here an undeclared input — runs no step, so
// a span opened inside [v1.RunWithInputs]'s evaluation would leave an operator
// with nothing at all for the request that failed. The classification is the
// whole of what is recorded: an error's text can quote what it was given.
func TestLocalRunSpanRecordsARefusal(t *testing.T) {
	recorder := conformance.RecordSpans(t)

	workflow := conformance.TaskSpanWorkflow()

	_, err := v1.RunWithInputs(t.Context(), workflow,
		map[string]*v1.Value{"nothing-declares-this": v1.NewLiteral(conformance.TaskSpanSecret)})
	if err == nil {
		t.Fatal("an undeclared input was accepted, so this test no longer exercises a refusal")
	}

	want := v1.RunSpanName(workflow.GetName())

	var found bool
	for _, stub := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		if stub.Name != want {
			continue
		}
		found = true

		if stub.Status.Code != codes.Error {
			t.Fatalf("a refused submission left a run span with status %s", stub.Status.Code)
		}
	}

	for _, rendered := range renderedSpanShapes(recorder) {
		if strings.Contains(rendered, conformance.TaskSpanSecret) {
			t.Fatal("the refused input's value reached the span that recorded the refusal")
		}
	}
	if !found {
		t.Fatalf("a refused submission opened no %s span, so the outcome an operator most wants to see is the one that leaves no trace", want)
	}
}
