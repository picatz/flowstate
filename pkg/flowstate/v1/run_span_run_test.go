package flowstatev1_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// TestTheRunSpanVocabularyIsTheSchemasVocabulary is what keeps the two
// spellings of these keys identical, since `runspan.go` writes each as a
// literal rather than importing the schema package (its doc says why: the
// import ratchet in `imports_test.go`).
//
// A test rather than a shared constant is the trade this makes explicit — the
// keys can only disagree for as long as it takes this to run — and it also
// asserts the half a shared constant would not: that each key is *classified*,
// so "may this reach a metric" has an answer somebody wrote down.
func TestTheRunSpanVocabularyIsTheSchemasVocabulary(t *testing.T) {
	t.Parallel()

	for key, want := range map[string]metricschema.Class{
		v1.SpanAttributeWorkflowName:   metricschema.ClassConfiguration,
		v1.SpanAttributeTriggerName:    metricschema.ClassConfiguration,
		v1.SpanAttributeDeliveryJoined: metricschema.ClassConstruction,

		// Peer-controlled: one value per delivery, chosen by an external
		// sender, so it is a span attribute and never a metric label.
		v1.SpanAttributeDeliveryID: metricschema.ClassPeerControlled,
	} {
		var declared bool
		for _, attr := range metricschema.Table {
			if attr.Key != key {
				continue
			}
			declared = true

			if attr.Class != want {
				t.Fatalf("%q is classified %s in the schema, want %s", key, attr.Class, want)
			}
		}
		if !declared {
			t.Fatalf("the run span writes %q, which metricschema.Table does not declare — "+
				"add the row, so one place says what the key is and whether a metric may carry it", key)
		}
	}
}

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

// TestLocalRunSpanRecordsAPanicAsAFailure is the run-level half of #888's
// defect, found by the same review on #903.
//
// A task that panics never returns through the assignment that records the
// outcome, so a run span ended by a plain `defer span.End()` closes UNSET: the
// task span underneath correctly says the execution failed (through
// [v1.ObserveTask], which already handles this) while the span an operator looks
// at *first* says nothing at all. A crashed run would be indistinguishable from
// a successful one, in the direction that reads as health.
//
// Three claims, because two of them are the ways a fix for the third goes wrong:
// the run span is failed, the task span is still failed, and the panic still
// reaches the caller with its own value — an observation that swallowed a crash
// would be worse than the defect it fixed.
//
// Local-only, deliberately: durably the run span is Temporal's own and the panic
// is recovered by its activity executor, which is
// `engine.TestTaskPanicIsRecordedAsAFailure`'s subject rather than this one's.
func TestLocalRunSpanRecordsAPanicAsAFailure(t *testing.T) {
	recorder := conformance.RecordSpans(t)
	conformance.RegisterPanickingTask(t)

	workflow := conformance.PanicWorkflow()

	// The panic reaches the caller, unchanged. [conformance.RegisterPanickingTask]
	// panics with a string containing the secret, and the local driver propagates
	// it rather than recovering — so this asserts the observation is transparent.
	require.PanicsWithValue(t,
		"the task panicked with "+conformance.TaskPanicSecret,
		func() { _, _ = v1.Run(t.Context(), workflow) },
		"the panic did not reach the caller with its own value, so observing the run changed what a crash does")

	var runSpan, taskSpan tracetest.SpanStub
	for _, stub := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		switch stub.Name {
		case v1.RunSpanName(workflow.GetName()):
			runSpan = stub
		case v1.TaskSpanName(conformance.PanicTaskName):
			taskSpan = stub
		}
	}

	require.Equal(t, v1.RunSpanName(workflow.GetName()), runSpan.Name,
		"a run that panicked opened no run span, or never ended one — an unended span is never exported")
	require.Equal(t, codes.Error, runSpan.Status.Code,
		"the run span of a crashed run is %s, so a crash is indistinguishable from a success at the run level",
		runSpan.Status.Code)

	// The level below still says what it said, so this fix did not move the
	// failure from one span to the other.
	require.Equal(t, codes.Error, taskSpan.Status.Code,
		"the task span of a panicking task is %s", taskSpan.Status.Code)

	// And the panic's own words stay out of the collector, the rule an error
	// message is held to — checked over the rendered shapes, not the attribute
	// this code happens to write.
	for _, rendered := range renderedSpanShapes(recorder) {
		if strings.Contains(rendered, conformance.TaskPanicSecret) {
			t.Fatal("a panic value reached a span, which is exported to a collector")
		}
	}
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
