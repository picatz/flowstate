package flowstatev1_test

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

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

// oversizedName is a name far past any schema bound, in the size class the
// finding named: a submission is refused above 1 MiB, so this is a name that
// reaches telemetry and is then refused.
var oversizedName = strings.Repeat("n", 1<<20)

// TestSpanNamesAreBoundedBeforeExport is #903's round-6 finding: a name that
// never passed validation must not be exported to a collector verbatim.
//
// The hole was real on the path an embedder takes. [v1.RunWithInputs] checks
// only that a workflow has steps and then opens the run span; the bound that
// would refuse this workflow — [v1.CheckSubmissionSize] — runs *after* it. So a
// 1 MiB name became a 1 MiB span name and a 1 MiB attribute, exported before the
// submission carrying it was refused. The same applied one level down, since a
// hand-built [v1.Task] reaches [v1.StartTaskSpan] by the same unvalidated route.
//
// Both constructors are asserted, because bounding one and not the other would
// have moved the unbounded value rather than removed it.
func TestSpanNamesAreBoundedBeforeExport(t *testing.T) {
	recorder := conformance.RecordSpans(t)

	// The run span, through the real entry point rather than by calling the
	// constructor: the claim is about the path, and the path is what had the
	// hole. The run is refused, which is the point — the span exists anyway.
	_, err := v1.RunWithInputs(t.Context(), &v1.Workflow{
		Name:    oversizedName,
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{{Id: "never", Kind: &v1.Node_Value{Value: v1.NewLiteral("x")}}},
	}, nil)
	require.Error(t, err, "a 1 MiB workflow name was accepted, so this test no longer exercises the refused path")

	// And the task span, whose name comes from a Task an embedder built by hand.
	_, taskSpan := v1.StartTaskSpan(t.Context(), &v1.Task{Name: oversizedName}, oversizedName)
	taskSpan.End()

	var checked int
	for _, stub := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		var limit int
		switch {
		case strings.HasPrefix(stub.Name, "flowstate.run/"):
			limit = v1.MaxWorkflowNameLen
		case strings.HasPrefix(stub.Name, "flowstate.task/"):
			limit = v1.MaxTaskNameLen
		default:
			continue
		}
		checked++

		// The name is bounded, and says it was cut. Counted in runes, since that
		// is what the schema's rule counts.
		//
		// The failure message quotes a *prefix* of the span name and never the
		// whole of it: when this assertion fails the name is by definition
		// enormous, and a message that interpolated it would be truncated by the
		// test runner's own line scanner — a failure nobody can read is a failure
		// that costs an hour to understand. Found by mutating the bound away.
		name := stub.Name[strings.Index(stub.Name, "/")+1:]
		require.LessOrEqual(t, utf8.RuneCountInString(name), limit+1,
			"a span exports a name of %d characters, past the schema's bound of %d; it begins %q",
			utf8.RuneCountInString(name), limit, stub.Name[:min(len(stub.Name), 64)])
		require.True(t, strings.HasSuffix(name, "…"),
			"a truncated name carries no marker, so a reader cannot tell it was cut; it ends %q",
			name[max(0, len(name)-32):])

		// And every attribute carrying a name is bounded the same way, which is
		// the half that would otherwise relocate the value rather than remove it.
		for _, attr := range stub.Attributes {
			key := string(attr.Key)
			if key != v1.SpanAttributeWorkflowName && key != v1.SpanAttributeTaskName && key != v1.SpanAttributeStepID {
				continue
			}
			attributeLimit := limit
			if key == v1.SpanAttributeStepID {
				attributeLimit = v1.MaxStepIDLen
			}
			require.LessOrEqual(t, utf8.RuneCountInString(attr.Value.AsString()), attributeLimit+1,
				"%s carries an unbounded %s attribute", stub.Name, key)
		}
	}
	require.Equal(t, 2, checked, "want one bounded run span and one bounded task span")

	// The containment direction, over the rendered shapes: no span anywhere holds
	// the oversized name, in any rendering.
	//
	// Checked with strings.Contains and reported by hand rather than with
	// require.NotContains, which prints the haystack — here a megabyte of it.
	for _, rendered := range renderedSpanShapes(recorder) {
		if strings.Contains(rendered, oversizedName) {
			t.Fatal("an unvalidated oversized name reached a span, which is exported to a collector")
		}
	}
}

// TestTaskSpanOmitsFactsItsCallerDoesNotKnow pins the honest absence case.
// StartTaskSpan owns neither execution driver's attempt counter, and an old
// activity payload may carry no appended step id, so neither is guessed from a
// timestamp, span name, or message.
func TestTaskSpanOmitsFactsItsCallerDoesNotKnow(t *testing.T) {
	recorder := conformance.RecordSpans(t)

	_, span := v1.StartTaskSpan(t.Context(), &v1.Task{Name: "log"}, "")
	span.End()

	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())
	require.Len(t, stubs, 1)
	attrs := map[string]struct{}{}
	for _, attr := range stubs[0].Attributes {
		attrs[string(attr.Key)] = struct{}{}
	}
	require.NotContains(t, attrs, v1.SpanAttributeStepID,
		"an unknown step id was exported as if it were known")
	require.NotContains(t, attrs, v1.SpanAttributeAttempt,
		"the shared span constructor inferred an attempt outside either driver's durable source")
}

// TestALegalNameIsExportedExactly is the other half, and the reason the bound is
// truncation rather than a hash or a fixed fallback: the overwhelmingly common
// case must be untouched, byte for byte.
func TestALegalNameIsExportedExactly(t *testing.T) {
	t.Parallel()

	// The longest name the schema permits, which is the value most likely to be
	// mangled by an off-by-one in the bound.
	exact := strings.Repeat("w", v1.MaxWorkflowNameLen)

	require.Equal(t, "flowstate.run/"+exact, v1.RunSpanName(exact),
		"a name exactly at the bound was altered")
	require.Equal(t, "flowstate.task/"+exact, v1.TaskSpanName(exact),
		"a name exactly at the bound was altered")

	// One character further is the first that may be cut.
	require.NotEqual(t, "flowstate.run/"+exact+"w", v1.RunSpanName(exact+"w"),
		"a name past the bound was exported unchanged")
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
