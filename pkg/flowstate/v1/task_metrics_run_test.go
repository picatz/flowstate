package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// TestRunWorkflowTaskMetrics runs [conformance.AssertTaskMetrics] against the
// local driver. The same case runs against the durable driver in the engine
// package (TestRunWorkflowTaskMetrics there) — two verified callers, which is
// what invariant 3 asks a shared case to have.
//
// No t.Parallel: [conformance.RecordMetrics] swaps the global meter provider,
// the posture every other process-wide-state test in this package takes.
func TestRunWorkflowTaskMetrics(t *testing.T) {
	reader := conformance.RecordMetrics(t)

	out, err := v1.Run(t.Context(), conformance.TaskMetricWorkflow())

	conformance.AssertTaskMetrics(t, reader, metricschema.DriverLocal, out, err)
}

// TestRunWorkflowRecordsNoMetricsWithoutAMeterProvider is the zero-config half,
// the same promise [TestRunWorkflowOpensNoSpansWithoutATracerProvider] makes for
// traces: a process that configured no telemetry stays silent.
//
// Asserted the same way — the reader is installed only *after* the run, so
// anything the run recorded went to the global no-op provider and left nothing
// behind.
func TestRunWorkflowRecordsNoMetricsWithoutAMeterProvider(t *testing.T) {
	if _, err := v1.Run(t.Context(), conformance.TaskMetricWorkflow()); err != nil {
		t.Fatalf("the run failed: %v", err)
	}

	reader := conformance.RecordMetrics(t)

	conformance.AssertNoMetrics(t, reader)
}

// TestALocalTaskPanicIsRecordedAsAFailure is the local half of the defect Codex
// found on #888: a panicking task was recorded as outcome=success.
//
// Two claims, and both matter. The measurement says the execution failed and
// says a panic is what failed it — and the panic still reaches the caller, with
// nothing here having recovered it, because a local run's contract is that a
// crash crashes the way the author's own code would.
func TestALocalTaskPanicIsRecordedAsAFailure(t *testing.T) {
	conformance.RegisterPanickingTask(t)

	reader := conformance.RecordMetrics(t)

	require.PanicsWithValue(t,
		"the task panicked with "+conformance.TaskPanicSecret,
		func() { _, _ = v1.Run(t.Context(), conformance.PanicWorkflow()) },
		"the observation must not swallow or replace the panic")

	conformance.AssertPanicRecordedAsFailure(t, reader, metricschema.DriverLocal)
}
