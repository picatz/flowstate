package flowstatev1_test

import (
	"testing"

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
