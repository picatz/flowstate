package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// TestRunWorkflowRecordsRunLifecycleMetrics is the local half of #917's
// run-lifecycle metrics: one successful run records exactly one start and one
// completion, both attributed to the workflow's own name and
// [metricschema.DriverLocal].
//
// No t.Parallel, for [conformance.RecordMetrics]'s reason: it swaps the global
// meter provider.
func TestRunWorkflowRecordsRunLifecycleMetrics(t *testing.T) {
	reader := conformance.RecordMetrics(t)

	_, err := v1.Run(t.Context(), conformance.RunMetricsWorkflow())
	if err != nil {
		t.Fatalf("the run failed: %v", err)
	}

	conformance.AssertRunMetrics(t, reader, metricschema.DriverLocal, conformance.RunMetricsWorkflow().GetName(),
		metricschema.OutcomeSuccess, "")
}

// TestRunWorkflowRecordsAFailedRunAsAFailure is the failure half: a run that
// never completes still records exactly one start and one completion, the
// completion carrying outcome=error and the run's own [v1.ErrorKind].
func TestRunWorkflowRecordsAFailedRunAsAFailure(t *testing.T) {
	reader := conformance.RecordMetrics(t)

	_, err := v1.Run(t.Context(), conformance.FailingRunMetricsWorkflow())
	if err == nil {
		t.Fatalf("the run must fail for this case to test anything")
	}

	conformance.AssertRunMetrics(t, reader, metricschema.DriverLocal, conformance.FailingRunMetricsWorkflow().GetName(),
		metricschema.OutcomeError, v1.ErrorKindUnknownTask.String())
}

// TestRunWorkflowRecordsNoRunMetricsWithoutAMeterProvider is the zero-config
// half: a run with no meter provider configured leaves the run-lifecycle
// instruments untouched, exactly as [TestRunWorkflowRecordsNoMetricsWithoutAMeterProvider]
// already asserts for the task ones.
func TestRunWorkflowRecordsNoRunMetricsWithoutAMeterProvider(t *testing.T) {
	if _, err := v1.Run(t.Context(), conformance.RunMetricsWorkflow()); err != nil {
		t.Fatalf("the run failed: %v", err)
	}

	reader := conformance.RecordMetrics(t)

	conformance.AssertNoMetrics(t, reader)
}
