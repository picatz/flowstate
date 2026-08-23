package engine_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// TestRunWorkflowTaskMetrics runs [conformance.AssertTaskMetrics] against the
// durable driver — the same case [flowstatev1_test.TestRunWorkflowTaskMetrics]
// runs against the local one.
//
// Neither side had these instruments before #526's first slice, so unlike the
// span case there is no "this side always had it" half: both drivers gained
// them from one call, [v1.ObserveTask], which is the arrangement that makes
// "the drivers agree about metrics" a thing a test can fail on rather than a
// coincidence between two recording sites.
func TestRunWorkflowTaskMetrics(t *testing.T) {
	reader := conformance.RecordMetrics(t)

	outputs, err := runTaskMetricWorkflow(t)

	conformance.AssertTaskMetrics(t, reader, metricschema.DriverDurable, outputs, err)
}

// TestRunWorkflowRecordsNoMetricsWithoutAMeterProvider is the durable half of
// the zero-config claim: with nothing configured, the run records nothing of
// ours. The reader is installed only after the run, so anything recorded went to
// the global no-op provider.
func TestRunWorkflowRecordsNoMetricsWithoutAMeterProvider(t *testing.T) {
	if _, err := runTaskMetricWorkflow(t); err != nil {
		t.Fatalf("the run failed: %v", err)
	}

	reader := conformance.RecordMetrics(t)

	conformance.AssertNoMetrics(t, reader)
}

// TestADurableTaskPanicIsRecordedAsAFailure is the durable half of the defect
// Codex found on #888.
//
// The substrate's half of the claim is what this side can assert that the local
// one cannot: Temporal's activity executor recovers the panic into a failed
// activity, and the workflow fails with it. The observation must have recorded
// the same event as a failure — before the fix it recorded outcome=success,
// which is the disagreement in its most visible form, since the same execution
// was simultaneously a success in the metric and a failure in the substrate.
//
// A single attempt: [conformance.PanicWorkflow] pins MaxAttempts to 1, so the
// activity is not retried and the count is about the crash rather than about
// the retry policy.
func TestADurableTaskPanicIsRecordedAsAFailure(t *testing.T) {
	conformance.RegisterPanickingTask(t)

	reader := conformance.RecordMetrics(t)

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.TaskV2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskV2)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: conformance.PanicWorkflow()})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(),
		"the substrate must still see the panic as a failed activity")

	conformance.AssertPanicRecordedAsFailure(t, reader, metricschema.DriverDurable)
}

// runTaskMetricWorkflow drives [conformance.TaskMetricWorkflow] through the
// durable driver's test environment, the same way the span case's own durable
// half does.
func runTaskMetricWorkflow(t *testing.T) (*v1.Workflow_StepOutputs, error) {
	t.Helper()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.TaskV2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskV2)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScopeV2, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScopeV2)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: conformance.TaskMetricWorkflow()})
	require.True(t, env.IsWorkflowCompleted())

	if err := env.GetWorkflowError(); err != nil {
		return nil, err
	}

	outputs := &v1.Workflow_StepOutputs{}
	require.NoError(t, env.GetWorkflowResult(outputs))

	return outputs, nil
}
