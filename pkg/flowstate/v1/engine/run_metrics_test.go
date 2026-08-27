package engine_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// TestRunWorkflowRecordsRunLifecycleMetrics is the durable half of #917's
// run-lifecycle metrics — the same case
// [flowstatev1_test.TestRunWorkflowRecordsRunLifecycleMetrics] runs against
// the local driver.
//
// [engine.Run] is workflow code, so it cannot call [v1.RecordRunStart] or
// [v1.RecordRunExecution] directly the way the local driver's [v1.RunWithInputs]
// does — see `engine/runmetrics.go`'s doc for why. What it calls instead is
// Temporal's own [workflow.GetMetricsHandler], and the only way a test can see
// what that handler recorded is to give the test environment one built against
// the same reader [conformance.RecordMetrics] installed as the global meter
// provider, via [testsuite.WorkflowTestSuite.SetMetricsHandler]. Without that
// wiring this test would pass vacuously — the handler defaults to a no-op, and
// a no-op recording nothing is indistinguishable from a correct recording of
// nothing.
func TestRunWorkflowRecordsRunLifecycleMetrics(t *testing.T) {
	reader := conformance.RecordMetrics(t)

	outputs, err := runRunMetricsWorkflow(t, conformance.RunMetricsWorkflow())
	require.NoError(t, err)
	require.NotNil(t, outputs)

	conformance.AssertRunMetrics(t, reader, metricschema.DriverDurable, conformance.RunMetricsWorkflow().GetName(),
		metricschema.OutcomeSuccess, "")
}

// TestRunWorkflowRecordsAFailedRunAsAFailure is the durable half of the
// failure case: a run that fails outright still records exactly one start and
// one completion, the completion carrying outcome=error and the run's own
// [v1.ErrorKind] — read back from the [temporal.ApplicationError] Type
// [engine.classifyRunError] wraps a terminal [*engine.ErrRunFailed] in, the
// same way the transcript's own failure record already does.
func TestRunWorkflowRecordsAFailedRunAsAFailure(t *testing.T) {
	reader := conformance.RecordMetrics(t)

	_, err := runRunMetricsWorkflow(t, conformance.FailingRunMetricsWorkflow())
	require.Error(t, err, "the run must fail for this case to test anything")

	conformance.AssertRunMetrics(t, reader, metricschema.DriverDurable, conformance.FailingRunMetricsWorkflow().GetName(),
		metricschema.OutcomeError, v1.ErrorKindUnknownTask.String())
}

// TestRunWorkflowRecordsNoRunMetricsWithoutAMetricsHandler is the durable
// zero-config half: with no metrics handler installed on the test
// environment — the default every other durable test in this package already
// runs under — a run records nothing of ours.
func TestRunWorkflowRecordsNoRunMetricsWithoutAMetricsHandler(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: conformance.RunMetricsWorkflow()})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	reader := conformance.RecordMetrics(t)

	conformance.AssertNoMetrics(t, reader)
}

// runRunMetricsWorkflow drives w through the durable driver's test
// environment with a metrics handler wired to whatever
// [conformance.RecordMetrics] most recently installed as the global meter
// provider, and returns the run's outputs and error the way [v1.Run] returns
// them.
//
// The handler is built the same way `cmd/flow/telemetry.go`'s
// [initTelemetry] builds the process's real one — [opentelemetry.NewMetricsHandler]
// over a Meter named "temporal-sdk" — so this test exercises the identical
// bridge production uses rather than a shortcut that happens to make the
// assertion pass.
func runRunMetricsWorkflow(t *testing.T, w *v1.Workflow) (*v1.Workflow_StepOutputs, error) {
	t.Helper()

	handler := opentelemetry.NewMetricsHandler(opentelemetry.MetricsHandlerOptions{
		Meter: otel.GetMeterProvider().Meter("temporal-sdk"),
	})

	testSuite := &testsuite.WorkflowTestSuite{}
	testSuite.SetMetricsHandler(handler)

	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(engine.Run)

	// The real activity, run through the mock so a task named "nosuchtask" is
	// classified by [engine.Task]'s own registry lookup rather than by the
	// test environment failing to find an activity to schedule at all — the
	// same wiring [runTaskMetricWorkflow] uses one file over.
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: w})
	require.True(t, env.IsWorkflowCompleted())

	if err := env.GetWorkflowError(); err != nil {
		return nil, err
	}

	outputs := &v1.Workflow_StepOutputs{}
	require.NoError(t, env.GetWorkflowResult(outputs))

	return outputs, nil
}
