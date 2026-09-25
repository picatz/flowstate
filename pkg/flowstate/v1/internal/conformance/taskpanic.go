package conformance

import (
	"context"
	"strings"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// The shared case for the defect Codex found on #888: a task that panics was
// recorded as a *success*.
//
// The observation's ending call read an error variable the panicking assignment
// never reached, so `flowstate.task.executions` counted `outcome=success` with
// no `error.type` for an execution that crashed, and the span agreed with it.
// Temporal meanwhile reported the activity as failed. An operator's error rate
// therefore omitted precisely the failures that matter most, and omitted them in
// the direction that looks like health.
//
// It is a shared case rather than one per driver because it is the same code
// under both — [v1.ObserveTask] — and because "the numbers agree" has to keep
// holding on the crash path, which is where a rehearsal is least like
// production: a local run propagates the panic to whoever called it, a durable
// run has Temporal's executor recover it into a failed activity, and the
// *measurement* has to say the same thing either way.

// PanicTaskName is the task both drivers register for this case.
const PanicTaskName = "conformance_panicking_task"

// TaskPanicSecret is the value the panicking task panics *with*.
//
// A panic can quote whatever it was handed — `panic(fmt.Sprintf("bad token %s",
// tok))` is ordinary code — which puts a panic value in the same class as an
// error message: it may never reach a span or a label. Panicking with something
// distinctive is what lets the assertion check that, rather than trusting the
// recording site to have been careful.
const TaskPanicSecret = "s3cr3t-panic-value-that-must-never-be-exported"

// RegisterPanickingTask registers [PanicTaskName] for the duration of a test.
//
// The registry is process-wide, so the task is removed afterwards — a task that
// panics, left registered, is a landmine for every later test in the binary.
func RegisterPanickingTask(tb testing.TB) {
	tb.Helper()

	registry := v1.DefaultRegistry()
	if err := registry.Register(v1.TaskDef{
		Name: PanicTaskName,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			panic("the task panicked with " + TaskPanicSecret)
		},
	}); err != nil {
		tb.Fatalf("registering the panicking task failed: %v", err)
	}

	tb.Cleanup(func() { registry.Unregister(PanicTaskName) })
}

// PanicWorkflow returns a one-step workflow whose only step panics.
//
// One attempt, because the assertion is about what a single crashed execution
// records: with the driver default a local run would retry and record several,
// and the count would then be measuring the retry policy rather than the fix.
func PanicWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "task-panic",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:     "boom",
			Kind:   &v1.Node_Task{Task: &v1.Task{Name: PanicTaskName}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}},
		}},
	}
}

// AssertPanicRecordedAsFailure is the shared assertion.
//
// It is deliberately about the *metric*, not about how each driver surfaces the
// crash — that difference is real (a propagated panic here, a failed activity
// there) and is asserted by each driver's own test, which knows what its
// substrate does. What must not differ is the measurement.
func AssertPanicRecordedAsFailure(tb testing.TB, reader *sdkmetric.ManualReader, driver string) {
	tb.Helper()

	collected := CollectFlowstateMetrics(tb, reader)

	for _, name := range []string{
		metricschema.InstrumentTaskExecutions,
		metricschema.InstrumentTaskDuration,
	} {
		points, ok := collected[name]
		if !ok {
			tb.Fatalf("a panicking task recorded nothing on %s — a crash that is missing from an error rate reads as health",
				name)
		}
		if len(points) != 1 {
			tb.Fatalf("%s recorded %d attribute sets, want one — %v", name, len(points), points)
		}

		want := map[string]string{
			metricschema.TaskName:    PanicTaskName,
			metricschema.Driver:      driver,
			metricschema.TaskOutcome: metricschema.OutcomeError,
			metricschema.ErrorType:   metricschema.ErrorTypePanic,
		}
		if !sameAttributes(points[0].Attributes, want) {
			tb.Fatalf("%s carries %v, want %v — an execution that panicked is a failure, and its error.type says which kind",
				name, points[0].Attributes, want)
		}
	}

	// And the panic's own words stay out of the collector, the same rule an
	// error message is held to.
	assertDeclaredAttributesOnly(tb, collected)

	for _, points := range collected {
		for _, point := range points {
			for key, value := range point.Attributes {
				if strings.Contains(value, TaskPanicSecret) {
					tb.Fatalf("a panic value reached the %q label, which is exported to a collector", key)
				}
			}
		}
	}
}
