package conformance

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// The shared case for #526's third question: are retries climbing.
//
// # Why it is shared, and what it would catch
//
// Retrying is the one place in this repository where the two drivers do the
// same observable thing by completely different means: the local driver loops
// in process (`eval.go`'s runStepWithPolicy), and the durable driver does not
// loop at all — Temporal schedules the activity again, so a retry is a fresh
// entry into `engine.Task` on a worker that may not be the one that failed.
// Two recording sites for one signal is exactly the shape CLAUDE.md's "a value
// with one meaning, written down twice" section is about, so the count is
// recorded by the one call both drivers make ([v1.ObserveTask]) and asserted
// here against both.
//
// The failure this guards against is quiet in the direction that matters. A
// retry counter that fired on only one driver would make a rehearsal of a flaky
// dependency look calm while production's panel climbed, or the reverse — and
// either way the number an operator divides by (`flowstate.task.executions`)
// would keep agreeing, because that instrument counts every attempt on both
// sides already.

// FlakyTaskName is the task both drivers register for this case.
const FlakyTaskName = "conformance-flaky-task"

// RegisterFlakyTask registers [FlakyTaskName] for the duration of a test: it
// fails once, with a retryable failure, and succeeds on every attempt after.
//
// A plain error, which [v1.ClassifyError] classifies as
// [v1.ErrorKindInternal] and therefore as retryable under both drivers — the
// point being that the retry is decided by the ordinary classification rather
// than by anything this case arranges.
//
// The registry is process-wide, so the task is removed afterwards, and the
// attempt counter is per-registration so that two tests in one binary cannot
// hand each other a task that has already used up its one failure.
func RegisterFlakyTask(tb testing.TB) {
	tb.Helper()

	var attempts atomic.Int64

	registry := v1.DefaultRegistry()
	if err := registry.Register(v1.TaskDef{
		Name: FlakyTaskName,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			if attempts.Add(1) == 1 {
				return nil, errors.New("the dependency was unavailable")
			}

			return &v1.Node_Outputs{}, nil
		},
	}); err != nil {
		tb.Fatalf("registering the flaky task failed: %v", err)
	}

	tb.Cleanup(func() { registry.Unregister(FlakyTaskName) })
}

// ExpectedRetries is how many retries a [RetryWorkflow] run performs: the task
// fails once, so the step is tried exactly twice and one of those is a retry.
const ExpectedRetries = 1

// RetryWorkflow returns a one-step workflow whose step fails once and then
// succeeds.
//
// Two attempts and no more, so that a driver retrying more than it should fails
// this case rather than merely being slower than the other. The interval is a
// millisecond because the wait is not the subject: the local driver sleeps it
// on the run's clock and the durable driver hands it to Temporal, and neither
// arrangement is what this case is claiming anything about.
func RetryWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "task-retry",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:   "flaky",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: FlakyTaskName}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{
				MaxAttempts:     2,
				InitialInterval: durationpb.New(time.Millisecond),
			}},
		}},
	}
}

// AssertRetryRecorded is the shared assertion: the run counted one retry, under
// this driver's label, and counted the step's two executions alongside it.
//
// Both halves are asserted because either alone can be satisfied by a mistake.
// A retry counter that also counted first attempts reads as one retry for a
// step that never failed, which is why the executions total is checked against
// a number one larger; and a retry counted per *step* rather than per attempt
// would survive an assertion that only asked whether anything was recorded.
func AssertRetryRecorded(tb testing.TB, reader *sdkmetric.ManualReader, driver string) {
	tb.Helper()

	collected := collectFlowstateMetrics(tb, reader)

	points, ok := collected[metricschema.InstrumentTaskRetries]
	if !ok {
		tb.Fatalf("a run whose step failed once recorded nothing on %s — every instrument the run touched: %v",
			metricschema.InstrumentTaskRetries, sortedKeys(collected))
	}
	if len(points) != 1 {
		tb.Fatalf("%s recorded %d attribute sets, want one — %v",
			metricschema.InstrumentTaskRetries, len(points), points)
	}

	point := points[0]
	if point.Count != ExpectedRetries {
		tb.Fatalf("%s counted %d retries, want %d — counting first attempts too gives %d",
			metricschema.InstrumentTaskRetries, point.Count, ExpectedRetries, ExpectedRetries+1)
	}

	want := map[string]string{
		metricschema.TaskName: FlakyTaskName,
		metricschema.Driver:   driver,
	}
	if !sameAttributes(point.Attributes, want) {
		tb.Fatalf("%s carries %v, want %v", metricschema.InstrumentTaskRetries, point.Attributes, want)
	}

	// The denominator, so that "one retry" is read against the executions it is
	// a retry among rather than against nothing.
	var executions uint64
	for _, execution := range collected[metricschema.InstrumentTaskExecutions] {
		executions += execution.Count
	}
	if want := uint64(ExpectedRetries + 1); executions != want {
		tb.Fatalf("%s counted %d executions of a step tried twice, want %d",
			metricschema.InstrumentTaskExecutions, executions, want)
	}

	assertDeclaredAttributesOnly(tb, collected)
}
