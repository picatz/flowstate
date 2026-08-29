package conformance

import (
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// Shared cases for #917's run-lifecycle metrics — a run's own started,
// completed and failed, the counterpart [taskmetrics.go] already gives one
// task execution.
//
// # Why this is a shared case
//
// The same reason every other pair in this package is: a dashboard is built
// once and read against whatever ran, so a local rehearsal and a durable run
// have to record the same instruments under the same attribute keys, and
// differ in exactly [metricschema.Driver]. Unlike the task case, the two
// drivers reach these instruments through genuinely different code —
// [v1.RecordRunStart]/[v1.RecordRunExecution] locally, through the plain OTel
// meter API; `engine.recordRunStart`/`engine.recordRunCompletion` durably,
// through Temporal's replay-safe metrics handler — which is exactly why a
// shared assertion matters more here, not less: nothing but this case would
// catch the two recording sites drifting about what they attach.

// RunMetricsWorkflow is the workflow both drivers run for the success case: a
// single successful step, so the run's own instruments are asserted without a
// task's own outcome muddying which "outcome" a failure below.
func RunMetricsWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name: "run-metrics-success",
		Steps: []*v1.Node{{
			Id: "log",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
				"message": v1.NewValue("run metrics case"),
			}}},
		}},
	}
}

// FailingRunMetricsWorkflow is the workflow both drivers run for the failure
// case: a step naming a task nobody registered, which fails permanently and
// deterministically — no retry timing for a test to be flaky about — with
// [v1.ErrorKindUnknownTask], the same case [ErrorKindCases] uses for the
// identical reason.
func FailingRunMetricsWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name: "run-metrics-failure",
		Steps: []*v1.Node{{
			Id:   "bad",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "nosuchtask"}},
		}},
	}
}

// AssertRunMetrics is the shared assertion both drivers make about a run's
// lifecycle instruments.
//
// wantOutcome is [metricschema.OutcomeSuccess] or [metricschema.OutcomeError];
// wantErrorType is empty for a success and the [v1.ErrorKind] string the run
// must have been classified as for a failure.
func AssertRunMetrics(tb testing.TB, reader *sdkmetric.ManualReader, driver, workflowName, wantOutcome, wantErrorType string) {
	tb.Helper()

	collected := CollectFlowstateMetrics(tb, reader)

	startPoints, ok := collected[metricschema.InstrumentRunStarts]
	if !ok {
		tb.Fatalf("the run recorded nothing on %s — every instrument the run touched: %v",
			metricschema.InstrumentRunStarts, sortedKeys(collected))
	}
	if len(startPoints) != 1 {
		tb.Fatalf("%s recorded %d attribute sets, want one — %v", metricschema.InstrumentRunStarts, len(startPoints), startPoints)
	}
	if startPoints[0].Count != 1 {
		tb.Fatalf("%s counted %d starts, want 1 — a run must start exactly once, never once per Continue-As-New segment",
			metricschema.InstrumentRunStarts, startPoints[0].Count)
	}
	wantStartAttrs := map[string]string{
		metricschema.WorkflowName: workflowName,
		metricschema.Driver:       driver,
	}
	if !sameAttributes(startPoints[0].Attributes, wantStartAttrs) {
		tb.Fatalf("%s carries %v, want %v", metricschema.InstrumentRunStarts, startPoints[0].Attributes, wantStartAttrs)
	}

	wantOutcomeAttrs := map[string]string{
		metricschema.WorkflowName: workflowName,
		metricschema.Driver:       driver,
		metricschema.RunOutcome:   wantOutcome,
	}
	if wantErrorType != "" {
		wantOutcomeAttrs[metricschema.ErrorType] = wantErrorType
	}

	for _, name := range []string{metricschema.InstrumentRunExecutions, metricschema.InstrumentRunDuration} {
		points, ok := collected[name]
		if !ok {
			tb.Fatalf("the run recorded nothing on %s — every instrument the run touched: %v",
				name, sortedKeys(collected))
		}
		if len(points) != 1 {
			tb.Fatalf("%s recorded %d attribute sets, want one — %v", name, len(points), points)
		}
		if points[0].Count != 1 {
			tb.Fatalf("%s counted %d completions, want 1", name, points[0].Count)
		}
		if !sameAttributes(points[0].Attributes, wantOutcomeAttrs) {
			tb.Fatalf("%s carries %v, want %v", name, points[0].Attributes, wantOutcomeAttrs)
		}
	}

	assertDeclaredAttributesOnly(tb, collected)
}
