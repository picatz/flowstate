package engine

import (
	"time"

	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// The durable half of #917's run-lifecycle metrics.
//
// [v1.RecordRunStart] and [v1.RecordRunExecution] cannot be called from here:
// they record through the plain OTel meter API, which is safe from activity
// code — real Go, run once per attempt — and unsafe from workflow code, where
// a call recorded during a replay is a call recorded a second time. That is
// the identical hazard `taskspan.go` documents for a span minted in workflow
// code, one signal over, and [runInstruments]'s doc explains why the SDK's own
// answer to it — [workflow.GetMetricsHandler] — is a different interface
// (Temporal's own `metrics.Handler`, not an OTel `metric.Meter`) with no
// shared recording function possible between the two drivers. What *is*
// shared is [v1.RunMetricAttributes], which both drivers read for which
// attributes a run-lifecycle measurement may carry — this file bounds them
// through [metricschema.Attributes] exactly as [v1.RecordRunStart] bounds them
// through [metricschema.WithAttributes], so the two recording sites cannot
// drift about what is safe to attach.

// runMetrics returns the run-lifecycle counters and histogram against
// whatever metrics handler [workflow.GetMetricsHandler] resolves to for ctx —
// the process's real handler outside a test, [workflow.TestWorkflowEnvironment]'s
// no-op default or whatever a test installed via
// [go.temporal.io/sdk/testsuite.WorkflowTestSuite.SetMetricsHandler] inside
// one.
//
// tags is applied once, to every instrument this returns, through the
// handler's own `WithTags` — its equivalent of [metricschema.WithAttributes]
// — so a caller never has to repeat the attribute set per instrument, the
// same convenience the local driver's own multi-instrument return gives
// [v1.RecordRunExecution].
func runMetrics(ctx workflow.Context, tags map[string]string) (starts, executions func(int64), duration func(time.Duration)) {
	handler := workflow.GetMetricsHandler(ctx).WithTags(tags)

	return func(n int64) { handler.Counter(metricschema.InstrumentRunStarts).Inc(n) },
		func(n int64) { handler.Counter(metricschema.InstrumentRunExecutions).Inc(n) },
		func(d time.Duration) { handler.Timer(metricschema.InstrumentRunDuration).Record(d) }
}

// runMetricTags converts [v1.RunMetricAttributes]'s bounded attribute set into
// the `map[string]string` Temporal's metrics handler takes, which is the whole
// of the translation between the two APIs — building the attribute set itself
// is [v1.RunMetricAttributes]'s job, shared with the local driver, so this does
// nothing else.
func runMetricTags(workflowName, outcome, errorType string) map[string]string {
	bounded := metricschema.Attributes(v1.RunMetricAttributes(workflowName, metricschema.DriverDurable, outcome, errorType)...)

	tags := make(map[string]string, len(bounded))
	for _, attr := range bounded {
		tags[string(attr.Key)] = attr.Value.AsString()
	}

	return tags
}

// recordRunStart counts one run beginning, on the durable driver.
//
// Guarded on [workflow.Info.ContinuedExecutionRunID] being empty: that field is
// Temporal's own answer to "is this execution a Continue-As-New continuation of
// an earlier one", non-empty precisely when it is, and reading it is as
// replay-safe as [workflow.GetInfo] itself already is throughout this package
// (see [runAddress]'s doc). Without the guard a workload that continues five
// times would report five starts for one submission — see
// [metricschema.InstrumentRunStarts]'s doc for why that undercounts nothing and
// overcounts everything.
func recordRunStart(ctx workflow.Context, workflowName string) {
	if workflow.GetInfo(ctx).ContinuedExecutionRunID != "" {
		return
	}

	starts, _, _ := runMetrics(ctx, runMetricTags(workflowName, "", ""))
	starts(1)
}

// recordRunCompletion records one run's terminal outcome and its duration, on
// the durable driver — the [v1.RecordRunExecution] this driver cannot call
// directly, for [runMetrics]'s reason.
//
// Guarded on the error not being a Continue-As-New handover
// ([workflow.IsContinueAsNewError]): that is not a completion, it is this
// segment handing the run to its next one, and counting it as one would make
// one submission look like several runs — the completion-side half of the
// same argument [recordRunStart] makes about starts. A cancellation is not
// excluded, deliberately: it is a real terminal outcome, and
// [recordedStepKind] already gives it its own [v1.ErrorKind].
//
// errorType is classified by [recordedStepKind] rather than by
// [v1.ClassifyError] directly, because by the time [Run] calls this, err has
// already been through [classifyRunError]: an [*ErrRunFailed] is by then a
// [temporal.ApplicationError] whose Type carries the original [v1.ErrorKind],
// and [recordedStepKind] is the function that already knows how to read that
// back out — the same classifier the transcript uses for a step's own
// recorded failure, so a run's terminal classification and its last step's
// are never in tension. Empty for a success. This driver has no panic path of
// its own to classify here, unlike [v1.RecordRunExecution]'s local half: a
// workflow function that panics is recovered by Temporal's own dispatcher
// into a failed workflow task, which this function never observes because
// [Run] does not return through it.
func recordRunCompletion(ctx workflow.Context, workflowName string, err error, elapsed time.Duration) {
	if workflow.IsContinueAsNewError(err) {
		return
	}

	errorType := ""
	outcome := metricschema.OutcomeSuccess
	if err != nil {
		outcome = metricschema.OutcomeError
		errorType = recordedStepKind(err).String()
	}

	_, executions, duration := runMetrics(ctx, runMetricTags(workflowName, outcome, errorType))
	executions(1)
	duration(elapsed)
}
