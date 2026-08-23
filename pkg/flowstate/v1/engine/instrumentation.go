package engine

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/activity"
)

// This file is the instrumentation boundary between deterministic workflow code
// and ordinary Go code.
//
// Workflow code must not start or end OpenTelemetry spans. A workflow function is
// evaluated again for every replay, so a defer that ends a span describes an
// evaluation of history, not a durable Flowstate operation, and exports duplicates
// whenever another worker picks the workflow up. The workflow-side callers in
// execute.go and wait.go therefore only schedule Temporal commands. Temporal's
// tracing interceptor owns the workflow and command spans and suppresses replay
// duplicates. Flowstate's domain task span is emitted here, inside the activity,
// where context.Context and a conventional start/end lifetime are valid.
//
// Keeping this boundary activity-side also preserves the trace tree: the context
// passed here already contains Temporal's RunActivity span, so the Flowstate task
// span is its child rather than a replacement workflow root. Waits deliberately
// acquire no first-party span: Temporal owns their durable timer/signal lifecycle,
// and manufacturing one in runWait would create it once per replay.

// observeTask runs one activity's work inside the task observation.
//
// It takes the work rather than returning an end function so a panic is classified
// by the shared observer before Temporal reports it. Every durable caller is an
// activity entry point; workflow-side code must never call this helper.
func observeTask(ctx context.Context, task *v1.Task, stepID string, run func(context.Context, trace.Span) (*v1.Node_Outputs, error)) (*v1.Node_Outputs, error) {
	return v1.ObserveTask(ctx, task, stepID, metricschema.DriverDurable,
		func(ctx context.Context, span trace.Span) (*v1.Node_Outputs, error) {
			// Attempt is substrate-owned and only meaningful in an activity. A retry
			// is a distinct operation attempt and consequently a distinct span.
			if span.IsRecording() && activity.IsActivity(ctx) {
				span.SetAttributes(attribute.Int(v1.SpanAttributeAttempt, int(activity.GetInfo(ctx).Attempt)))
			}

			return run(ctx, span)
		})
}
