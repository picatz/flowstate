package flowstatev1

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Stable logical operation names.  These names describe Flowstate's execution
// model; transport implementations may insert their own spans between them.
const (
	TraceOperationRun          = "flowstate.run"
	TraceOperationStep         = "flowstate.step"
	TraceOperationAttempt      = "flowstate.attempt"
	TraceOperationWait         = "flowstate.wait"
	TraceOperationCompensation = "flowstate.compensation"
	SpanAttributeOperation     = "flowstate.operation"
)

func startLogicalSpan(ctx context.Context, operation, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	attrs = append(attrs, attribute.String(SpanAttributeOperation, operation))
	return otel.GetTracerProvider().Tracer(taskTracerName).Start(ctx, name, trace.WithAttributes(attrs...))
}

// StartStepSpan opens the logical step operation which owns attempts and waits.
func StartStepSpan(ctx context.Context, stepID string) (context.Context, trace.Span) {
	return startLogicalSpan(ctx, TraceOperationStep, "flowstate.step/"+boundedSpanName(stepID, MaxTaskNameLen),
		attribute.String(SpanAttributeStepID, stepID))
}

// StartWaitSpan opens the logical wait operation for a step.
func StartWaitSpan(ctx context.Context, stepID string) (context.Context, trace.Span) {
	return startLogicalSpan(ctx, TraceOperationWait, "flowstate.wait/"+boundedSpanName(stepID, MaxTaskNameLen),
		attribute.String(SpanAttributeStepID, stepID))
}

// StartCompensationSpan opens the logical operation which undoes a step.
func StartCompensationSpan(ctx context.Context, stepID string) (context.Context, trace.Span) {
	return startLogicalSpan(ctx, TraceOperationCompensation, "flowstate.compensation/"+boundedSpanName(stepID, MaxTaskNameLen),
		attribute.String(SpanAttributeStepID, stepID))
}
