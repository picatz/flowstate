package flowstatev1

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// executionOperation is deliberately private. Callers choose one of the
// operation-specific constructors below instead of supplying vocabulary that
// becomes a span name.
type executionOperation uint8

const (
	executionOperationRun executionOperation = iota + 1
	executionOperationStep
	executionOperationAttempt
	executionOperationWait
	executionOperationCompensation
)

const (
	runSpanName          = "flowstate.run"
	stepSpanName         = "flowstate.step"
	attemptSpanName      = "flowstate.attempt"
	waitSpanName         = "flowstate.wait"
	compensationSpanName = "flowstate.compensation"
)

// Execution-span attribute keys are kept together so an operation-specific
// constructor cannot grow a second spelling of an identifier.
const (
	SpanAttributeWaitID         = "flowstate.wait.id"
	SpanAttributeCompensationID = "flowstate.compensation.id"
)

// ExecutionOutcome is the closed outcome vocabulary used by execution
// telemetry. It intentionally contains classifications, never error text.
type ExecutionOutcome string

const (
	ExecutionOutcomeSuccess ExecutionOutcome = "success"
	ExecutionOutcomeError   ExecutionOutcome = "error"
)

func executionSpanName(operation executionOperation) (string, bool) {
	switch operation {
	case executionOperationRun:
		return runSpanName, true
	case executionOperationStep:
		return stepSpanName, true
	case executionOperationAttempt:
		return attemptSpanName, true
	case executionOperationWait:
		return waitSpanName, true
	case executionOperationCompensation:
		return compensationSpanName, true
	default:
		return "", false
	}
}

func startExecutionSpan(ctx context.Context, operation executionOperation, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	name, ok := executionSpanName(operation)
	if !ok {
		// This path is unreachable through the exported API. A fresh no-op span
		// ensures even an accidental internal unknown value cannot produce a
		// caller-controlled name or hand the caller its parent span to end.
		return trace.NewNoopTracerProvider().Tracer(taskTracerName).Start(ctx, "")
	}

	ctx, span := otel.GetTracerProvider().Tracer(taskTracerName).Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindInternal))
	if span.IsRecording() {
		span.SetAttributes(attrs...)
	}
	return ctx, span
}

// StartStepSpan opens a span for one step. A step has only its schema id at
// this boundary; task names and values belong to task instrumentation.
func StartStepSpan(ctx context.Context, stepID string) (context.Context, trace.Span) {
	return startExecutionSpan(ctx, executionOperationStep,
		attribute.String(SpanAttributeStepID, stepID))
}

// StartAttemptSpan opens a span for one attempt of a step.
func StartAttemptSpan(ctx context.Context, stepID string, attempt int) (context.Context, trace.Span) {
	return startExecutionSpan(ctx, executionOperationAttempt,
		attribute.String(SpanAttributeStepID, stepID),
		attribute.Int(SpanAttributeAttempt, attempt))
}

// StartWaitSpan opens a span for one named wait belonging to a step.
func StartWaitSpan(ctx context.Context, stepID, waitID string) (context.Context, trace.Span) {
	return startExecutionSpan(ctx, executionOperationWait,
		attribute.String(SpanAttributeStepID, stepID),
		attribute.String(SpanAttributeWaitID, waitID))
}

// StartCompensationSpan opens a span for one compensation belonging to a step.
func StartCompensationSpan(ctx context.Context, stepID, compensationID string) (context.Context, trace.Span) {
	return startExecutionSpan(ctx, executionOperationCompensation,
		attribute.String(SpanAttributeStepID, stepID),
		attribute.String(SpanAttributeCompensationID, compensationID))
}
