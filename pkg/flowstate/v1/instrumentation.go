package flowstatev1

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// InstrumentationScope is the stable scope used by execution-domain spans.
const InstrumentationScope = "github.com/picatz/flowstate/pkg/flowstate/v1"

// executionSpan is deliberately closed. Evaluated values, URLs, CEL source and
// errors have no place in this type, and operation names cannot be caller input.
type executionSpan struct {
	operation string
	stepID    string
	stepKind  string
	taskName  string
	attempt   int
}

type executionStepKey struct{}

func startExecutionSpan(ctx context.Context, spec executionSpan) (context.Context, trace.Span) {
	ctx, span := otel.GetTracerProvider().Tracer(InstrumentationScope).Start(ctx,
		"flowstate."+spec.operation, trace.WithSpanKind(trace.SpanKindInternal))
	if !span.IsRecording() {
		return ctx, span
	}
	attrs := []attribute.KeyValue{attribute.String("flowstate.operation", spec.operation)}
	if spec.stepID != "" {
		attrs = append(attrs, attribute.String("flowstate.step.id", spec.stepID))
	}
	if spec.stepKind != "" {
		attrs = append(attrs, attribute.String("flowstate.step.kind", spec.stepKind))
	}
	if spec.taskName != "" {
		attrs = append(attrs, attribute.String("flowstate.task.name", spec.taskName))
	}
	if spec.attempt > 0 {
		attrs = append(attrs, attribute.Int("flowstate.attempt", spec.attempt))
	}
	span.SetAttributes(attrs...)
	return ctx, span
}

func startChildExecutionSpan(ctx context.Context, spec executionSpan) (context.Context, trace.Span) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx, trace.SpanFromContext(ctx)
	}
	return startExecutionSpan(ctx, spec)
}

// StartRunSpan starts the stable run root. A caller that promises a zero-cost
// disabled path must check its telemetry configuration before calling it.
func StartRunSpan(ctx context.Context) (context.Context, trace.Span) {
	return startExecutionSpan(ctx, executionSpan{operation: "run"})
}

// StartStepSpan starts one logical DSL step.
func StartStepSpan(ctx context.Context, node *Node) (context.Context, trace.Span) {
	ctx, span := startChildExecutionSpan(ctx, executionSpan{
		operation: "step", stepID: node.GetId(), stepKind: nodeKind(node),
	})
	stepCtx := ctx
	ctx = ContextWithLogContext(ctx, stepCtx)
	return context.WithValue(ctx, executionStepKey{}, node.GetId()), span
}

func executionStepID(ctx context.Context) string {
	stepID, _ := ctx.Value(executionStepKey{}).(string)
	return stepID
}

// StartAttemptSpan starts one task attempt. Only schema-public identifiers cross
// this boundary; task inputs and outputs cannot.
func StartAttemptSpan(ctx context.Context, stepID, taskName string, attempt int) (context.Context, trace.Span) {
	return startChildExecutionSpan(ctx, executionSpan{
		operation: "attempt", stepID: stepID, taskName: taskName, attempt: attempt,
	})
}

// StartWaitSpan starts one wait operation.
func StartWaitSpan(ctx context.Context, stepID string) (context.Context, trace.Span) {
	return startChildExecutionSpan(ctx, executionSpan{
		operation: "wait", stepID: stepID, stepKind: "wait",
	})
}

// StartCompensationSpan starts one compensation operation.
func StartCompensationSpan(ctx context.Context, stepID, taskName string) (context.Context, trace.Span) {
	ctx, span := startChildExecutionSpan(ctx, executionSpan{
		operation: "compensation", stepID: stepID, taskName: taskName,
	})
	return context.WithValue(ctx, executionStepKey{}, stepID), span
}

// RecordExecutionOutcome records only the public error classification. Raw
// messages are intentionally neither status descriptions nor exception events.
func RecordExecutionOutcome(span trace.Span, err error) {
	if err == nil {
		return // OTel leaves successful internal operations unset by convention.
	}
	span.SetStatus(codes.Error, ClassifyError(err).String())
}

func nodeKind(node *Node) string {
	switch node.GetKind().(type) {
	case *Node_Task:
		return "task"
	case *Node_ForEach:
		return "for_each"
	case *Node_Loop:
		return "loop"
	case *Node_Parallel:
		return "parallel"
	case *Node_Wait:
		return "wait"
	case *Node_Call:
		return "call"
	default:
		return "unknown"
	}
}
