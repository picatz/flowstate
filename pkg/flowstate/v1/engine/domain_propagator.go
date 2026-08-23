package engine

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/workflow"
)

const (
	stepTraceHeader = "flowstate-step-traceparent"
	stepIDHeader    = "flowstate-step-id"
)

// DomainTracePropagator carries only the current Flowstate step span to an
// activity's log context. Temporal's tracing interceptor owns its independent
// runtime header and remains the activity/attempt parent.
func DomainTracePropagator() workflow.ContextPropagator { return domainTracePropagator{} }

type domainTracePropagator struct{}

func (domainTracePropagator) Inject(context.Context, workflow.HeaderWriter) error { return nil }

func (domainTracePropagator) Extract(ctx context.Context, reader workflow.HeaderReader) (context.Context, error) {
	if payload, ok := reader.Get(stepIDHeader); ok {
		ctx = context.WithValue(ctx, activityStepKey{}, string(payload.GetData()))
	}
	payload, ok := reader.Get(stepTraceHeader)
	if !ok || len(payload.GetData()) != 49 {
		return ctx, nil
	}
	value := payload.GetData()
	traceID, traceErr := trace.TraceIDFromHex(string(value[:32]))
	spanID, spanErr := trace.SpanIDFromHex(string(value[32:48]))
	if traceErr != nil || spanErr != nil {
		return ctx, nil
	}
	step := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.TraceFlags(value[48]), Remote: true,
	})
	return v1.ContextWithLogContext(ctx, trace.ContextWithRemoteSpanContext(ctx, step)), nil
}

func (domainTracePropagator) InjectFromWorkflow(ctx workflow.Context, writer workflow.HeaderWriter) error {
	span, ok := ctx.Value(domainSpanKey{}).(trace.Span)
	if !ok || !span.SpanContext().IsValid() {
		return nil
	}
	sc := span.SpanContext()
	value := append([]byte(sc.TraceID().String()+sc.SpanID().String()), byte(sc.TraceFlags()))
	writer.Set(stepTraceHeader, &commonpb.Payload{Data: value})
	if stepID, ok := ctx.Value(domainStepKey{}).(string); ok && stepID != "" {
		writer.Set(stepIDHeader, &commonpb.Payload{Data: []byte(stepID)})
	}
	return nil
}

func (domainTracePropagator) ExtractToWorkflow(ctx workflow.Context, _ workflow.HeaderReader) (workflow.Context, error) {
	return ctx, nil
}
