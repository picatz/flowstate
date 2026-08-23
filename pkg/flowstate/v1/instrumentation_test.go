package flowstatev1

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestExecutionOperationsHaveStableNamesAndClosedAttributes(t *testing.T) {
	tests := []struct {
		name         string
		operation    executionOperation
		start        func(context.Context) (context.Context, trace.Span)
		wantName     string
		wantAttrKeys []string
	}{
		{
			name: "run", operation: executionOperationRun,
			start: func(ctx context.Context) (context.Context, trace.Span) {
				return StartRunSpan(ctx, &Workflow{Name: "caller-controlled"})
			},
			wantName: runSpanName, wantAttrKeys: []string{SpanAttributeWorkflowName},
		},
		{
			name: "step", operation: executionOperationStep,
			start: func(ctx context.Context) (context.Context, trace.Span) {
				return StartStepSpan(ctx, "step-id")
			},
			wantName: stepSpanName, wantAttrKeys: []string{SpanAttributeStepID},
		},
		{
			name: "attempt", operation: executionOperationAttempt,
			start: func(ctx context.Context) (context.Context, trace.Span) {
				return StartAttemptSpan(ctx, "step-id", 2)
			},
			wantName:     attemptSpanName,
			wantAttrKeys: []string{SpanAttributeAttempt, SpanAttributeStepID},
		},
		{
			name: "wait", operation: executionOperationWait,
			start: func(ctx context.Context) (context.Context, trace.Span) {
				return StartWaitSpan(ctx, "step-id", "wait-id")
			},
			wantName:     waitSpanName,
			wantAttrKeys: []string{SpanAttributeStepID, SpanAttributeWaitID},
		},
		{
			name: "compensation", operation: executionOperationCompensation,
			start: func(ctx context.Context) (context.Context, trace.Span) {
				return StartCompensationSpan(ctx, "step-id", "compensation-id")
			},
			wantName:     compensationSpanName,
			wantAttrKeys: []string{SpanAttributeCompensationID, SpanAttributeStepID},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			name, ok := executionSpanName(tc.operation)
			require.True(t, ok)
			require.Equal(t, tc.wantName, name)

			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			previous := otel.GetTracerProvider()
			otel.SetTracerProvider(provider)
			t.Cleanup(func() {
				otel.SetTracerProvider(previous)
				require.NoError(t, provider.Shutdown(context.Background()))
			})

			_, span := tc.start(t.Context())
			span.End()
			stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())
			require.Len(t, stubs, 1)
			require.Equal(t, tc.wantName, stubs[0].Name)

			keys := make([]string, 0, len(stubs[0].Attributes))
			for _, attr := range stubs[0].Attributes {
				keys = append(keys, string(attr.Key))
			}
			sort.Strings(keys)
			sort.Strings(tc.wantAttrKeys)
			require.Equal(t, tc.wantAttrKeys, keys)
		})
	}
}

func TestUnknownExecutionOperationHasNoSpanName(t *testing.T) {
	name, ok := executionSpanName(executionOperation(255))
	require.False(t, ok)
	require.Empty(t, name)

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	_, span := startExecutionSpan(t.Context(), executionOperation(255))
	span.End()
	require.Empty(t, recorder.Ended(), "an unknown operation generated a span")
}
