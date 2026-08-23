package flowstatev1_test

import (
	"context"
	"fmt"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func executionRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		require.NoError(t, provider.Shutdown(context.Background()))
	})
	return recorder
}

// TestExecutionSpanNamesAreClosed pins the complete name vocabulary. Callers
// cannot supply an operation string, so an input or URL cannot become a name.
func TestExecutionSpanNamesAreClosed(t *testing.T) {
	recorder := executionRecorder(t)
	node := &v1.Node{Id: "publish", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}

	ctx, run := v1.StartRunSpan(t.Context())
	ctx, step := v1.StartStepSpan(ctx, node)
	_, attempt := v1.StartAttemptSpan(ctx, "publish", "log", 2)
	_, wait := v1.StartWaitSpan(ctx, "publish")
	_, compensation := v1.StartCompensationSpan(ctx, "publish", "log")
	for _, span := range []interface{ End(...trace.SpanEndOption) }{attempt, wait, compensation, step, run} {
		span.End()
	}

	var names []string
	for _, span := range recorder.Ended() {
		names = append(names, span.Name())
	}
	require.ElementsMatch(t, []string{
		"flowstate.run", "flowstate.step", "flowstate.attempt", "flowstate.wait", "flowstate.compensation",
	}, names)
}

// TestRealExecutionContainsValuesOutsideTelemetry runs the actual evaluator.
// Every forbidden value is supplied to or produced by the task, rather than
// merely placed in the assertion table.
func TestRealExecutionContainsValuesOutsideTelemetry(t *testing.T) {
	recorder := executionRecorder(t)
	const (
		taskName  = "telemetry-containment-task"
		sensitive = "sensitive-input-4917"
		url       = "https://user:pass@unrestricted.invalid/path?token=x"
		celValue  = "evaluated-cel-value-7281"
		failure   = "raw-failure-message-6632"
	)
	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name:    taskName,
		Summary: "Exercise telemetry containment.",
		Inputs:  (&v1.Task_Log_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: (&v1.Task_Log_Outputs{}).ProtoReflect().Descriptor(),
		Fn: func(_ context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			return nil, fmt.Errorf("%s: %s %s %s", failure, sensitive, url, celValue)
		},
	}))
	wf := &v1.Workflow{
		Name: "containment",
		DeclaredInputs: []*v1.InputDeclaration{{
			Name: "private", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true,
		}},
		Steps: []*v1.Node{{Id: "publish", Kind: &v1.Node_Task{Task: &v1.Task{
			Name: taskName,
			Inputs: map[string]*v1.Value{
				"message": v1.NewExpr("inputs.private"),
				"fields": v1.NewExpr(`{"url": "https://user:pass@unrestricted.invalid/path?token=x", ` +
					`"computed": "evaluated-cel-value-" + string(7000 + 281)}`),
			},
		}}}},
	}
	ctx, run := v1.StartRunSpan(t.Context())
	_, err := v1.RunWithInputs(ctx, wf, map[string]*v1.Value{"private": v1.NewLiteral(sensitive)})
	run.End()
	require.Error(t, err)

	rendered := fmt.Sprintf("%#v", tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()))
	for _, forbidden := range []string{sensitive, url, celValue, failure, `inputs.private`} {
		require.NotContains(t, rendered, forbidden)
	}
	require.Contains(t, rendered, "flowstate.step")
	require.Contains(t, rendered, "flowstate.attempt")
}
