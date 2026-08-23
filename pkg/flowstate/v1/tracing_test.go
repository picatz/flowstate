package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	shared "github.com/picatz/flowstate/pkg/flowstate/v1/tests"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestLocalTraceAgreesWithExecutionModel(t *testing.T) {
	recorder := executionRecorder(t)
	ctx, run := v1.StartRunSpan(t.Context())
	_, err := v1.Run(ctx, shared.TraceWorkflow())
	run.End()
	require.NoError(t, err)

	shared.AssertTraceOperations(t, localTraceOperations(recorder))
}

func TestLocalExecutionWithoutARootCreatesNoSpans(t *testing.T) {
	recorder := executionRecorder(t)
	_, err := v1.Run(t.Context(), shared.TraceWorkflow())
	require.NoError(t, err)
	require.Empty(t, recorder.Ended(),
		"an unconfigured embedding paid for execution spans without installing a run root")
}

func TestLocalCompensationTraceAgreesWithExecutionModel(t *testing.T) {
	recorder := executionRecorder(t)
	ctx, run := v1.StartRunSpan(t.Context())
	_, err := v1.Run(ctx, shared.TraceCompensationWorkflow())
	run.End()
	require.Error(t, err)
	shared.AssertCompensationTrace(t, localTraceOperations(recorder))
}

func localTraceOperations(recorder *tracetest.SpanRecorder) []shared.TraceOperation {
	var operations []shared.TraceOperation
	for _, span := range recorder.Ended() {
		op := shared.TraceOperation{Name: span.Name()}
		for _, attr := range span.Attributes() {
			switch string(attr.Key) {
			case "flowstate.step.id":
				op.StepID = attr.Value.AsString()
			case "flowstate.attempt":
				op.Attempt = attr.Value.AsInt64()
			}
		}
		operations = append(operations, op)
	}
	return operations
}
