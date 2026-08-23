package engine

import (
	"context"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func propagatedLogSpan(ctx context.Context) ([]string, error) {
	return []string{
		trace.SpanContextFromContext(v1.LogContextFrom(ctx)).SpanID().String(),
		ctx.Value(activityStepKey{}).(string),
	}, nil
}

func propagationWorkflow(ctx workflow.Context) ([]string, error) {
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}, TraceFlags: trace.FlagsSampled,
	})
	ctx = workflow.WithValue(ctx, domainSpanKey{}, trace.SpanFromContext(trace.ContextWithSpanContext(context.Background(), parent)))
	ctx = workflow.WithValue(ctx, domainStepKey{}, "log-step")
	var got []string
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, defaultActivityOptions()), propagatedLogSpan).Get(ctx, &got)
	return got, err
}

func TestDomainPropagatorCarriesTheStepLogContext(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{DomainTracePropagator()})
	env.RegisterWorkflow(propagationWorkflow)
	env.RegisterActivity(propagatedLogSpan)
	env.ExecuteWorkflow(propagationWorkflow)
	require.NoError(t, env.GetWorkflowError())
	var got []string
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Equal(t, []string{"0102030405060708", "log-step"}, got)
}
