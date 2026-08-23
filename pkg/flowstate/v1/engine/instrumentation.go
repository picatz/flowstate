package engine

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.opentelemetry.io/otel/trace"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/workflow"
)

type domainSpanKey struct{}
type domainStepKey struct{}
type activityStepKey struct{}

// workflowSpanParent keeps Flowstate nesting coherent without replacing the
// span Temporal's interceptor stores in its own private context key.
func workflowSpanParent(wctx workflow.Context) context.Context {
	ctx := context.Background()
	if parent, ok := wctx.Value(domainSpanKey{}).(trace.Span); ok {
		return trace.ContextWithSpan(ctx, parent)
	}
	if parent, ok := temporalotel.SpanFromWorkflowContext(wctx); ok {
		return trace.ContextWithSpan(ctx, parent)
	}
	return ctx
}

func startWorkflowStepSpan(wctx workflow.Context, node *v1.Node) (workflow.Context, trace.Span) {
	if workflow.IsReplaying(wctx) {
		return wctx, trace.SpanFromContext(context.Background())
	}
	_, span := v1.StartStepSpan(workflowSpanParent(wctx), node)
	wctx = workflow.WithValue(wctx, domainSpanKey{}, span)
	return workflow.WithValue(wctx, domainStepKey{}, node.GetId()), span
}

func startWorkflowWaitSpan(wctx workflow.Context, stepID string) trace.Span {
	if workflow.IsReplaying(wctx) {
		return trace.SpanFromContext(context.Background())
	}
	_, span := v1.StartWaitSpan(workflowSpanParent(wctx), stepID)
	return span
}

func startWorkflowCompensationSpan(wctx workflow.Context, stepID, taskName string) (workflow.Context, trace.Span) {
	if workflow.IsReplaying(wctx) {
		return wctx, trace.SpanFromContext(context.Background())
	}
	_, span := v1.StartCompensationSpan(workflowSpanParent(wctx), stepID, taskName)
	wctx = workflow.WithValue(wctx, domainSpanKey{}, span)
	return workflow.WithValue(wctx, domainStepKey{}, stepID), span
}
