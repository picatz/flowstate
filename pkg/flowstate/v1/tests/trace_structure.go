// Package tests contains assertions shared by the local and durable driver
// suites. It is deliberately expressed only in Flowstate's stable operation
// vocabulary; Temporal transport/runtime spans are transparent edges.
package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// AssertCanonicalTraceStructure checks logical ancestry while allowing spans
// owned by Temporal (or another transport) between logical operations.
// runSpanNames names transport spans which canonically represent a durable run.
func AssertCanonicalTraceStructure(tb testing.TB, spans []tracetest.SpanStub, runSpanNames ...string) {
	tb.Helper()
	byID := make(map[trace.SpanID]tracetest.SpanStub, len(spans))
	runs := make(map[trace.SpanID]bool)
	for _, span := range spans {
		byID[span.SpanContext.SpanID()] = span
		if operation(span) == v1.TraceOperationRun || contains(runSpanNames, span.Name) {
			runs[span.SpanContext.SpanID()] = true
		}
	}
	require.NotEmpty(tb, runs, "trace has no logical Flowstate run")

	seenStep, seenAttempt := false, false
	for _, span := range spans {
		switch operation(span) {
		case v1.TraceOperationStep:
			seenStep = true
			require.True(tb, nearestLogicalParent(span, byID, runs, v1.TraceOperationRun, v1.TraceOperationStep),
				"step %q is not logically descended from its run or enclosing step", span.Name)
		case v1.TraceOperationAttempt:
			seenAttempt = true
			require.True(tb, nearestLogicalParent(span, byID, runs, v1.TraceOperationStep, v1.TraceOperationCompensation),
				"attempt %q is not logically descended from its step or compensation", span.Name)
		case v1.TraceOperationWait:
			require.True(tb, nearestLogicalParent(span, byID, runs, v1.TraceOperationStep),
				"wait %q is not logically descended from its step", span.Name)
		case v1.TraceOperationCompensation:
			require.True(tb, nearestLogicalParent(span, byID, runs, v1.TraceOperationRun),
				"compensation %q is not logically descended from its run", span.Name)
		}
	}
	require.True(tb, seenStep, "trace has no logical Flowstate step")
	require.True(tb, seenAttempt, "trace has no logical Flowstate attempt")
}

func nearestLogicalParent(span tracetest.SpanStub, byID map[trace.SpanID]tracetest.SpanStub, runs map[trace.SpanID]bool, want ...string) bool {
	parentID := span.Parent.SpanID()
	for parentID.IsValid() {
		parent, ok := byID[parentID]
		if !ok {
			return false
		}
		op := operation(parent)
		if runs[parentID] {
			op = v1.TraceOperationRun
		}
		if op != "" {
			return contains(want, op)
		}
		parentID = parent.Parent.SpanID()
	}
	return false
}

func operation(span tracetest.SpanStub) string {
	for _, attr := range span.Attributes {
		if string(attr.Key) == v1.SpanAttributeOperation {
			return attr.Value.AsString()
		}
	}
	return ""
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
