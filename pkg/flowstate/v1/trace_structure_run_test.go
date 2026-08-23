package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	flowtests "github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

func TestLocalDriverCanonicalTraceStructure(t *testing.T) {
	recorder := conformance.RecordSpans(t)
	_, err := v1.Run(t.Context(), conformance.TaskSpanWorkflow())
	require.NoError(t, err)
	flowtests.AssertCanonicalTraceStructure(t,
		tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()))
}
