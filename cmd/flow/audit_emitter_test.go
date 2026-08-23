package main

import (
	"bytes"
	"context"
	"testing"

	flowaudit "github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace"
)

func TestAuditPolicyIsIndependentOfTelemetry(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "")
	t.Setenv("FLOWSTATE_AUDIT_DESTINATION", "stderr")
	p, err := loadAuditPolicy()
	require.NoError(t, err)
	var out bytes.Buffer
	e, shutdown, err := initAudit(t.Context(), p, &out)
	require.NoError(t, err)
	require.NoError(t, e.Emit(t.Context(), flowaudit.NewRecord(nil, "signal", "accepted", "a", "n", "r", "")))
	require.NoError(t, shutdown(context.Background()))
	require.Contains(t, out.String(), `"action":"signal"`)
}

func TestAuditDoesNotConsultTraceSampling(t *testing.T) {
	for _, sampler := range []trace.Sampler{trace.NeverSample(), trace.TraceIDRatioBased(.01), trace.AlwaysSample()} {
		_ = sampler // Audit has no tracer or span processor in its dependency graph.
		var out bytes.Buffer
		e, _, err := initAudit(t.Context(), auditPolicy{destination: "stderr"}, &out)
		require.NoError(t, err)
		for range 7 {
			require.NoError(t, e.Emit(t.Context(), flowaudit.NewRecord(nil, "run", "accepted", "", "", "", "")))
		}
		require.Equal(t, 7, bytes.Count(out.Bytes(), []byte("\n")))
	}
}

func TestRequiredAuditRefusesNoSink(t *testing.T) {
	t.Setenv("FLOWSTATE_AUDIT_DESTINATION", "none")
	t.Setenv("FLOWSTATE_AUDIT_REQUIRED", "true")
	_, err := loadAuditPolicy()
	require.ErrorContains(t, err, "required")
}

type auditFailingWriter struct{}

func (auditFailingWriter) Write([]byte) (int, error) { return 0, context.DeadlineExceeded }
func TestStderrSinkFailureIsReturned(t *testing.T) {
	e, _, err := initAudit(t.Context(), auditPolicy{destination: "stderr", required: true}, auditFailingWriter{})
	require.NoError(t, err)
	require.Error(t, e.Emit(t.Context(), flowaudit.NewRecord(nil, "run", "accepted", "", "", "", "")))
}
