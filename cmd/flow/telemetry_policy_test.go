package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

func TestTelemetryPolicyEnvironmentAndInvalidValues(t *testing.T) {
	t.Setenv("FLOWSTATE_TELEMETRY_BAGGAGE_ALLOWED_KEYS", "request.id, region")
	t.Setenv("FLOWSTATE_TELEMETRY_BAGGAGE_MAX_KEYS", "2")
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")
	p, err := loadTelemetryPolicy()
	require.NoError(t, err)
	require.Contains(t, p.allowedBaggage, "request.id")
	require.Equal(t, 2, p.maxKeys)

	t.Setenv("FLOWSTATE_TELEMETRY_BAGGAGE_MAX_KEYS", "0")
	_, err = loadTelemetryPolicy()
	require.ErrorContains(t, err, "positive integer")
	t.Setenv("FLOWSTATE_TELEMETRY_BAGGAGE_MAX_KEYS", "2")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "2")
	_, err = loadTelemetryPolicy()
	require.ErrorContains(t, err, "0 to 1")
}

func TestBaggagePolicyDropsUnknownSensitiveAndOversized(t *testing.T) {
	p := telemetryPolicy{allowedBaggage: csvSet("request.id,token,long"), redact: csvSet("token"), maxKeys: 2, maxKeyLen: 32, maxValueLen: 4, maxEncodedBytes: 20}
	in, err := baggage.Parse("request.id=good,unknown=no,token=hide,long=oversized")
	require.NoError(t, err)
	out := p.filterBaggage(in)
	require.Equal(t, "good", out.Member("request.id").Value())
	require.Empty(t, out.Member("unknown").Value())
	require.Empty(t, out.Member("token").Value())
	require.Empty(t, out.Member("long").Value())
}

func TestPolicyPropagatorPreservesTraceHeaderAndFiltersEveryHop(t *testing.T) {
	p := telemetryPolicy{allowedBaggage: csvSet("request.id"), redact: csvSet("token"), maxKeys: 4, maxKeyLen: 64, maxValueLen: 64, maxEncodedBytes: 256}
	prop := policyPropagator{delegate: propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}), policy: p}
	carrier := propagation.MapCarrier{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "baggage": "request.id=ok,tenant=forged"}
	ctx := prop.Extract(context.Background(), carrier)
	require.Equal(t, "ok", baggage.FromContext(ctx).Member("request.id").Value())
	require.Empty(t, baggage.FromContext(ctx).Member("tenant").Value())
	out := propagation.MapCarrier{}
	prop.Inject(ctx, out)
	require.Equal(t, carrier["traceparent"], out["traceparent"])
	require.Equal(t, "request.id=ok", out["baggage"])
}
