package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTelemetryPolicyFilterBaggageStripsProperties(t *testing.T) {
	tests := map[string]string{
		"ordinary property":        "tenant=blue;region=west",
		"oversized property value": "tenant=blue;note=" + strings.Repeat("x", maxTelemetryBaggageEncodedBytes+1),
		"many properties":          "tenant=blue" + strings.Repeat(";tag=x", 1000),
		"sensitive property name":  "tenant=blue;authorization=Bearer%20secret",
		"percent encoded property": "tenant=blue;note=secret%3Btoken%3Dvalue",
	}

	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, "tenant=blue", (telemetryPolicy{}).filterBaggage(input))
		})
	}
}

func TestTelemetryPolicyFilterBaggageAppliesPolicyToDecodedMembers(t *testing.T) {
	policy := telemetryPolicy{}

	require.Empty(t, policy.filterBaggage("api%5Fkey=value"),
		"percent encoding must not make an invalid or sensitive key acceptable")
	require.Empty(t, policy.filterBaggage("token=not-secret"),
		"a sensitive field name is refused regardless of its value")
	require.Empty(t, policy.filterBaggage("safe="+strings.Repeat("%41", maxTelemetryBaggageValueBytes+1)),
		"the decoded value is independently bounded")
	require.Equal(t, "safe=%3B%2C=", policy.filterBaggage("safe=%3b%2c%3d"),
		"accepted content is rebuilt in its canonical encoded representation")
}

func TestTelemetryPolicyFilterBaggageDropsMalformedMembers(t *testing.T) {
	got := (telemetryPolicy{}).filterBaggage("good=one,missing-equals,bad=%GG,also=two;broken==%GG")
	require.Equal(t, "good=one", got)
}

func TestTelemetryPolicyFilterBaggageCountsSanitizedEncoding(t *testing.T) {
	// The original member is far over the aggregate limit solely because of
	// properties. Since properties are not part of the retained representation,
	// the exact representation charged to the limit is only "small=value".
	input := "small=value;unused=" + strings.Repeat("x", maxTelemetryBaggageEncodedBytes+1)
	require.Equal(t, "small=value", (telemetryPolicy{}).filterBaggage(input))

	first := "first=" + strings.Repeat("a", maxTelemetryBaggageValueBytes)
	members := []string{first}
	for i := 0; i < 10; i++ {
		members = append(members, "next"+strings.Repeat("x", i)+"="+strings.Repeat("b", maxTelemetryBaggageValueBytes))
	}
	got := (telemetryPolicy{}).filterBaggage(strings.Join(members, ","))
	require.LessOrEqual(t, len(got), maxTelemetryBaggageEncodedBytes)
	require.NotContains(t, got, "nextxxx=", "a member that crosses the exact encoded bound must be dropped")
}
