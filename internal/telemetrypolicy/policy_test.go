package telemetrypolicy

import (
	"strings"
	"testing"
	"unicode/utf8"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

func signal(kind v1.TelemetrySignalKind) *v1.TelemetrySignalPolicy {
	return &v1.TelemetrySignalPolicy{Kind: kind, AllowedKeys: []string{"safe", "token", "api_key", "tenant", "message"}, MaxValueBytes: 64}
}

func TestNormalizeEverySignalAndSensitiveAlias(t *testing.T) {
	for _, kind := range []v1.TelemetrySignalKind{
		v1.TelemetrySignalKind_TELEMETRY_SIGNAL_KIND_LOG, v1.TelemetrySignalKind_TELEMETRY_SIGNAL_KIND_SPAN,
		v1.TelemetrySignalKind_TELEMETRY_SIGNAL_KIND_EXECUTION_EVENT, v1.TelemetrySignalKind_TELEMETRY_SIGNAL_KIND_AUDIT_RECORD,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			policy, err := New(&v1.TelemetryPolicy{Signals: []*v1.TelemetrySignalPolicy{signal(kind)}, RedactedKeys: []string{"token", "api_key"}})
			require.NoError(t, err)
			require.Equal(t, []Field{{"safe", "yes"}}, policy.Normalize(kind,
				Field{"safe", "yes"}, Field{"unknown", "no"}, Field{"token", "secret"}, Field{"api_key", "secret"}))
		})
	}
}

func TestCELProvidesHigherOrderAllowAndRedaction(t *testing.T) {
	cfg := signal(v1.TelemetrySignalKind_TELEMETRY_SIGNAL_KIND_AUDIT_RECORD)
	cfg.AllowIf = []string{`attributes["tenant"] == "verified"`}
	cfg.RedactIf = []string{`key == "message" && value.contains("secret")`}
	policy, err := New(&v1.TelemetryPolicy{Signals: []*v1.TelemetrySignalPolicy{cfg}})
	require.NoError(t, err)
	require.Equal(t, []Field{{"safe", "yes"}, {"tenant", "verified"}}, policy.Normalize(cfg.Kind,
		Field{"tenant", "verified"}, Field{"safe", "yes"}, Field{"message", "a secret"}))
}

func TestNormalizeBoundsUnicodeAndUsesDistinctMarkers(t *testing.T) {
	cfg := signal(v1.TelemetrySignalKind_TELEMETRY_SIGNAL_KIND_LOG)
	cfg.MaxValueBytes = 32
	policy, err := New(&v1.TelemetryPolicy{Signals: []*v1.TelemetrySignalPolicy{cfg}})
	require.NoError(t, err)
	first := policy.Normalize(cfg.Kind, Field{"message", strings.Repeat("界", 20)})[0].Value
	second := policy.Normalize(cfg.Kind, Field{"message", strings.Repeat("界", 19) + "海"})[0].Value
	require.LessOrEqual(t, len(first), 32)
	require.True(t, utf8.ValidString(first))
	require.NotEqual(t, first, second)
}

func TestVerifiedTenantWinsAndZeroTelemetryEmitsNothing(t *testing.T) {
	cfg := signal(v1.TelemetrySignalKind_TELEMETRY_SIGNAL_KIND_AUDIT_RECORD)
	policy, err := New(&v1.TelemetryPolicy{Signals: []*v1.TelemetrySignalPolicy{cfg}})
	require.NoError(t, err)
	require.Contains(t, policy.Normalize(cfg.Kind, Field{"tenant", "verified"}, Field{"tenant", "baggage"}), Field{"tenant", "verified"})
	off, err := New(nil)
	require.NoError(t, err)
	require.Empty(t, off.Normalize(cfg.Kind, Field{"tenant", "anything"}))
}

func TestInvalidCELFailsAtConstruction(t *testing.T) {
	cfg := signal(v1.TelemetrySignalKind_TELEMETRY_SIGNAL_KIND_LOG)
	cfg.AllowIf = []string{`value + 1`}
	_, err := New(&v1.TelemetryPolicy{Signals: []*v1.TelemetrySignalPolicy{cfg}})
	require.Error(t, err)
}
