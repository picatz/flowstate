package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestCallerOf(t *testing.T) {
	require.Zero(t, v1.CallerOf(nil).Normalized().Kind)
	require.NotNil(t, v1.CallerOf(nil).Claims.Map(), "nil identity renders non-nil containers")
	require.NotNil(t, v1.CallerOf(nil).Actions)
	require.Empty(t, v1.CallerOf(nil).Principal)

	got := v1.CallerOf(&v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://idp", Subject: "ci", Namespace: "team-a", Claims: v1.StringClaimValues(map[string]string{"repo": "x/y"}), Kind: v1.PrincipalKind_PRINCIPAL_KIND_AGENT}})
	require.Equal(t, "agent", got.Kind)
	require.Equal(t, "https://idp#ci", got.Principal)
	require.Equal(t, "team-a", got.Namespace)
	require.Equal(t, map[string]any{"repo": "x/y"}, got.Claims.Map())
	require.Empty(t, got.Actions)

	// No kind assigned renders empty, never "workload".
	require.Empty(t, v1.CallerOf(&v1.WorkloadIdentity{Principal: &v1.Principal{Subject: "s"}}).Kind)
}
