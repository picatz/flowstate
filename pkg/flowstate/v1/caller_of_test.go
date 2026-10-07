package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

func TestCallerOf(t *testing.T) {
	require.Zero(t, v1.CallerOf(nil).Normalized().Kind)
	require.NotNil(t, v1.CallerOf(nil).Claims, "nil identity renders non-nil containers")
	require.NotNil(t, v1.CallerOf(nil).Actions)
	require.Empty(t, v1.CallerOf(nil).Principal)

	got := v1.CallerOf(&v1.WorkloadIdentity{
		Issuer: "https://idp", Subject: "ci", Namespace: "team-a",
		Claims:        map[string]string{"repo": "x/y"},
		PrincipalKind: v1.PrincipalKind_PRINCIPAL_KIND_AGENT,
	})
	require.Equal(t, "agent", got.Kind)
	require.Equal(t, "https://idp#ci", got.Principal)
	require.Equal(t, "team-a", got.Namespace)
	require.Equal(t, map[string]string{"repo": "x/y"}, got.Claims)
	require.Empty(t, got.Actions)

	// No kind assigned renders empty, never "workload".
	require.Empty(t, v1.CallerOf(&v1.WorkloadIdentity{Subject: "s"}).Kind)
}

// TestHTTPTaskEgressRuleKeyedOnIdentityKind runs an http step whose egress
// policy admits only workload callers, proving the task renders the run's
// principal kind into the rule's `identity`: the workload run egresses, the
// human run and the run that names nobody are refused.
//
// No t.Parallel: it swaps the process-wide http task registration, as
// TestRunWorkflowEgressIdentity does.
func TestHTTPTaskEgressRuleKeyedOnIdentityKind(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithAllowRules(`identity.kind == "workload"`))
	require.NoError(t, err)

	registry := v1.DefaultRegistry()
	original, existed := registry.Lookup("http")
	require.NoError(t, registry.Replace(v1.HTTPTaskDef(policy)))
	t.Cleanup(func() {
		if existed {
			_ = registry.Replace(original)
		}
	})

	for _, tc := range []conformance.EgressIdentityCase{
		{Name: "workload egresses", Identity: &v1.WorkloadIdentity{Subject: "ci", PrincipalKind: v1.PrincipalKind_PRINCIPAL_KIND_WORKLOAD}},
		{Name: "human is refused", Identity: &v1.WorkloadIdentity{Subject: "kent", PrincipalKind: v1.PrincipalKind_PRINCIPAL_KIND_HUMAN}, Denied: true},
		{Name: "no kind is refused", Identity: &v1.WorkloadIdentity{Subject: "ci"}, Denied: true},
		{Name: "no identity is refused", Denied: true},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := v1.NewContextWithRehearsalIdentity(t.Context(), tc.Identity)
			out, err := v1.Run(ctx, conformance.EgressIdentityWorkflow(baseURL))
			conformance.AssertEgressIdentityOutcome(t, tc, out, err)
		})
	}
}
