package flowstatev1_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// TestIdentityNamespaceIsPortableAcrossPolicySurfaces is #568's regression test.
//
// #565 fixed one shape of "a name that means two things is worse than two
// names": `identity` and `workload` were split into two objects because they
// named two different principals. #568 is the same defect class one field
// down, on the same name rather than two: `identity.namespace` meant "_default"
// on auth's own surfaces (secret-access and assumption rules, both built from
// [auth]'s shared assumeVars) and the raw empty string everywhere else
// (task-shape policy, egress policy, signal policy, run.identity) — one field,
// one spelling, two encodings depending on which surface evaluated it.
//
// This drives one caller with no attested namespace through the three exported
// policy surfaces that evaluate `identity.namespace` as CEL — auth's secret
// rules, task-shape rules, and egress rules — with a single rule text as the
// only deny rule and `allow: ["true"]` so only the deny rule decides, exactly
// the reproduction the issue itself used. A table so a fifth CEL surface added
// later has somewhere to be added.
//
// Signal policy and run.identity are deliberately not columns here: both read
// WorkloadIdentity.GetNamespace() as a raw Go string compared with `==`, never
// through CEL, so there is no second encoding for them to disagree with in the
// first place — see [auth.WorkloadIdentity], `pkg/flowstate/v1/signalpolicy.go`,
// and `pkg/flowstate/v1/run_identity.go`.
func TestIdentityNamespaceIsPortableAcrossPolicySurfaces(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		rule string
		// want is the decision every surface must agree on for a caller
		// attested with no namespace: true means the deny rule matches (the
		// request is refused), false means it does not (the request the lone
		// allow rule permits goes through).
		want bool
	}{
		{
			name: `identity.namespace == "" matches the unnamespaced caller everywhere`,
			rule: `identity.namespace == ""`,
			want: true,
		},
		{
			name: `identity.namespace == "_default" must not match on any surface`,
			rule: `identity.namespace == "_default"`,
			want: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			authDenied := secretSurfaceDenies(t, test.rule)
			taskDenied := taskSurfaceDenies(t, test.rule)
			netDenied := netSurfaceDenies(t, test.rule)

			if authDenied != test.want {
				t.Errorf("auth secret-access surface: deny rule %q matched=%v, want %v", test.rule, authDenied, test.want)
			}
			if taskDenied != test.want {
				t.Errorf("task-shape surface: deny rule %q matched=%v, want %v", test.rule, taskDenied, test.want)
			}
			if netDenied != test.want {
				t.Errorf("egress surface: deny rule %q matched=%v, want %v", test.rule, netDenied, test.want)
			}
			if authDenied != taskDenied || taskDenied != netDenied {
				t.Errorf("surfaces disagree on %q: auth=%v task=%v net=%v", test.rule, authDenied, taskDenied, netDenied)
			}
		})
	}
}

// secretSurfaceDenies drives rule through auth's secret-access policy, the
// surface #568 found substituting "_default" for an unset namespace.
func secretSurfaceDenies(t *testing.T, rule string) bool {
	t.Helper()

	policy, err := auth.SecretAccessPolicy{
		Allow: []string{"true"},
		Deny:  []string{rule},
	}.Compile()
	require.NoError(t, err)

	identity := auth.WorkloadIdentity{
		Subject: "repo:picatz/flowstate:ref:refs/heads/main",
		Issuer:  "https://token.actions.githubusercontent.com",
		// Namespace deliberately unset: this is the caller #568 is about.
	}
	ref := auth.StepRef{Workflow: "deploy", Run: "run-1", Step: "push"}

	err = policy.Authorize(t.Context(), identity, ref, &v1.SecretRef{Scheme: "env", Name: "x"})
	return err != nil
}

// taskSurfaceDenies drives rule through task-shape policy.
func taskSurfaceDenies(t *testing.T, rule string) bool {
	t.Helper()

	policy, err := v1.TaskPolicyConfig{
		Allow: []string{"true"},
		Deny:  []string{rule},
	}.Policy()
	require.NoError(t, err)

	identity := &v1.WorkloadIdentity{
		Subject: "repo:picatz/flowstate:ref:refs/heads/main",
		Issuer:  "https://token.actions.githubusercontent.com",
		// Namespace deliberately unset.
	}

	err = policy.Check(context.Background(), "log", identity)
	return err != nil
}

// netSurfaceDenies drives rule through egress policy.
func netSurfaceDenies(t *testing.T, rule string) bool {
	t.Helper()

	policy, err := netpolicy.New(
		netpolicy.WithAllowRules("true"),
		netpolicy.WithDenyRules(rule),
	)
	require.NoError(t, err)

	ctx := netpolicy.ContextWithIdentity(context.Background(), netpolicy.Identity{
		Subject: "repo:picatz/flowstate:ref:refs/heads/main",
		Issuer:  "https://token.actions.githubusercontent.com",
		// Namespace deliberately unset.
	})

	u, err := url.Parse("https://api.example.com/v1/things")
	require.NoError(t, err)

	err = policy.CheckURL(ctx, "GET", u)
	return err != nil
}
