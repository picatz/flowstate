package flowstatev1_test

import (
	"context"
	"net/url"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/execpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// The identities #2426 is about: a team-a caller whose `contractors` claim is
// nested past [auth.MaxCarriedClaimDepth], so it is refused rather than read,
// and the same caller without that claim. A rule that only tests the claim's
// absence admits the second and must not admit the first, on any surface.
func refusedClaimsCaller(t *testing.T, overBound bool) *v1.WorkloadIdentity {
	t.Helper()

	claims := map[string]*structpb.Value{"team": structpb.NewStringValue("platform")}
	if overBound {
		deep := structpb.NewStringValue("contractor")
		for range auth.MaxCarriedClaimDepth + 1 {
			deep = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{deep}})
		}
		claims["contractors"] = deep
	}

	return &v1.WorkloadIdentity{Principal: &v1.Principal{
		Issuer: "https://issuer.example", Subject: "svc", Namespace: "team-a", Claims: claims,
	}}
}

const (
	refusedClaimsAllow = `identity.namespace == "team-a" && !("contractors" in identity.claims)`
	refusedClaimsDeny  = `"contractors" in identity.claims`

	// The operator forms: equality dispatches on the left operand and `!=` reads
	// a non-true answer as true, so these admit a caller whose claims were
	// refused unless the claims value itself errors. Both admit the readable one.
	refusedClaimsNeq = `identity.claims != {}`
	refusedClaimsEq  = `{} == identity.claims`
)

func TestRefusedClaimsAreRefusedByTheSharedCaller(t *testing.T) {
	t.Parallel()

	refused := v1.CallerOf(refusedClaimsCaller(t, true))
	require.Error(t, refused.Claims.Refused())
	require.NoError(t, v1.CallerOf(refusedClaimsCaller(t, false)).Claims.Refused())

	// A normal identity renders exactly as it did: the readable claims, no carrier.
	require.Equal(t, map[string]any{"team": "platform"}, v1.IdentityShape(refusedClaimsCaller(t, false))["claims"])
}

func TestRefusedClaimsDenyOnTheTaskShapeSurface(t *testing.T) {
	t.Parallel()

	for name, cfg := range map[string]v1.TaskPolicyConfig{
		"allow":          {Allow: []string{refusedClaimsAllow}},
		"deny":           {Allow: []string{`true`}, Deny: []string{refusedClaimsDeny}},
		"operator allow": {Allow: []string{refusedClaimsNeq}},
		"operator deny":  {Allow: []string{`true`}, Deny: []string{refusedClaimsEq}},
	} {
		policy, err := cfg.Policy()
		require.NoError(t, err, name)

		require.NoError(t, policy.Check(context.Background(), "log", refusedClaimsCaller(t, false)), name)
		require.Error(t, policy.Check(context.Background(), "log", refusedClaimsCaller(t, true)),
			"%s rule: a refused claim must not read as an absent one", name)
	}
}

func TestRefusedClaimsDenyOnTheSignalPredicateSurface(t *testing.T) {
	t.Parallel()

	for name, src := range map[string]string{
		"allow":              `!("contractors" in sender.identity.claims)`,
		"deny":               `!("contractors" in sender.identity.claims) || sender.identity.namespace == "never"`,
		"operator not equal": `sender.identity.claims != {}`,
		"operator equal":     `!({} == sender.identity.claims)`,
	} {
		policy := &v1.SignalPolicy{Allow: src}

		require.NoError(t, v1.SignalPolicyCheck(context.Background(), policy, refusedClaimsCaller(t, false), nil, false, nil), name)
		require.Error(t, v1.SignalPolicyCheck(context.Background(), policy, refusedClaimsCaller(t, true), nil, false, nil),
			"%s predicate: a refused claim must not read as an absent one", name)
	}
}

func TestRefusedClaimsDenyOnTheEgressSurface(t *testing.T) {
	t.Parallel()

	target := &url.URL{Scheme: "http", Host: "127.0.0.1:1", Path: "/"}

	for name, option := range map[string]netpolicy.Option{
		"allow":          netpolicy.WithAllowRules(refusedClaimsAllow),
		"deny":           netpolicy.WithDenyRules(refusedClaimsDeny),
		"operator allow": netpolicy.WithAllowRules(refusedClaimsNeq),
		"operator deny":  netpolicy.WithDenyRules(refusedClaimsEq),
	} {
		policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), option)
		require.NoError(t, err, name)

		admitted := netpolicy.ContextWithIdentity(t.Context(), v1.CallerOf(refusedClaimsCaller(t, false)))
		require.NoError(t, policy.CheckURL(admitted, "GET", target), name)

		refused := netpolicy.ContextWithIdentity(t.Context(), v1.CallerOf(refusedClaimsCaller(t, true)))
		require.Error(t, policy.CheckURL(refused, "GET", target),
			"%s rule: a refused claim must not read as an absent one", name)
	}
}

func TestRefusedClaimsDenyOnTheExecSurface(t *testing.T) {
	t.Parallel()

	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("sh is not installed: %v", err)
	}
	sh, err = filepath.EvalSymlinks(sh)
	require.NoError(t, err)
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)

	for name, rules := range map[string]struct{ allow, deny []string }{
		"allow":          {allow: []string{refusedClaimsAllow}},
		"deny":           {allow: []string{`true`}, deny: []string{refusedClaimsDeny}},
		"operator allow": {allow: []string{refusedClaimsNeq}},
		"operator deny":  {allow: []string{`true`}, deny: []string{refusedClaimsEq}},
	} {
		policy, err := execpolicy.New(execpolicy.Config{
			Executables: map[string]string{"sh": sh}, Roots: []string{root},
			Timeout: 30 * time.Second, MaxOutputBytes: 64 << 10,
			Allow: rules.allow, Deny: rules.deny,
		})
		require.NoError(t, err, name)

		request := execpolicy.Request{Argv: []string{"sh", "-c", "true"}, Dir: root}

		request.Identity = v1.CallerOf(refusedClaimsCaller(t, false))
		_, err = policy.Check(t.Context(), request)
		require.NoError(t, err, name)

		request.Identity = v1.CallerOf(refusedClaimsCaller(t, true))
		_, err = policy.Check(t.Context(), request)
		require.ErrorIs(t, err, execpolicy.ErrDenied, "%s rule: a refused claim must not read as an absent one", name)
	}
}
