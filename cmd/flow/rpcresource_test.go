package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// bearerPolicy is a trust policy with a kind: oidc entry — a deployment that
// does have an audience to bind Connect RPC to.
func bearerPolicy(audiences ...string) *auth.Policy {
	return &auth.Policy{Issuers: []auth.TrustedIssuer{{
		Name: "idp", Issuer: "https://idp.example.com", Audiences: audiences,
	}}}
}

func TestResolveRPCResourceMakesStrictBindingTheDefault(t *testing.T) {
	t.Parallel()
	policy := bearerPolicy("https://flowstate.example.com/rpc")
	authCfg := authFlags{policyPath: "policy.yaml"}

	_, err := resolveRPCResource(rpcResourceFlags{}, authCfg, policy)
	require.ErrorContains(t, err, "--rpc-resource")
	require.ErrorContains(t, err, "policy.yaml", "the refusal should name the policy that made it required")

	resource, err := resolveRPCResource(rpcResourceFlags{resource: "https://flowstate.example.com/rpc"}, authCfg, policy)
	require.NoError(t, err)
	require.Equal(t, "https://flowstate.example.com/rpc", resource)

	resource, err = resolveRPCResource(rpcResourceFlags{allowIssuerWideAudiences: true}, authCfg, policy)
	require.NoError(t, err)
	require.Empty(t, resource, "the migration flag deliberately restores the unnarrowed authenticator")
}

func TestResolveRPCResourceRefusesInvalidOrUnacceptedConfiguration(t *testing.T) {
	t.Parallel()
	policy := bearerPolicy("https://flowstate.example.com/mcp")
	authCfg := authFlags{policyPath: "policy.yaml"}
	for _, resource := range []string{"flowstate-rpc", "https://flowstate.example.com/rpc/", "https://flowstate.example.com/rpc"} {
		_, err := resolveRPCResource(rpcResourceFlags{resource: resource}, authCfg, policy)
		require.Error(t, err, resource)
	}
}

// TestResolveRPCResourceRequiresAResourceOnlyWhereOneCouldBeChecked walks both
// directions of the requirement, because a fail-closed default that cannot be
// satisfied is not a default, it is an unstartable server.
//
// The refusing direction is the hole: an issuer that mints bearer tokens, no
// RPC audience named, and a trust policy entry whose audience list is
// therefore the whole of the check — which is how a token minted for the MCP
// surface gets spent on Connect RPC.
//
// The admitting direction is a deployment with no bearer path at all. A
// kind: mtls entry admits a caller by client certificate and
// [auth.TrustedIssuer]'s validation refuses it an `audiences` list outright,
// so there is no string an operator could pass --rpc-resource that would ever
// validate: requiring one would make every certificate-only deployment
// unstartable except through the migration flag, which is a flag for
// migrating away from a behaviour these deployments never had. Reported by
// Copilot on picatz/flowstate#1007.
func TestResolveRPCResourceRequiresAResourceOnlyWhereOneCouldBeChecked(t *testing.T) {
	t.Parallel()

	authCfg := authFlags{policyPath: "policy.yaml"}
	certificateOnly := mtlsPolicy(testClientCAFile(t))
	bearer := oidcOnlyPolicy()

	// Both are policies this tree would really load, so neither direction is
	// a claim about a shape that cannot exist.
	require.NoError(t, certificateOnly.Validate())
	require.NoError(t, bearer.Validate())

	t.Run("a certificate-only deployment starts without naming a resource", func(t *testing.T) {
		resource, err := resolveRPCResource(rpcResourceFlags{}, authCfg, certificateOnly)
		require.NoError(t, err, "an mTLS-only deployment was refused for not naming an audience nothing would check")
		require.Empty(t, resource)
	})

	t.Run("a bearer deployment is refused without one", func(t *testing.T) {
		_, err := resolveRPCResource(rpcResourceFlags{}, authCfg, bearer)
		require.ErrorContains(t, err, "--rpc-resource",
			"an issuer that mints bearer tokens with no RPC audience named is the hole this flag closes")
	})

	// And the flags are refused rather than ignored where they cannot take
	// effect, so an operator is never left believing an audience is enforced
	// on a surface that has none.
	t.Run("a certificate-only deployment refuses the flags", func(t *testing.T) {
		for _, flags := range []rpcResourceFlags{
			{resource: "https://flowstate.example.com/rpc"},
			{allowIssuerWideAudiences: true},
		} {
			_, err := resolveRPCResource(flags, authCfg, certificateOnly)
			require.ErrorContains(t, err, "kind: oidc", "%+v", flags)
		}
	})

	t.Run("an insecure deployment refuses the flags", func(t *testing.T) {
		insecure := authFlags{insecure: true}
		resource, err := resolveRPCResource(rpcResourceFlags{}, insecure, nil)
		require.NoError(t, err)
		require.Empty(t, resource)

		_, err = resolveRPCResource(rpcResourceFlags{resource: "https://flowstate.example.com/rpc"}, insecure, nil)
		require.ErrorContains(t, err, "--insecure-no-auth")
	})
}
