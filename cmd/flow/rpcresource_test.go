package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

func TestResolveRPCResourceMakesStrictBindingTheDefault(t *testing.T) {
	t.Parallel()
	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{{Audiences: []string{"https://flowstate.example.com/rpc"}}}}
	authCfg := authFlags{policyPath: "policy.yaml"}

	_, err := resolveRPCResource(rpcResourceFlags{}, authCfg, policy)
	require.ErrorContains(t, err, "--rpc-resource")

	resource, err := resolveRPCResource(rpcResourceFlags{resource: "https://flowstate.example.com/rpc"}, authCfg, policy)
	require.NoError(t, err)
	require.Equal(t, "https://flowstate.example.com/rpc", resource)

	resource, err = resolveRPCResource(rpcResourceFlags{allowIssuerWideAudiences: true}, authCfg, policy)
	require.NoError(t, err)
	require.Empty(t, resource, "the migration flag deliberately restores the unnarrowed authenticator")
}

func TestResolveRPCResourceRefusesInvalidOrUnacceptedConfiguration(t *testing.T) {
	t.Parallel()
	policy := &auth.Policy{Issuers: []auth.TrustedIssuer{{Audiences: []string{"https://flowstate.example.com/mcp"}}}}
	authCfg := authFlags{policyPath: "policy.yaml"}
	for _, resource := range []string{"flowstate-rpc", "https://flowstate.example.com/rpc/", "https://flowstate.example.com/rpc"} {
		_, err := resolveRPCResource(rpcResourceFlags{resource: resource}, authCfg, policy)
		require.Error(t, err, resource)
	}
}
