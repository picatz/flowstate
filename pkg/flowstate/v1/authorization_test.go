package flowstatev1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthorizationRegistryIsUnambiguous(t *testing.T) {
	seenActions := map[string]bool{}
	seenOperations := map[string]string{}
	for _, action := range ActionRegistry() {
		require.NotEmpty(t, action.Name)
		require.NotEmpty(t, action.ResourceType)
		require.False(t, seenActions[action.Name], "duplicate action %q", action.Name)
		seenActions[action.Name] = true
		for surface, operations := range map[string][]string{"rpc": action.RPCs, "mcp": action.MCPTools, "plugin": action.PluginOperations, "lsp": action.LSPCapabilities, "internal": action.InternalOperations} {
			for _, operation := range operations {
				key := surface + ":" + operation
				require.Empty(t, seenOperations[key], "%s maps to both %s and %s", key, seenOperations[key], action.Name)
				seenOperations[key] = action.Name
				mapped, err := ActionForSurface(surface, operation)
				require.NoError(t, err)
				require.Equal(t, action.Name, mapped.Name)
			}
		}
		if action.Parent != "" {
			_, ok := LookupAction(action.Parent)
			require.True(t, ok, "%s has unknown parent %s", action.Name, action.Parent)
		}
	}
}

func TestEveryRegisteredRPCAndDerivedMCPToolHasOneTypedAction(t *testing.T) {
	services := File_flowstate_v1_service_proto.Services()
	require.Equal(t, 1, services.Len())
	methods := services.Get(0).Methods()
	for i := 0; i < methods.Len(); i++ {
		name := string(methods.Get(i).Name())
		action, err := ActionForSurface("rpc", name)
		require.NoError(t, err, name)
		require.NotEmpty(t, action.ResourceType, name)
		mcpAction, err := ActionForSurface("mcp", "flowstate_"+camelToSnake(name))
		require.NoError(t, err, name)
		require.Equal(t, action.Name, mcpAction.Name)
	}
}

func TestAuthorizationCELHasExactlyFourTypedRoots(t *testing.T) {
	_, _, err := CompileAuthorizationPolicy(`principal.principal_id != "" && action.name == "secret.read" && resource.type == "secret" && context.deployment == "prod"`)
	require.NoError(t, err)
	for _, forbidden := range []string{"request", "headers", "token", "assertion", "claims", "workflow", "plugin"} {
		_, _, err := CompileAuthorizationPolicy(forbidden + ` == null`)
		require.Error(t, err, forbidden)
	}
}

func TestAuthorizationEvaluationFailsClosedAndChecksResourceType(t *testing.T) {
	request := &AuthorizationRequest{
		Principal: &AuthorizationPrincipal{Issuer: "https://issuer.example", Subject: "runner", PrincipalId: "https://issuer.example#runner", Kind: AuthorizationPrincipalKind_AUTHORIZATION_PRINCIPAL_KIND_WORKLOAD},
		Action:    &AuthorizationAction{Name: "secret.read", Group: "secret"},
		Resource:  &AuthorizationResource{Type: "secret", Id: "env:DEPLOY_TOKEN"},
		Context:   &AuthorizationContext{Deployment: "prod"},
	}
	decision, err := EvaluateAuthorizationPolicy(context.Background(), `principal.kind == 2 && resource.type == "secret"`, request)
	require.NoError(t, err)
	require.True(t, decision.Allowed)

	request.Resource.Type = "run"
	decision, err = EvaluateAuthorizationPolicy(context.Background(), `true`, request)
	require.NoError(t, err)
	require.False(t, decision.Allowed)
}
