package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// actionsToolRequest verifies a real token against a policy entry holding
// granted actions and returns the request a handler would receive, so the
// effective actions are the ones production derives (entry list narrowed by
// the token's scope claim) and not a hand-built principal's.
func actionsToolRequest(t *testing.T, granted auth.ActionScopes, claims map[string]any) *mcp.CallToolRequest {
	t.Helper()

	issuer := authtest.NewIssuer()
	t.Cleanup(func() { _ = issuer.Close() })

	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name: "agent-idp", Issuer: issuer.URL(), Audiences: []string{principalTestResource}, Actions: granted,
		}},
	}, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	token := issuer.MintToken(claims, authtest.WithSubject("agent"), authtest.WithAudience(principalTestResource))
	info, err := auth.MCPTokenVerifier(verifier, principalTestResource)(t.Context(), token, nil)
	require.NoError(t, err)

	return &mcp.CallToolRequest{
		Params: &mcp.CallToolParamsRaw{Arguments: json.RawMessage(`{}`)},
		Extra:  &mcp.RequestExtra{TokenInfo: info},
	}
}

// TestMCPToolsAreGatedByTheCallersEffectiveActions proves the MCP surface
// applies the same allowlist the RPC surface does, including the narrowing a
// token's scope claim performs, and that a refused call never reaches the tool.
func TestMCPToolsAreGatedByTheCallersEffectiveActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		granted auth.ActionScopes
		claims  map[string]any
		tool    string
		allowed bool
	}{
		{"no action list restricts nothing", nil, nil, "flowstate_run_local", true},
		{"listed action is allowed", auth.ActionScopes{"mcp.run_local"}, nil, "flowstate_run_local", true},
		{"unlisted action is refused", auth.ActionScopes{"mcp.test"}, nil, "flowstate_run_local", false},
		{"an empty list grants nothing", auth.ActionScopes{}, nil, "flowstate_run_local", false},
		{"the token's scope narrows the grant", auth.ActionScopes{"mcp.run_local", "mcp.test"},
			map[string]any{"scope": "mcp.test"}, "flowstate_run_local", false},
		{"a scope the entry never granted adds nothing", auth.ActionScopes{"mcp.test"},
			map[string]any{"scope": "mcp.test mcp.run_local"}, "flowstate_run_local", false},
		{"a narrowed grant still allows what it keeps", auth.ActionScopes{"mcp.run_local", "mcp.test"},
			map[string]any{"scope": "mcp.test"}, "flowstate_test", true},
		{"a tool bound to no action is refused for a restricted caller", auth.ActionScopes{"mcp.test"}, nil,
			"flowstate_unbound_tool", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var reached bool
			handler := wrapToolHandler(Deps{}, test.tool, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				reached = true
				return &mcp.CallToolResult{}, nil
			})

			result, err := handler(t.Context(), actionsToolRequest(t, test.granted, test.claims))
			require.NoError(t, err)
			require.Equal(t, test.allowed, reached)
			require.Equal(t, !test.allowed, result.IsError, "a refusal is a tool error, not a protocol error")
		})
	}
}
