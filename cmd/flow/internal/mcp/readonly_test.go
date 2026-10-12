package mcp_test

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// readOnlySession serves the full stdio-shaped surface plus a write tool and a
// tool declaring no annotations, under deps, and connects a client to it.
func readOnlySession(t *testing.T, deps flowmcp.Deps) *mcp.ClientSession {
	t.Helper()

	local, err := server.New(nil)
	require.NoError(t, err)

	reached := func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "reached"}}}, nil
	}
	srv := flowmcp.NewServer("test")
	flowmcp.AddTools(srv, local, func() flowstatev1connect.WorkflowServiceClient { return nil }, deps,
		flowmcp.ToolRegistration{Tool: flowmcp.RunLocalTool(), Handler: reached},
		flowmcp.ToolRegistration{Tool: flowmcp.ReducedTestTool(), Handler: reached},
		flowmcp.ToolRegistration{
			Tool:    &mcp.Tool{Name: "flowstate_unannotated", InputSchema: map[string]any{"type": "object"}},
			Handler: reached,
		},
	)

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	go func() { _ = srv.Run(t.Context(), serverTransport) }()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "test"}, nil).Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	return session
}

func listed(t *testing.T, session *mcp.ClientSession) map[string]*mcp.Tool {
	t.Helper()

	tools, err := session.ListTools(t.Context(), nil)
	require.NoError(t, err)

	byName := map[string]*mcp.Tool{}
	for _, tool := range tools.Tools {
		byName[tool.Name] = tool
	}

	return byName
}

// TestReadOnlyServesOnlyReadOnlyTools: under Deps.ReadOnly tools/list holds
// only tools declaring readOnlyHint, the read RPCs are still there, and a tool
// with no annotations is withheld rather than assumed safe.
func TestReadOnlyServesOnlyReadOnlyTools(t *testing.T) {
	t.Parallel()

	served := listed(t, readOnlySession(t, flowmcp.Deps{ReadOnly: true}))

	for name, tool := range served {
		require.NotNil(t, tool.Annotations, "%s is served with no annotations", name)
		require.True(t, tool.Annotations.ReadOnlyHint, "%s is served under --read-only but is not read-only", name)
	}
	for _, rpc := range []string{"Validate", "Get", "List", "GetCatalog"} {
		require.Contains(t, served, flowmcp.ToolName(rpc))
	}
	require.Contains(t, served, flowmcp.TestToolName)
	for _, withheld := range []string{
		flowmcp.ToolName("Terminate"), flowmcp.ToolName("Signal"), flowmcp.RunLocalToolName, "flowstate_unannotated",
	} {
		require.NotContains(t, served, withheld)
	}
}

// TestWithoutReadOnlyTheListIsUnchanged guards the filter against narrowing
// anything when the flag is off.
func TestWithoutReadOnlyTheListIsUnchanged(t *testing.T) {
	t.Parallel()

	served := listed(t, readOnlySession(t, flowmcp.Deps{}))

	for _, method := range flowmcp.WorkflowServiceMethods() {
		require.Contains(t, served, flowmcp.ToolName(method.Name))
	}
	require.Contains(t, served, flowmcp.RunLocalToolName)
	require.Contains(t, served, "flowstate_unannotated")
}

// TestReadOnlyRefusesACallToAWithheldTool: a client with a stale list that
// names a write tool gets a tool error rather than a protocol error, and the
// tool never runs. The audit half is TestReadOnlyAuditsTheRefusal, which needs
// a verified caller that an in-memory transport does not carry.
func TestReadOnlyRefusesACallToAWithheldTool(t *testing.T) {
	t.Parallel()

	session := readOnlySession(t, flowmcp.Deps{ReadOnly: true})

	for _, name := range []string{flowmcp.RunLocalToolName, "flowstate_unannotated"} {
		result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: map[string]any{}})
		require.NoError(t, err, "a refusal is a tool error, not a protocol error")
		require.True(t, result.IsError)
		require.Contains(t, result.Content[0].(*mcp.TextContent).Text, "read-only")
		require.NotContains(t, result.Content[0].(*mcp.TextContent).Text, "reached")
	}

	// A name this server never had keeps the SDK's own answer.
	_, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "flowstate_nope", Arguments: map[string]any{}})
	require.Error(t, err)
}
