package mcp_test

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// TestAddToolsWrapsEveryRPCTool: a Deps.WrapHandler given to AddTools sees
// every RPC tool by name and its wrapper is the handler served, so a surface
// that fences some of them — stdio's registry readers during a stubbed debug
// session — fences what it names.
func TestAddToolsWrapsEveryRPCTool(t *testing.T) {
	t.Parallel()

	local, err := server.New(nil)
	require.NoError(t, err)
	wrapped := map[string]bool{}
	deps := flowmcp.Deps{WrapHandler: func(tool string, next mcp.ToolHandler) mcp.ToolHandler {
		wrapped[tool] = true

		return func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "fenced"}}}, nil
		}
	}}
	srv := flowmcp.NewServer("test")
	flowmcp.AddTools(srv, local, func() flowstatev1connect.WorkflowServiceClient { return nil }, deps)

	for _, method := range flowmcp.WorkflowServiceMethods() {
		assert.True(t, wrapped[flowmcp.ToolName(method.Name)], "%s was registered unwrapped", method.Name)
	}

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	go func() { _ = srv.Run(t.Context(), serverTransport) }()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "test"}, nil).Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: flowmcp.ToolName("Validate"), Arguments: map[string]any{}})
	require.NoError(t, err)
	assert.True(t, result.IsError, "the wrapper was not the handler served")
}
