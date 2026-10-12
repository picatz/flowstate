package mcp

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// TestReadOnlyAuditsTheRefusal: the call a stale client sends to a withheld
// tool is refused with a tool error and recorded as exactly one POLICY_DENIED
// decision naming the tool and the verified caller, the way withMCPActions
// records its own; a name that was not withheld passes through untouched.
func TestReadOnlyAuditsTheRefusal(t *testing.T) {
	t.Parallel()

	var records []*v1.AuditRecord
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(toolAuditEmitterFunc(
		func(_ context.Context, record *v1.AuditRecord) error {
			records = append(records, record)
			return nil
		},
	)))
	require.NoError(t, err)

	middleware := withheldRefusal(Deps{ReadOnly: true, Audit: recorder}, []string{"flowstate_terminate"})
	var passed bool
	handler := middleware(func(context.Context, string, mcp.Request) (mcp.Result, error) {
		passed = true
		return &mcp.CallToolResult{}, nil
	})

	req := actionsToolRequest(t, nil, nil)
	req.Params.Name = "flowstate_terminate"
	result, err := handler(t.Context(), "tools/call", req)
	require.NoError(t, err)
	require.False(t, passed, "a refused call must not reach the tool")
	require.True(t, result.(*mcp.CallToolResult).IsError)

	require.Len(t, records, 1)
	require.Equal(t, "flowstate_terminate", records[0].GetMcpTool())
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, records[0].GetDecision())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, records[0].GetDenyCode())
	require.NotEmpty(t, records[0].GetIdentity(), "the verified caller is named")

	req = actionsToolRequest(t, nil, nil)
	req.Params.Name = "flowstate_validate"
	_, err = handler(t.Context(), "tools/call", req)
	require.NoError(t, err)
	require.True(t, passed)
	require.Len(t, records, 1, "a tool that was not withheld owes no denial")
}

// TestReadOnlyLocalCapabilitiesWithholdAndAuditAWriteTool is the guard on the
// surface `flow mcp serve` builds: a write tool passed to AddLocalCapabilities
// under ReadOnly is absent from tools/list, and a call naming it is refused and
// recorded as exactly one POLICY_DENIED decision. A verified caller is
// installed by an outer middleware, as bearer admission does over HTTP.
func TestReadOnlyLocalCapabilitiesWithholdAndAuditAWriteTool(t *testing.T) {
	t.Parallel()

	var records []*v1.AuditRecord
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(toolAuditEmitterFunc(
		func(_ context.Context, record *v1.AuditRecord) error {
			records = append(records, record)
			return nil
		},
	)))
	require.NoError(t, err)

	local, err := server.New(nil)
	require.NoError(t, err)
	srv := NewServer("test")
	AddLocalCapabilities(srv, local, Deps{ReadOnly: true, Audit: recorder},
		ToolRegistration{Tool: RunLocalTool(), Handler: func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "reached"}}}, nil
		}})

	info := actionsToolRequest(t, nil, nil).Extra.TokenInfo
	srv.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if call, ok := req.(*mcp.CallToolRequest); ok {
				call.Extra = &mcp.RequestExtra{TokenInfo: info}
			}
			return next(ctx, method, req)
		}
	})

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	go func() { _ = srv.Run(t.Context(), serverTransport) }()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "test"}, nil).Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	listed, err := session.ListTools(t.Context(), nil)
	require.NoError(t, err)
	for _, tool := range listed.Tools {
		require.NotEqual(t, RunLocalToolName, tool.Name)
	}

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: RunLocalToolName, Arguments: map[string]any{}})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Contains(t, result.Content[0].(*mcp.TextContent).Text, "not declared read-only")

	require.Len(t, records, 1)
	require.Equal(t, RunLocalToolName, records[0].GetMcpTool())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_POLICY_DENIED, records[0].GetDenyCode())
}
