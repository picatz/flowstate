package mcp

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
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
