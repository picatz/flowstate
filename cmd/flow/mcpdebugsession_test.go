package main

import (
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sessionTests = `tests:
  - name: it ships
    inputs:
      release: "2026.9.0"
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [build, ship]
`

type sessionReply struct {
	SessionID string `json:"session_id"`
	Text      string `json:"text"`
	Receipt   struct {
		Status   string `json:"status"`
		Revision string `json:"revision"`
	} `json:"receipt"`
	Snapshot struct {
		Revision   string `json:"revision"`
		State      string `json:"state"`
		Reason     string `json:"reason"`
		Occurrence struct {
			Address string `json:"address"`
		} `json:"occurrence"`
		Capabilities map[string]bool `json:"capabilities"`
	} `json:"snapshot"`
	Inspect struct {
		Value struct {
			Type     string `json:"type"`
			Rendered string `json:"rendered"`
		} `json:"value"`
	} `json:"inspect"`
	Report json.RawMessage `json:"report"`
	Note   string          `json:"note"`
	raw    string
}

func callSession(t *testing.T, session *mcp.ClientSession, tool string, args map[string]any) (*mcp.CallToolResult, sessionReply) {
	t.Helper()

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: tool, Arguments: args})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)
	text := result.Content[0].(*mcp.TextContent).Text

	var reply sessionReply
	_ = json.Unmarshal([]byte(text), &reply)
	reply.raw = text

	return result, reply
}

// TestARetainedSessionIsDrivenAcrossCalls is #2127's loop: start once, then
// command, inspect and observe across calls, retry without moving twice, and
// end with the verdict.
func TestARetainedSessionIsDrivenAcrossCalls(t *testing.T) {
	t.Parallel()

	client := connectMCP(t, defaultLocalRunPosture())

	result, started := callSession(t, client, debugSessionStartTool, map[string]any{
		"workflow": debugWorkflow, "tests": sessionTests, "request_id": "start-1",
	})
	require.False(t, result.IsError, started.raw)
	require.NotEmpty(t, started.SessionID)
	assert.Equal(t, "DEBUG_RUN_STATE_HELD", started.Snapshot.State)
	assert.Equal(t, "build", started.Snapshot.Occurrence.Address)
	assert.True(t, started.Snapshot.Capabilities["stepOver"])

	// Starting again with the same request id continues, never restarts.
	_, again := callSession(t, client, debugSessionStartTool, map[string]any{
		"workflow": debugWorkflow, "tests": sessionTests, "request_id": "start-1",
	})
	assert.Equal(t, started.SessionID, again.SessionID)
	assert.Contains(t, again.Note, "not started again")

	_, inspected := callSession(t, client, debugSessionCommandTool, map[string]any{
		"session_id": started.SessionID, "command": "inspect inputs.release",
	})
	assert.Equal(t, "string", inspected.Inspect.Value.Type)
	assert.Equal(t, `"2026.9.0"`, inspected.Inspect.Value.Rendered)

	args := map[string]any{"session_id": started.SessionID, "command": "next", "request_id": "move-1"}
	_, moved := callSession(t, client, debugSessionCommandTool, args)
	assert.Equal(t, "DEBUG_COMMAND_STATUS_APPLIED", moved.Receipt.Status)
	assert.Equal(t, "ship", moved.Snapshot.Occurrence.Address)

	_, retried := callSession(t, client, debugSessionCommandTool, args)
	assert.Equal(t, moved.raw, retried.raw, "a retry is answered from memory")
	_, observed := callSession(t, client, debugSessionObserveTool, map[string]any{"session_id": started.SessionID})
	assert.Equal(t, "ship", observed.Snapshot.Occurrence.Address, "the retry did not move the run a second time")

	// A command for a revision the session has left is refused.
	_, stale := callSession(t, client, debugSessionCommandTool, map[string]any{
		"session_id": started.SessionID, "command": "next", "expected_revision": 1,
	})
	assert.Equal(t, "DEBUG_COMMAND_STATUS_STALE", stale.Receipt.Status)

	result, ended := callSession(t, client, debugSessionEndTool, map[string]any{"session_id": started.SessionID})
	require.False(t, result.IsError, ended.raw)
	assert.Contains(t, string(ended.Report), "it ships", "ending a stubbed session returns its verdict")

	result, _ = callSession(t, client, debugSessionCommandTool, map[string]any{
		"session_id": started.SessionID, "command": "status",
	})
	assert.True(t, result.IsError, "an ended session is gone, and is said to be")
}

func TestARetainedSessionRefusesAMalformedCall(t *testing.T) {
	t.Parallel()

	client := connectMCP(t, defaultLocalRunPosture())
	result, _ := callSession(t, client, debugSessionStartTool, map[string]any{"workflow": debugWorkflow, "tests": sessionTests, "stray": 1})
	assert.True(t, result.IsError, "an unknown argument must be refused, not ignored")

	result, _ = callSession(t, client, debugSessionCommandTool, map[string]any{"session_id": "nope", "command": "next"})
	assert.True(t, result.IsError)
}
