package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
)

// TestToolArgumentSyntaxErrorsDoNotEchoTheDocument is #2665: every tool that
// decodes its own arguments reports a syntax error as a position, because
// `encoding/json` would otherwise name the offending character, and in
// `{"pin": Ž9secret}` that character is the first rune of a secret.
//
// The handlers are called directly with the malformed document. A served client
// cannot deliver one: the SDK parses the JSON-RPC envelope first and refuses a
// frame whose `arguments` are not JSON, so this is the defense that holds if
// that ever stops being true.
func TestToolArgumentSyntaxErrorsDoNotEchoTheDocument(t *testing.T) {
	t.Parallel()

	handlers := map[string]mcp.ToolHandler{
		flowmcp.RunLocalToolName:    runLocalToolHandler(defaultLocalRunPosture(), nil),
		flowmcp.TestToolName:        testToolHandler(0),
		flowmcp.CheckPolicyToolName: checkPolicyToolHandler(),
		flowmcp.DebugToolName:       debugToolHandler(0),
	}
	for _, registration := range newDebugSessions(nil).tools() {
		handlers[registration.Tool.Name] = registration.Handler
	}
	require.Greater(t, len(handlers), 4, "the retained-session tools decode their own arguments too")

	const document = `{"inputs": {"pin": Ž9secret}}`

	for name, handler := range handlers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			result, err := handler(context.Background(), &mcp.CallToolRequest{
				Params: &mcp.CallToolParamsRaw{Name: name, Arguments: json.RawMessage(document)},
			})
			require.NoError(t, err)
			require.True(t, result.IsError)

			text := result.Content[0].(*mcp.TextContent).Text
			assert.NotContains(t, text, "Ž", "the first rune of the value")
			assert.NotContains(t, text, "secret")
			assert.NotContains(t, text, "invalid character")
			assert.Contains(t, text, "invalid JSON syntax at line 1, column", "the position is what remains to find the mistake by")
		})
	}
}

// TestToolArgumentUnknownFieldsStillNameTheField pins the other half: only the
// syntax error is redacted, because a misspelt field name is vocabulary the
// caller can correct and the schema's additionalProperties:false is only useful
// if the refusal says which one.
func TestToolArgumentUnknownFieldsStillNameTheField(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.RunLocalToolName,
		Arguments: map[string]any{"sourse": "edition: v2026.4"},
	})
	require.NoError(t, err)
	require.True(t, result.IsError)
	assert.Contains(t, result.Content[0].(*mcp.TextContent).Text, "sourse")
}

// TestAServedTypeErrorNamesTheFieldNotTheValue: valid JSON of the wrong kind
// does reach the decoder, and `encoding/json` quotes a number that overflows its
// field. The refusal keeps the field and the position, never the token.
func TestAServedTypeErrorNamesTheFieldNotTheValue(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "flowstate_debug_session_observe",
		Arguments: json.RawMessage(`{"session_id":"s","after_revision":18446744073709551616123}`),
	})
	require.NoError(t, err)
	require.True(t, result.IsError)

	text := result.Content[0].(*mcp.TextContent).Text
	assert.NotContains(t, text, "18446744073709551616123")
	assert.Contains(t, text, "after_revision")
	assert.Contains(t, text, "line 1, column")
}

// TestAServedRPCArgumentErrorNamesThePositionNotTheToken: protojson quotes the
// token it stopped at, and a bare string where a message belongs is well-formed
// JSON a client can send.
func TestAServedRPCArgumentErrorNamesThePositionNotTheToken(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	call := func(arguments string) string {
		result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
			Name: flowmcp.ToolName("Run"), Arguments: json.RawMessage(arguments),
		})
		require.NoError(t, err)
		require.True(t, result.IsError)

		return result.Content[0].(*mcp.TextContent).Text
	}

	text := call(`{"inputs":{"pin":"hunter2-secret"}}`)
	assert.NotContains(t, text, "hunter2")
	assert.Contains(t, text, "flowstate.v1.RunRequest")
	assert.Contains(t, text, "invalid JSON at line 1, column")

	// What stays useful: the misspelt field is named.
	assert.Contains(t, call(`{"sourse":"x"}`), `unknown field "sourse"`)
}
