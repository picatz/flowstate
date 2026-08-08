package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnRPCToolAnswerIsBounded is the direction #300 found missing.
//
// Three tests already assert that `run_local` stays under [maxMCPResultBytes].
// None asserted it of the tools that answer over [mcpHandler] — which is every
// other tool — and that was exactly where the bound was absent. The untested
// direction and the unbounded one were the same direction, so this test is the
// bound's only witness on that path.
//
// `Validate` is the tool used here because it is the one whose answer a test can
// grow arbitrarily with no server, no Temporal and no network: a diagnostic per
// bad file, and the response carries them all.
func TestAnRPCToolAnswerIsBounded(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	// A request the schema accepts whose *answer* is enormous. `files` is
	// capped at 64 items, so the size comes from diagnostics per file rather
	// than from file count: every step names a key the grammar does not have,
	// and each one is reported. That is the realistic shape — a caller sends a
	// legal request and the reply is what gets large.
	var source strings.Builder
	source.WriteString("edition: v2026.2\nname: x\nsteps:\n")
	for i := range 400 {
		fmt.Fprintf(&source, "  - id: step%04d\n    nope:\n      x: y\n", i)
	}

	files := make([]map[string]any, 0, 64)
	for i := range 64 {
		files = append(files, map[string]any{
			"name":   fmt.Sprintf("%s-%02d.yaml", strings.Repeat("d", 48), i),
			"source": []byte(source.String()),
		})
	}

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      mcpToolName("Validate"),
		Arguments: map[string]any{"files": files},
	})
	require.NoError(t, err, "the call itself must succeed; the answer is what is bounded")
	require.NotEmpty(t, result.Content)

	text := result.Content[0].(*mcp.TextContent).Text

	// Both halves together, which is the pair the run_local tests already
	// assert and the reason neither is enough alone: an answer that is small
	// because it was cut in half is not a bounded answer, it is a broken one.
	assert.LessOrEqual(t, len(text), maxMCPResultBytes,
		"an RPC-backed tool answered over the surface's ceiling")
	assert.True(t, result.IsError,
		"an answer that could not be given within the ceiling has to say so, not look like a small result")

	// The refusal has to be actionable: an agent that cannot tell why it got
	// nothing will ask the same question again.
	assert.Contains(t, text, mcpToolName("Validate"), "the refusal should name the tool that overflowed")
	assert.Contains(t, text, fmt.Sprint(maxMCPResultBytes), "the refusal should name the limit")
	assert.Contains(t, text, "ask for less", "the refusal should say what to do instead")
}

// TestAnRPCToolAnswerUnderTheCeilingIsUnchanged is the control.
//
// A bound that also mangles the answers under it would pass the test above
// while making the tool useless, so this pins that an ordinary call still comes
// back as the protojson of its response message and parses.
func TestAnRPCToolAnswerUnderTheCeilingIsUnchanged(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name: mcpToolName("Validate"),
		Arguments: map[string]any{
			"files": []map[string]any{{
				"name":   "ok.yaml",
				"source": []byte("edition: v2026.2\nname: x\nsteps:\n  - id: a\n    log:\n      message: hi\n"),
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, result.IsError, "a small answer must not be refused: %v", result.Content)

	text := result.Content[0].(*mcp.TextContent).Text
	assert.LessOrEqual(t, len(text), maxMCPResultBytes)

	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(text), &document),
		"the answer stopped being a parseable document")
}
