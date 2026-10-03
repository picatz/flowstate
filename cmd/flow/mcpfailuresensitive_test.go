package main

import (
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// sensitiveFailureWorkflow fails deterministically with a sentence quoting
// its `sensitive:` input: a `sleep:` computed from a value that is not a
// duration says what it produced.
const sensitiveFailureWorkflow = `edition: v2026.4
name: sensitive-failure
inputs:
  token:
    type: string
    required: true
    sensitive: true
steps:
  - id: pause
    sleep: ${inputs.token}
`

// sensitiveFailureToken is the argument neither surface may quote.
const sensitiveFailureToken = "sk-live-0123456789abcdef"

// runLocalFailureMessage is the failure sentence `flow run local -o json`
// writes for sensitiveFailureWorkflow, and the error it returns.
func runLocalFailureMessage(t *testing.T, token string, extra ...string) (string, error) {
	t.Helper()

	stdout, _, err := runLocal(t, sensitiveFailureWorkflow, append([]string{"--input", "token=" + token, "-o", "json"}, extra...)...)
	require.Error(t, err, "a sleep of a value that is not a duration fails the run")

	var document struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &document), "the failure document: %s", stdout)

	return document.Error.Message, err
}

// TestTheRunLocalToolRedactsASensitiveInputInTheFailureText is #2188: the MCP
// `flowstate_run_local` tool answers a run whose failure quotes a
// `sensitive:` input with the sentence `flow run local` prints for the same
// run, the value removed from both.
func TestTheRunLocalToolRedactsASensitiveInputInTheFailureText(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())
	result, answer := callRunLocal(t, session, map[string]any{
		"source": sensitiveFailureWorkflow,
		"inputs": map[string]any{"token": sensitiveFailureToken},
	})
	require.True(t, result.IsError, "the run fails")

	assert.NotContains(t, result.Content[0].(*mcp.TextContent).Text, sensitiveFailureToken,
		"the sensitive input reached the tool result")
	assert.Contains(t, answer.Run.Error.Message, v1.SensitiveMarker,
		"a redaction, not a withholding: the sentence says a value was removed")
	assert.Contains(t, answer.Run.Error.Message, `step "pause"`, "the sentence no longer says which step failed")

	cli, err := runLocalFailureMessage(t, sensitiveFailureToken)
	assert.NotContains(t, err.Error(), sensitiveFailureToken)
	assert.Equal(t, cli, answer.Run.Error.Message, "the tool and `flow run local` rendered one failure two ways")
}

// TestTheRunLocalToolRevealSensitiveShowsTheFailureText is the escape hatch:
// with the operator's --reveal-sensitive, both surfaces quote the value.
func TestTheRunLocalToolRevealSensitiveShowsTheFailureText(t *testing.T) {
	t.Parallel()

	posture := defaultLocalRunPosture()
	require.NoError(t, posture.Flags().Set(revealSensitiveFlagName, "true"))
	session := connectMCP(t, posture)
	_, answer := callRunLocal(t, session, map[string]any{
		"source": sensitiveFailureWorkflow,
		"inputs": map[string]any{"token": sensitiveFailureToken},
	})
	assert.Contains(t, answer.Run.Error.Message, sensitiveFailureToken)

	cli, _ := runLocalFailureMessage(t, sensitiveFailureToken, "--reveal-sensitive")
	assert.Equal(t, cli, answer.Run.Error.Message)
}
