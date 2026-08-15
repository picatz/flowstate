package main

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// TestTheMCPServerTakesThePluginFlags is [TestTheWorkerTakesThePluginFlags]'s
// own check for `flow mcp`, which until #241 was the only plugin-relevant
// command without it: worker, server, plugins and lsp all called
// addPluginFlags, and an agent asking this surface to author a `codex.exec:`
// workflow — the flagship agentic story — was told `unknown task` by the one
// command built for agents.
func TestTheMCPServerTakesThePluginFlags(t *testing.T) {
	var cmd *cobra.Command
	for _, c := range newRootCommand().Commands() {
		if c.Name() == "mcp" {
			cmd = c

			break
		}
	}
	require.NotNil(t, cmd, "there is no mcp command")

	for _, name := range []string{"plugin-dir", "plugin", "plugin-scheme", "allow-insecure-plugin-dir"} {
		assert.NotNil(t, cmd.Flags().Lookup(name),
			"`flow mcp` does not take --%s, so a deployment cannot tell it about a plugin", name)
	}
}

// buildExamplePluginDir compiles the repo's own example plugin
// (pkg/flowstate/v1/plugin/examples/flowstate-plugin-example) into a fresh
// directory named the way pkg/flowstate/v1/plugin/discover.go requires, so
// the wiring test below points a real host at a real plugin rather than
// merely asserting the flag's existence.
func buildExamplePluginDir(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("building the example plugin is slow; the flag wiring is covered without it")
	}

	dir := t.TempDir()
	bin := filepath.Join(dir, plugin.BinaryPrefix+"example")

	cmd := exec.Command("go", "build", "-o", bin,
		"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/examples/flowstate-plugin-example")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "building the example plugin: %s", out)

	return dir
}

// mcpCatalogText asks flowstate_get_catalog over a real MCP session and
// returns its answer as text — the same call an agent would make, so what
// this test asserts against is what an agent actually sees rather than
// [v1.DefaultRegistry]'s internals.
func mcpCatalogText(t *testing.T, posture *cobra.Command) string {
	t.Helper()

	session := connectMCP(t, posture)

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.ToolName("GetCatalog"),
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	require.False(t, result.IsError, "flowstate_get_catalog answered with an error: %v", result.Content)
	require.Len(t, result.Content, 1)

	text, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok, "flowstate_get_catalog answered with %T", result.Content[0])

	return text.Text
}

// mcpValidateDiagnostics validates one inline source over a real MCP session
// and returns the diagnostic messages it reported.
func mcpValidateDiagnostics(t *testing.T, posture *cobra.Command, source string) []string {
	t.Helper()

	session := connectMCP(t, posture)

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name: flowmcp.ToolName("Validate"),
		Arguments: map[string]any{
			"files": []map[string]any{{
				"name":   "wf.yaml",
				"source": []byte(source),
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, result.IsError, "flowstate_validate answered with an error: %v", result.Content)
	require.Len(t, result.Content, 1)

	text, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok, "flowstate_validate answered with %T", result.Content[0])

	var response struct {
		Report struct {
			Files []struct {
				Diagnostics []struct {
					Message string `json:"message"`
				} `json:"diagnostics"`
			} `json:"files"`
		} `json:"report"`
	}
	require.NoError(t, json.Unmarshal([]byte(text.Text), &response), "not a ValidateResponse: %s", text.Text)

	var messages []string
	if len(response.Report.Files) == 1 {
		for _, d := range response.Report.Files[0].Diagnostics {
			messages = append(messages, d.Message)
		}
	}

	return messages
}

// TestPluginDirWiresPluginTasksIntoTheMCPSurface is #241's P3 proven, not just
// declared: --plugin-dir has to reach flowstate_get_catalog and
// flowstate_validate, the same registration [runWorker] and [runLSP] perform
// through the same [startPlugins] — a flag with nothing behind it would still
// pass [TestTheMCPServerTakesThePluginFlags] above.
//
// Registering into [v1.DefaultRegistry] is a one-way door (see
// [plugin.Host.Register]'s own doc), so the absence is asserted *first*: once
// this test registers the example plugin, "example.greet" is in this test
// binary's catalog for good. That is the same tradeoff
// server.TestGetCatalogAnswersWithTheCatalog already accepts by asserting
// Contains against the live registry rather than an exact set — there is no
// Unregister for a test to restore either.
func TestPluginDirWiresPluginTasksIntoTheMCPSurface(t *testing.T) {
	dir := buildExamplePluginDir(t)

	workflow := `edition: v2026.3
name: greet
steps:
  - id: hi
    example.greet:
      name: world
`

	// Without --plugin-dir: nothing registered "example.greet", so the
	// catalog does not mention it and validating a step naming it fails.
	without := defaultLocalRunPosture()
	assert.NotContains(t, mcpCatalogText(t, without), `"example.greet"`,
		"the catalog names a plugin task before any plugin was ever registered")

	diagnostics := mcpValidateDiagnostics(t, without, workflow)
	require.NotEmpty(t, diagnostics, "a step naming a task nothing registered validated clean")
	assert.Contains(t, strings.Join(diagnostics, "\n"), "example.greet",
		"the diagnostic for an unregistered task does not name it: %v", diagnostics)

	// With it: the same flags and the same startPlugins call runMCP itself
	// makes.
	with := &cobra.Command{Use: "mcp"}
	addLocalRunFlags(with)
	addPluginFlags(with)
	with.SetContext(t.Context())
	require.NoError(t, with.Flags().Set("plugin-dir", dir))

	_, closePlugins, err := startPlugins(with, nil)
	require.NoError(t, err, "starting the example plugin")
	t.Cleanup(closePlugins)

	assert.Contains(t, mcpCatalogText(t, with), `"example.greet"`,
		"a task the plugin host registered did not reach the catalog flowstate_get_catalog answers with")

	assert.Empty(t, mcpValidateDiagnostics(t, with, workflow),
		"a step naming a now-registered plugin task still failed to validate")
}
