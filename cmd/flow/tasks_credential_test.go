package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTasksNamesTheCredentialAnInputReceives reads the shipped catalog, as CI does,
// so the line comes from the claims the plugin's descriptor carries and not from a
// fixture written to match.
func TestTasksNamesTheCredentialAnInputReceives(t *testing.T) {
	bin := buildFlowBinary(t)
	catalog, err := filepath.Abs("../../examples/plugins/plugins.lock.json")
	require.NoError(t, err)

	res := runFlowBinary(t, bin, "tasks", "--"+pluginCatalogFlag, catalog, "slack.post")
	require.NoError(t, res.Err, res.Output())
	assert.Contains(t, res.Stdout, "Needs a credential: bot_token")
	assert.Contains(t, res.Stdout, "`token` receives the plugin's `bot_token` credential")

	respond := runFlowBinary(t, bin, "tasks", "--"+pluginCatalogFlag, catalog, "slack.respond")
	require.NoError(t, respond.Err, respond.Output())
	assert.NotContains(t, respond.Stdout, "Needs a credential", "a task with no credential input is described as needing one")
}
