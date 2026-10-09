package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// repeatedSlackToken is a Flowfile that writes one bot token on both of its Slack
// steps, which is what a plugin credential binding exists to say once.
const repeatedSlackToken = `edition: v2026.4
name: repeated-token
plugins:
  slack: v0.2.0
inputs:
  channel:
    type: string
    required: true
steps:
  - id: first
    slack.post:
      channel: ${inputs.channel}
      idempotency_key: 018f0e6c-7b42-7cc1-8a31-65c0f8758f4b
      text: one
      token: ${secret('env:SLACK_BOT_TOKEN')}
  - id: second
    slack.post:
      channel: ${inputs.channel}
      idempotency_key: 018f0e6c-7b42-7cc1-8a31-65c0f8758f4c
      text: two
      token: ${secret('env:SLACK_BOT_TOKEN')}
`

// TestFixConsolidatesARepeatedCredentialAgainstACatalog runs the command, not the
// rewriter: the plugin facts come from the shipped catalog (`--plugin-catalog`),
// which is how CI reads them; that the result means what the original did is the
// rewriter's own test (flowfile.TestFixBindsARepeatedCredentialOnceAndKeepsAnOverride).
// Without a catalog or plugin directory the command has no credential to read and
// leaves the file alone, which the `without` assertion pins.
func TestFixConsolidatesARepeatedCredentialAgainstACatalog(t *testing.T) {
	bin := buildFlowBinary(t)
	catalog, err := filepath.Abs("../../examples/plugins/plugins.lock.json")
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(repeatedSlackToken), 0o600))

	check := runFlowBinary(t, bin, "fix", "--check", "--"+pluginCatalogFlag, catalog, path)
	require.Error(t, check.Err, "--check found a repeated credential and exited zero:\n%s", check.Output())
	assert.Contains(t, check.Output(), "bot_token", "the report does not name the credential:\n%s", check.Output())
	assert.NotContains(t, check.Output(), "SLACK_BOT_TOKEN", "the report echoed the reference")
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, repeatedSlackToken, string(got), "--check wrote to the file")

	without := runFlowBinary(t, bin, "fix", "--check", path)
	assert.NoError(t, without.Err, "with no plugin source there is nothing to read a credential from:\n%s", without.Output())

	fixed := runFlowBinary(t, bin, "fix", "--"+pluginCatalogFlag, catalog, path)
	require.NoError(t, fixed.Err, "fix:\n%s", fixed.Output())
	got, err = os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(got), "bot_token: ${secret('env:SLACK_BOT_TOKEN')}")
	assert.NotContains(t, string(got), "token: ${secret('env:SLACK_BOT_TOKEN')}\n  - id")

	again := runFlowBinary(t, bin, "fix", "--check", "--"+pluginCatalogFlag, catalog, path)
	require.NoError(t, again.Err, "the rewritten file is not a fixed point:\n%s", again.Output())
}
