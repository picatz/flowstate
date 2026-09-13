package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --plugin-env and --plugin-env-file feed plugin.Config.EnvByPlugin, which is
// how a deployment configures one plugin's process. Before it existed a plugin
// was launched with the protocol's variables and nothing else, so documented
// operator configuration — plugins/codex's FLOWSTATE_CODEX_BASE_CONFIG,
// plugins/git's GIT_SECRET_* — named a variable no worker could deliver.

// TestPluginEnvFlagIsWiredOnEveryVerb confirms both flags reach every command
// that builds a host through pluginFlags.host, the same way
// [TestPluginPinFlagIsWiredOnEveryVerb] confirms the pinning pair do.
func TestPluginEnvFlagIsWiredOnEveryVerb(t *testing.T) {
	for _, path := range [][]string{{"worker"}, {"server"}, {"plugins"}, {"lsp"}, {"mcp"}, {"run", "local"}} {
		cmd := flowCommand(t, path...)

		for _, name := range []string{"plugin-env", "plugin-env-file"} {
			assert.NotNil(t, cmd.Flags().Lookup(name),
				"`flow %s` does not take --%s", strings.Join(path, " "), name)
		}
	}
}

// TestPluginEnvFlagRefusesAMalformedEntry covers the flag's own shape. Both
// halves are named in the message because "plugin=KEY=VALUE" has two places to
// get wrong and an operator who typed one of them cannot tell which from
// "invalid".
func TestPluginEnvFlagRefusesAMalformedEntry(t *testing.T) {
	for _, entry := range []string{"no-assignment", "oci=KEYONLY", "oci==novalue"} {
		res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(), "--plugin-env", entry)
		require.Error(t, res.Err, "--plugin-env %q was accepted", entry)
		assert.Contains(t, res.Err.Error(), "plugin=KEY=VALUE")
	}
}

// TestPluginEnvNameNoPluginCouldAnswerToIsRefusedAtStartup: the key is a plugin
// name, and one discovery could never produce is configuration that silently
// reaches nothing — the same fail-open typo a misspelled pin makes.
func TestPluginEnvNameNoPluginCouldAnswerToIsRefusedAtStartup(t *testing.T) {
	res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(), "--plugin-env", "OCI=A=b")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "not a valid plugin name")
}

// TestPluginEnvFileMergesWithTheFlag: the file is the base and the flag extends
// it, proved by giving the file a well-formed entry and the flag one that is
// refused for its own reason — so only the flag's entry can be what failed.
func TestPluginEnvFileMergesWithTheFlag(t *testing.T) {
	envFile := filepath.Join(t.TempDir(), "plugin-env.yaml")
	require.NoError(t, os.WriteFile(envFile, []byte(
		"env:\n  oci:\n    FLOWSTATE_OCI_REGISTRIES: /etc/flowstate/oci.yaml\n"), 0o644))

	res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(),
		"--plugin-env-file", envFile,
		"--plugin-env", "GHOST=A=b")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "not a valid plugin name",
		"the file's own valid entry should have let the merge proceed to the flag's")
}

// TestPluginEnvFileAndFlagConflictOnOneVariableIsRefused: a variable set for one
// plugin by both sources is ambiguous even when the values agree, and resolving
// it by whichever source ran last would make the effective value depend on an
// order nothing states.
func TestPluginEnvFileAndFlagConflictOnOneVariableIsRefused(t *testing.T) {
	envFile := filepath.Join(t.TempDir(), "plugin-env.yaml")
	require.NoError(t, os.WriteFile(envFile, []byte(
		"env:\n  oci:\n    FLOWSTATE_OCI_REGISTRIES: /etc/flowstate/oci.yaml\n"), 0o644))

	res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(),
		"--plugin-env-file", envFile,
		"--plugin-env", "oci=FLOWSTATE_OCI_REGISTRIES=/tmp/other.yaml")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "--plugin-env-file and --plugin-env both set")
}

// TestPluginEnvFlagSetsOneVariableTwiceIsRefused is the same ambiguity arriving
// through one flag given twice.
func TestPluginEnvFlagSetsOneVariableTwiceIsRefused(t *testing.T) {
	res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(),
		"--plugin-env", "oci=FLOWSTATE_OCI_REGISTRIES=/etc/a.yaml",
		"--plugin-env", "oci=FLOWSTATE_OCI_REGISTRIES=/etc/b.yaml")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "more than once")
}

// TestPluginEnvFileRefusesAnUnknownKeyAtStartup: a misspelled top-level key
// fails the command that loaded it, rather than launching plugins with less
// configuration than the file's author wrote.
func TestPluginEnvFileRefusesAnUnknownKeyAtStartup(t *testing.T) {
	envFile := filepath.Join(t.TempDir(), "plugin-env.yaml")
	require.NoError(t, os.WriteFile(envFile, []byte(
		"envs:\n  oci:\n    FLOWSTATE_OCI_REGISTRIES: /etc/flowstate/oci.yaml\n"), 0o644))

	res := runFlow(t, "plugins", "--plugin-dir", t.TempDir(), "--plugin-env-file", envFile)
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "parsing plugin environment")
}
