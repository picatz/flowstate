package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runPluginsInto executes `flow plugins` with the given flags and captures both
// streams.
func runPluginsInto(t *testing.T, format string, args ...string) (stdout, stderr string, err error) {
	t.Helper()

	var out, errOut bytes.Buffer

	cmd := &cobra.Command{}
	addOutputFlag(cmd)
	addPluginFlags(cmd)

	if format != "" {
		require.NoError(t, cmd.Flags().Set("output", format))
	}
	for i := 0; i+1 < len(args); i += 2 {
		require.NoError(t, cmd.Flags().Set(args[i], args[i+1]))
	}

	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetContext(t.Context())

	err = runPlugins(cmd, nil)

	return out.String(), errOut.String(), err
}

// TestAnUnconfiguredWorkerSaysSoRatherThanShowingNothing.
//
// The two states an empty listing can mean — nothing installed, and nowhere
// configured to look — are one keystroke apart for an operator and completely
// different problems. An empty list satisfies both, so the answer has to say
// which.
func TestAnUnconfiguredWorkerSaysSoRatherThanShowingNothing(t *testing.T) {
	t.Setenv(pluginSearchPathEnv, "")

	stdout, _, err := runPluginsInto(t, "")
	require.NoError(t, err)

	assert.Contains(t, stdout, "No plugin directory is configured",
		"an operator with no --plugin-dir is shown an empty listing, which reads as "+
			"'nothing installed' when it means 'nowhere to look'")
	assert.Contains(t, stdout, pluginSearchPathEnv,
		"the message does not name the environment variable that would fix it")
}

// TestTheEmptyAnswerIsStillADocument.
//
// A consumer indexing `.plugins[]` must not have to special-case the
// unconfigured worker, so the machine form is the same message either way.
func TestTheEmptyAnswerIsStillADocument(t *testing.T) {
	t.Setenv(pluginSearchPathEnv, "")

	stdout, _, err := runPluginsInto(t, "json")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &doc),
		"the machine form is not a document: %s", stdout)

	// EmitUnpopulated is what makes the key present and null rather than absent,
	// which is the property `jq '.plugins | length'` depends on.
	assert.Contains(t, doc, "plugins")
	assert.Contains(t, doc, "searchPath")
}

// TestADirectoryWithNoPluginsInItIsNotAnError.
//
// Distinct from the case above and easy to conflate: the operator did configure a
// directory, and there is nothing in it. That is a fact rather than a failure —
// exiting non-zero would make `flow plugins` unusable in the scripts that would
// most want it.
func TestADirectoryWithNoPluginsInItIsNotAnError(t *testing.T) {
	dir := t.TempDir()

	stdout, _, err := runPluginsInto(t, "json", "plugin-dir", dir)
	require.NoError(t, err)

	var doc struct {
		Plugins    []any    `json:"plugins"`
		SearchPath []string `json:"searchPath"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &doc))

	assert.Empty(t, doc.Plugins)
	assert.Equal(t, []string{dir}, doc.SearchPath,
		"the answer does not say where it looked, which is the whole difference "+
			"between this case and an unconfigured one")
}

// TestARelativePluginDirectoryIsResolvedRatherThanRefused.
//
// The host requires absolute paths, for a good reason it states: a relative one
// resolves against a working directory the worker does not control. But a person
// typing `--plugin-dir ./plugins` means the directory they are standing in, and
// refusing them to preserve an invariant they cannot see is the wrong half of
// the trade. So it is resolved once, at the edge, where the working directory is
// still the one they meant.
func TestARelativePluginDirectoryIsResolvedRatherThanRefused(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "plugins"), 0o755))

	t.Chdir(dir)

	stdout, _, err := runPluginsInto(t, "json", "plugin-dir", "plugins")
	require.NoError(t, err, "a relative --plugin-dir was refused")

	var doc struct {
		SearchPath []string `json:"searchPath"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &doc))
	require.Len(t, doc.SearchPath, 1)
	assert.True(t, filepath.IsAbs(doc.SearchPath[0]),
		"the search path reported back is not absolute: %q", doc.SearchPath[0])
}

// TestATrailingSeparatorDoesNotAddTheWorkingDirectory.
//
// `FLOWSTATE_PLUGIN_DIR=/opt/plugins:` is a typo, and the empty entry it produces
// resolves to the working directory. Honouring it would make a worker execute
// whatever happens to be named flowstate-plugin-* wherever it was launched from,
// which is the shape of a search-path attack rather than a cosmetic bug.
func TestATrailingSeparatorDoesNotAddTheWorkingDirectory(t *testing.T) {
	dir := t.TempDir()

	t.Setenv(pluginSearchPathEnv, dir+string(os.PathListSeparator))

	stdout, _, err := runPluginsInto(t, "json")
	require.NoError(t, err)

	var doc struct {
		SearchPath []string `json:"searchPath"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &doc))

	assert.Equal(t, []string{dir}, doc.SearchPath,
		"an empty path entry became a directory to execute code from")
}

// TestTheWorkerTakesThePluginFlags is the wiring check.
//
// Cheap and worth having: every flag this file reads is declared on `plugins` by
// its own command, and on `worker` by a separate call. A worker missing one would
// read the zero value forever, silently, and the failure would look like a plugin
// that does not load.
func TestTheWorkerTakesThePluginFlags(t *testing.T) {
	var worker *cobra.Command
	for _, c := range newRootCommand().Commands() {
		if c.Name() == "worker" {
			worker = c

			break
		}
	}
	require.NotNil(t, worker, "there is no worker command")

	for _, name := range []string{"plugin-dir", "plugin", "plugin-scheme", "allow-insecure-plugin-dir"} {
		assert.NotNil(t, worker.Flags().Lookup(name),
			"`flow worker` does not take --%s, so a deployment cannot configure it", name)
	}
}
