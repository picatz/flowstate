package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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
	// The server too: it answers Validate and GetCatalog from the same registry,
	// so a deployment whose workers load plugins points the server at the same
	// directory, or the capability it reports is the built-ins alone.
	for _, command := range []string{"worker", "server"} {
		var cmd *cobra.Command
		for _, c := range newRootCommand().Commands() {
			if c.Name() == command {
				cmd = c

				break
			}
		}
		require.NotNil(t, cmd, "there is no %s command", command)

		for _, name := range []string{"plugin-dir", "plugin", "plugin-scheme", "allow-insecure-plugin-dir"} {
			assert.NotNil(t, cmd.Flags().Lookup(name),
				"`flow %s` does not take --%s, so a deployment cannot configure it", command, name)
		}
	}
}

// TestTheLanguageServerTakesThePluginFlags is the same wiring check for the
// editor's side of the seam.
//
// Its own test rather than another entry in the loop above, because the reason
// differs and the reasons are the point. A worker and a server take these flags
// so a deployment can be configured; `flow lsp` takes them so that one person,
// on their own machine, can tell their editor about plugins they have installed
// — which is why the flag is the only way in and there is no configuration
// request or workspace setting that reaches the same code.
func TestTheLanguageServerTakesThePluginFlags(t *testing.T) {
	var cmd *cobra.Command
	for _, c := range newRootCommand().Commands() {
		if c.Name() == "lsp" {
			cmd = c

			break
		}
	}
	require.NotNil(t, cmd, "there is no lsp command")

	for _, name := range []string{"plugin-dir", "plugin", "plugin-scheme", "allow-insecure-plugin-dir"} {
		assert.NotNil(t, cmd.Flags().Lookup(name),
			"`flow lsp` does not take --%s, so an author cannot tell their editor "+
				"about a plugin their worker runs", name)
	}
}

// TestTheLanguageServerFailsLoudlyWhenAPluginWillNotStart.
//
// Degrading quietly to no plugins is the state this whole path exists to end: an
// editor underlining tasks that run perfectly well on a worker, with nothing
// anywhere saying why. So a pinned plugin with no binary behind it stops the
// command before a byte of protocol is exchanged, exactly as it stops a worker.
func TestTheLanguageServerFailsLoudlyWhenAPluginWillNotStart(t *testing.T) {
	cmd := &cobra.Command{}
	addOutputFlag(cmd)
	addPluginFlags(cmd)

	require.NoError(t, cmd.Flags().Set("plugin-dir", t.TempDir()))
	require.NoError(t, cmd.Flags().Set("plugin", "ghost"))

	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetContext(t.Context())

	err := runLSP(cmd, nil)
	require.Error(t, err,
		"a plugin the operator pinned was missing and the server started anyway, "+
			"which is the silent half-configured state this is meant to prevent")
	assert.Contains(t, err.Error(), "ghost",
		"the failure does not name the plugin that was not there")
}

// TestPluginCatalogRendersTheClaimsWithSecurityWeight is #712: needs_scope and
// secret_inputs change what a plugin task can see and receive, and neither
// appeared anywhere `flow plugins` printed. An operator deciding whether to trust
// a plugin had to read its source to find out.
//
// Checked in the words the issue asked for, because the two lines are the whole
// deliverable — a field that reached the wire and never reached a sentence an
// operator reads would repeat the bug one layer up.
func TestPluginCatalogRendersTheClaimsWithSecurityWeight(t *testing.T) {
	t.Parallel()

	catalog := &v1.PluginCatalog{
		Plugins: []*v1.PluginDescription{
			{
				Name: "git",
				Path: "/usr/local/bin/flowstate-plugin-git",
				Tasks: []*v1.TaskDescription{
					{
						Name:         "commit_push",
						Summary:      "writes a commit to a branch",
						SecretInputs: []string{"token"},
						NeedsScope:   false,
					},
					{
						Name:       "quiet_task",
						Summary:    "asks for nothing extra",
						NeedsScope: false,
					},
				},
			},
		},
	}

	var out bytes.Buffer
	surface := ui.Plain(&out, &bytes.Buffer{})

	require.NoError(t, writePluginCatalog(surface, catalog))

	rendered := out.String()
	assert.Contains(t, rendered, "accepts a secret in: token",
		"commit_push declares secret_inputs and the rendering does not say so")
	assert.Contains(t, rendered, "receives prior step outputs: no",
		"commit_push does not need scope and the rendering does not say so")

	// The negative direction: a task that declares neither claim gets no
	// "accepts a secret in" line at all, and its scope line reads "no".
	quietSection := rendered[strings.Index(rendered, "quiet_task"):]
	assert.NotContains(t, quietSection, "accepts a secret in:",
		"quiet_task declares no secret_inputs and the rendering invented one")
}
