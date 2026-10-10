package main

import (
	"bytes"
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

func digestOf(b string) string { return "sha256:" + strings.Repeat(b, 32) }

func catalogOf(pairs ...string) *v1.PluginCatalog {
	catalog := &v1.PluginCatalog{}
	for i := 0; i+1 < len(pairs); i += 2 {
		catalog.Plugins = append(catalog.Plugins, &v1.PluginDescription{Name: pairs[i], DistributionDigest: pairs[i+1]})
	}

	return catalog
}

func writePins(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "plugin-pins.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	return path
}

// TestEmittedPluginPinsRoundTripThroughThePinLoader proves the emitted file is
// the format --plugin-pins reads, by reading it with that loader.
func TestEmittedPluginPinsRoundTripThroughThePinLoader(t *testing.T) {
	t.Parallel()

	measured, err := measuredPluginDigests(catalogOf("github", digestOf("ab"), "slack", digestOf("cd")))
	require.NoError(t, err)

	var out bytes.Buffer
	require.NoError(t, writePluginPins(ui.Plain(&out, &bytes.Buffer{}), measured))

	assert.Contains(t, out.String(), "trust on first use")

	loaded, err := pluginPinsOf(writePins(t, out.String()), nil)
	require.NoError(t, err)
	assert.Equal(t, measured, loaded)
	assert.Empty(t, diffPluginPins(loaded, measured).drifted(), "a fresh emit has no drift against itself")
}

func TestAnEmptyEmitIsStillALoadablePinsFile(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, writePluginPins(ui.Plain(&out, &bytes.Buffer{}), nil))

	loaded, err := pluginPinsOf(writePins(t, out.String()), nil)
	require.NoError(t, err)
	assert.Empty(t, loaded)
}

func TestAPluginWithNoMeasuredDigestIsNotSilentlyOmitted(t *testing.T) {
	t.Parallel()

	_, err := measuredPluginDigests(catalogOf("bare", ""))
	require.ErrorContains(t, err, "bare")
}

func TestPluginPinDriftInEachDirection(t *testing.T) {
	t.Parallel()

	pinned := map[string]string{"same": digestOf("11"), "swapped": digestOf("22"), "gone": digestOf("33")}
	measured := map[string]string{"same": digestOf("11"), "swapped": digestOf("44"), "new": digestOf("55")}

	drift := diffPluginPins(pinned, measured)
	assert.Equal(t, []string{"new"}, drift.added)
	assert.Equal(t, []pluginPinChange{{"swapped", digestOf("22"), digestOf("44")}}, drift.changed)
	assert.Equal(t, []string{"gone"}, drift.missing)
	assert.Equal(t, 1, drift.matched)

	var out bytes.Buffer
	drift.write(ui.Plain(&out, &bytes.Buffer{}))
	rendered := out.String()
	assert.Contains(t, rendered, "changed  swapped")
	assert.Contains(t, rendered, digestOf("22"))
	assert.Contains(t, rendered, digestOf("44"))
	assert.Contains(t, rendered, "added    new")
	assert.Contains(t, rendered, "missing  gone")
	require.ErrorContains(t, drift.err(), "1 changed, 1 added, 1 missing")

	for name, test := range map[string]struct {
		pinned, measured map[string]string
		want             string
	}{
		"added only":   {nil, map[string]string{"a": digestOf("66")}, "1 added"},
		"missing only": {map[string]string{"a": digestOf("66")}, nil, "1 missing"},
		"changed only": {map[string]string{"a": digestOf("66")}, map[string]string{"a": digestOf("77")}, "1 changed"},
	} {
		assert.ErrorContains(t, diffPluginPins(test.pinned, test.measured).err(), test.want, name)
	}

	assert.NoError(t, diffPluginPins(measured, measured).err())
}

func pinReportCommand(t *testing.T, flagArgs ...string) (*cobra.Command, *bytes.Buffer) {
	t.Helper()

	var out bytes.Buffer

	cmd := &cobra.Command{}
	addOutputFlag(cmd)
	addPluginFlags(cmd)
	addPluginPinReportFlags(cmd)
	require.NoError(t, cmd.Flags().Parse(flagArgs))
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetContext(t.Context())

	return cmd, &out
}

// TestPluginsDiffPinsReportsDriftThroughTheCommand drives runPlugins with a
// real, empty plugin directory: the pinned plugin is not there, so the command
// must report it and exit non-zero rather than refuse to launch.
func TestPluginsDiffPinsReportsDriftThroughTheCommand(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pins := writePins(t, "pins:\n  github: "+digestOf("ab")+"\n")

	cmd, out := pinReportCommand(t, "--plugin-dir", dir, "--diff-pins", pins)
	err := runPlugins(cmd, nil)
	require.ErrorContains(t, err, "1 missing")
	assert.Contains(t, out.String(), "missing  github")

	cmd, out = pinReportCommand(t, "--plugin-dir", dir, "--diff-pins", writePins(t, "pins: {}\n"))
	require.NoError(t, runPlugins(cmd, nil))
	assert.Contains(t, out.String(), "plugin pins match")

	cmd, out = pinReportCommand(t, "--plugin-dir", dir, "--emit-pins")
	require.NoError(t, runPlugins(cmd, nil))
	assert.Contains(t, out.String(), "pins: {}")
}

func TestPluginPinReportFlagsConflict(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	cmd, _ := pinReportCommand(t, "--plugin-dir", dir, "--emit-pins", "--diff-pins", "x")
	err := runPlugins(cmd, nil)
	require.ErrorContains(t, err, "pass one")
	assert.True(t, isUsageError(err))

	cmd, _ = pinReportCommand(t, "--plugin-dir", dir, "--emit-pins", "-o", "json")
	err = runPlugins(cmd, nil)
	require.ErrorContains(t, err, "no --output format")
	assert.True(t, isUsageError(err))
}
