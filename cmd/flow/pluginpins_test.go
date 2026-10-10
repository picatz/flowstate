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
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

func digestOf(b string) string { return "sha256:" + strings.Repeat(b, 32) }

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

	measured := map[string]string{"github": digestOf("ab"), "slack": digestOf("cd")}

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

	for _, format := range []string{"json", "text"} {
		cmd, _ = pinReportCommand(t, "--plugin-dir", dir, "--emit-pins", "-o", format)
		err = runPlugins(cmd, nil)
		require.ErrorContains(t, err, "no --output format", format)
		assert.True(t, isUsageError(err))
	}
}

func TestPluginPinReportRefusesPinsAppliedToTheMeasurement(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	for _, pin := range [][]string{
		{"--plugin-pin", "example=sha256:" + strings.Repeat("a", 64)},
		{"--plugin-pins", "pins.yaml"},
	} {
		args := append([]string{"--plugin-dir", dir, "--emit-pins"}, pin...)
		cmd, _ := pinReportCommand(t, args...)
		err := runPlugins(cmd, nil)
		require.ErrorContains(t, err, "measure with no pins applied")
		assert.True(t, isUsageError(err))
	}
}

func TestPluginPinReportNeedsAPluginDirectory(t *testing.T) {
	t.Setenv("FLOWSTATE_PLUGIN_DIR", "")

	for _, flag := range [][]string{{"--emit-pins"}, {"--diff-pins", "pins.yaml"}} {
		cmd, _ := pinReportCommand(t, flag...)
		err := runPlugins(cmd, nil)
		require.ErrorContains(t, err, "need a plugin directory")
		assert.True(t, isUsageError(err))
	}
}

func TestPluginsDiffPinsRefusesAMalformedPinsFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	for name, body := range map[string]string{
		"bad name":   "pins:\n  GitHub: " + digestOf("ab") + "\n",
		"bad digest": "pins:\n  github: sha256:tooshort\n",
	} {
		cmd, _ := pinReportCommand(t, "--plugin-dir", dir, "--diff-pins", writePins(t, body))
		err := runPlugins(cmd, nil)
		require.ErrorIs(t, err, plugin.ErrDigestPin, name)
	}
}

// TestPluginPinReportMeasuresWithoutLaunching uses a file that cannot run: were
// the plugin launched the command would fail, so success shows it was only
// hashed, and a rewrite of it shows as drift.
func TestPluginPinReportMeasuresWithoutLaunching(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))

	bin := filepath.Join(dir, plugin.BinaryPrefix+"alpha")
	require.NoError(t, os.WriteFile(bin, []byte("not a program v1"), 0o700))

	cmd, out := pinReportCommand(t, "--plugin-dir", dir, "--emit-pins")
	require.NoError(t, runPlugins(cmd, nil))
	pins := writePins(t, out.String())

	cmd, out = pinReportCommand(t, "--plugin-dir", dir, "--diff-pins", pins)
	require.NoError(t, runPlugins(cmd, nil))
	assert.Contains(t, out.String(), "plugin pins match: 1")

	require.NoError(t, os.WriteFile(bin, []byte("swapped"), 0o700))

	cmd, out = pinReportCommand(t, "--plugin-dir", dir, "--diff-pins", pins)
	err := runPlugins(cmd, nil)
	require.ErrorContains(t, err, "1 changed")
	assert.Contains(t, out.String(), "changed  alpha")
}
