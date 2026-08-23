package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/internal/covbuild"
)

// `--no-color` has to reach the exact plumbing NO_COLOR already goes through
// rather than a second one invented beside it — see [environForSurface]. Two
// things follow, and both are worth a test of their own: the flag really does
// fold into the environment [ui.Detect] resolves from, and — because that is
// easy to get backwards — a `false` value must not inject `NO_COLOR=1` on every
// command that has never heard of the flag.

// TestEnvironForSurfaceAddsNoColorOnlyWhenAsked is the unit-level proof.
func TestEnvironForSurfaceAddsNoColorOnlyWhenAsked(t *testing.T) {
	newCmdWithFlag := func(set bool) *cobra.Command {
		cmd := &cobra.Command{Use: "x", RunE: func(*cobra.Command, []string) error { return nil }}
		cmd.Flags().Bool("no-color", set, "")
		return cmd
	}

	environ := environForSurface(newCmdWithFlag(true))
	assert.Contains(t, environ, "NO_COLOR=1",
		"--no-color=true did not fold NO_COLOR into the environment ui.Detect reads")

	environ = environForSurface(newCmdWithFlag(false))
	assert.NotContains(t, environ, "NO_COLOR=1",
		"a command that was never asked for --no-color injected NO_COLOR anyway, "+
			"which would disable colour for every command in the tree")
}

// TestEnvironForSurfaceWinsOverAnExportedNoColor proves the precedence
// docs/CLI.md and the flag's own help both promise: the flag is the most explicit
// ask, so it must not lose to whatever the shell already exported.
//
// It has to *win*, not merely agree — appending after os.Environ() is what makes
// that true, since colorprofile's own environ map takes the last entry for a
// repeated key. A version that appended before, or that skipped entirely because
// NO_COLOR was already unset, would pass a test that only checked "NO_COLOR ends up
// present" without checking which value survives.
func TestEnvironForSurfaceWinsOverAnExportedNoColor(t *testing.T) {
	t.Setenv("NO_COLOR", "")

	cmd := &cobra.Command{Use: "x", RunE: func(*cobra.Command, []string) error { return nil }}
	cmd.Flags().Bool("no-color", true, "")

	environ := environForSurface(cmd)

	// The flag's entry has to be the one a scan of the slice finds last, which is
	// the one colorprofile's own environ map keeps.
	var lastNoColor string
	for _, entry := range environ {
		if rest, ok := strings.CutPrefix(entry, "NO_COLOR="); ok {
			lastNoColor = rest
		}
	}
	assert.Equal(t, "1", lastNoColor,
		"the flag's NO_COLOR=1 did not win over the exported (empty) NO_COLOR")
}

// TestNoColorFlagSuppressesColourThroughTheRealBinary is the end-to-end proof:
// the real plumbing, not a unit test of the function that feeds it.
//
// TTY_FORCE=1 is colorprofile's own escape hatch for exactly this situation — a
// test with no pty to hand — and it is read by the same colorprofile.Detect call
// [ui.New] already makes, so this is not a second detection path invented for the
// test. With it, `flow validate` over a pipe renders as though attached to a
// colour-capable terminal, which is what makes "and --no-color turns it back off"
// a real assertion instead of one that would have passed anyway because a pipe is
// never coloured.
func TestNoColorFlagSuppressesColourThroughTheRealBinary(t *testing.T) {
	bin := buildFlowBinary(t)

	path := filepath.Join(t.TempDir(), "broken.yaml")
	require.NoError(t, os.WriteFile(path, []byte(brokenWorkflow), 0o600))

	// GOCOVERDIR set explicitly (see internal/covbuild), for the same reason
	// runFlow sets it: os.Environ() alone can carry a GOCOVERDIR that belongs
	// to `go test -cover`'s own internal bookkeeping for this process, not the
	// directory a merge ever reads counters back out of.
	env := append(append(os.Environ(), "TTY_FORCE=1", "TERM=xterm-256color"), covbuild.Env()...)

	withColor := exec.Command(bin, "validate", path)
	withColor.Env = env
	coloredOut, _ := withColor.CombinedOutput()
	require.Contains(t, string(coloredOut), "\x1b[",
		"the forced-terminal environment did not produce any colour to suppress, "+
			"so this test cannot prove --no-color turns it off:\n%s", coloredOut)

	withoutColor := exec.Command(bin, "--no-color", "validate", path)
	withoutColor.Env = env
	plainOut, _ := withoutColor.CombinedOutput()
	assert.NotContains(t, string(plainOut), "\x1b[",
		"--no-color did not suppress an escape sequence on a stream it should have "+
			"reached:\n%s", plainOut)

	// And the message itself has to still be there — --no-color removes colour,
	// never information.
	assert.Contains(t, string(plainOut), path,
		"--no-color removed content along with colour")
}
