package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// picatz/flowstate#398: `flow lsp` and `flow mcp` greet a person who runs them at
// a terminal — following the root help's own example — with silence
// indistinguishable from a hang. The fix is a banner on stderr, gated on stdin
// being a terminal, and the property that matters most is the one a false
// positive would be worst at: the banner must never reach the protocol stream.
//
// [writeStdioBanner] takes "is this interactive" as a literal bool rather than
// deriving it, so the rule is testable without a pty — the same split
// cmd/flow/internal/ui draws between [ui.settledBackground] (tested directly) and
// the terminal query itself (not re-tested, because reaching it needs a real
// terminal a Go test does not have).

// TestWriteStdioBannerOnlyWhenInteractive pins the rule directly: the banner
// appears when told the stream is interactive, and stderr stays empty when told
// it is not — the two branches [stdinIsInteractive] chooses between in a real
// invocation.
func TestWriteStdioBannerOnlyWhenInteractive(t *testing.T) {
	var interactive bytes.Buffer
	writeStdioBanner(&interactive, true, lspBanner)
	assert.Equal(t, lspBanner, interactive.String(),
		"the banner did not appear when told the stream is interactive")

	var piped bytes.Buffer
	writeStdioBanner(&piped, false, lspBanner)
	assert.Empty(t, piped.String(),
		"the banner appeared when told the stream is not interactive, which is what "+
			"a real editor's or agent host's pipe answers")
}

// TestStdioBannerTextNamesTheProtocolAndHowToStop is a content pin: the two
// banners are the CLI's one teaching moment for someone who just typed the
// command and is looking at nothing, so each has to say what this is, that it is
// not interactive, where to look next, and how to get back to a prompt.
func TestStdioBannerTextNamesTheProtocolAndHowToStop(t *testing.T) {
	for _, test := range []struct {
		name     string
		banner   string
		verb     string
		protocol string
	}{
		{"lsp", lspBanner, "flow lsp", "Language Server Protocol"},
		{"mcp", mcpBanner, "flow mcp", "Model Context Protocol"},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Contains(t, test.banner, test.verb)
			assert.Contains(t, test.banner, test.protocol)
			assert.Contains(t, test.banner, "not interactive")
			assert.Contains(t, test.banner, "--help")
			assert.Contains(t, test.banner, "Ctrl-C")
			assert.True(t, strings.HasSuffix(test.banner, "\n"),
				"the banner should end its own line rather than run into whatever prints next")
		})
	}
}

// TestStdinIsInteractiveIsFalseForABuffer covers the shape every test in this
// package gives a command's input: a buffer or a strings.Reader, never a real
// terminal. [stdinIsInteractive] has to say false for that shape, or every test
// that calls through a command carrying no explicit --output would start
// growing an unwanted banner on stderr.
func TestStdinIsInteractiveIsFalseForABuffer(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.SetIn(strings.NewReader(""))

	assert.False(t, stdinIsInteractive(cmd))
}

// TestStdinIsInteractiveIsFalseForAPipe covers the shape a real client's stdin
// has: an *os.File, so the type assertion in [stdinIsInteractive] succeeds, but
// one end of a pipe rather than a terminal, so [term.IsTerminal] answers false.
// This is the closest a test in this repository can come to the positive path
// without a pty (see cmd/flow/internal/ui's own note on the same limit), and it
// is also the exact shape an editor or an agent host hands this process: proof
// that the real client's own stdin does not trip the gate.
func TestStdinIsInteractiveIsFalseForAPipe(t *testing.T) {
	read, write, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = read.Close()
		_ = write.Close()
	})

	cmd := &cobra.Command{}
	cmd.SetIn(read)

	assert.False(t, stdinIsInteractive(cmd),
		"one end of a pipe is not a terminal, which is what a real editor or agent host connects with")
}

// TestLSPBannerNeverReachesTheProtocolStreamWhenNotATTY is the mutation-proof
// case #398 asks for. It runs the real command against a piped stdin — the shape
// a real editor gives it — through the same failure path
// TestTheLanguageServerFailsLoudlyWhenAPluginWillNotStart already exercises (a
// pinned plugin with no binary, refused before a byte of protocol is read), and
// asserts stdout is empty both with the banner wiring present and with it
// removed: [writeStdioBanner] has no handle to stdout at all, so it cannot write
// to it no matter what interactive is computed as. What this actually catches is
// a *future* regression that started passing surface.Out (or cmd.OutOrStdout())
// into the banner call instead of surface.Err.
func TestLSPBannerNeverReachesTheProtocolStreamWhenNotATTY(t *testing.T) {
	read, write, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = read.Close()
		_ = write.Close()
	})

	cmd := &cobra.Command{}
	addOutputFlag(cmd)
	addPluginFlags(cmd)
	require.NoError(t, cmd.Flags().Set("plugin-dir", t.TempDir()))
	require.NoError(t, cmd.Flags().Set("plugin", "ghost"))

	var out, errOut bytes.Buffer
	cmd.SetIn(read)
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetContext(t.Context())

	err = runLSP(cmd, nil)
	require.Error(t, err, "a pinned plugin with no binary must still be refused")

	assert.Empty(t, out.String(),
		"stdout carried bytes before the JSON-RPC connection was even built; "+
			"the protocol stream must stay untouched by anything but the connection itself")
}
