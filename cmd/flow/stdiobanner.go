package main

import (
	"fmt"
	"io"
	"os"

	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"
)

// `flow lsp` and `flow mcp` speak a machine protocol over stdin and stdout, and
// answer nothing at all until a real client — an editor, an agent host — writes to
// them. The root help's own example block invites the interactive attempt
// (`flow lsp`, `flow mcp`), and the person who follows it gets what reads exactly
// like a hung program: no prompt, no output, the cursor parked (picatz/flowstate#398).
//
// The fix is one line on stderr, gated on stdin being a terminal a person is
// typing at rather than a pipe a client attached. That gate is the whole safety
// property this file exists to keep: an editor or agent host spawns the process
// with a pipe for stdin, never a terminal, so the banner cannot fire for a real
// client, and it is written to stderr, which neither protocol reads, so it cannot
// land in the middle of a document a client is parsing even if the gate were
// somehow wrong.
//
// The gate and the message are deliberately two functions. [term.IsTerminal] needs
// a real terminal to answer true, which a Go test does not have without a pty — the
// same limit [ui.Detect] documents on its own TTY check — so the *rule* about what
// to write is tested directly against a literal true/false ([writeStdioBanner]),
// and the *detection* ([stdinIsInteractive]) is exercised in a real invocation
// against a pipe, which is exactly the shape both a test and a real client's stdin
// have: not a terminal, so the banner never fires and the protocol stream is
// provably untouched.

// lspBanner is what a person sees if they run `flow lsp` at a terminal, following
// the root help's own example.
const lspBanner = "flow lsp speaks the Language Server Protocol over stdio and is waiting for an\n" +
	"editor to connect. It is not interactive: configure your editor to launch it,\n" +
	"or see `flow lsp --help` for setup. Ctrl-C exits.\n"

// mcpBanner is the same note for `flow mcp`, naming the protocol and the client an
// agent host is.
const mcpBanner = "flow mcp speaks the Model Context Protocol over stdio and is waiting for an\n" +
	"agent host to connect. It is not interactive: configure your agent host to\n" +
	"launch it, or see `flow mcp --help` for setup. Ctrl-C exits.\n"

// writeStdioBanner writes banner to stderr when interactive is true, and does
// nothing at all otherwise.
//
// interactive is passed in rather than computed here so the rule is testable
// without a terminal: a test asserts the banner appears when interactive is
// literally true and that stderr stays empty when it is literally false, which is
// the same split [ui.darkBackground] draws between the rule ([ui.settledBackground],
// tested directly) and the question that needs a real terminal to answer
// ([term.IsTerminal] itself, not re-tested here).
func writeStdioBanner(stderr io.Writer, interactive bool, banner string) {
	if !interactive {
		return
	}

	fmt.Fprint(stderr, banner)
}

// stdinIsInteractive reports whether the command's input stream is a terminal a
// person is typing at, as opposed to a pipe a client attached or a buffer a test
// supplied.
//
// Checked against cmd.InOrStdin() rather than os.Stdin directly, the same reason
// [newSurface] checks cmd.OutOrStdout()/cmd.ErrOrStderr() rather than the process
// streams: a test that has replaced the command's input with a buffer must get a
// real answer (false, since a buffer is never a terminal) rather than this
// function silently asking the *process's* stdin, which would answer a question
// about whoever is running the test suite instead of about the invocation under
// test.
func stdinIsInteractive(cmd *cobra.Command) bool {
	stdin, ok := cmd.InOrStdin().(*os.File)
	if !ok {
		return false
	}

	return term.IsTerminal(stdin.Fd())
}
