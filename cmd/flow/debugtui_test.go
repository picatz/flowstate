package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/debugtui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// `flow debug attach --tui` is opt-in, and where there is no terminal to draw on
// it is declined with a sentence and the attach is the one it would have been.

func heldServer(t *testing.T) string {
	t.Helper()

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(heldRun{}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv.URL
}

// TestTUIIsOptInAndDeclinedWithoutATerminal: the flag changes nothing on stdout
// where it cannot be honoured, says why on stderr, and its absence says nothing.
func TestTUIIsOptInAndDeclinedWithoutATerminal(t *testing.T) {
	t.Parallel()

	address := heldServer(t)
	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("status\nbreakpoints\ndisconnect\n"), 0o600))

	attach := func(extra ...string) flowResult {
		return runFlow(t, append([]string{"debug", "attach", "w", "--session", "held-1", "--address", address}, extra...)...)
	}

	for name, test := range map[string]struct {
		args   []string
		stdin  string
		reason string
	}{
		"a script":         {args: []string{"--script", script}, reason: "--script supplies the commands"},
		"a machine format": {args: []string{"--script", script, "-o", "jsonl"}, reason: "--script supplies the commands"},
		"jsonl, no script": {args: []string{"-o", "jsonl"}, stdin: "status\ndisconnect\n", reason: "a machine output format is not a screen"},
		"a pipe":           {stdin: "status\nbreakpoints\ndisconnect\n", reason: "stdin is not a terminal"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			run := func(extra ...string) flowResult {
				args := append(append([]string{"debug", "attach", "w", "--session", "held-1", "--address", address}, test.args...), extra...)
				if test.stdin != "" {
					return runFlowStdin(t, test.stdin, args...)
				}

				return runFlow(t, args...)
			}

			without := run()
			with := run("--tui")
			require.NoError(t, without.Err)
			require.NoError(t, with.Err)
			require.NotEmpty(t, without.Stdout, "the attach printed nothing, so the equality below is vacuous")

			assert.Equal(t, without.Stdout, with.Stdout, "--tui changed what a command with no terminal prints")
			assert.Contains(t, with.Stderr, "--tui is not used: "+test.reason)
			assert.Contains(t, with.Stderr, "using the line editor")
			assert.NotContains(t, without.Stderr, "--tui", "the notice was printed without the flag")
		})
	}

	// And with the flag spelled false it is the same as not spelling it.
	assert.Equal(t, attach("--script", script).Stdout, attach("--script", script, "--tui=false").Stdout)
}

func TestTUIDefaultsOff(t *testing.T) {
	t.Parallel()

	flag := flowCommand(t, "debug", "attach").Flags().Lookup("tui")
	require.NotNil(t, flag)
	assert.Equal(t, "false", flag.DefValue, "the full-screen debugger became the default")
	assert.Contains(t, flag.Usage, "60x12")
}

func TestOtherDebugCommandsHaveNoTUIFlag(t *testing.T) {
	t.Parallel()

	for _, path := range [][]string{{"debug", "get"}, {"debug", "do"}, {"debug", "history"}, {"debug", "replay"}} {
		assert.Nil(t, flowCommand(t, path...).Flags().Lookup("tui"), "%v", path)
	}
	assert.Equal(t, 60, debugtui.MinWidth)
}
