package main

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// instants are the wall-clock times a local run stamps on its answer, the one
// thing two otherwise identical runs differ in.
var instants = regexp.MustCompile(`\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z`)

// TestMachineOutputIsUnchangedByTheDefaultFlip is the negative direction of
// making the full-screen debugger the default: every command line a script, a
// pipe or a machine format reaches prints exactly the bytes it printed before
// the screen existed, which is `--tui=false`, and says nothing about a screen it
// was never going to draw. The existing goldens of these fronts are the other
// half of the claim; this is the one that would fail if the default ever
// reached them.
func TestMachineOutputIsUnchangedByTheDefaultFlip(t *testing.T) {
	t.Parallel()

	address := heldServer(t)
	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("status\nbreakpoints\ndisconnect\n"), 0o600))
	workflow := writeRunLocalDebugFixture(t)
	suite := writeDebugFixture(t)
	attach := []string{"debug", "attach", "w", "--session", "held-1", "--address", address}

	for name, test := range map[string]struct {
		args  []string
		stdin string
	}{
		"attach --script":                {args: append(attach, "--script", script)},
		"attach --script -o jsonl":       {args: append(attach, "--script", script, "-o", "jsonl")},
		"attach --script -o json":        {args: append(attach, "--script", script, "-o", "json")},
		"attach piped stdin":             {args: attach, stdin: "status\nbreakpoints\ndisconnect\n"},
		"attach piped stdin -o jsonl":    {args: append(attach, "-o", "jsonl"), stdin: "status\ndisconnect\n"},
		"run local --debug piped":        {args: []string{"run", "local", workflow, "--debug"}, stdin: "step\ncontinue\n"},
		"run local --debug -o json":      {args: []string{"run", "local", workflow, "--debug", "-o", "json"}, stdin: "step\ncontinue\n"},
		"run local --debug -o jsonl":     {args: []string{"run", "local", workflow, "--debug", "-o", "jsonl"}, stdin: "step\ncontinue\n"},
		"run local --debug --reverse":    {args: []string{"run", "local", workflow, "--debug", "--reverse"}, stdin: "step\nback\ncontinue\n"},
		"test --debug piped":             {args: []string{"test", "--debug", "--run", "the debugged case", suite}, stdin: "step\nstep\n"},
		"debug replay":                   {args: []string{"debug", "replay", writeDebugScript(t, "step\ncontinue\n"), workflow}},
		"debug replay -o json":           {args: []string{"debug", "replay", writeDebugScript(t, "step\ncontinue\n"), workflow, "-o", "json"}},
		"run local piped, no --debug":    {args: []string{"run", "local", workflow}},
		"attach --history is a document": {args: append(attach, "--script", script, "--history", "--run-id", "r"), stdin: ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			run := func(extra ...string) flowResult {
				args := append(append([]string{}, test.args...), extra...)
				if test.stdin != "" {
					return runFlowStdin(t, test.stdin, args...)
				}

				return runFlow(t, args...)
			}

			asked, optedOut := run(), run("--tui=false")
			if name == "attach --history is a document" {
				// Refused the same way either way: the flip is not what decides it.
				assert.Equal(t, optedOut.Stderr, asked.Stderr)
				assert.Equal(t, optedOut.Err == nil, asked.Err == nil)

				return
			}
			require.NoError(t, asked.Err)
			require.NoError(t, optedOut.Err)
			require.NotEmpty(t, asked.Stdout+asked.Stderr, "the command printed nothing, so the equality below is vacuous")

			assert.Equal(t, instants.ReplaceAllString(optedOut.Stdout, "T"), instants.ReplaceAllString(asked.Stdout, "T"), "the default changed what stdout carries")
			assert.Equal(t, optedOut.Stderr, asked.Stderr, "the default changed what stderr carries")
			assert.NotContains(t, asked.Stderr, "--tui", "a default that declined said something")
		})
	}
}
