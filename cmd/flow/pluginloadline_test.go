package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestVerbsThatRunNothingLoadPluginsQuietly pins which verbs print the
// "loaded plugin" line (#1673): `run local` says on stderr what it loaded,
// since a step failing with `unknown task` and a process that found no plugin
// look identical from a Flowfile; `validate` runs nothing and says it at
// debug, so its transcript is the whole of what it prints.
func TestVerbsThatRunNothingLoadPluginsQuietly(t *testing.T) {
	// Through the built binary: the load line is written by the process's
	// infrastructure logger straight to its stderr, which the in-process
	// harness does not capture.
	bin := buildFlowBinary(t)
	dir := buildExamplePluginDir(t)

	output, err := runFlowCapturing(t, bin, "validate", "--plugin-dir", dir, exampleGreetWorkflow)
	require.NoError(t, err, output)
	require.NotContains(t, output, "loaded plugin",
		"validate runs nothing, so the load line belongs to the debug stream")

	// Debug is a stream, not a bin: the reader who asks for it gets the line.
	output, err = runFlowCapturing(t, bin, "validate", "--verbose", "--plugin-dir", dir, exampleGreetWorkflow)
	require.NoError(t, err, output)
	require.Contains(t, output, "loaded plugin", "--verbose asked for the debug stream")

	// The example plugin claims a secret scheme, so a run needs the policy
	// that authorizes reading it and the secret itself, the same way the
	// run-local plugin tests do.
	t.Setenv("FLOWSTATE_SECRET_GREET_TOKEN", "hunter2")
	output, err = runFlowCapturing(t, bin, "run", "local", exampleGreetWorkflow,
		"--plugin-dir", dir,
		"--secret-env", "GREET_TOKEN",
		"--auth-policy", localSecretPolicy(t))
	require.NoError(t, err, output)
	require.Contains(t, output, "loaded plugin", "run local says what it loaded")
	require.Contains(t, output, "example.greet")
}
