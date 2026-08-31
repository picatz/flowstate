package main

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExamplesREADMEFirstRunCommands executes the short, offline path promised
// to somebody starting from a repository checkout. The marked block is narrow on
// purpose: durable server commands and plugin prerequisites do not belong in a
// unit test, while a stale verb or flag in the first four commands should fail on
// the same command tree a user reaches.
func TestExamplesREADMEFirstRunCommands(t *testing.T) {
	root := "../.."
	data, err := os.ReadFile(root + "/examples/README.md")
	require.NoError(t, err)

	_, section, ok := strings.Cut(string(data), "<!-- first-run-smoke:start -->")
	require.True(t, ok, "examples/README.md lost the first-run smoke start marker")
	section, _, ok = strings.Cut(section, "<!-- first-run-smoke:end -->")
	require.True(t, ok, "examples/README.md lost the first-run smoke end marker")

	const prefix = "$ go run ./cmd/flow "
	var commands [][]string
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(line, "$ ") {
			continue
		}
		require.True(t, strings.HasPrefix(line, prefix),
			"first-run command %q is not runnable from a repository checkout", line)

		invocation := strings.TrimPrefix(line, prefix)
		if before, after, found := strings.Cut(invocation, " | "); found {
			require.Equal(t, "jq -r .name", after,
				"the first-run compile pipeline gained an untested shell command")
			invocation = before
		}
		args := strings.Fields(invocation)
		require.NotEmpty(t, args, "first-run command %q has no flow invocation", line)
		commands = append(commands, args)
	}

	require.Equal(t, 4, len(commands),
		"the first-run block must stay a bounded validate/compile/test/local journey")
	assert.Equal(t, []string{"validate", "compile", "test", "run"}, []string{
		commands[0][0], commands[1][0], commands[2][0], commands[3][0],
	})
	require.GreaterOrEqual(t, len(commands[3]), 2, "the first run must name its execution venue")
	require.Equal(t, "local", commands[3][1], "the first run must remain local and offline")

	t.Chdir(root)
	for _, args := range commands {
		result := runFlow(t, args...)
		require.NoError(t, result.Err, "documented `flow %s` failed:\n%s", strings.Join(args, " "), result.Stderr)
	}
}
