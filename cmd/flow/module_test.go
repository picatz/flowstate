package main

import (
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// moduleFile is a module in canonical form, so `flow fmt --check` accepts it as
// written.
const moduleFile = `edition: v2026.4
name: ids
types:
  Uuid:
    type: string
    must: isUuid(this)
errors:
  NotFound:
    description: The customer does not exist.
functions:
  isUuid:
    params:
      s: string
    returns: bool
    body: ${s.matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")}
`

func writeModuleFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ids.yaml")
	require.NoError(t, os.WriteFile(path, []byte(moduleFile), 0o600))
	return path
}

// TestAModuleIsAcceptedByTheAuthoringCommands states the positive half: a module
// validates, formats to itself and lints clean, as a first-class file.
func TestAModuleIsAcceptedByTheAuthoringCommands(t *testing.T) {
	t.Parallel()

	path := writeModuleFile(t)
	for _, args := range [][]string{
		{"validate", path},
		{"fmt", "--check", path},
		{"lint", "--strict", path},
		{"validate", filepath.Dir(path)},
		{"fmt", "--check", filepath.Dir(path)},
	} {
		res := runFlow(t, args...)
		assert.NoError(t, res.Err, "%v: %s%s", args, res.Stdout, res.Stderr)
	}
}

// TestAModuleIsRefusedWhereAWorkflowWouldRun is the negative half: every command
// that would execute or compile the file for submission names it a module and
// does not run it.
func TestAModuleIsRefusedWhereAWorkflowWouldRun(t *testing.T) {
	t.Parallel()

	path := writeModuleFile(t)
	for _, args := range [][]string{
		{"run", "local", path},
		{"compile", path},
		{"run", path},
	} {
		res := runFlow(t, args...)
		require.Error(t, res.Err, "%v ran a module", args)
		assert.Contains(t, res.Stdout+res.Stderr+res.Err.Error(),
			"is a module (no steps); import it with use:, don't run it", "%v", args)
	}
}

// TestTheMCPRunToolRefusesAModule covers the one entry that takes source
// instead of a path.
func TestTheMCPRunToolRefusesAModule(t *testing.T) {
	t.Parallel()

	_, err := parseFlowfileSource([]byte(moduleFile))
	require.Error(t, err)
	assert.ErrorIs(t, err, v1.ErrModule)
}
