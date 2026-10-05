package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func execWorkflow(dir string) string {
	return `edition: v2026.4
name: exec-probe
steps:
  - id: say
    exec:
      argv: [echo, hello]
      dir: ` + dir + `
`
}

// writeExecPolicy writes a policy admitting echo under a fresh root and returns
// the policy path and a directory under that root.
func writeExecPolicy(t *testing.T) (policy, workdir string) {
	t.Helper()
	echo, err := exec.LookPath("echo")
	if err != nil {
		t.Skipf("echo is not installed: %v", err)
	}
	echo, err = filepath.EvalSymlinks(echo)
	require.NoError(t, err)
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)

	policy = filepath.Join(t.TempDir(), "exec-policy.yaml")
	require.NoError(t, os.WriteFile(policy, []byte("exec:\n  executables: {echo: "+echo+"}\n  roots: ["+root+"]\n  timeout: 30s\n  max_output_bytes: 64KiB\n"), 0o600))

	return policy, root
}

// TestExecIsDeniedUntilAPolicyEnablesIt is the default posture end to end: a
// worker or local run with no --exec-policy refuses the step, and the refusal
// names the flag that is the remedy. Not parallel: the flag-driven half
// replaces the process-wide task.
func TestExecIsDeniedUntilAPolicyEnablesIt(t *testing.T) {
	restoreDefaultRegistryAfter(t) // applyExecPolicy replaces the process-wide exec task.
	t.Setenv(v1.ExecPolicyEnv, "")

	policy, root := writeExecPolicy(t)

	_, stderr, err := runLocal(t, execWorkflow(root))
	require.Error(t, err)
	assert.Contains(t, stderr, "no exec policy")
	assert.Contains(t, stderr, "--exec-policy")
	assert.Contains(t, stderr, v1.ExecPolicyEnv)

	stdout, stderr, err := runLocal(t, execWorkflow(root), "--exec-policy", policy)
	require.NoError(t, err, stderr)

	var outputs struct {
		Steps map[string]map[string]any `json:"steps"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &outputs), stdout)
	assert.Equal(t, "hello\n", outputs.Steps["say"]["stdout"])
	assert.EqualValues(t, 0, outputs.Steps["say"]["exit_code"])
	assert.Equal(t, "ran", outputs.Steps["say"]["outcome"])
}

func TestAnExecPolicyThatDoesNotLoadRefusesTheCommand(t *testing.T) {
	restoreDefaultRegistryAfter(t) // applyExecPolicy replaces the process-wide exec task.

	for name, contents := range map[string]string{
		"unknown key":      "exec:\n  timeout: 30s\n  max_output_bytes: 64KiB\n  surprise: true\n",
		"no timeout":       "exec:\n  max_output_bytes: 64KiB\n",
		"missing program":  "exec:\n  executables: {x: /nonexistent/program}\n  timeout: 30s\n  max_output_bytes: 64KiB\n",
		"no exec section":  "{}\n",
		"timeout too long": "exec:\n  timeout: 2h\n  max_output_bytes: 64KiB\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "p.yaml")
			require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))

			cmd := &cobra.Command{Use: "worker"}
			addExecPolicyFlag(cmd)
			require.NoError(t, cmd.Flags().Set("exec-policy", path))

			err := applyExecPolicy(cmd)
			require.Error(t, err)
			assert.Contains(t, err.Error(), path)
		})
	}

	t.Run("oversized", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "p.yaml")
		require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{'#'}, v1.MaxExecPolicyBytes+1), 0o600))
		cmd := &cobra.Command{Use: "worker"}
		addExecPolicyFlag(cmd)
		require.NoError(t, cmd.Flags().Set("exec-policy", path))
		require.ErrorContains(t, applyExecPolicy(cmd), "limit")
	})

	t.Run("no flag leaves the task denied", func(t *testing.T) {
		cmd := &cobra.Command{Use: "worker"}
		addExecPolicyFlag(cmd)
		require.NoError(t, applyExecPolicy(cmd))
		def, ok := v1.DefaultRegistry().Lookup("exec")
		require.True(t, ok)
		_, err := def.Fn(t.Context(), nil, &v1.Scope{})
		require.ErrorContains(t, err, "--exec-policy")
	})
}

// TestTheExecFlagIsOnExactlyTheCommandsThatRunTasks pins which commands take
// the flag. `flow server` and `flow validate` never run a task, and `flow test`
// stubs every one, so a flag there would be one that does nothing.
func TestTheExecFlagIsOnExactlyTheCommandsThatRunTasks(t *testing.T) {
	t.Parallel()

	has := func(path ...string) bool {
		return flowCommand(t, path...).Flags().Lookup("exec-policy") != nil
	}

	for _, path := range [][]string{{"worker"}, {"run", "local"}, {"task", "run"}, {"server", "dev"}, {"mcp"}, {"dap"}} {
		assert.True(t, has(path...), "%v should take --exec-policy", path)
	}
	for _, path := range [][]string{{"server"}, {"validate"}, {"test"}} {
		assert.False(t, has(path...), "%v must not take --exec-policy", path)
	}
}

// The environment variable is spelled as a literal in this package, so the
// documentation test can see it is read; this keeps it the library's constant.
func TestTheExecPolicyEnvironmentVariableIsTheLibrarysName(t *testing.T) {
	t.Parallel()

	if execPolicyEnv != v1.ExecPolicyEnv {
		t.Fatalf("cmd/flow reads %q but the library documents %q", execPolicyEnv, v1.ExecPolicyEnv)
	}
}
