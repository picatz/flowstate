package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The built-in `exec` task is denied until a deployment says otherwise, and this
// is how it says so: a YAML policy file loaded before anything starts and
// registered over the denied task, the way [applyEgressPolicy] registers an
// egress policy over the built-in http task.
//
// Taken by the commands that run tasks: `flow worker`, `flow run local`, `flow
// task run`, `flow server dev` (whose embedded worker is a real one), `flow mcp`
// and `flow dap`. Not by `flow server` and `flow validate`, for the reason
// egress.go states: they never run a task, a diagnostic drawn from deployment
// configuration would tell an author their file is wrong on settings the machine
// they are typing on may not share, and a flag that does nothing teaches
// operators it does something. `flow test` does not take it either — it replaces
// every task with a stub, so no program is ever started there.
//
// With no file the task stays denied, and the denial names this flag. That is
// the default and the safe direction: only an operator-supplied file enables it
// (by the flag or its environment variable), there is no loopback-style shortcut, and nothing a workflow can write.

// execPolicyEnv names the policy file the same way the flag does, for a
// container image that bakes configuration into the environment.
const execPolicyEnv = "FLOWSTATE_EXEC_POLICY"

// addExecPolicyFlag declares --exec-policy on a command.
func addExecPolicyFlag(cmd *cobra.Command) {
	cmd.Flags().String("exec-policy", os.Getenv(execPolicyEnv),
		"path to an exec policy (YAML) enabling the built-in exec task (default $"+execPolicyEnv+"); "+
			"unset, every exec step is denied. The file lists the programs a workflow may name, the directory "+
			"roots they may run in, the environment they see, and the time and output bounds; it is an "+
			"allowlist of what may be started, not a sandbox: a started program runs with this process's "+
			"privileges and is not confined by the egress policy")
}

// applyExecPolicy loads the configured policy file and registers the exec task
// enforcing it, replacing the denied built-in.
//
// With no file configured it does nothing: the built-in task keeps denying. With
// a file, the file is the whole policy. Every failure refuses the command,
// because a worker that started anyway would deny every exec step while its
// operator believes the file enabled them, or, worse, enforce half a file. The
// executables the file names are verified here, at start-up, so a missing or
// world-writable program is the operator's error at deploy time and not a
// workflow's failure at three in the morning.
func applyExecPolicy(cmd *cobra.Command) error {
	path, _ := cmd.Flags().GetString("exec-policy")
	if path == "" {
		return nil
	}

	data, err := readBoundedFile(path, "an exec policy", v1.MaxExecPolicyBytes)
	if err != nil {
		return fmt.Errorf("reading exec policy: %w", err)
	}

	policy, err := v1.ParseExecPolicy(data)
	if err != nil {
		return fmt.Errorf("exec policy %s: %w", path, err)
	}

	if err := v1.DefaultRegistry().Replace(v1.ExecTaskDef(policy)); err != nil {
		return fmt.Errorf("registering the exec task for exec policy %s: %w", path, err)
	}

	return nil
}
