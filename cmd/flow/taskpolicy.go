package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// #187 slice 1: a worker-side task-shape policy over which identities may
// dispatch which tasks, mirroring `--egress-policy` (egress.go) almost
// exactly. Where it diverges, the divergence is load-bearing:
//
// Egress policy attaches to *one* task by re-registering [v1.HTTPTaskDef]
// over the http task. Task-shape policy is a predicate over every dispatch,
// so it cannot ride on one task's registration — it is installed process-wide
// via [v1.SetDefaultTaskPolicy], consulted at the shared seam every task's
// call funnels through ([v1.Task.EvalInScope]/[v1.CheckTaskPolicy]), before
// `flow worker` polls or `flow run local` runs.
//
// `flow server` and `flow validate` deliberately do not take this flag, for
// the reason `egress.go` already states for the egress policy: a diagnostic
// drawn from deployment configuration would tell an author their file is
// wrong on the strength of settings the machine they are typing on may not
// share. A task-policy denial is a deployment refusal surfaced at dispatch,
// never a file diagnostic.

// taskPolicyEnv names the policy file the same way a flag does, mirroring
// [egressPolicyEnv] for a container image that bakes configuration into the
// environment rather than into every command line.
const taskPolicyEnv = "FLOWSTATE_TASK_POLICY"

// addTaskPolicyFlag declares --task-policy on a command.
func addTaskPolicyFlag(cmd *cobra.Command) {
	cmd.Flags().String("task-policy", os.Getenv(taskPolicyEnv),
		"path to a task-shape policy (YAML) governing which identities may dispatch which "+
			"tasks (default $"+taskPolicyEnv+"); with nothing configured, every task dispatches "+
			"exactly as it does today — see #187")
}

// applyTaskPolicy loads the configured policy file and installs it as the
// process-wide task-shape policy, replacing whatever was installed before.
//
// With no file configured it does nothing: dispatch stays unrestricted, the
// zero case [v1.TaskPolicy.Check] documents. With a file, the file is the
// whole policy — precedence is replacement, not merging, for the identical
// reason [applyEgressPolicy] replaces rather than merges: a policy assembled
// from two places is a policy nobody can read in either.
//
// Every failure refuses the command. A worker that started anyway would
// dispatch every task unrestricted while its operator believes the file
// applies — the fail-open this flag exists to prevent.
func applyTaskPolicy(cmd *cobra.Command) error {
	path, _ := cmd.Flags().GetString("task-policy")
	if path == "" {
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading task-shape policy: %w", err)
	}

	cfg, err := v1.ParseTaskPolicyConfig(data)
	if err != nil {
		return fmt.Errorf("parsing task-shape policy %s: %w", path, err)
	}

	policy, err := cfg.Policy()
	if err != nil {
		return fmt.Errorf("task-shape policy %s: %w", path, err)
	}

	v1.SetDefaultTaskPolicy(policy)

	return nil
}
