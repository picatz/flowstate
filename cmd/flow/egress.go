package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// The netpolicy package had twenty-two options and one production lever: an
// environment variable that flips loopback. Everything else — CEL rules, CIDR
// lists, port rules, the body cap — was reachable only from Go, which for an
// operator running `flow worker` is not reachable at all. This file is the
// surface: a YAML policy file, loaded before anything starts, registered over the
// built-in http task the same way a plugin's tasks are registered over nothing.
//
// The mechanism is [v1.HTTPTaskDef]: registering it into [v1.DefaultRegistry]
// replaces the built-in http task with one enforcing the given policy, and every
// lookup the engine makes goes through that registry. Registration happens before
// the worker polls and before a local run executes, so a policy that cannot load
// refuses the command instead of governing some steps and not others.
//
// `flow run local` takes the same flag because local runs exist to tell an author
// what production will do: rehearsing under the policy the workers run is the
// point of rehearsing at all.
//
// `flow server` deliberately does not take it. The server consults the task
// registry only for Validate and GetCatalog, and validation asks the *task* what
// it can never do — [v1.TaskDef.CheckLiteral] takes no policy, by design, because
// a diagnostic drawn from deployment configuration would tell an author their
// file is wrong on the strength of settings the machine they are typing on may
// not share. A policy registered on the server would change nothing the server
// answers, and a flag that does nothing teaches operators it does something.

// egressPolicyEnv names the policy file the same way a flag does, for a container
// image that bakes configuration into the environment rather than into every
// command line — the same split --plugin-dir and FLOWSTATE_PLUGIN_DIR have.
const egressPolicyEnv = "FLOWSTATE_EGRESS_POLICY"

// addEgressPolicyFlag declares --egress-policy on a command.
func addEgressPolicyFlag(cmd *cobra.Command) {
	cmd.Flags().String("egress-policy", os.Getenv(egressPolicyEnv),
		"path to an egress policy (YAML) governing the http task (default $"+egressPolicyEnv+"); "+
			"when set it replaces the default policy entirely, and "+v1.AllowLoopbackEgressEnv+
			" is ignored — a file that wants loopback says allow_loopback: true")
}

// applyEgressPolicy loads the configured policy file and registers the http task
// enforcing it, replacing the built-in one.
//
// With no file configured it does nothing: the built-in task keeps the default
// policy, with [v1.AllowLoopbackEgressEnv] as its one lever. With a file, the
// file is the whole policy — precedence is replacement, not merging, because a
// policy assembled from two places is a policy nobody can read in either.
//
// Every failure refuses the command. A worker that started anyway would run the
// default policy while its operator believes the file applies, which is the
// fail-open this flag exists to prevent. The file's path is on every error;
// [netpolicy.New] already names the rule and the compile problem.
func applyEgressPolicy(cmd *cobra.Command) error {
	path, _ := cmd.Flags().GetString("egress-policy")
	if path == "" {
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading egress policy: %w", err)
	}

	cfg, err := netpolicy.ParseConfig(data)
	if err != nil {
		return fmt.Errorf("parsing egress policy %s: %w", path, err)
	}

	policy, err := cfg.Policy()
	if err != nil {
		return fmt.Errorf("egress policy %s: %w", path, err)
	}

	if err := v1.DefaultRegistry().Register(v1.HTTPTaskDef(policy)); err != nil {
		return fmt.Errorf("registering the http task for egress policy %s: %w", path, err)
	}

	return nil
}
