package main

import (
	"errors"
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
			" is ignored; a file that wants loopback says allow_loopback: true")
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

// hasEgressPolicyFile reports whether cmd was given an explicit --egress-policy
// (or its FLOWSTATE_EGRESS_POLICY default), which is exactly the condition
// [wrapLoopbackDenial] needs to stay silent under: an operator's own policy
// file, not the built-in default, decided the denial, so the remedy is that
// file, not an environment variable that would read as an invitation to
// bypass it. See #387.
func hasEgressPolicyFile(cmd *cobra.Command) bool {
	path, _ := cmd.Flags().GetString("egress-policy")
	return path != ""
}

// wrapLoopbackDenial adds the loopback opt-in to a run's own denial, and only
// there: the built-in default policy refuses loopback fail-closed, and the
// two ways to say "this is my own machine" (the env var, and
// `allow_loopback: true` in a policy file) already ship in `flow run local
// --help`, so a denial withholding them makes the CLI hide an answer it
// already has (#387). The default policy is the only case where teaching the
// bypass is right: it exists precisely because the caller is presumed to be
// the machine's own owner, which is not true of an operator's explicit
// --egress-policy; see [hasEgressPolicyFile].
//
// err is returned unchanged whenever the denial is not a loopback address
// denial from the default policy, so a caller can wrap every run error
// through this unconditionally rather than duplicating the check at each
// call site.
func wrapLoopbackDenial(cmd *cobra.Command, err error) error {
	if err == nil || hasEgressPolicyFile(cmd) {
		return err
	}

	var denied *netpolicy.DenyError
	if !errors.As(err, &denied) {
		return err
	}
	if denied.Reason != netpolicy.ReasonAddress || denied.Detail != "loopback addresses are not allowed" {
		return err
	}

	return &loopbackDenialError{target: denied.Target, err: err}
}

// loopbackDenialError adds the default policy's own opt-ins to a loopback
// denial, as the one suggested-next-step element every other CLI remedy uses
// (see [commandSuggester]), rather than a second way of saying "try this".
type loopbackDenialError struct {
	target string
	err    error
}

func (e *loopbackDenialError) Error() string { return e.err.Error() }

func (e *loopbackDenialError) Unwrap() error { return e.err }

// nextCommands offers both opt-ins the default policy itself understands.
// Neither is phrased as advice inside the error text, which stays exactly
// what netpolicy produced (per [renderError]'s own rule): both are commands
// an author could type verbatim, which is what this element exists for.
func (e *loopbackDenialError) nextCommands() []commandBlock {
	return []commandBlock{
		{commands: []string{"FLOWSTATE_ALLOW_LOOPBACK_EGRESS=1 flow run local <file>"}},
		{
			lead:     "or, in an egress policy passed with --egress-policy:",
			commands: []string{"allow_loopback: true"},
		},
	}
}
