package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
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
// lookup the engine makes goes through that registry. That registration is the
// only place this file *enforces* anything. The same bytes are also handed to
// every launched plugin as a grant ([plugin.Config.EgressPolicy]), and a grant is
// not enforcement: a plugin process opens its own sockets, so whether the policy
// governs a plugin task is that plugin's code. Every first-party plugin now
// applies it — `git`, `github`, `slack`, `sql` and `vcs`, each on its own
// connection path (#1332) — and a third-party plugin that asks the SDK for a
// client gets the same policy without this file knowing its name. Registration
// happens before the worker polls and before a local run executes, so a policy
// that cannot load refuses the command instead of governing some steps and not
// others.
//
// With no file, the grant is the deployment default rather than nothing: see
// [egressPolicySnapshot].
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

// maxEgressPolicyBytes keeps policy loading bounded and leaves room below
// Linux's per-environment-string exec limit after the snapshot every plugin is
// granted (#1332) is base64 encoded. A policy is configuration, not a data
// transport; 64 KiB is ample for the supported rules while refusing
// comment-heavy or accidental files before they can prevent every plugin process
// from launching.
//
// The number is the plugin package's, not a second one that happens to match:
// this is where an operator's own file meets the bound, and a CLI that accepted
// a file the host would then refuse — or refused one the host would accept —
// would be two answers to the same question with only this comment holding them
// together.
const maxEgressPolicyBytes = plugin.MaxEgressPolicyBytes

// egressPolicySnapshotKey carries the exact policy bytes [applyEgressPolicy]
// parsed. Protocol-native plugins receive this immutable snapshot rather than
// reopening a pathname that an operator or ConfigMap update could replace
// between the host and plugin reads.
//
// Nil means no --egress-policy was configured; a non-nil empty slice means one
// was and its document is empty, which is a policy. [slices.Clone] preserves
// that distinction, which is why it is the clone used on both hops.
type egressPolicySnapshotKey struct{}

// egressPolicySnapshot returns the policy every plugin this command launches is
// granted: the operator's file when one was configured, and otherwise the
// deployment default written down ([v1.DefaultEgressPolicyDocument]).
//
// The default is a grant, not the absence of one. A worker started with no
// --egress-policy still runs its own built-in http task under a policy, and a
// plugin handed nothing cannot tell that deployment from one that never
// launched it through a worker at all — so it either refuses work a default
// worker has always done or, worse, decides for itself what "no policy" means.
// Forwarding the default leaves `sdk.EgressPolicy`'s fail-closed refusal to
// mean exactly one thing: nothing granted this process anything (#1332).
//
// It defaults here rather than in [applyEgressPolicy] so that a command which
// launches plugins without taking the flag — `flow plugins`, `flow tasks`,
// `flow validate` — grants the same document as one that does. Those commands
// only ask a plugin what it can do, but a grant that appears and disappears
// with the command doing the asking is a difference a plugin would have to
// explain, and there is nothing to explain.
func egressPolicySnapshot(cmd *cobra.Command) []byte {
	data, _ := commandContext(cmd).Value(egressPolicySnapshotKey{}).([]byte)
	if data == nil {
		return v1.DefaultEgressPolicyDocument()
	}
	return slices.Clone(data)
}

// setEgressPolicySnapshot records the document every plugin this command
// launches will be granted.
//
// One writer, because the grant and the policy registered over the built-in http
// task are the same deployment's answer, and a command that set one without the
// other governs its own task and its plugins differently. That is exactly what
// `flow mcp` did before it called this: it registered a deny-everything policy
// for the built-in task and left the snapshot unset, so plugins were granted the
// ordinary default and a model could drive a git or github task to a public host
// the same process refused to fetch.
//
// A nil document means no policy was configured, which [egressPolicySnapshot]
// answers with the deployment default.
func setEgressPolicySnapshot(cmd *cobra.Command, document []byte) {
	cmd.SetContext(context.WithValue(commandContext(cmd), egressPolicySnapshotKey{}, document))
}

func commandContext(cmd *cobra.Command) context.Context {
	if ctx := cmd.Context(); ctx != nil {
		return ctx
	}
	return context.Background()
}

// addEgressPolicyFlag declares --egress-policy on a command.
//
// The help says granted where it used to say governing, and it names both what
// enforces the grant and where enforcement stops. The difference is not pedantry
// at this boundary: a plugin process is not confined, so handing it the policy is
// all a worker can do, and whether a deny rule actually stops a request is that
// plugin's own code. Every first-party plugin now applies it, which is what the
// help says; what it must not say is that the flag governs *any* plugin, because
// a third-party process can open its own socket and this build cannot stop it.
// A deployment that must stop one confines it (THREAT_MODEL.md).
func addEgressPolicyFlag(cmd *cobra.Command) {
	cmd.Flags().String("egress-policy", os.Getenv(egressPolicyEnv),
		"path to an egress policy (YAML) governing built-in HTTP and granted to every plugin the worker launches "+
			"(default $"+egressPolicyEnv+"); the first-party git, github, slack, sql and vcs plugins enforce the "+
			"grant on their own connections, while a third-party plugin is a separate process that can ignore it; "+
			"with no file every plugin is granted the same default policy built-in HTTP runs under, which sql "+
			"refuses to reach a database under; when set it replaces the default policy entirely, and "+
			v1.AllowLoopbackEgressEnv+" is ignored; a file that wants loopback says allow_loopback: true")
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
	setEgressPolicySnapshot(cmd, nil)
	path, _ := cmd.Flags().GetString("egress-policy")
	if path == "" {
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("reading egress policy: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, maxEgressPolicyBytes+1))
	if err != nil {
		return fmt.Errorf("reading egress policy: %w", err)
	}
	if len(data) > maxEgressPolicyBytes {
		return fmt.Errorf("reading egress policy %s: file exceeds the %d-byte limit", path, maxEgressPolicyBytes)
	}

	cfg, err := netpolicy.ParseConfig(data)
	if err != nil {
		return fmt.Errorf("parsing egress policy %s: %w", path, err)
	}

	// deployment_default is the worker's own signature on the document it grants
	// a plugin when no operator file was configured, and a plugin decides what
	// it will do under the default from it (sql refuses a database; git, vcs,
	// github and slack accept). An operator file wearing that signature would be
	// telling those plugins the operator had written nothing — refused here, at
	// the one place an operator's own bytes enter, rather than left to mean
	// something different in each plugin that reads it.
	if cfg.DeploymentDefault {
		return fmt.Errorf(
			"egress policy %s sets deployment_default; that key marks the default policy a worker "+
				"grants its plugins when no --egress-policy is configured, and is not something a "+
				"policy file says about itself — delete it", path)
	}

	policy, err := cfg.Policy()
	if err != nil {
		return fmt.Errorf("egress policy %s: %w", path, err)
	}

	// Non-nil even for a zero-byte file. Nil is how this command spells "no
	// operator file", which [egressPolicySnapshot] answers with the deployment
	// default — and an operator who named a file configured a policy, the one
	// this function just registered over the built-in http task. Letting an
	// empty file arrive as nil is what made the two sides disagree: the built-in
	// task ran the policy an empty document builds while every plugin was
	// granted the default instead of the operator's own empty one.
	snapshot := slices.Clone(data)
	if snapshot == nil {
		snapshot = []byte{}
	}
	setEgressPolicySnapshot(cmd, snapshot)

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
//
// "Verbatim" is the whole contract, and it was broken: this offered
// `FLOWSTATE_ALLOW_LOOPBACK_EGRESS=1`, while the variable is read as
// `os.Getenv(...) == "true"` (`eval_task_http_def.go`, the same spelling
// `cmd/flow/serverdev.go` and `cmd/flow/credentials.go` read their own
// variables with). An author who typed the suggested command got the
// identical denial back with no indication of why, which is worse than
// offering nothing — a diagnostic that hands out a remedy that does not
// work teaches the reader to distrust the next one. The value is written
// from [v1.AllowLoopbackEgressEnv] and [v1.AllowLoopbackEgressValue] rather
// than spelled here, so the suggestion cannot drift from the variable it
// suggests again.
func (e *loopbackDenialError) nextCommands() []commandBlock {
	return []commandBlock{
		{commands: []string{v1.AllowLoopbackEgressEnv + "=" + v1.AllowLoopbackEgressValue + " flow run local <file>"}},
		{
			lead:     "or, in an egress policy passed with --egress-policy:",
			commands: []string{"allow_loopback: true"},
		},
	}
}
