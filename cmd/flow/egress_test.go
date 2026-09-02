package main

import (
	"bytes"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// loopbackWorkflow dials a port nothing listens on, on loopback, so the http
// task always fails with the same denial regardless of what else is running
// on the machine running the test.
const loopbackWorkflow = `edition: v2026.3
name: loopback-probe
steps:
  - id: fetch
    http:
      method: GET
      url: http://127.0.0.1:1/nope
`

// TestLoopbackDenialUnderTheDefaultPolicyNamesItsOwnRemedy is #387's positive
// direction: a run refused by the built-in default policy's loopback denial
// prints both documented opt-ins (the env var and the policy field), because
// the CLI already ships that answer in its own --help text and withholding it
// leaves the author to rediscover it.
func TestLoopbackDenialUnderTheDefaultPolicyNamesItsOwnRemedy(t *testing.T) {
	// Not t.Parallel(): `applyEgressPolicy` registers into the process-wide
	// [v1.DefaultRegistry] with no lock of its own (only flowtest's cases take
	// [v1.LockDefaultRegistry]), so running this beside another test that
	// exercises --egress-policy can observe that other test's policy instead
	// of the default one this test means to check. A pre-existing gap in this
	// package's test isolation, outside #387's scope; staying serial here
	// avoids inheriting its flakiness rather than fixing it.
	_, stderr, err := runLocal(t, loopbackWorkflow)
	require.Error(t, err)

	require.Contains(t, stderr, "denied by egress policy")
	require.Contains(t, stderr, "loopback")
	require.Contains(t, stderr, "FLOWSTATE_ALLOW_LOOPBACK_EGRESS")
	require.Contains(t, stderr, "allow_loopback: true")
	require.Contains(t, stderr, "--egress-policy")

	// The remedy has to be the one the reader of the variable accepts, which
	// is the whole claim a suggested command makes. It offered
	// `FLOWSTATE_ALLOW_LOOPBACK_EGRESS=1` while the check is
	// `os.Getenv(...) == "true"`, so an author who typed it got the identical
	// denial back with nothing to distinguish "the opt-in did not apply" from
	// "the opt-in did not help" (#184). Asserting the *assignment* rather than
	// the variable name is what makes that visible: the previous assertion
	// above passes on either spelling.
	require.Contains(t, stderr, v1.AllowLoopbackEgressEnv+"="+v1.AllowLoopbackEgressValue)

	// And not the spelling that did nothing. Asserting the absence as well as
	// the presence is what keeps a suggestion that prints *both* — a plausible
	// way to "fix" this by adding rather than correcting — from passing.
	require.NotContains(t, stderr, v1.AllowLoopbackEgressEnv+"=1")
}

func TestSQLPluginReceivesThePolicySnapshotTheHostParsed(t *testing.T) {
	cmd := &cobra.Command{Use: "snapshot"}
	addEgressPolicyFlag(cmd)
	addPluginFlags(cmd)

	path := filepath.Join(t.TempDir(), "policy.yaml")
	original := []byte("egress:\n  schemes: [postgres]\n  allow_ports: [5432]\n")
	replacement := []byte("egress:\n  schemes: [postgres]\n  allow_ports: [6432]\n")
	require.NoError(t, os.WriteFile(path, original, 0o600))
	require.NoError(t, cmd.Flags().Set("egress-policy", path))
	require.NoError(t, applyEgressPolicy(cmd))

	// Simulate an atomic ConfigMap/symlink replacement after the host parsed
	// the policy but before it launches the plugin.
	require.NoError(t, os.WriteFile(path, replacement, 0o600))
	flags, err := pluginFlagsOf(cmd)
	require.NoError(t, err)
	require.True(t, bytes.Equal(original, flags.egressPolicy),
		"SQL plugin policy = %q, want the host's original immutable snapshot", flags.egressPolicy)
}

func TestAnOversizedEgressPolicyIsRefusedBeforeItCanReachAPluginEnvironment(t *testing.T) {
	cmd := &cobra.Command{Use: "oversized"}
	addEgressPolicyFlag(cmd)

	path := filepath.Join(t.TempDir(), "policy.yaml")
	require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{'#'}, maxEgressPolicyBytes+1), 0o600))
	require.NoError(t, cmd.Flags().Set("egress-policy", path))

	err := applyEgressPolicy(cmd)
	require.ErrorContains(t, err, "exceeds the 65536-byte limit")
	require.NotContains(t, string(egressPolicySnapshot(cmd)), "#",
		"an oversized policy reached the plugin grant despite being refused")
}

// TestAWorkerWithNoPolicyGrantsItsOwnDefault is point 6 of #1332's decision: the
// grant is never absent under the host.
//
// A worker started with no --egress-policy still runs its built-in http task
// under a policy, and handing its plugins nothing made "the deployment's
// default" and "nothing launched me" the same value — so a plugin migrated onto
// the SDK constructor would refuse on every default worker, which is what a dev
// laptop's `flow worker` is. The grant is the default written down, and it says
// that it is the default so a plugin can decide what to do under it.
//
// Compared against [v1.DefaultEgressPolicyDocument] rather than a document this
// test spells, because what makes the grant right is that it is the *same*
// default the http task on this worker is enforcing, not that it looks like a
// default.
func TestAWorkerWithNoPolicyGrantsItsOwnDefault(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{Use: "default-grant"}
	addEgressPolicyFlag(cmd)
	addPluginFlags(cmd)
	require.NoError(t, applyEgressPolicy(cmd))

	flags, err := pluginFlagsOf(cmd)
	require.NoError(t, err)
	require.NotNil(t, flags.egressPolicy,
		"a worker with no --egress-policy granted its plugins nothing, so every one of them fails closed")
	require.Equal(t, v1.DefaultEgressPolicyDocument(), flags.egressPolicy,
		"the grant is not the default policy this worker's own http task runs under")

	cfg, err := netpolicy.ParseConfig(flags.egressPolicy)
	require.NoError(t, err)
	require.True(t, cfg.DeploymentDefault,
		"the default grant does not identify itself, so a plugin reads it as a policy an operator wrote")
}

// TestACommandThatOnlyDescribesPluginsGrantsTheSameDefault keeps the grant from
// depending on which command did the launching. `flow plugins` does not take
// --egress-policy because it runs no task, but a plugin that finds no grant
// there and one everywhere else has a difference to explain that means nothing.
func TestACommandThatOnlyDescribesPluginsGrantsTheSameDefault(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{Use: "describe-only"}
	addPluginFlags(cmd)

	flags, err := pluginFlagsOf(cmd)
	require.NoError(t, err)
	require.Equal(t, v1.DefaultEgressPolicyDocument(), flags.egressPolicy,
		"a command that only describes plugins granted something other than the deployment default")
}

// TestAnOperatorPolicyCannotClaimToBeTheDeploymentDefault closes the one hole
// the in-document marker opens.
//
// `deployment_default` is the worker's signature on a policy no operator wrote,
// and plugins act on it — sql refuses a database under the default, and would
// refuse under an operator's real policy that claimed to be one. The refusal is
// here, where an operator's own bytes enter, rather than in each plugin.
func TestAnOperatorPolicyCannotClaimToBeTheDeploymentDefault(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{Use: "claimed-default"}
	addEgressPolicyFlag(cmd)

	path := filepath.Join(t.TempDir(), "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte("deployment_default: true\negress:\n  schemes: [https]\n"), 0o600))
	require.NoError(t, cmd.Flags().Set("egress-policy", path))

	err := applyEgressPolicy(cmd)
	require.ErrorContains(t, err, "deployment_default")
	require.ErrorContains(t, err, path,
		"the refusal does not name the file the operator has to edit")
}

// TestLoopbackDenialUnderAnExplicitPolicyStaysSilent is #387's negative
// direction: when an operator's own --egress-policy file denied the dial, the
// remedy is that file, not an environment variable that would read as an
// invitation to bypass it. The CLI must not teach the bypass in this case.
func TestLoopbackDenialUnderAnExplicitPolicyStaysSilent(t *testing.T) {
	// Not t.Parallel(): see the comment on
	// TestLoopbackDenialUnderTheDefaultPolicyNamesItsOwnRemedy.
	policyPath := filepath.Join(t.TempDir(), "policy.yaml")
	require.NoError(t, os.WriteFile(policyPath, []byte("egress:\n  schemes: [http, https]\n"), 0o600))

	_, stderr, err := runLocal(t, loopbackWorkflow, "--egress-policy", policyPath)
	require.Error(t, err)

	require.Contains(t, stderr, "denied by egress policy")
	require.NotContains(t, stderr, "FLOWSTATE_ALLOW_LOOPBACK_EGRESS")
	require.NotContains(t, stderr, "NEXT")
}

// TestTheEgressPolicyFlagSaysWhichPluginsEnforceTheGrantAndWhereItStops pins the
// words, because the words are the security claim.
//
// A worker cannot enforce a policy inside a plugin: a plugin is a separate
// process, and the operating system opens whatever socket it asks for. What the
// worker does is *grant* the policy, and whether a deny rule stops a request is
// the receiving plugin's own code. Every first-party plugin now applies it
// (#1321, #1322, #1323), which is a promise this build does keep and the help
// has to make — an operator reading that git, github and vcs "do not read it
// yet" would confine tasks that no longer need confining. What the help must
// still refuse to claim is enforcement over a plugin this build cannot confine.
//
// Held here because nothing else would notice: the sentence is generated into
// docs/reference/cli.md, where this one string is repeated for every command
// that takes the flag, and it will read as more than it means the moment someone
// shortens it. The negative assertion is the load-bearing half — a test that
// only checked for the plugin names would pass on help text that also claimed to
// govern every plugin the worker launches.
func TestTheEgressPolicyFlagSaysWhichPluginsEnforceTheGrantAndWhereItStops(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{Use: "probe"}
	addEgressPolicyFlag(cmd)

	usage := cmd.Flags().Lookup("egress-policy").Usage

	// Granted, not governed, where plugins are concerned.
	require.Contains(t, usage, "granted to every plugin the worker launches")
	require.NotContains(t, usage, "governing built-in HTTP and every plugin",
		"the flag help claims to govern every plugin; a grant is not enforcement, and a third-party process can ignore it")

	// The claim it does keep: the built-in task is what this flag actually
	// enforces over.
	require.Contains(t, usage, "governing built-in HTTP")

	// The plugins that do enforce it, by name — the promise this build now
	// keeps, and the reason an operator no longer needs a workaround for three
	// of them.
	for _, named := range []string{"git", "github", "slack", "sql", "vcs"} {
		require.Containsf(t, usage, named,
			"the flag help does not name %q, so an operator cannot tell which plugins the policy actually stops", named)
	}
	require.Contains(t, usage, "enforce the grant on their own connections")

	// And the limit that survives every migration: a plugin is a process, not a
	// sandbox.
	require.Contains(t, usage, "a third-party plugin is a separate process that can ignore it")

	// The stale hedge, in the two spellings it had. Naming them keeps the help
	// from drifting back to a gap that no longer exists.
	require.NotContains(t, usage, "do not read it yet")
	require.NotContains(t, usage, "#1332",
		"the help still points at the migration issue as though it were pending")
}

// TestTheMCPPostureReachesLaunchedPluginsToo is the hole the deployment-default
// fallback opened, closed at the one place the grant is chosen.
//
// `flow mcp` with no --egress-policy deliberately denies everything for the
// built-in http task: its caller is a model composing a workflow, not the person
// who wrote it. That posture is only a posture if it reaches the plugins that
// command launches — and once every first-party plugin obeys the grant (#1332),
// granting them the ordinary deployment default would have meant a model that
// cannot make this process fetch a URL could still ask a `git`, `github` or
// `vcs` task to reach one.
//
// The assertions are about the *document* rather than about a rule, because what
// went wrong was two policies where there should be one: the grant is compared
// against what this command registers for its own http task, and against the
// deployment default it must not be.
func TestTheMCPPostureReachesLaunchedPluginsToo(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{Use: "mcp-grant"}
	addEgressPolicyFlag(cmd)
	addPluginFlags(cmd)
	require.NoError(t, applyMCPEgressPolicy(cmd))

	flags, err := pluginFlagsOf(cmd)
	require.NoError(t, err)

	_, document, err := mcpEgressPolicy()
	require.NoError(t, err)
	require.Equal(t, document, flags.egressPolicy,
		"the plugins this command launches are not granted the policy it enforces on itself")
	require.NotEqual(t, v1.DefaultEgressPolicyDocument(), flags.egressPolicy,
		"flow mcp granted its plugins the ordinary deployment default, which permits the public internet")

	// The grant denies what the built-in task denies, on the plugin's side of
	// the process boundary. Computed by building the granted document, which is
	// what a plugin's sdk.EgressPolicy does with it.
	cfg, err := netpolicy.ParseConfig(flags.egressPolicy)
	require.NoError(t, err)
	require.True(t, cfg.DeploymentDefault,
		"the grant does not say no operator wrote it, so sql would treat it as an authorized destination")

	granted, err := cfg.Policy()
	require.NoError(t, err)
	require.Error(t, granted.CheckURL(t.Context(), http.MethodGet, mustParseURL(t, "https://example.com/")),
		"a plugin under flow mcp's grant may reach a public destination this command refuses to fetch itself")
}

// mustParseURL is the two lines the check above would otherwise inline.
func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()

	parsed, err := url.Parse(raw)
	require.NoError(t, err)
	return parsed
}

// TestTheMCPTaskAndItsGrantAreBuiltFromOneDocument is the structural half of the
// fix, and the one that keeps it fixed.
//
// The bug was two constructions of one posture: a policy built from options for
// the built-in http task, and nothing at all for the grant, so the two could —
// and did — say different things. Asserting only that both deny today would not
// notice the next edit to one of them, because two independently built
// deny-everything policies behave identically. What has to hold is that they
// come from the same bytes, so a change to those bytes moves both.
//
// So the comparison is between the policy `flow mcp` registers and a policy
// built the way a *plugin* builds one — parse the granted document, build it —
// across the categories a policy can disagree about. A second construction that
// drifted in any of them fails here.
func TestTheMCPTaskAndItsGrantAreBuiltFromOneDocument(t *testing.T) {
	t.Parallel()

	registered, document, err := mcpEgressPolicy()
	require.NoError(t, err)

	// What sdk.EgressPolicy does with the bytes it was granted.
	cfg, err := netpolicy.ParseConfig(document)
	require.NoError(t, err)
	granted, err := cfg.Policy()
	require.NoError(t, err)

	for name, probe := range map[string]string{
		"a public host":      "https://example.com/",
		"a loopback host":    "http://127.0.0.1:8080/",
		"a private host":     "https://10.0.0.1/",
		"a metadata address": "http://169.254.169.254/latest/meta-data/",
	} {
		target := mustParseURL(t, probe)
		registeredErr := registered.CheckURL(t.Context(), http.MethodGet, target)
		grantedErr := granted.CheckURL(t.Context(), http.MethodGet, target)

		require.Equalf(t, registeredErr == nil, grantedErr == nil,
			"the policy this command enforces and the one it grants its plugins disagree about %s; "+
				"they are supposed to be the same document", name)
	}

	// The bounds, which a deny rule cannot mask. Every destination check above
	// is answered "denied" by the rule regardless of what else the two policies
	// carry, so two separately built policies pass that comparison while
	// disagreeing about everything a rule does not decide. These are the fields
	// that catch the drift: a second construction that set a different response
	// cap, request timeout or TLS floor — or that was built from a document the
	// other side never saw — differs here.
	require.Equal(t, registered.MaxResponseBytes(), granted.MaxResponseBytes(),
		"the enforced policy and the granted one cap responses differently, so they are not one document")
	require.Equal(t, registered.Timeout(), granted.Timeout(),
		"the enforced policy and the granted one bound requests differently, so they are not one document")
	require.Equal(t, registered.MinTLSVersion(), granted.MinTLSVersion(),
		"the enforced policy and the granted one have different TLS floors, so they are not one document")

	// And the posture itself, so the parity above cannot be satisfied by two
	// policies that permit everything.
	require.Error(t, registered.CheckURL(t.Context(), http.MethodGet, mustParseURL(t, "https://example.com/")),
		"flow mcp's own http task may reach a public destination")
}
