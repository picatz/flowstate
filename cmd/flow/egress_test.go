package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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
	require.Empty(t, egressPolicySnapshot(cmd),
		"an oversized policy reached the SQL plugin snapshot despite being refused")
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
