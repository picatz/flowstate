package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// loopbackWorkflow dials a port nothing listens on, on loopback, so the http
// task always fails with the same denial regardless of what else is running
// on the machine running the test.
const loopbackWorkflow = `edition: v2026.2
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
