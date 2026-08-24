package main

import (
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestAuthVerifierRefusesInsecureBesideAPolicy walks both flags together and
// each of them alone.
//
// The pair is the interesting one, and it used to be resolved by priority:
// authVerifier returned the anonymous verifier before it looked at the policy
// path, so `flow server --insecure-no-auth --auth-policy /nonexistent/policy.yaml`
// started — verified on dadf2279, where it reached the Temporal dial rather
// than failing to read a file that does not exist. That is the assertion the
// nonexistent path buys here: were the refusal deleted, the anonymous verifier
// would be built and this test would see no error at all, and were the refusal
// replaced by "read the policy anyway" it would see a different one.
//
// Each flag alone is asserted too, because a refusal written a shade too wide
// is an unstartable server: --insecure-no-auth is how every local walkthrough
// in this repository starts a server, and --auth-policy alone is every real
// deployment.
func TestAuthVerifierRefusesInsecureBesideAPolicy(t *testing.T) {
	t.Parallel()

	t.Run("both, from the command line", func(t *testing.T) {
		t.Parallel()
		_, _, err := authVerifier(authFlags{
			insecure:        true,
			policyPath:      "/nonexistent/policy.yaml",
			policyPathGiven: true,
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "--insecure-no-auth")
		require.ErrorContains(t, err, "--auth-policy")
		require.ErrorContains(t, err, "/nonexistent/policy.yaml",
			"the refusal should name the policy that would have gone unread")
		require.ErrorContains(t, err, "never be read",
			"the refusal should say what resolving this by priority would have done")
	})

	t.Run("both, with the policy inherited from the environment", func(t *testing.T) {
		t.Parallel()
		_, _, err := authVerifier(authFlags{
			insecure:   true,
			policyPath: "/etc/flowstate/trust.yaml",
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "--insecure-no-auth")
		require.ErrorContains(t, err, "FLOWSTATE_AUTH_POLICY",
			"a policy nobody typed on this command line is remedied by unsetting the variable")
		require.ErrorContains(t, err, "/etc/flowstate/trust.yaml")
	})

	t.Run("anonymous alone still starts", func(t *testing.T) {
		t.Parallel()
		verifier, policy, err := authVerifier(authFlags{insecure: true})
		require.NoError(t, err)
		require.NotNil(t, verifier)
		require.Nil(t, policy)
	})

	t.Run("a policy alone is read", func(t *testing.T) {
		t.Parallel()
		_, _, err := authVerifier(authFlags{policyPath: "/nonexistent/policy.yaml"})
		require.ErrorContains(t, err, "reading auth policy",
			"an authenticated server opens its policy, which is what the pair above never got to do")
	})

	t.Run("neither is still the original refusal", func(t *testing.T) {
		t.Parallel()
		_, _, err := authVerifier(authFlags{})
		require.ErrorContains(t, err, "no authentication configured")
	})
}

// TestServerRefusesInsecureBesideAnInheritedPolicy is the same refusal reached
// the way an operator reaches it, through the command's own flag set, because
// the contradiction an operator is most likely to arrive at is not two flags:
// it is $FLOWSTATE_AUTH_POLICY exported for the deployment and
// --insecure-no-auth typed once for a reproduction. That path only refuses if
// the flag's environment default is carried into authFlags, which a unit test
// on authVerifier alone cannot see.
func TestServerRefusesInsecureBesideAnInheritedPolicy(t *testing.T) {
	policyPath := filepath.Join(t.TempDir(), "trust.yaml")
	t.Setenv("FLOWSTATE_AUTH_POLICY", policyPath)

	// Built after the variable is set: the flag's default is read at
	// construction time, which is exactly how the deployment supplies it.
	server := serverCommandOf(t, newRootCommand())
	require.NoError(t, server.ParseFlags([]string{"--insecure-no-auth"}))

	flags := authFlagsOf(server)
	require.Equal(t, policyPath, flags.policyPath, "the environment supplies the flag's default")
	require.False(t, flags.policyPathGiven, "nobody typed --auth-policy on this command line")

	_, _, err := authVerifier(flags)
	require.ErrorContains(t, err, "FLOWSTATE_AUTH_POLICY")
	require.ErrorContains(t, err, "--insecure-no-auth")
}

func serverCommandOf(t *testing.T, root *cobra.Command) *cobra.Command {
	t.Helper()
	for _, cmd := range root.Commands() {
		if cmd.Name() == "server" {
			return cmd
		}
	}
	t.Fatal("no `server` command in the root command tree")
	return nil
}
