package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// workerCommand returns a fresh `flow worker`, with the environment defaults its
// versioning flags read cleared.
//
// Cleared rather than trusted: `--deployment-name` and `--build-id` default from
// FLOWSTATE_DEPLOYMENT_NAME and FLOWSTATE_BUILD_ID, so a developer's shell — or a
// CI job that sets a build id for something else entirely — would otherwise decide
// whether these tests are exercising a versioned worker or an unversioned one.
//
// Not parallel, and neither is anything below: newRootCommand binds flags to
// process state, which help_test.go already runs serially for the same reason.
func workerCommand(t *testing.T) *cobra.Command {
	t.Helper()

	t.Setenv("FLOWSTATE_DEPLOYMENT_NAME", "")
	t.Setenv("FLOWSTATE_BUILD_ID", "")

	cmd, _, err := newRootCommand().Find([]string{"worker"})
	require.NoError(t, err)
	require.Equal(t, "worker", cmd.Name())

	return cmd
}

// TestWorkerRefusesToStartUnversioned is the enforcement of the precondition
// docs/DSL.md states: expressions are evaluated in workflow code, so the binary
// decides what they mean, so something has to pin which binary a run in flight is
// handed to.
//
// The assertion is on the text and not merely on the error, because the whole
// reason a gate is tolerable here is that it says what to type. An operator who
// reads this message and still does not know their two options has been given a
// wall rather than a decision.
func TestWorkerRefusesToStartUnversioned(t *testing.T) {
	cmd := workerCommand(t)

	_, err := workerDeployment(cmd, temporalFlagsOf(cmd))

	require.Error(t, err)
	for _, want := range []string{
		"refusing to start an unversioned worker",
		"in workflow code",
		"already in flight",
		"--deployment-name",
		"--build-id",
		"--" + allowUnversionedFlag,
	} {
		require.Contains(t, err.Error(), want)
	}
}

// TestWorkerRefusalIsReachedBeforeTemporalIs checks where the gate sits.
//
// A check that runs after the client dials reports a connection failure on a
// laptop with no Temporal running, which tells the operator to go fix the wrong
// thing. Nothing in this test provides a server, so the refusal below is proof the
// posture is settled from flags alone — as are the plugin hosts and secret
// providers this never opens.
func TestWorkerRefusalIsReachedBeforeTemporalIs(t *testing.T) {
	t.Setenv("FLOWSTATE_DEPLOYMENT_NAME", "")
	t.Setenv("FLOWSTATE_BUILD_ID", "")

	root := newRootCommand()
	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"worker", "--address", "127.0.0.1:1"})

	err := root.ExecuteContext(t.Context())

	require.ErrorContains(t, err, "refusing to start an unversioned worker")
}

// TestWorkerStartsVersioned is the intended posture: both halves, versioning on,
// and the pair carried through to the worker's options unchanged.
func TestWorkerStartsVersioned(t *testing.T) {
	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set("deployment-name", "flowstate"))
	require.NoError(t, cmd.Flags().Set("build-id", "abc123"))

	deployment, err := workerDeployment(cmd, temporalFlagsOf(cmd))

	require.NoError(t, err)
	require.True(t, deployment.UseVersioning)
	require.Equal(t, "flowstate", deployment.Version.DeploymentName)
	require.Equal(t, "abc123", deployment.Version.BuildID)
}

// TestWorkerStartsUnversionedWhenAccepted is invariant 8's half of the bargain:
// zero-config self-hosted development keeps working, at the price of one flag that
// says what it is accepting.
//
// The worker really is unversioned afterwards — the flag accepts the exposure, it
// does not invent a version — because a version nobody can address is worse than
// none, and because a run pinned to a build id somebody's laptop made up cannot be
// drained by anyone.
func TestWorkerStartsUnversionedWhenAccepted(t *testing.T) {
	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set(allowUnversionedFlag, "true"))

	deployment, err := workerDeployment(cmd, temporalFlagsOf(cmd))

	require.NoError(t, err)
	require.False(t, deployment.UseVersioning)
	require.Empty(t, deployment.Version.DeploymentName)
	require.Empty(t, deployment.Version.BuildID)
}

// TestWorkerRefusesHalfAVersion covers the half-configured worker, in both
// directions.
//
// This used to be silent: one flag set meant versioning off, with no message and a
// worker that started happily — an operator who asked for the guarantee, was not
// given it, and was not told. The accept flag deliberately does not rescue it
// either. Half a version is a mistake in the command line rather than a posture
// anyone chose, so the answer is to name the missing half, not to offer to proceed
// without either.
func TestWorkerRefusesHalfAVersion(t *testing.T) {
	for _, test := range []struct {
		name    string
		set     map[string]string
		wantErr string
	}{
		{
			name:    "a deployment name with no build id",
			set:     map[string]string{"deployment-name": "flowstate"},
			wantErr: "--build-id",
		},
		{
			name:    "a build id with no deployment name",
			set:     map[string]string{"build-id": "abc123"},
			wantErr: "--deployment-name",
		},
		{
			name: "a deployment name with no build id, unversioned accepted",
			set: map[string]string{
				"deployment-name":    "flowstate",
				allowUnversionedFlag: "true",
			},
			wantErr: "--build-id",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := workerCommand(t)
			for flag, value := range test.set {
				require.NoError(t, cmd.Flags().Set(flag, value))
			}

			_, err := workerDeployment(cmd, temporalFlagsOf(cmd))

			require.ErrorContains(t, err, test.wantErr)
			// The half that *was* given is echoed, so the message identifies which
			// worker's command line is wrong when several are being deployed.
			for flag, value := range test.set {
				if flag != allowUnversionedFlag {
					require.Contains(t, err.Error(), value, "the configured %s is not named", flag)
				}
			}
		})
	}
}

// TestWorkerVersioningFlagsDefaultFromTheEnvironment pins the other way a version
// arrives.
//
// A build id is a property of the artifact, so the thing that built it is what
// knows the value — a CI job exports it and the command line stays the same across
// every environment. If these defaults stopped being read, every such deployment
// would start refusing to start, which is safe but would look like the gate itself
// had broken.
func TestWorkerVersioningFlagsDefaultFromTheEnvironment(t *testing.T) {
	t.Setenv("FLOWSTATE_DEPLOYMENT_NAME", "flowstate")
	t.Setenv("FLOWSTATE_BUILD_ID", "deadbeef")

	cmd, _, err := newRootCommand().Find([]string{"worker"})
	require.NoError(t, err)

	deployment, err := workerDeployment(cmd, temporalFlagsOf(cmd))

	require.NoError(t, err)
	require.True(t, deployment.UseVersioning)
	require.Equal(t, "flowstate", deployment.Version.DeploymentName)
	require.Equal(t, "deadbeef", deployment.Version.BuildID)
}
