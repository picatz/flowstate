package main

import (
	"os"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// workerCommand returns a fresh `flow worker`, with the environment defaults its
// versioning flags read cleared.
//
// Cleared rather than trusted: `--deployment-name` and `--build-id` default from
// FLOWSTATE_DEPLOYMENT_NAME and FLOWSTATE_BUILD_ID, so a developer's shell — or a
// CI job that sets a build id for something else entirely — would otherwise decide
// whether these tests are exercising a versioned worker or an unversioned one.
// FLOWSTATE_WORKER_IDENTITY is cleared for the identical reason (#752).
//
// Not parallel, and neither is anything below: newRootCommand binds flags to
// process state, which help_test.go already runs serially for the same reason.
func workerCommand(t *testing.T) *cobra.Command {
	t.Helper()

	t.Setenv("FLOWSTATE_DEPLOYMENT_NAME", "")
	t.Setenv("FLOWSTATE_BUILD_ID", "")
	t.Setenv("FLOWSTATE_WORKER_IDENTITY", "")
	// Same reasoning, for #783's capacity flags: a developer's shell should not
	// decide whether these tests exercise the SDK default or a tuned value.
	t.Setenv("FLOWSTATE_WORKER_MAX_CONCURRENT_ACTIVITIES", "")
	t.Setenv("FLOWSTATE_WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", "")
	t.Setenv("FLOWSTATE_WORKER_MAX_ACTIVITIES_PER_SECOND", "")
	t.Setenv("FLOWSTATE_WORKER_TASK_QUEUE_ACTIVITIES_PER_SECOND", "")

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

	err := runFlow(t, "worker", "--address", "127.0.0.1:1").Err

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

// TestWorkerIdentityOverride is #752's escape hatch: an operator with a
// platform-native identifier (a Kubernetes pod name from the downward API)
// wants it used verbatim, not folded into a composed default.
func TestWorkerIdentityOverride(t *testing.T) {
	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set(allowUnversionedFlag, "true"))
	require.NoError(t, cmd.Flags().Set("identity", "pod/flowstate-worker-7f9c-abcde"))

	deployment, err := workerDeployment(cmd, temporalFlagsOf(cmd))
	require.NoError(t, err)

	require.Equal(t, "pod/flowstate-worker-7f9c-abcde",
		workerIdentity(cmd, deployment, temporalFlagsOf(cmd)))
}

// TestWorkerIdentityOverrideFromEnvironment covers FLOWSTATE_WORKER_IDENTITY,
// the flag's default source — the shape an operator setting it through a
// container's environment rather than its command line actually uses.
func TestWorkerIdentityOverrideFromEnvironment(t *testing.T) {
	t.Setenv("FLOWSTATE_DEPLOYMENT_NAME", "")
	t.Setenv("FLOWSTATE_BUILD_ID", "")
	t.Setenv("FLOWSTATE_WORKER_IDENTITY", "pod/flowstate-worker-7f9c-abcde")

	cmd, _, err := newRootCommand().Find([]string{"worker"})
	require.NoError(t, err)
	require.NoError(t, cmd.Flags().Set(allowUnversionedFlag, "true"))

	deployment, err := workerDeployment(cmd, temporalFlagsOf(cmd))
	require.NoError(t, err)

	require.Equal(t, "pod/flowstate-worker-7f9c-abcde",
		workerIdentity(cmd, deployment, temporalFlagsOf(cmd)))
}

// TestWorkerIdentityDefaultIsMoreSpecificThanTheSDKs is the un-overridden
// case: no platform identifier was given, so the default is built from what
// this worker already knows about its own versioning and tenant
// restriction, plus the hostname the SDK's own default would have used
// unchanged — still an improvement, per #752, because the versioned prefix
// disambiguates a stuck task's Event History entry without any lookup at
// all, and the hostname still disambiguates replicas of that same pair.
func TestWorkerIdentityDefaultIsMoreSpecificThanTheSDKs(t *testing.T) {
	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set("deployment-name", "flowstate"))
	require.NoError(t, cmd.Flags().Set("build-id", "abc123"))

	deployment, err := workerDeployment(cmd, temporalFlagsOf(cmd))
	require.NoError(t, err)

	host, hostErr := os.Hostname()
	require.NoError(t, hostErr)

	identity := workerIdentity(cmd, deployment, temporalFlagsOf(cmd))

	require.Equal(t, "flowstate/abc123@"+host, identity)
}

// TestWorkerIdentityDefaultNamesTheTenant covers the composition's other
// half: a worker restricted to one tenant (--tenant) records that
// restriction in its own identity too, since a run misrouted to the wrong
// tenant's worker is exactly the kind of thing #752 exists to make
// traceable.
func TestWorkerIdentityDefaultNamesTheTenant(t *testing.T) {
	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set("deployment-name", "flowstate"))
	require.NoError(t, cmd.Flags().Set("build-id", "abc123"))
	require.NoError(t, cmd.Flags().Set("tenant", "team-a"))

	deployment, err := workerDeployment(cmd, temporalFlagsOf(cmd))
	require.NoError(t, err)

	identity := workerIdentity(cmd, deployment, temporalFlagsOf(cmd))

	require.Contains(t, identity, "flowstate/abc123")
	require.Contains(t, identity, "tenant=team-a")
}

// TestWorkerIdentityDefaultTenantIsNamedExplicitly covers `--tenant=`
// (the empty string), which selects the default tenant of an untenanted
// deployment rather than meaning "no tenant declared" — the same
// distinction temporalFlags.tenantSet exists for. The identity has to spell
// this the same unambiguous way, rather than rendering an empty segment
// that reads as though --tenant were never passed at all.
func TestWorkerIdentityDefaultTenantIsNamedExplicitly(t *testing.T) {
	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set(allowUnversionedFlag, "true"))
	require.NoError(t, cmd.Flags().Set("tenant", ""))

	deployment, err := workerDeployment(cmd, temporalFlagsOf(cmd))
	require.NoError(t, err)

	identity := workerIdentity(cmd, deployment, temporalFlagsOf(cmd))

	require.Contains(t, identity, "tenant=_default")
}

// TestWorkerStopTimeoutDefaultsToTheDocumentedValue is the unit-level half of
// #751's fix: the value CLAUDE.md's both-drivers reasoning does not cover
// (WorkerStopTimeout is durable-only, nothing to compare against), so this is the
// only place a regression to the SDK's own 0s default — which drains for
// effectively no time at all, see size.go's [v1.DefaultWorkerStopTimeout] doc —
// would be caught before a real deploy caught it instead.
func TestWorkerStopTimeoutDefaultsToTheDocumentedValue(t *testing.T) {
	t.Setenv("FLOWSTATE_WORKER_STOP_TIMEOUT", "")

	cmd := workerCommand(t)

	got, err := workerStopTimeout(cmd)

	require.NoError(t, err)
	require.Equal(t, v1.DefaultWorkerStopTimeout, got)
}

// TestWorkerStopTimeoutDefaultsFromTheEnvironment mirrors
// TestWorkerVersioningFlagsDefaultFromTheEnvironment above for this flag: an
// operator's deployment sets the variable once and every worker in the fleet
// picks it up, without a --worker-stop-timeout on every command line.
func TestWorkerStopTimeoutDefaultsFromTheEnvironment(t *testing.T) {
	t.Setenv("FLOWSTATE_WORKER_STOP_TIMEOUT", "90s")

	cmd, _, err := newRootCommand().Find([]string{"worker"})
	require.NoError(t, err)

	got, err := workerStopTimeout(cmd)

	require.NoError(t, err)
	require.Equal(t, 90*time.Second, got)
}

// TestWorkerStopTimeoutFlagWinsOverTheEnvironment is the same precedence every
// other overridable flag in this command promises.
func TestWorkerStopTimeoutFlagWinsOverTheEnvironment(t *testing.T) {
	t.Setenv("FLOWSTATE_WORKER_STOP_TIMEOUT", "90s")

	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set("worker-stop-timeout", "45s"))

	got, err := workerStopTimeout(cmd)

	require.NoError(t, err)
	require.Equal(t, 45*time.Second, got)
}

// TestWorkerStopTimeoutAcceptsTheDSLsDurationGrammar pins that
// --worker-stop-timeout means exactly what a Flowfile's sleep: means for the same
// characters — v1.ParseDuration, not the stricter time.ParseDuration — so `7d` is
// legal here too, and not only in a workflow.
func TestWorkerStopTimeoutAcceptsTheDSLsDurationGrammar(t *testing.T) {
	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set("worker-stop-timeout", "1d"))

	got, err := workerStopTimeout(cmd)

	require.NoError(t, err)
	require.Equal(t, 24*time.Hour, got)
}

// TestWorkerStopTimeoutRefusesAnUnparsableValue is reached before Temporal is
// dialed, a plugin is launched, or a secret provider is opened — see runWorker's
// ordering comment above the call to workerStopTimeout — for the same reason
// TestWorkerRefusalIsReachedBeforeTemporalIs gives for the versioning gate: a
// mistake in the command line should say so immediately, not as a connection
// failure that sends the operator looking at the wrong thing.
func TestWorkerStopTimeoutRefusesAnUnparsableValue(t *testing.T) {
	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set("worker-stop-timeout", "not-a-duration"))

	_, err := workerStopTimeout(cmd)

	require.Error(t, err)
	require.Contains(t, err.Error(), "--worker-stop-timeout")
	require.Contains(t, err.Error(), "not-a-duration")
}

// TestWorkerStopTimeoutRefusesANegativeValue is the fix for a review finding on
// #757's PR: v1.ParseDuration parses a negative value like "-1s" without error,
// and the SDK's own Stop treats a negative (or zero) stopTimeout as "don't wait" —
// its internal timer fires immediately. Passed through unchecked, a negative
// --worker-stop-timeout or FLOWSTATE_WORKER_STOP_TIMEOUT would silently disable
// the drain this whole flag exists to configure, reintroducing the exact
// in-flight-work loss #751 reports through the fix for it. So this is refused
// explicitly, at the same point the unparsable case above is, before Temporal is
// dialed.
func TestWorkerStopTimeoutRefusesANegativeValue(t *testing.T) {
	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set("worker-stop-timeout", "-1s"))

	_, err := workerStopTimeout(cmd)

	require.Error(t, err)
	require.Contains(t, err.Error(), "--worker-stop-timeout")
	require.Contains(t, err.Error(), "-1s")
	require.Contains(t, err.Error(), "negative")
}

// TestWorkerCapacityOptionsDefaultToTheSDKsOwnDefault is the byte-identical
// requirement #783's acceptance criteria states: an unset flag must produce
// exactly the zero value worker.Options already had, so any existing
// deployment sees no behavior change.
func TestWorkerCapacityOptionsDefaultToTheSDKsOwnDefault(t *testing.T) {
	cmd := workerCommand(t)

	got, err := workerCapacityOptions(cmd)

	require.NoError(t, err)
	require.Equal(t, workerCapacity{}, got)
}

// TestWorkerCapacityOptionsFromFlags pins that all four flags reach
// worker.Options's fields of the same meaning.
func TestWorkerCapacityOptionsFromFlags(t *testing.T) {
	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set("max-concurrent-activities", "50"))
	require.NoError(t, cmd.Flags().Set("max-concurrent-workflow-tasks", "25"))
	require.NoError(t, cmd.Flags().Set("max-activities-per-second", "10.5"))
	require.NoError(t, cmd.Flags().Set("task-queue-activities-per-second", "20"))

	got, err := workerCapacityOptions(cmd)

	require.NoError(t, err)
	require.Equal(t, workerCapacity{
		maxConcurrentActivities:      50,
		maxConcurrentWorkflowTasks:   25,
		activitiesPerSecond:          10.5,
		taskQueueActivitiesPerSecond: 20,
	}, got)
}

// TestWorkerCapacityOptionsDefaultFromTheEnvironment mirrors
// TestWorkerStopTimeoutDefaultsFromTheEnvironment for the four new variables:
// an operator's deployment sets them once and every worker in the fleet picks
// them up.
func TestWorkerCapacityOptionsDefaultFromTheEnvironment(t *testing.T) {
	t.Setenv("FLOWSTATE_WORKER_MAX_CONCURRENT_ACTIVITIES", "100")
	t.Setenv("FLOWSTATE_WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", "40")
	t.Setenv("FLOWSTATE_WORKER_MAX_ACTIVITIES_PER_SECOND", "5")
	t.Setenv("FLOWSTATE_WORKER_TASK_QUEUE_ACTIVITIES_PER_SECOND", "15")

	cmd, _, err := newRootCommand().Find([]string{"worker"})
	require.NoError(t, err)

	got, err := workerCapacityOptions(cmd)

	require.NoError(t, err)
	require.Equal(t, workerCapacity{
		maxConcurrentActivities:      100,
		maxConcurrentWorkflowTasks:   40,
		activitiesPerSecond:          5,
		taskQueueActivitiesPerSecond: 15,
	}, got)
}

// TestWorkerCapacityOptionsFlagWinsOverTheEnvironment is the same precedence
// every other overridable flag in this command promises.
func TestWorkerCapacityOptionsFlagWinsOverTheEnvironment(t *testing.T) {
	t.Setenv("FLOWSTATE_WORKER_MAX_CONCURRENT_ACTIVITIES", "100")

	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set("max-concurrent-activities", "7"))

	got, err := workerCapacityOptions(cmd)

	require.NoError(t, err)
	require.Equal(t, 7, got.maxConcurrentActivities)
}

// TestWorkerCapacityOptionsRefuseNegativeValues covers all four flags: a
// negative execution size or rate limit does not mean what an operator typing
// "-1" for "no limit" would expect, since 0 is the sentinel that already means
// that, so each is refused before Temporal is dialed, a plugin is launched, or
// a secret provider is opened.
func TestWorkerCapacityOptionsRefuseNegativeValues(t *testing.T) {
	for _, flag := range []string{
		"max-concurrent-activities",
		"max-concurrent-workflow-tasks",
		"max-activities-per-second",
		"task-queue-activities-per-second",
	} {
		t.Run(flag, func(t *testing.T) {
			cmd := workerCommand(t)
			require.NoError(t, cmd.Flags().Set(flag, "-1"))

			_, err := workerCapacityOptions(cmd)

			require.Error(t, err)
			require.Contains(t, err.Error(), "--"+flag)
			require.Contains(t, err.Error(), "negative")
		})
	}
}

// TestWorkerCapacityOptionsRefuseNonFiniteRateValues covers the two rate
// flags specifically: strconv.ParseFloat accepts "NaN" and "Inf" without
// error, and neither is caught by the negative-value check below it — NaN
// compares false to everything, and +Inf is not negative — so both need
// their own refusal rather than falling through into an undefined value
// reaching Temporal's rate-limit configuration.
func TestWorkerCapacityOptionsRefuseNonFiniteRateValues(t *testing.T) {
	for _, flag := range []string{"max-activities-per-second", "task-queue-activities-per-second"} {
		for _, value := range []string{"NaN", "Inf", "+Inf", "-Inf"} {
			t.Run(flag+"/"+value, func(t *testing.T) {
				cmd := workerCommand(t)
				require.NoError(t, cmd.Flags().Set(flag, value))

				_, err := workerCapacityOptions(cmd)

				require.Error(t, err)
				require.Contains(t, err.Error(), "--"+flag)
				require.Contains(t, err.Error(), "finite")
			})
		}
	}
}

// TestWorkerCapacityOptionsRefuseUnparsableValues covers all four flags: a
// non-numeric value is a mistake in the command line, refused with the flag
// named, not left to fail obscurely once worker.New rejects it.
func TestWorkerCapacityOptionsRefuseUnparsableValues(t *testing.T) {
	for _, flag := range []string{
		"max-concurrent-activities",
		"max-concurrent-workflow-tasks",
		"max-activities-per-second",
		"task-queue-activities-per-second",
	} {
		t.Run(flag, func(t *testing.T) {
			cmd := workerCommand(t)
			require.NoError(t, cmd.Flags().Set(flag, "not-a-number"))

			_, err := workerCapacityOptions(cmd)

			require.Error(t, err)
			require.Contains(t, err.Error(), "--"+flag)
			require.Contains(t, err.Error(), "not-a-number")
		})
	}
}

// TestWorkerCapacityOptionsRefusesMaxConcurrentWorkflowTasksOfOne is the
// SDK-specific case: internal_worker.go panics on this exact value ("cannot
// set MaxConcurrentWorkflowTaskExecutionSize to 1"), so this is refused here
// as an ordinary command-line error instead of reaching worker.New as a
// crashed process.
func TestWorkerCapacityOptionsRefusesMaxConcurrentWorkflowTasksOfOne(t *testing.T) {
	cmd := workerCommand(t)
	require.NoError(t, cmd.Flags().Set("max-concurrent-workflow-tasks", "1"))

	_, err := workerCapacityOptions(cmd)

	require.Error(t, err)
	require.Contains(t, err.Error(), "--max-concurrent-workflow-tasks")
}

// TestWorkerCapacityOptionsRefusalIsReachedBeforeTemporalIs mirrors
// TestWorkerRefusalIsReachedBeforeTemporalIs for the capacity flags: nothing
// in this test provides a Temporal server, so the refusal is proof the gate
// sits before any I/O.
func TestWorkerCapacityOptionsRefusalIsReachedBeforeTemporalIs(t *testing.T) {
	t.Setenv("FLOWSTATE_DEPLOYMENT_NAME", "")
	t.Setenv("FLOWSTATE_BUILD_ID", "")

	err := runFlow(t,
		"worker", "--address", "127.0.0.1:1",
		"--"+allowUnversionedFlag, "--max-concurrent-activities", "-5",
	).Err

	require.ErrorContains(t, err, "--max-concurrent-activities")
}
