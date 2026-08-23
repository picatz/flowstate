// Command flow is the Flowstate CLI: it validates, fixes, tests, and runs
// Flowfiles locally, and serves as the control plane, worker, and operator
// tooling for durable execution against Temporal. Run `flow --help` for the
// command tree; docs/reference/ is generated from it.
package main

import (
	"cmp"
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile/lsp"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
	"github.com/picatz/jose/pkg/jwk"
	"github.com/sourcegraph/jsonrpc2"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// Set by the build system, e.g. using -ldflags="-X main.version=1.0.0"
var version = "dev"

// temporalFlags is what a command needs in order to reach Temporal, and what a
// worker needs in order to identify itself.
//
// Read off the command being run rather than held in package variables, which is
// what these were — and what main.go's own TODO asked to be rid of. pflag writes a
// flag's default into its bound pointer at declaration, so building the CLI wrote
// every one of them.
//
// The Temporal connection settings are deliberately empty when unset, which is not
// the same as absent: empty means "use Temporal's own environment configuration" —
// the standard TEMPORAL_* variables and the TOML profile the `temporal` CLI reads —
// so these override that configuration rather than replacing it.
type temporalFlags struct {
	address   string
	namespace string
	profile   string
	taskQueue string

	// Per-tenant task-queue routing, off unless a prefix is configured.
	//
	// taskQueuePrefix is the deployment's spelling of a tenant's queue and has
	// to match on the server and on every worker; tenant is the one namespace a
	// worker will execute, and tenantSet distinguishes "the default tenant"
	// (--tenant=) from "no tenant declared", which are different postures and
	// spell the same empty string.
	//
	// taskQueueExplicit records whether --task-queue was named on the command
	// line, because its default is a value rather than empty: with a tenant and
	// a prefix, the queue is derived, and an operator who named one anyway meant
	// it.
	taskQueuePrefix   string
	tenant            string
	tenantSet         bool
	taskQueueExplicit bool

	// Worker Deployment Versioning, off unless both halves are configured.
	//
	// A version is the pair, so honouring one without the other would produce a
	// worker claiming a version nothing can address. Defaulted from the environment
	// because the build id is a property of the artifact and belongs in whatever
	// built it — a CI job knows the commit; a person typing `flow worker` does not.
	//
	// See pkg/flowstate/v1/engine/versioning.go for what turning this on buys.
	deploymentName string
	buildID        string

	// verbose says whether to describe the connection that was resolved, which is
	// the one thing that makes a misconfigured TEMPORAL_* variable findable.
	verbose bool
}

// temporalFlagsOf reads them off the command being run.
func temporalFlagsOf(cmd *cobra.Command) temporalFlags {
	// A flag names the noun it belongs to (picatz/flowstate#580): these are
	// Temporal's settings, so they carry Temporal's prefix. They used to be
	// spelled --address, --namespace and --profile, which meant `flow server
	// --address` named Temporal's frontend while `flow get --address` named the
	// Flowstate server — one spelling, two meanings, decided by which command
	// declared it. The old spellings are still registered on these two commands
	// so that a command line written against them fails saying so; see
	// cmd/flow/renamedflags.go.
	address, _ := cmd.Flags().GetString("temporal-address")
	namespace, _ := cmd.Flags().GetString("temporal-namespace")
	profile, _ := cmd.Flags().GetString("temporal-profile")
	taskQueue, _ := cmd.Flags().GetString("task-queue")
	taskQueuePrefix, _ := cmd.Flags().GetString("task-queue-prefix")
	tenant, _ := cmd.Flags().GetString("tenant")
	deploymentName, _ := cmd.Flags().GetString("deployment-name")
	buildID, _ := cmd.Flags().GetString("build-id")
	verbose, _ := cmd.Flags().GetBool("verbose")

	return temporalFlags{
		address:           address,
		namespace:         namespace,
		profile:           profile,
		taskQueue:         taskQueue,
		taskQueuePrefix:   taskQueuePrefix,
		tenant:            tenant,
		tenantSet:         cmd.Flags().Changed("tenant"),
		taskQueueExplicit: cmd.Flags().Changed("task-queue"),
		deploymentName:    deploymentName,
		buildID:           buildID,
		verbose:           verbose,
	}
}

// authFlags is how `flow server` decides who it will accept.
//
// There is no default that accepts callers: either a trust policy is configured, or
// anonymous access is requested explicitly.
type authFlags struct {
	policyPath string
	insecure   bool

	// identityKeyPaths holds the keys Flowstate publishes when the trust policy
	// configures federation, in the order they were given. Empty means the
	// server verifies callers but issues nothing, which is the inbound-only
	// deployment.
	//
	// The first names the private key assertions are signed with. Any after it
	// are published for verification only, so assertions a previous process
	// signed keep verifying across the restart that rotation actually is — see
	// [identityBroker].
	identityKeyPaths []string

	// identityClaims names the caller token claims carried into each run's
	// identity, where `workload.claims[...]` rules and downstream relying parties
	// read them. Empty means a run's identity records the subject and issuer and
	// nothing more.
	identityClaims []string
}

// identityKeyDefault is what --identity-key holds when it is not given: the one
// path $FLOWSTATE_IDENTITY_KEY names, or nothing.
//
// Nothing, rather than a list holding one empty string, which is what wrapping
// an unset variable would produce — and which would then be read as a key path
// of "" and refuse start-up on a deployment that never configured federation.
// A repeated flag replaces this default outright rather than appending to it
// (pflag's StringArray), so an operator who sets both names the whole key set on
// the command line, which is the reading that makes rotation's order explicit.
// The variable stays single-valued: it is the inbound-only deployment's one key,
// and a list of paths in an environment variable would need a separator this
// repository has no convention for.
func identityKeyDefault() []string {
	if path := os.Getenv("FLOWSTATE_IDENTITY_KEY"); path != "" {
		return []string{path}
	}
	return nil
}

// identityKeyUsage is the help text every command that loads identity keys
// shows for --identity-key, so the rotation rule is worded once.
const identityKeyUsage = "PKCS#8 PEM key used to mint short-lived workload assertions for federation " +
	"targets (repeatable: the first signs, and every later one is published for verification only, " +
	"so assertions signed before a restart keep verifying)"

// authFlagsOf reads them off the command being run.
func authFlagsOf(cmd *cobra.Command) authFlags {
	policyPath, _ := cmd.Flags().GetString("auth-policy")
	insecure, _ := cmd.Flags().GetBool("insecure-no-auth")
	identityKeyPaths, _ := cmd.Flags().GetStringArray("identity-key")
	identityClaims, _ := cmd.Flags().GetStringArray("identity-claim")

	return authFlags{
		policyPath:       policyPath,
		insecure:         insecure,
		identityKeyPaths: identityKeyPaths,
		identityClaims:   identityClaims,
	}
}

// infraLogger is the server's and worker's own voice: slog, text, stderr.
//
// These two processes logged through the stdlib log package — unstructured,
// unleveled — while everything around them was structured: the plugin host
// speaks slog, the activities bridge slog into Temporal's tagged logger, and
// the `log:` task renders through slog handlers. The infrastructure's own
// lines were the odd ones out, which mattered the moment telemetry landed:
// a fleet's collector can parse key=value pairs and cannot parse prose.
//
// Now with a second destination when one is configured, and stderr either way:
// [telemetryLogHandler] adds the OTLP bridge beside the text handler rather than
// in place of it, so a collector parses nothing and a person watching a terminal
// loses nothing. These lines carry no trace id — they are start-up and shutdown
// commentary, emitted outside any span; see the note above [telemetryLogHandler]
// for which paths do correlate.
func infraLogger() *slog.Logger {
	return slog.New(telemetryLogHandler(slog.NewTextHandler(os.Stderr, nil)))
}

// initTemporalClient connects to Temporal.
//
// Configuration comes from Temporal's own environment configuration — the
// standard TEMPORAL_* variables and the same TOML profile file the `temporal` CLI
// reads — so a self-hosted cluster, Temporal Cloud, and a local development
// server differ only in configuration. Flags override whatever that resolves to.
// Takes its configuration rather than reading it, and no longer memoizes the
// client. The memo was a package variable guarding a second call that cannot
// happen: the two callers are `flow worker` and `flow server`, and a process runs
// one command. What it did do was outlive whatever set it.
func initTemporalClient(ctx context.Context, flags temporalFlags) (client.Client, error) {
	cfg, err := temporalConfig(ctx, flags)
	if err != nil {
		return nil, err
	}

	return temporalclient.Dial(ctx, cfg)
}

// temporalConfig resolves the connection configuration a command's flags describe,
// with telemetry attached.
//
// Split from [initTemporalClient] because `flow server` needs the configuration
// twice when the trust policy maps tenants onto Temporal namespaces: once for the
// client it was configured with, and once for the pool that dials each mapped
// namespace. Resolving it in one place keeps the two from ever describing
// different clusters.
func temporalConfig(ctx context.Context, flags temporalFlags) (temporalclient.Config, error) {
	// Telemetry first, so the client is born instrumented, and before the RPC
	// interceptors are built: otelconnect captures the global tracer provider
	// and propagator at construction, so an interceptor built ahead of this
	// captures the no-op ones and keeps them for the life of the process.
	//
	// Off unless the operator pointed OTEL_EXPORTER_OTLP_* somewhere. Started
	// rather than initialized, because `flow server` reaches here twice when
	// the trust policy maps tenants onto namespaces, and the flush this
	// registers has to reach one set of providers rather than the last of
	// several — runServer and runWorker call flushTelemetry at their teardown.
	metricsHandler, err := startTelemetry(ctx)
	if err != nil {
		return temporalclient.Config{}, err
	}

	// The payload codec, resolved once for the process and carried on the
	// configuration rather than passed to Dial: `flow server` builds a pool from
	// this same value, one client per mapped Temporal namespace, and a codec
	// that covered only the fallback client would leave every mapped tenant's
	// payloads in plaintext. See [payloadCodecConfig].
	codec, err := payloadCodecConfig()
	if err != nil {
		return temporalclient.Config{}, err
	}

	cfg := temporalclient.Config{
		Address:        flags.address,
		Namespace:      flags.namespace,
		Profile:        flags.profile,
		MetricsHandler: metricsHandler,
		Interceptors:   temporalClientInterceptors(),
		Codec:          codec,
	}

	if flags.verbose {
		if opts, err := cfg.Options(); err == nil {
			infraLogger().Info("temporal connection resolved", "config", temporalclient.Describe(opts))
		}
	}

	return cfg, nil
}

// allowUnversionedFlag is how an operator says they accept running the
// interpreter with nothing pinning it.
//
// Named for what is being accepted rather than for the check being skipped, in the
// same spirit as --allow-insecure-plugin-dir: the flag that permits a plugin
// directory other users can write to says so, instead of saying "--no-plugin-check".
// A person reading a command line should be able to see the risk without reading
// the code that enforces it.
const allowUnversionedFlag = "allow-unversioned-interpreter"

// workerDeployment resolves the versioning posture a `flow worker` command
// describes, and refuses the ones that are unsafe or incoherent.
//
// The refusal exists because the interpreter evaluates CEL *in workflow code* —
// step conditions, a loop's `items:`, a step's own `vars:`, and every task input
// that does not declare `needs_prev_outputs`. What those expressions mean is
// decided by the cel-go compiled into this binary, and on an unversioned worker
// nothing pins which binary a run in flight is handed to. docs/DSL.md states that
// dependency as a deployment precondition; this is where it stops being a note.
//
// Zero-config local development still works, and invariant 8 is what says how much
// it has to: every feature must work against `temporal server start-dev` with no
// cloud dependency. Worker Deployment Versioning is not a cloud dependency — the
// versioning end-to-end test drives two builds against a dev server — so an
// operator's honest options on a laptop are both open. What invariant 8 does forbid
// is a dead end, so the refusal is written to be a signpost: it names the flag to
// type, and typing it is the whole of the fix.
//
// Detecting the dev server and exempting it was the alternative, and it was
// rejected for being a guess. The address a dev server listens on is configurable,
// a production cluster can be reached at localhost through a tunnel, and a rule
// that decides how much safety to enforce by pattern-matching a hostname fails open
// on exactly the deployment that most needs it.
func workerDeployment(cmd *cobra.Command, flags temporalFlags) (worker.DeploymentOptions, error) {
	deployment, err := engine.DeploymentOptions(flags.deploymentName, flags.buildID)
	if err != nil {
		return worker.DeploymentOptions{}, err
	}

	if deployment.UseVersioning {
		return deployment, nil
	}

	if allowed, _ := cmd.Flags().GetBool(allowUnversionedFlag); allowed {
		return deployment, nil
	}

	return worker.DeploymentOptions{}, fmt.Errorf(
		"refusing to start an unversioned worker: this worker evaluates workflow expressions "+
			"(step conditions, a loop's items:, a step's vars:, task inputs) in workflow code, so the "+
			"expression engine built into this binary decides what they mean; with no version, "+
			"deploying a different binary changes what every run already in flight computes, including "+
			"where a run resumes after continue-as-new. Pass --deployment-name and --build-id "+
			"(or FLOWSTATE_DEPLOYMENT_NAME and FLOWSTATE_BUILD_ID) to pin each run to the interpreter "+
			"it started on, or --%s to accept that exposure, which is what a local "+
			"`temporal server start-dev` session usually wants",
		allowUnversionedFlag)
}

// workerIdentity is what this worker calls itself to Temporal — the string an
// operator sees in Event History and in a Task Queue's poller list when
// tracing a stuck task back to the process that owns it (#752).
//
// Left unset, the SDK falls back to "pid@hostname@taskqueue", and Temporal's
// own worker documentation calls that out as weak in exactly the deployment
// shapes docs/DEPLOYMENT.md documents: a container's PID 1 is always `1`, and
// a Kubernetes-assigned or Docker-assigned hostname is not something an
// operator can act on without first looking up which pod or container it
// belongs to. Several replicas of the same worker Deployment differ, under
// the SDK default, only by that hostname fragment.
//
// So the default here is built from what this process already knows is
// stable and meaningful: the Worker Deployment version (--deployment-name/
// --build-id), which is the same identity the worker already logs at
// startup, and the tenant restriction when --tenant is set, since a run
// misrouted to the wrong tenant's worker is exactly the kind of thing this
// string exists to make traceable. The hostname is appended last, unchanged
// from what the SDK would have used — it still disambiguates replicas of the
// identical deployment/tenant pair sharing a task queue, which the versioned
// prefix alone cannot.
//
// --identity (or FLOWSTATE_WORKER_IDENTITY) overrides all of this outright,
// for an operator with a platform-native identifier worth using directly —
// a Kubernetes pod name from the downward API, an ECS task id — rather than
// composed from what this binary can see of its own configuration.
func workerIdentity(cmd *cobra.Command, deployment worker.DeploymentOptions, flags temporalFlags) string {
	if override, _ := cmd.Flags().GetString("identity"); override != "" {
		return override
	}

	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}

	var parts []string
	if deployment.UseVersioning {
		parts = append(parts, deployment.Version.DeploymentName+"/"+deployment.Version.BuildID)
	}
	if flags.tenantSet {
		tenant := flags.tenant
		if tenant == "" {
			tenant = "_default"
		}
		parts = append(parts, "tenant="+tenant)
	}
	parts = append(parts, host)

	return strings.Join(parts, "@")
}

// workerStopTimeout reads --worker-stop-timeout, which arrives as a string flag
// rather than cobra's Duration type for the reason explained where the flag is
// registered: pflag's own Duration formatting would hide the flag from
// `flow docs generate`'s environment-mirror detection. Parsed with
// [v1.ParseDuration], the same grammar the DSL itself accepts, so a value that is
// legal in a Flowfile's `sleep:` is legal here too.
//
// A negative value is refused rather than passed through: [v1.ParseDuration]
// accepts one (`-1s`), and the SDK's own Stop treats a negative stopTimeout the
// same as a zero one — its shutdown timer fires immediately, so the drain this
// whole flag exists to configure would silently not happen. That is exactly the
// in-flight-work loss #751 reports, reintroduced through the fix for it.
func workerStopTimeout(cmd *cobra.Command) (time.Duration, error) {
	raw, _ := cmd.Flags().GetString("worker-stop-timeout")

	stopTimeout, err := v1.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid --worker-stop-timeout %q: %w", raw, err)
	}

	if stopTimeout < 0 {
		return 0, fmt.Errorf(
			"invalid --worker-stop-timeout %q: must not be negative; a negative value "+
				"disables the drain instead of shortening it", raw)
	}

	return stopTimeout, nil
}

// workerCapacity holds the subset of worker.Options this worker exposes as
// flags: what Temporal's slot-exhaustion runbook names first
// (MaxConcurrentActivityExecutionSize, MaxConcurrentWorkflowTaskExecutionSize),
// plus the two rate limits #785 folded into this issue's scope
// (WorkerActivitiesPerSecond, TaskQueueActivitiesPerSecond). Poller counts and
// the sticky workflow cache size are the next tier (#783's own scoping call)
// and are not represented here.
type workerCapacity struct {
	maxConcurrentActivities      int
	maxConcurrentWorkflowTasks   int
	activitiesPerSecond          float64
	taskQueueActivitiesPerSecond float64
}

// workerCapacityOptions reads --max-concurrent-activities,
// --max-concurrent-workflow-tasks, --max-activities-per-second, and
// --task-queue-activities-per-second, each defaulted from a FLOWSTATE_WORKER_*
// environment variable exactly like --worker-stop-timeout above.
//
// Every one of these arrives as a string flag, not cobra's Int or Float64,
// for the identical reason workerStopTimeout's doc comment gives: pflag
// reprints a typed default in its own canonical form, which
// `flow docs generate`'s environment-mirror detection cannot tell apart from
// a literal constant. A plain string default passes the sentinel through
// unchanged, so these stay visible to the docs generator the same way
// --worker-stop-timeout is.
//
// Zero is the sentinel for "take the Temporal SDK's own default" on all four
// fields — verified against the SDK's own augmentWorkerOptions
// (go.temporal.io/sdk@v1.47.0/internal/internal_worker.go:2807-2834), which
// substitutes its default whenever MaxConcurrentActivityExecutionSize is <= 0
// or WorkerActivitiesPerSecond/TaskQueueActivitiesPerSecond equal 0. So an
// unset flag changes nothing: it produces the same zero value worker.Options
// already had before this flag existed.
//
// A negative value is refused before it reaches worker.New, the same posture
// workerStopTimeout takes: passed through unchecked, a negative
// MaxConcurrentActivityExecutionSize or rate limit does not mean what an
// operator typing "-1" for "no limit" would expect, and each field would
// silently take a different meaning than the zero sentinel this flag
// documents. And --max-concurrent-workflow-tasks=1 is refused explicitly:
// the SDK panics on that exact value (internal_worker.go:2308,
// "cannot set MaxConcurrentWorkflowTaskExecutionSize to 1", because a worker
// with one workflow-task slot only ever polls the sticky queue) — a refusal
// here is a command-line error message; the same value reaching worker.New
// is a crashed process.
func workerCapacityOptions(cmd *cobra.Command) (workerCapacity, error) {
	maxActivities, err := parseWorkerCapacityInt(cmd, "max-concurrent-activities")
	if err != nil {
		return workerCapacity{}, err
	}

	maxWorkflowTasks, err := parseWorkerCapacityInt(cmd, "max-concurrent-workflow-tasks")
	if err != nil {
		return workerCapacity{}, err
	}
	if maxWorkflowTasks == 1 {
		return workerCapacity{}, fmt.Errorf(
			"invalid --max-concurrent-workflow-tasks \"1\": the Temporal SDK refuses this exact " +
				"value (a worker with one workflow-task slot never polls its regular queue); use 0 " +
				"for the SDK default or a value of 2 or more")
	}

	activitiesPerSecond, err := parseWorkerCapacityFloat(cmd, "max-activities-per-second")
	if err != nil {
		return workerCapacity{}, err
	}

	taskQueueActivitiesPerSecond, err := parseWorkerCapacityFloat(cmd, "task-queue-activities-per-second")
	if err != nil {
		return workerCapacity{}, err
	}

	return workerCapacity{
		maxConcurrentActivities:      maxActivities,
		maxConcurrentWorkflowTasks:   maxWorkflowTasks,
		activitiesPerSecond:          activitiesPerSecond,
		taskQueueActivitiesPerSecond: taskQueueActivitiesPerSecond,
	}, nil
}

// parseWorkerCapacityInt parses one of the two execution-size flags: a
// non-negative integer, with 0 meaning "take the SDK default" (see
// workerCapacityOptions).
func parseWorkerCapacityInt(cmd *cobra.Command, flag string) (int, error) {
	raw, _ := cmd.Flags().GetString(flag)

	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("invalid --%s %q: must be a whole number; 0 takes the Temporal SDK default", flag, raw)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid --%s %q: must not be negative; use 0 for the Temporal SDK default", flag, raw)
	}

	return n, nil
}

// parseWorkerCapacityFloat parses one of the two rate-limit flags: a
// non-negative number, with 0 meaning "take the SDK default" (effectively
// unlimited — see workerCapacityOptions).
func parseWorkerCapacityFloat(cmd *cobra.Command, flag string) (float64, error) {
	raw, _ := cmd.Flags().GetString(flag)

	n, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid --%s %q: must be a number; 0 takes the Temporal SDK default", flag, raw)
	}
	// NaN and Inf both parse successfully and both fail "< 0" (NaN compares
	// false to everything, +Inf is not negative), so neither is caught by the
	// bound below — checked explicitly rather than trusted to it.
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, fmt.Errorf("invalid --%s %q: must be a finite number; 0 takes the Temporal SDK default", flag, raw)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid --%s %q: must not be negative; use 0 for the Temporal SDK default", flag, raw)
	}

	return n, nil
}

// runWorker implements the worker sub-command to start a Temporal worker
// to process Flowstate workflows and activities.
func runWorker(cmd *cobra.Command, args []string) error {
	flags := temporalFlagsOf(cmd)

	// First, because it reads flags and nothing else: a worker whose versioning is
	// half-configured or unaccounted for should say so before it dials Temporal,
	// launches a plugin, or opens a secret provider.
	deployment, err := workerDeployment(cmd, flags)
	if err != nil {
		return err
	}

	// Same reasoning, one flag later: which queue this worker polls, and whether
	// it is restricted to one tenant, are decided from flags alone and refused
	// here rather than after a connection, a plugin launch, and a secret
	// provider have been opened on a worker that is about to be told it may not
	// start. See [workerTaskQueue] for the combinations it refuses.
	taskQueue, err := workerTaskQueue(flags)
	if err != nil {
		return err
	}

	// Before any I/O, and before the worker can poll: a policy file that does not
	// load must refuse the command, not leave a worker running the default policy
	// its operator believes was replaced.
	if err := applyEgressPolicy(cmd); err != nil {
		return err
	}

	// Same reasoning, for #187's task-shape policy: a worker that started
	// anyway would dispatch every task unrestricted while its operator
	// believes the file governs them.
	if err := applyTaskPolicy(cmd); err != nil {
		return err
	}

	// Same reasoning again: an unparsable --worker-stop-timeout is a mistake in
	// the command line, not something that should wait until Temporal is dialed,
	// a plugin is launched, and a secret provider is opened to surface.
	stopTimeout, err := workerStopTimeout(cmd)
	if err != nil {
		return err
	}

	// Same reasoning, one flag family later: an unparsable or out-of-range
	// capacity flag is a command-line mistake, not something a connection
	// failure or a plugin-launch failure should be the first sign of (#783).
	capacity, err := workerCapacityOptions(cmd)
	if err != nil {
		return err
	}

	secretProviders, secretsConfigured, closeSecretProviders, err := secretRegistry(cmd)
	if err != nil {
		return err
	}
	defer closeSecretProviders()

	c, err := initTemporalClient(cmd.Context(), flags)
	if err != nil {
		return err
	}
	defer c.Close()

	// The interpreter's own copy of the converter this client was built with.
	// Workflow-side code replaces the context's converter to decode a signal in
	// either wire shape, and the SDK offers no way to read the one it is
	// replacing, so without this the wrapper would fall back to the default
	// converter and quietly fail to decode every signal on a deployment with a
	// codec. See engine/codec.go.
	workerCodec, err := payloadCodecConfig()
	if err != nil {
		return err
	}
	engine.UseCodec(workerCodec)

	// Before the worker starts polling, because a worker that accepted a step for
	// a plugin task it has not registered yet would answer `unknown task` for a
	// workflow that is correct — and Open is strict, so a plugin that cannot come
	// up fails the command here rather than one step at a time later.
	pluginCatalog, closePlugins, err := startPlugins(cmd, secretProviders)
	if err != nil {
		return err
	}
	defer closePlugins()

	runtime, err := workerRuntime(cmd, secretProviders, secretsConfigured)
	if err != nil {
		return err
	}

	// The worker's own catalog, kept rather than dropped once the tasks were
	// registered. Registration says which tasks this worker can dispatch; this
	// says which *build* of each it dispatches them to, which is what a run pinned
	// at submit is admitted against before any step of it executes here. Carried
	// on the configuration this worker is registered with rather than installed
	// process-wide, so the answer belongs to this worker and no other worker in
	// this process can overwrite it. See engine/plugins.go.
	runtime = runtime.WithPluginCatalog(pluginCatalog)

	interceptors := temporalWorkerInterceptors()
	if flags.tenantSet {
		// Appended rather than replacing, so a tenant-restricted worker is still
		// traced: the refusal is a thing an operator will want to see in a trace
		// beside the runs that succeeded.
		interceptors = append(interceptors, engine.TenantInterceptor(flags.tenant))
	}

	identity := workerIdentity(cmd, deployment, flags)

	w := worker.New(c, taskQueue, worker.Options{
		DeploymentOptions:                      deployment,
		Interceptors:                           interceptors,
		DeadlockDetectionTimeout:               v1.WorkerDeadlockDetectionTimeout,
		Identity:                               identity,
		WorkerStopTimeout:                      stopTimeout,
		MaxConcurrentActivityExecutionSize:     capacity.maxConcurrentActivities,
		MaxConcurrentWorkflowTaskExecutionSize: capacity.maxConcurrentWorkflowTasks,
		WorkerActivitiesPerSecond:              capacity.activitiesPerSecond,
		TaskQueueActivitiesPerSecond:           capacity.taskQueueActivitiesPerSecond,
	})

	engine.Register(w, runtime)

	if flags.tenantSet {
		// Said at startup as well as at each refusal, because the operator
		// reading a refused run's failure a month later is usually not the one
		// who wrote this command line — the same argument the unversioned
		// warning below makes.
		infraLogger().Info("worker restricted to one tenant; runs belonging to any other will be refused",
			"tenant", flags.tenant, "task_queue", taskQueue)
	}

	if deployment.UseVersioning {
		infraLogger().Info("starting worker",
			"task_queue", taskQueue,
			"deployment", deployment.Version.DeploymentName,
			"build_id", deployment.Version.BuildID,
			"identity", identity)
	} else {
		// Reached only with --allow-unversioned-interpreter, since workerDeployment
		// refuses otherwise. Still said out loud on every start rather than only at
		// the moment the flag was typed: the person reading a worker's logs a month
		// later is usually not the person who wrote its command line.
		infraLogger().Warn("starting worker unversioned; deploying this binary changes every run in flight",
			"task_queue", taskQueue,
			"accepted_with", "--"+allowUnversionedFlag,
			"identity", identity,
			"fix", "set FLOWSTATE_DEPLOYMENT_NAME and FLOWSTATE_BUILD_ID, or --deployment-name and --build-id")
	}

	// Start worker (non-blocking) such that it can run in the background
	// while we wait for shutdown signals.
	err = w.Start()
	if err != nil {
		return fmt.Errorf("unable to start worker: %w", err)
	}

	// Listen for shutdown signals to gracefully stop the worker. Stop() itself
	// does the draining: it stops polling for new work immediately, then blocks
	// until every in-flight activity and workflow task finishes or
	// stopTimeout elapses, whichever comes first (WorkerStopTimeout above).
	<-cmd.Context().Done()
	infraLogger().Info("shutting down worker, draining in-flight work",
		"worker_stop_timeout", stopTimeout)
	w.Stop()

	// After the worker has stopped, so the last activity's spans and the last
	// interval's metrics are in the batch this pushes — those are the ones
	// somebody is looking for when they come to ask what a worker was doing
	// when it went away. Bounded and best-effort; see flushTelemetry.
	flushTelemetry()

	infraLogger().Info("worker stopped")

	return nil
}

// runWorkflow starts a workload on a Flowstate server and follows it to the end.
//
// Starting and following are one command because that is what somebody running a
// workload from a terminal wants, and they are two verbs underneath: `flow watch`
// exists for the run that outlived the terminal that started it. Following therefore
// happens through exactly the machinery that command uses, rather than through a poll
// loop of its own.
//
// That is not tidiness for its own sake. The loop this replaces reported RUNNING and
// COMPLETED and FAILED, and treated every other status as "still going" — so a
// canceled, terminated, or timed-out run left `flow run` printing "still going"
// forever about a run that had stopped. Anything observable about following a run is
// now decided in one place, which is the only way the two commands can agree.
func runWorkflow(cmd *cobra.Command, args []string) error {
	rendering, err := resolveRunRendering(cmd)
	if err != nil {
		return err
	}

	format := rendering.format

	// Before the file is read, which is the earliest this command can say
	// anything: a run that never starts still says where it was going, so
	// `flow run typo.yaml` in a shell holding a production address reports the
	// address as well as the typo. See venue.go for the model, and for why the
	// tenant the server will derive is not part of the sentence.
	announceVenue(cmd, serverVenue(serverFlagsOf(cmd), os.Getenv))

	workflow, err := loadWorkflow(args[0])
	if err != nil {
		return err
	}

	surface := newSurface(cmd)

	// The arguments this run is started with, coerced against what the file declares.
	// Checked here as well as at the server, for the message rather than for the
	// control: a missing or mistyped argument is a fact about the command line, and
	// reading it back as a remote invalid-argument sends an author looking at the
	// wrong machine. The server binds them again regardless.
	inputs, err := runInputs(cmd, workflow)
	if err != nil {
		return err
	}
	if err := checkRunInputs(workflow, inputs); err != nil {
		return err
	}

	reason, _ := cmd.Flags().GetString("reason")

	server := serverFlagsOf(cmd)

	// Built once and used for both the request that starts the run and every
	// poll of the follow phase below, rather than once per RPC — see
	// [newFollowClient]. That also moves a misconfigured --credential-source
	// ahead of the Flowfile being submitted at all: refused here, before
	// anything has started, instead of surfacing from the transport on the
	// first request the same as any other refusal.
	client, err := newFollowClient(server)
	if err != nil {
		return err
	}

	started, err := client.Run(cmd.Context(),
		connect.NewRequest(&v1.RunRequest{Workflow: workflow, Inputs: inputs, Reason: reason}))
	if err != nil {
		arguments, redacted := runArgumentFlags(cmd, workflow)
		return refusedStart(args[0], workflow.GetName(), arguments, redacted, server, err)
	}

	workflowID := started.Msg.GetWorkflowId()

	// What this run is called, for the prose. The file has just been parsed here,
	// so the workflow's own name is available and is the noun the author already
	// has for it; the id is a string they have never seen before and cannot have
	// an opinion about. See [watch.Named].
	subject := runSubject(workflow, workflowID)

	// Said before the following begins, because it is the one fact somebody needs in
	// order to come back to this run later, and following is where they might stop
	// paying attention. Only to a person: the machine formats carry the id in every
	// document they emit, so saying it again in prose would be something a reader has
	// to parse past.
	//
	// The id appears exactly once, inside the command it is for. It used to appear
	// twice on this line and then again on every line of the follow, which is most
	// of what made a finished run something to decipher (picatz/flowstate#544): the
	// signal is that a run started and can be returned to, and every other character
	// was identifier. Once, in the `flow watch` hint, is where it earns its width —
	// that is the one place a reader does something with it rather than reads it.
	if format == FormatText {
		fmt.Fprintf(surface.Err, "started workflow %s; come back to it with `flow watch %s`\n",
			subject, workflowID)
	}

	// Deliberately not pinned to the run just started. A workload that continues as
	// new gets a fresh run id, and a watch pinned to the first one would report the
	// state of a run that has already handed over — or stop finding it at all.
	//
	// What the run just started *is* handed over, as the state the follow begins from.
	// That is what a machine-readable caller falls back on when it is interrupted
	// before the first poll: without it, `flow run -o json` stopped with a durable
	// workload running and no document naming it.
	interval, _ := cmd.Flags().GetDuration("interval")
	plain, _ := cmd.Flags().GetBool("plain")

	// Unlike `flow watch` on a later invocation, `flow run` just parsed and
	// submitted workflow itself — but holding a specification is not the same as
	// holding the one that ran, and this deployment may have substituted its own
	// copy for it. The poller redacts precisely only against a specification the
	// server attested is the executed one, and falls back to the fail-closed case
	// otherwise; see [executedSpecification] and #734.
	reveal := revealSensitiveRequested(cmd)
	executed := executedSpecification(workflow, started.Msg)
	switch {
	case reveal:
		noteRevealedSensitiveValues(surface)
	case executed == nil:
		noteUnattestedSpecification(surface)
	}

	// spec is the executed specification, not the submitted workflow: the poller
	// redacts against what the server attested it ran (#734/#826), and only the run
	// naming — namedRun(subject) — comes from this branch. The two are independent:
	// what a run is *called* in prose is the workflow's own name, while what it is
	// *redacted against* has to be the attested copy or the fail-closed case.
	return watchRun(cmd.Context(), surface, rendering,
		clientPoller{workflowID: workflowID, server: server, client: client, spec: executed, reveal: reveal},
		clampWatchInterval(interval), plain, workflowID, startedRun(started.Msg), namedRun(subject))
}

// runSubject is how a run this command started is named in prose.
//
// The workflow's own name, which is what the author typed and what `flow run local`
// already says for the same run — the two drivers describing themselves the same
// way is the requirement, not an incidental nicety, because a rehearsal exists to
// tell an author what production will do.
//
// Falls back to the id when a workflow has no usable name. `flow validate` refuses a
// nameless file, so this is the fallback for a document that reached the server some
// other way rather than an expected case; naming nothing at all would be the one
// unreadable answer.
//
// Read from the submitted workflow rather than the executed specification on
// purpose: a deployment may substitute its own copy of what runs, but the name a
// person typed is the name they have, and prose is addressed to that person. The
// redaction spec above is the one that must be the attested copy; this one must not.
func runSubject(workflow *v1.Workflow, workflowID string) string {
	if name := strings.TrimSpace(workflow.GetName()); name != "" {
		return name
	}

	return workflowID
}

// startedRun is what `Run` answered, in the shape a follow reports.
//
// RunResponse and GetResponse are the same five fields under two names — one is what
// starting a run answers and the other what asking about one answers — so a follow that
// begins from a start has to say which it is holding. Converted rather than the schema
// being collapsed, because that is a wire contract and this is four lines.
func startedRun(started *v1.RunResponse) *v1.GetResponse {
	run := &v1.GetResponse{
		WorkflowId: started.GetWorkflowId(),
		RunId:      started.GetRunId(),
		Status:     started.GetStatus(),
	}

	if failure := started.GetError(); failure != nil {
		run.Kind = &v1.GetResponse_Error{Error: failure}
	}

	return run
}

// runServer implements the server sub-command to start a Flowstate server
// that listens for incoming workflow requests and serves them using the
// Flowstate service implementation over HTTP (via Connect RPC).
func runServer(cmd *cobra.Command, args []string) error {
	logger := infraLogger()

	// Resolve configuration before doing any I/O, so a misconfiguration is
	// reported immediately rather than after waiting on a connection attempt.
	authCfg := authFlagsOf(cmd)
	temporalCfg := temporalFlagsOf(cmd)

	// The internal listener's address is validated now — loopback or
	// empty, nothing else, per cmd/flow/internallistener.go — but not bound
	// until this function is about to serve, so a later failure does not
	// leave a socket open behind an error return.
	internalFlags := internalListenerFlagsOf(cmd)
	if err := checkInternalListenAddress(internalFlags.address); err != nil {
		return err
	}

	// The trust policy is loaded before the public listener's certificate is
	// resolved, not after as an earlier version of this function did,
	// because [resolveACMESettings]'s federation cross-check (see
	// cmd/flow/acme.go) needs policy.Federation.Issuer to compare against
	// --tls-acme-hosts, and a value cannot be cross-checked before it is
	// read.
	verifier, policy, err := authVerifier(authCfg)
	if err != nil {
		return err
	}

	broker, err := identityBroker(authCfg, policy)
	if err != nil {
		return err
	}

	// RFC 9728 protected resource metadata, resolved against the same policy
	// the verifier was built from: an advertised authorization server this
	// policy would not trust a token from is a start-up failure here, not a
	// per-request 401 a client discovers after already trusting the document.
	// See cmd/flow/protectedresource.go.
	protectedResource, err := resolveProtectedResource(protectedResourceFlagsOf(cmd), policy)
	if err != nil {
		return err
	}
	if err := checkProtectedResourceRouteCollision(protectedResource, broker); err != nil {
		return err
	}

	// The public listener's certificate, and whether the address it is about
	// to bind may go without one. Checked before any further I/O beyond the
	// files and policy already read above: a certificate this process cannot
	// load or obtain, or a non-loopback address with none configured, is a
	// start-up failure rather than something discovered after Temporal has
	// been dialed. See cmd/flow/tls.go and cmd/flow/acme.go.
	tlsListenerFlags := tlsFlagsOf(cmd)
	acmeListenerFlags := acmeFlagsOf(cmd)

	var federationIssuer string
	if policy != nil && policy.Federation != nil {
		federationIssuer = policy.Federation.Issuer
	}

	acmeCfg, err := resolveACMESettings(acmeListenerFlags, tlsListenerFlags, internalFlags.address, federationIssuer)
	if err != nil {
		return err
	}

	var tlsCfg *tls.Config
	if acmeCfg != nil {
		tlsCfg = acmeCfg.tlsConfig()
	} else {
		tlsCfg, err = serverTLSConfig(tlsListenerFlags)
		if err != nil {
			return err
		}
	}

	// --listen defaults to $FLOWSTATE_ADDRESS (cmp.Or at registration, above in
	// this file), so an operator who only ever set the environment variable
	// sees no change; --listen is what lets them override it from the command
	// line, which nothing did before.
	publicAddr, _ := cmd.Flags().GetString("listen")
	if err := refusePlaintextListener(publicAddr, tlsCfg, tlsListenerFlags.tlsTerminatedUpstream); err != nil {
		return err
	}

	// mTLS, picatz/flowstate#582: whether the public listener requires a
	// client certificate, and whether a verified one also authenticates the
	// caller. Resolved (and, if requested, applied to tlsCfg in place) after
	// refusePlaintextListener, so a plaintext posture is refused for its own
	// reason first and mTLS's own refusals — --tls-terminated-upstream, no
	// server TLS at all, no kind: mtls policy entry — are reported in terms
	// of the listener this process actually ended up with.
	mtlsListenerFlags := mtlsFlagsOf(cmd)
	peerVerifier, err := resolveMTLS(mtlsListenerFlags, policy, tlsListenerFlags.tlsTerminatedUpstream, tlsCfg)
	if err != nil {
		return err
	}

	// picatz/flowstate#629: when ACME also terminates this listener,
	// resolveMTLS just mutated the very tls.Config autocert's GetCertificate
	// answers every handshake from, including the ACME CA's own
	// TLS-ALPN-01 validation connection — which holds no certificate from
	// the deployment's private client CA pool. Left alone,
	// --tls-client-auth require would refuse that connection at the TLS
	// layer before autocert ever decides anything, and renewal would fail
	// quietly, sixty days out. Exempting it is safe only because it keys on
	// the ALPN offer autocert itself uses to recognize the same connection
	// (see acme.go's doc on this), so it is wired here, after tlsCfg's
	// ClientAuth is whatever --tls-client-auth actually resolved to, and
	// only for a listener this file's own ACME settings built.
	if acmeCfg != nil {
		exemptACMETLSALPN01ChallengeFromClientAuth(tlsCfg)
	}

	// Acquisition is startup, but it cannot happen *here*: see the ordering
	// note where priming actually runs, below the point this listener starts
	// serving. #628.

	// Fetch every trusted issuer's keys now, so an issuer that is misconfigured
	// or unreachable is reported at startup instead of as a puzzling
	// authentication failure on the first request. Log-and-continue rather than
	// refuse to start, per Prime's own contract: keys are fetched on demand
	// anyway, and an identity provider having a bad minute should not keep a
	// deployment down.
	if oidc, ok := verifier.(*auth.OIDCVerifier); ok {
		if err := oidc.Prime(cmd.Context()); err != nil {
			logger.Warn("could not prefetch every trusted issuer's keys; verification will retry on demand",
				"error", err)
		}
	}

	cfg, err := temporalConfig(cmd.Context(), temporalCfg)
	if err != nil {
		return err
	}

	c, err := temporalclient.Dial(cmd.Context(), cfg)
	if err != nil {
		return err
	}
	defer c.Close()

	// Before any of it is used, so a prefix that cannot compose a legal queue
	// name stops the server rather than failing every submission after it — the
	// rule CLAUDE.md states for policy surfaces, applied to this one.
	taskQueues := engine.TaskQueues{Prefix: temporalCfg.taskQueuePrefix}
	if err := taskQueues.Validate(); err != nil {
		return fmt.Errorf("--task-queue-prefix: %w", err)
	}

	// The same converter every client dialed from cfg was built with, taken
	// from cfg itself rather than resolved a second time, so the two cannot
	// disagree about which codec this process runs.
	//
	// This is the read side of the write side. A memo is encoded with the
	// client's converter, so on a deployment with a codec configured every memo
	// this server writes is ciphertext; a server reading them back with the SDK
	// default would decode none of them, answer "this run is not yours" for
	// every run, and hide the whole deployment from the tenants that own it.
	// See [server.WithDataConverter].
	serverOpts := []server.Option{server.WithDataConverter(cfg.Codec.DataConverter())}
	if taskQueues.Enabled() {
		serverOpts = append(serverOpts, server.WithTaskQueues(taskQueues))
	}
	if temporalCfg.deploymentName != "" {
		serverOpts = append(serverOpts, server.WithDeployment(temporalCfg.deploymentName))
	}
	if len(authCfg.identityClaims) > 0 {
		serverOpts = append(serverOpts, server.WithIdentityClaims(authCfg.identityClaims...))
	}
	if policy != nil {
		var targets []string
		if broker != nil {
			targets = broker.Targets()
		}
		serverOpts = append(serverOpts, server.WithCredentialTargets(targets...))
	}

	// Search attributes are registered — idempotently, once, before the server
	// starts serving — only in the single-namespace configuration. A trust
	// policy that maps tenants onto several Temporal namespaces would need
	// this attempted once per mapped namespace, which is not done here; see
	// [server.EnsureSearchAttributesRegistered]'s doc for why that is an
	// honest cut and not an oversight. Every deployment keeps listing and
	// filtering correctly either way — this only decides whether Temporal's
	// visibility store can additionally be asked to carry the index.
	//
	// A failure here is reported and not fatal: it is exactly the case
	// [server.WithSearchAttributesRegistered] exists to keep out of Run and
	// CreateSchedule, so a `temporal server start-dev` with no operator setup,
	// or a production cluster where this identity lacks the operator role,
	// starts and serves precisely as it always has.
	if policy == nil || policy.Tenancy == nil {
		opts, optsErr := cfg.Options()
		if optsErr != nil {
			logger.Warn("could not resolve the Temporal namespace to register search attributes on; "+
				"`flow list --filter` still works, scanning executions rather than querying an index",
				"error", optsErr)
		} else if err := server.EnsureSearchAttributesRegistered(cmd.Context(), c, opts.Namespace); err != nil {
			logger.Warn("could not register Flowstate's search attributes; "+
				"`flow list --filter` still works, scanning executions rather than querying an index",
				"error", err)
		} else {
			serverOpts = append(serverOpts, server.WithSearchAttributesRegistered())
		}
	}

	// A trust policy that maps tenants onto Temporal namespaces needs a client
	// per namespace it can route to, dialed and verified to exist now so a
	// mistyped or unregistered namespace fails the start rather than the first
	// tenant to submit. The server refuses a tenant the mapping cannot place —
	// see FlowstateServer.clientFor — so this only has to hand it the pool.
	if policy != nil && policy.Tenancy != nil {
		pool, err := temporalclient.NewPool(cmd.Context(), cfg, policy.Tenancy, logger)
		if err != nil {
			return fmt.Errorf("dialing the Temporal namespaces the trust policy maps tenants onto: %w", err)
		}
		defer pool.Close()

		serverOpts = append(serverOpts, server.WithNamespacePool(pool))
		logger.Info("routing tenants to mapped Temporal namespaces", "namespaces", pool.Namespaces())

		// The mapping-completeness half: the pool proved every mapped namespace
		// is *dialable* and knows nothing about whether anything polls it. See
		// [warnUnpolledTenantQueues] for why this warns rather than refuses.
		warnUnpolledTenantQueues(cmd.Context(), logger, pool, taskQueues, routableTenants(policy.Tenancy))
	}

	// The server answers Validate and GetCatalog from the process-wide registry,
	// so a deployment whose workers load plugins points the server at the same
	// directory — otherwise the capability it reports is the built-ins alone, and
	// a caller authoring against GetCatalog would be told a task its workers run
	// does not exist. The plugins launched here serve descriptors and health
	// checks; execution still happens on the workers.
	pluginCatalog, closePlugins, err := startPlugins(cmd, nil)
	if err != nil {
		return err
	}
	defer closePlugins()

	// And handed to the server, which is what makes a `plugins:` requirement
	// resolvable: the server pins every submission against this snapshot, and
	// without it a deployment that launched the plugin would refuse every workflow
	// asking for it as "not installed".
	serverOpts = append(serverOpts, server.WithPluginCatalog(pluginCatalog))

	// No error to handle since connectrpc.com/validate v0.6.0: the interceptor
	// builds its validator lazily on first use, so construction cannot fail.
	interceptor := validate.NewInterceptor()

	otelInterceptor, err := otelconnect.NewInterceptor()
	if err != nil {
		return fmt.Errorf("error creating OpenTelemetry interceptor: %w", err)
	}

	flowServer, err := server.New(c, serverOpts...)
	if err != nil {
		return err
	}

	// The webhook receiver, if this deployment serves any. Built before the
	// server starts listening, because every decision it can make in advance is
	// made when it is built: a Flowfile that will not compile, a scheme this
	// build cannot verify, a signing key this deployment cannot resolve. A
	// receiver that exists is one whose whole configuration was satisfiable, and
	// a deployment that cannot satisfy it does not start — which is the same
	// fail-closed rule --task-queue-prefix follows a hundred lines above.
	receiver, err := webhookReceiver(cmd, flowServer, logger)
	if err != nil {
		return err
	}

	rpcMux := http.NewServeMux()
	rpcMux.Handle(
		flowstatev1connect.NewWorkflowServiceHandler(
			flowServer,
			connect.WithInterceptors(
				interceptor,
				otelInterceptor,
			),
			// Bound how much an unauthenticated caller can make the server
			// allocate. connect-go defaults to unlimited, so without this a
			// single request — or a compressed one that inflates enormously —
			// can exhaust memory.
			connect.WithReadMaxBytes(maxRequestBytes),
		),
	)

	httpServer := &http.Server{
		// Where this server *listens* (--listen / $FLOWSTATE_ADDRESS), which used
		// to share a package variable with the address a client dials to
		// *connect* — different facts that happened to share a default. --listen
		// is this command's own flag for its own half of that; see its
		// registration, above in this file, and cmd/flow/tls.go for why it is not
		// spelled --address.
		//
		// The same value [refusePlaintextListener] already checked above, so a
		// deployment cannot have this listener bind an address this function
		// already refused.
		Addr:    publicAddr,
		Handler: serverHandler(logger, verifier, peerVerifier, broker, rpcMux, receiver, protectedResource),

		// nil when no certificate was configured, which is only reachable here
		// when publicAddr is loopback — anything else already returned above.
		TLSConfig: tlsCfg,

		// Without these a client that opens a connection and sends bytes
		// slowly, or never, occupies a connection indefinitely. Go's zero
		// values mean no timeout at all, so they must be set explicitly.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       2 * time.Minute,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,

		// The count beside the bytes; see [maxHeaderValueCount] for why one
		// does not imply the other.
		MaxHeaderValueCount: maxHeaderValueCount,
	}

	// Bound now, so a port already in use or a permission error is reported
	// before the banner-equivalent log lines below claim the server is up,
	// and so the internal listener (below) binds its own socket only after
	// this one is known good.
	publicListener, err := net.Listen("tcp", httpServer.Addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", httpServer.Addr, err)
	}

	logger.Info("starting server", "address", httpServer.Addr, "tls", tlsCfg != nil,
		"client_certificate_required", tlsCfg != nil && tlsCfg.ClientAuth == tls.RequireAndVerifyClientCert,
		"client_certificate_authenticates", peerVerifier != nil)
	if authCfg.insecure {
		logger.Warn("authentication is disabled; every caller is anonymous and can start workflows",
			"use", "local development only")
	}
	if broker != nil {
		// Log the discovery URL rather than the fact of federation: an operator
		// configuring a relying party needs this exact string, and finding it by
		// reading source is the sort of friction that gets solved by guessing.
		// The key ids as well, because a rotation is performed by restarting
		// with a different --identity-key list and its whole result is which
		// keys this process publishes and which one of them signs. An operator
		// who cannot read that back has rehearsed nothing.
		logger.Info("issuing workload identity assertions",
			"discovery", broker.Issuer().URL()+auth.DiscoveryPath,
			"signing_key", broker.Issuer().ActiveKeyID(),
			"verify_only_keys", verifyOnlyKeyIDs(broker.Issuer()))
	}
	if protectedResource != nil {
		logger.Info("serving RFC 9728 protected resource metadata",
			"metadata_url", protectedResource.MetadataURL())
	}

	// The internal listener binds only now, once the public one is already
	// known good — see [checkInternalListenAddress] above for why its address
	// was already validated, and this file's package comment on
	// cmd/flow/internallistener.go for what it carries. internalServer is nil
	// when the operator never opted into it (--internal-listen unset, the
	// default), which every branch below treats as "nothing more to do".
	internalServer, internalListener, err := startInternalListener(logger, internalFlags.address)
	if err != nil {
		publicListener.Close()
		return err
	}
	if internalServer != nil {
		logger.Info("starting internal listener", "address", internalListener.Addr().String())
	}

	// Buffered for both producers below, so neither goroutine blocks trying to
	// report an outcome nobody is listening for anymore once shutdown starts.
	serveErr := make(chan error, 2)
	go func() {
		// Report a listen failure instead of terminating the process from a
		// goroutine, so shutdown still runs and the error reaches the caller.
		// TLS is served through this same call: [http.Server.ServeTLS] reads
		// the certificate from httpServer.TLSConfig when no file paths are
		// given, which is exactly what [serverTLSConfig] populated it with.
		var err error
		if tlsCfg != nil {
			err = httpServer.ServeTLS(publicListener, "", "")
		} else {
			err = httpServer.Serve(publicListener)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("serving on %s: %w", httpServer.Addr, err)
			return
		}
		serveErr <- nil
	}()
	if internalServer != nil {
		go func() {
			if err := internalServer.Serve(internalListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serveErr <- fmt.Errorf("serving the internal listener on %s: %w",
					internalListener.Addr(), err)
				return
			}
			serveErr <- nil
		}()
	}

	// Acquisition is startup — and it runs here, with the listener above
	// already serving, because TLS-ALPN-01 is answered on that very socket.
	// The CA completes the challenge by connecting back to it, so priming
	// before the socket existed made first-time issuance impossible and left
	// ACME working only from a warm cache (#628; [primeACMECertificates] has
	// the mechanism).
	//
	// Failing here still fails startup: the deferred shutdown below runs, and
	// the error is returned rather than logged, which is the property #581
	// asked for. What changed is only that the challenge can now be answered
	// while it is asked.
	if acmeCfg != nil {
		primeCtx, cancel := context.WithTimeout(cmd.Context(), 3*time.Minute)
		err := primeACMECertificates(primeCtx, acmeCfg.manager, acmeCfg.hosts)
		cancel()
		if err != nil {
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 1*time.Minute)
			_ = httpServer.Shutdown(shutdownCtx)
			cancelShutdown()
			if internalServer != nil {
				internalShutdownCtx, cancelInternal := context.WithTimeout(context.Background(), 1*time.Minute)
				_ = internalServer.Shutdown(internalShutdownCtx)
				cancelInternal()
			}
			return fmt.Errorf("obtaining ACME certificates: %w", err)
		}
		logger.Info("obtained ACME certificates", "hosts", acmeCfg.hosts)
	}

	// The renewal watchdog, only running when ACME is configured. A nil
	// channel blocks forever in the select below, which is exactly "there is
	// nothing more for this to contribute" for a deployment using an
	// explicit certificate or no TLS at all. See [acmeExpiryWatchdog]'s doc
	// for #581's renewal-failure decision: silent while a valid certificate
	// is on hand, loud well before expiry, fatal only once a certificate has
	// actually expired with no successful renewal.
	var acmeFatal <-chan error
	if acmeCfg != nil {
		watchdogCtx, cancelWatchdog := context.WithCancel(context.Background())
		defer cancelWatchdog()
		acmeFatal = acmeExpiryWatchdog(watchdogCtx, logger, cacheCertExpiry(acmeCfg.manager.Cache), acmeCfg.hosts)
	}

	var runErr error
	select {
	case runErr = <-serveErr:
	case runErr = <-acmeFatal:
	case <-cmd.Context().Done():
	}

	logger.Info("shutting down server")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		// Flushed on the way out of this path too, because a server that had to
		// be forced down is the case where the last spans matter most.
		flushTelemetry()

		return fmt.Errorf("server forced to shutdown: %w", err)
	}
	if internalServer != nil {
		if err := internalServer.Shutdown(shutdownCtx); err != nil {
			logger.Warn("the internal listener was forced down with requests still in flight",
				"error", err)
		}
	}

	// After the in-flight requests have drained, so their spans are in the
	// batch rather than in the one nobody sends.
	flushTelemetry()

	if runErr != nil {
		return runErr
	}

	logger.Info("server stopped")

	return nil
}

// maxRequestBytes bounds a single RPC request body.
//
// A workflow specification is text and a large one is still small; this leaves
// generous room while keeping an unauthenticated caller from choosing how much
// memory the server allocates.
const maxRequestBytes = 4 << 20 // 4 MiB

// maxHeaderValueCount bounds how many header values a listener will parse from
// one request, which is a different resource from how many bytes they occupy.
//
// MaxHeaderBytes already bounds the bytes. It does not bound the count, and the
// two are not the same thing a peer controls: a megabyte spent on one enormous
// header value costs one map entry, and the same megabyte spent on thirty
// thousand `X-a: b` lines costs thirty thousand of them plus the slice growth
// underneath. That is the ratio the sender picks, so the count is bounded on its
// own — the rule this repository applies everywhere else, that bounding one
// resource does not bound another the peer chooses the ratio to.
//
// Set explicitly even though Go 1.27's [http.DefaultMaxHeaderValueCount] is this
// same 500, for the reason MaxHeaderBytes is written out beside it while being
// exactly [http.DefaultMaxHeaderBytes]: an operator reading one of these structs
// should see every bound the listener imposes, rather than having to know which
// of them the standard library happens to supply this year.
//
// 500 is far past what a real client sends — a webhook delivery from a payments
// provider carries a few dozen headers, and a proxy chain adds a handful more —
// and far short of a count worth allocating for.
const maxHeaderValueCount = 500

// authVerifier builds the token verifier the server authenticates with.
//
// Anonymous access requires --insecure-no-auth. It is not a fallback for a
// missing or unreadable policy: a server that cannot load its trust policy must
// refuse to start rather than quietly begin accepting everyone, which is exactly
// the failure this replaces.
func authVerifier(flags authFlags) (auth.Verifier, *auth.Policy, error) {
	if flags.insecure {
		return auth.InsecureAnonymousVerifier(), nil, nil
	}

	if flags.policyPath == "" {
		return nil, nil, fmt.Errorf("no authentication configured: pass --auth-policy with a trust policy, " +
			"or --insecure-no-auth to allow anonymous access for local development")
	}

	data, err := os.ReadFile(flags.policyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("reading auth policy: %w", err)
	}
	policy, err := auth.ParsePolicy(data)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing auth policy %s: %w", flags.policyPath, err)
	}
	verifier, err := auth.NewOIDCVerifier(policy)
	if err != nil {
		return nil, nil, fmt.Errorf("configuring token verification: %w", err)
	}
	return verifier, &policy, nil
}

// identityBroker builds the broker that issues Flowstate's own assertions, or
// returns nil when the deployment does not federate outward.
//
// The signing key is a file rather than a policy field because a policy is
// configuration a person edits and reads, and a private key is neither. The key
// id is published in the JWKS and named in every assertion, so a date makes
// rotation self-documenting: `2026-08.pem` publishes as "2026-08".
//
// # Rotation is a restart, so the flag repeats
//
// The issuer can rotate in place ([auth.Issuer.Rotate]) and nothing a deployment
// runs ever calls it: the rotation an operator performs is to change what the
// process is started with and restart it. A process that loaded exactly one key
// publishes exactly that key from its first request onward, so every assertion
// the previous process signed — still valid for the rest of its five minutes —
// stops verifying at any relying party that refetches the key set on an unknown
// "kid" (picatz/flowstate#891).
//
// So --identity-key repeats, and the order is the whole rule:
//
//   - **the first occurrence signs**, and
//   - every later one is published for verification only, without its private
//     half ever being retained.
//
// First rather than last because the command line then reads in the order the
// operator thinks: the key being rotated *to* is the one they just generated and
// the one they type first, and the older keys trail it in the order they are
// eventually dropped. Nothing is derived from the file name or its modification
// time — a key id is an operator-chosen file name, and a rotation that depended
// on mtime would be decided by whichever file `cp` touched last.
//
// A verify-only entry may be either half: the operator's existing PKCS#8
// private key file, whose public half is taken and whose private half is
// dropped on the spot, or a PKIX public key PEM for a deployment that would
// rather not mount the old private key at all. This process never signs with
// one either way, since [auth.WithVerifyOnlyKey] takes a public key.
//
// Everything here fails closed: an entry that cannot be read or parsed, or two
// entries publishing the same key id, refuses start-up rather than being
// skipped. A key silently left out is a rotation the operator believes is
// covered and is not.
func identityBroker(flags authFlags, policy *auth.Policy) (*auth.Broker, error) {
	if policy == nil || policy.Federation == nil {
		if len(flags.identityKeyPaths) > 0 {
			return nil, fmt.Errorf("--identity-key was given but the trust policy configures no federation: " +
				"add a federation section, or drop the key")
		}
		return nil, nil
	}

	if len(flags.identityKeyPaths) == 0 {
		return nil, fmt.Errorf("the trust policy configures federation but no signing key was given: " +
			"pass --identity-key with a PKCS#8 PEM private key, since Flowstate cannot issue an " +
			"assertion it cannot sign")
	}

	signingPath, verifyOnlyPaths := flags.identityKeyPaths[0], flags.identityKeyPaths[1:]

	pem, err := os.ReadFile(signingPath)
	if err != nil {
		return nil, fmt.Errorf("reading identity key: %w", err)
	}
	key, err := parseSigningKey(signingPath, pem)
	if err != nil {
		return nil, err
	}

	opts := make([]auth.FederationOption, 0, len(verifyOnlyPaths))
	for _, path := range verifyOnlyPaths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading verify-only identity key: %w", err)
		}
		id, public, err := parseVerifyOnlyKey(path, data)
		if err != nil {
			return nil, err
		}
		opts = append(opts, auth.WithFederationVerifyOnlyKey(id, public))
	}

	// A duplicate id is refused by [auth.NewIssuer], which is where the key set
	// being published actually lives, so the refusal cannot be bypassed by any
	// other caller — see [auth.WithVerifyOnlyKey].
	broker, err := policy.Federation.Broker(key, opts...)
	if err != nil {
		return nil, fmt.Errorf("configuring identity federation: %w", err)
	}
	return broker, nil
}

// verifyOnlyKeyIDs names the keys the issuer publishes that it does not sign
// with, in the order the key set serves them.
//
// Read back out of the published key set rather than remembered from the flags,
// so the line an operator reads at start-up describes what relying parties will
// actually fetch: a key that was named but not published, for whatever reason,
// must not appear here.
func verifyOnlyKeyIDs(issuer *auth.Issuer) []string {
	active := issuer.ActiveKeyID()

	var ids []string
	for _, published := range issuer.KeySet().Keys {
		id, _ := published[jwk.KeyID].(string)
		if id == "" || id == active {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

// parseVerifyOnlyKey decodes one --identity-key entry that is not the first, as
// the public key it will be published as, deriving the key id from the file's
// name exactly as [parseSigningKey] does.
//
// Both PEM shapes are accepted because both are what an operator has in hand: a
// rotation names the previous *private* key file, which is what is already
// mounted, while a deployment that would rather not mount a superseded private
// key at all can hand over just its public half (openssl pkey -pubout). A private
// key given here is read for its public half and nothing else — no
// [auth.SigningKey] is built from it, so no signer closure ever captures it and
// this process cannot sign with the key however the flags are ordered.
func parseVerifyOnlyKey(path string, data []byte) (string, crypto.PublicKey, error) {
	id := keyIDFromPath(path)

	block, _ := pem.Decode(data)
	if block == nil {
		return "", nil, fmt.Errorf("identity key %s is not PEM-encoded", path)
	}

	if public, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		return id, public, nil
	}

	private, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return "", nil, fmt.Errorf("identity key %s is neither a PKCS#8 private key nor a PKIX public key "+
			"(a key named after the first --identity-key is published for verification only, so either half will do; "+
			"extract the public half of an existing key with: openssl pkey -in %s -pubout -out %s.pub.pem)", path, path, path)
	}

	public, err := publicKeyOf(private)
	if err != nil {
		return "", nil, fmt.Errorf("identity key %s: %w", path, err)
	}
	return id, public, nil
}

// parseSigningKey decodes a PKCS#8 PEM private key, deriving the key id from the
// file's name.
func parseSigningKey(path string, data []byte) (auth.SigningKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return auth.SigningKey{}, fmt.Errorf("identity key %s is not PEM-encoded", path)
	}

	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return auth.SigningKey{}, fmt.Errorf("identity key %s is not a PKCS#8 private key "+
			"(convert one with: openssl pkcs8 -topk8 -nocrypt -in old.pem -out new.pem): %w", path, err)
	}

	// The file's base name becomes the key id, so `2026-07.pem` publishes as
	// "2026-07". Naming the file is the whole of key rotation.
	id := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	key, err := auth.NewSigningKey(id, parsed)
	if err != nil {
		return auth.SigningKey{}, fmt.Errorf("identity key %s: %w", path, err)
	}
	return key, nil
}

// stdio adapts the process's standard input and output into the single
// ReadWriteCloser a JSON-RPC connection expects.
//
// Language servers speak over stdin and stdout, so reads and writes must come
// from different files. Passing os.Stdin alone would send every reply back into
// the input stream, where no editor would ever see it.
type stdio struct{}

func (stdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (stdio) Close() error {
	err := os.Stdin.Close()
	if outErr := os.Stdout.Close(); err == nil {
		err = outErr
	}
	return err
}

// runLSP serves the Flowfile language server over stdin and stdout.
func runLSP(cmd *cobra.Command, args []string) error {
	// Diagnostics go to stderr; stdout carries the JSON-RPC protocol and must
	// not be polluted with log output.
	log.SetFlags(0)
	log.SetOutput(os.Stderr)

	// Plugins are launched here, before the first byte of protocol is read, and
	// nowhere else. An editor asks this process a question per keystroke, and
	// launching a binary is not an answer to a keystroke; nor may the workspace
	// decide it, because a repository somebody cloned would then choose what
	// their editor executes. The only thing that turns this on is an *absolute*
	// --plugin-dir on the command line the person configured their editor with,
	// which is an operator saying yes about their own machine. That is the whole
	// of the opt-in, and it is why there is no configuration path to the same effect.
	//
	// Both halves of that — a relative path refused, and $FLOWSTATE_PLUGIN_DIR
	// not read — are properties of how this command's flags were *registered*,
	// by [addEditorPluginFlags], and not of this call. So the reading below is
	// the same [startPlugins] a worker does, with the same --plugin pin refusal
	// and the same host: a narrower trust boundary, not a second reader that
	// can drift from the first (#958).
	//
	// Strict, as a worker is: a plugin that will not come up fails the command
	// here rather than leaving an editor quietly reporting `unknown task` for
	// tasks the author asked for and had every reason to expect.
	_, closePlugins, err := startPlugins(cmd, nil)
	if err != nil {
		return err
	}
	// Registered, so it must be closed: nothing else kills the plugin processes,
	// and an editor restarting its server would otherwise leave one behind per
	// restart.
	defer closePlugins()

	// A person who follows the root help's own example (`flow lsp`) gets
	// silence indistinguishable from a hang: this server speaks nothing until
	// an editor writes to it. The banner is the account of that, gated on
	// stdin being a terminal and written to stderr, so a real editor's pipe
	// never sees it and the JSON-RPC stream on stdout is never touched
	// (picatz/flowstate#398).
	//
	// It comes after startPlugins, not before: printed earlier it announces
	// readiness this command may be about to refuse, and a plugin that will
	// not come up exits here. Say "waiting" only once waiting is the truth.
	writeStdioBanner(cmd.ErrOrStderr(), stdinIsInteractive(cmd), lspBanner)

	conn := jsonrpc2.NewConn(
		cmd.Context(),
		// lsp.NewBoundedStream rather than jsonrpc2.NewBufferedStream: the
		// same codec and the same buffering, with each frame bounded by
		// lsp.MaxFrameBytes. Everything this server reads *inside* a frame was
		// bounded already; the frame itself was not, and the codec's header
		// parse is an unbounded accumulating read on the first bytes anything
		// on this process's standard input sends. See lsp.MaxFrameBytes.
		lsp.NewBoundedStream(stdio{}),
		// The registry the host registered into, handed over rather than reached
		// for, so that what this server knows is what this command launched.
		// lsp.NewHandler rather than jsonrpc2.AsyncHandler: same
		// goroutine-per-message serving, with document builds announced in
		// arrival order first, which is what keeps a request behind a didOpen
		// from answering before the open lands.
		lsp.NewHandler(&lsp.FlowfileServer{Tasks: v1.DefaultRegistry()}),
	)

	// NewConn serves in a background goroutine and returns immediately, so
	// without waiting here the process exits before handling a single request.
	select {
	case <-conn.DisconnectNotify():
		return nil
	case <-cmd.Context().Done():
		return conn.Close()
	}
}

// runValidate implements the validate sub-command, reporting problems in one or
// more Flowfiles without executing anything.
//
// Checking a workflow by running it is not an option for a workload engine: the
// steps have side effects. This is the command that makes a Flowfile safe to
// check.
//
// # Plugins
//
// Given --plugin-dir, the plugins on it are launched here first and registered
// into the registry the validator reads, exactly as [runLSP] does it and through
// the same [startPlugins] (#724, #710). It is the difference between a file that
// names `example.greet` reading as a mistake and reading as what it is, and it
// is what makes the landing rule CLAUDE.md states — a Flowfile expresses it,
// `flow validate` accepts it, an example exercises it — reachable for a plugin
// task at all.
//
// Three properties of doing it here rather than anywhere else, because this is
// the verb an editor runs on a keystroke through `flow lsp` and a person runs in
// CI:
//
//   - It is opt-in from the command line and from nowhere else. Without
//     --plugin-dir this command launches nothing, which is the behaviour every
//     invocation in the tree has today.
//   - It is bounded by the host's own bounds, which are the ones a worker runs
//     under: plugin.DefaultHandshakeTimeout on coming up,
//     plugin.DefaultDescribeTimeout on the descriptors, the stderr line and
//     rate caps, and the transport's byte cap below the RPC library. A plugin
//     that will not come up cannot hang this command.
//   - A plugin that fails to launch fails *this command*, saying so. The
//     tempting alternative — carry on with the plugin-free registry — reports
//     every one of that plugin's tasks as an unknown task, which is a false
//     diagnostic about the file on the strength of something that went wrong
//     with a process. False diagnostics are worse than missing ones.
//
// Given --plugin-catalog instead, the same facts are read out of a document
// `flow plugins --output json` wrote, and no process is launched at all
// (#710) — the form the surfaces that cannot exec need, and the one a CI job
// uses to check a repository's plugin examples with no plugin binaries in the
// runner. The two flags are mutually exclusive on the command line; see
// [loadPluginCatalog].
//
// What is deliberately unchanged is the answer with neither flag: a step
// naming a plugin task still gets the installation-question diagnostic rather
// than a pass, because whether a plugin is installed is a deployment's decision
// and this process has not been told. See unknownTaskMessage in the flowfile
// package.
func runValidate(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	// Before either rendering path, so that both answer from the same registry:
	// `-o json` is the same check written down for a machine, and a consumer that
	// got a different set of diagnostics from the same flags would be reading a
	// second implementation of this verb.
	//
	// The catalog is kept, not discarded: it is what a file's `plugins:`
	// requirements resolve against, and checking them is what makes this verb
	// agree with the two drivers rather than pass a file both of them refuse —
	// see [validatePluginRequirements] (#835 review). It is nil when nothing was
	// launched, which is the same fact said the other way.
	catalog, closePlugins, err := startPlugins(cmd, nil)
	if err != nil {
		// A wrong command line is passed through as it is. The sentence below
		// says a plugin would not start, and for a refusal made before anything
		// was launched — a pinned plugin with nowhere to look, or both plugin
		// sources named at once — that is an account of something that never
		// happened.
		if isUsageError(err) {
			return err
		}

		return fmt.Errorf("the plugins on --plugin-dir are what these files are checked "+
			"against, and one of them would not start, so nothing was checked: %w", err)
	}
	defer closePlugins()

	// Or the same facts without launching anything, from a document (#710).
	// Same failure rule as the launch above, for the identical reason: a
	// catalog that will not load fails this command naming the file, because
	// carrying on would report every task the catalog was carrying as unknown.
	// Same catalog value afterwards, so a `plugins:` requirement resolves
	// against whichever source this invocation named — the two are mutually
	// exclusive on the command line (see [pluginFlagsOf]), so only one of them
	// ever answers.
	if fromFile, err := loadPluginCatalog(cmd); err != nil {
		return fmt.Errorf("the catalog on --%s is what these files are checked against, "+
			"and it could not be read, so nothing was checked: %w", pluginCatalogFlag, err)
	} else if fromFile != nil {
		catalog = fromFile
	}

	if format.Machine() {
		return validateMachine(cmd, args, format, catalog)
	}

	// Through the surface, and with its theme, for the reason renderHelp and
	// renderError no longer take a writer at all: a styled byte written past the
	// surface skips the layer that degrades the palette to what the stream carries.
	surface := newSurface(cmd)
	out, theme := surface.Out, surface.Theme

	// Directory or file, on the same rule every other file-taking verb applies
	// (#394): a named path is taken as given, a directory is walked, and a
	// walk sorts what it finds into workflows and tests rather than refusing
	// the directory outright.
	targets, err := collectValidateTargets(args, cmd.InOrStdin())
	if err != nil {
		return err
	}

	var failed bool

	for _, target := range targets {
		path := target.path

		if target.isTest {
			diagnostics := validateTestFile(target)
			if len(diagnostics) == 0 {
				fmt.Fprintf(out, "%s: %s\n", theme.Muted.Render(path), theme.Success.Render("ok"))
				continue
			}
			failed = true
			for _, d := range diagnostics {
				fmt.Fprintf(out, "%s: %s\n", theme.Muted.Render(path), d.Message)
			}
			continue
		}

		diagnostics, err := validateWorkflowTarget(target)
		if err != nil {
			var parsed flowfile.Diagnostics
			if !errors.As(err, &parsed) {
				// A file that cannot be read is a fact about the invocation, not
				// about the workflow.
				return fmt.Errorf("%s: %w", path, err)
			}
			// A parse failure already carries its own line and column, and each of
			// them is written on a line naming this file, exactly as a validation
			// diagnostic below is. Handing the whole error to `%v` instead is what
			// gave one report two position spellings: a space after the filename,
			// and no filename at all from the second diagnostic on (#384).
			failed = true
			writeDiagnostics(out, theme.Muted.Render(path), parsed)
			continue
		}

		// The file's `plugins:` requirements against the launched catalog, which
		// is a no-op with no --plugin-dir. Appended to the same list so a version
		// mismatch and an unknown-task read the same on the line and in the exit
		// status.
		diagnostics = append(diagnostics, validatePluginRequirements(target, catalog)...)

		if len(diagnostics) == 0 {
			// The one word worth finding in a run over nineteen files: everything
			// else on the line is the path, and a reader scanning for the failure
			// among them is scanning for this.
			fmt.Fprintf(out, "%s: %s\n", theme.Muted.Render(path), theme.Success.Render("ok"))
			continue
		}

		failed = true
		writeDiagnostics(out, theme.Muted.Render(path), diagnostics)
	}

	if failed {
		return errValidationFailed
	}
	return nil
}

// validateMachine writes the same answer as a schema message, for whatever is driving
// the CLI rather than reading it.
//
// One [v1.DiagnosticReport] per file, which is what makes `jsonl` the useful shape
// here: a line per file, so `flow validate examples/*/workflow.yaml -o jsonl | jq
// 'select(.diagnostics | length > 0)'` is the whole of "show me the broken ones".
//
// A clean file still gets a report, with no diagnostics in it. "Checked and clean" and
// "not checked" are different facts and a consumer that only saw failures could not
// tell them apart — which is the same reason a `log` step is present-and-empty in a run
// record rather than absent.
//
// # A file that cannot be read is not a diagnostic
//
// A missing file or an unreadable one stops the command, as it does in the text form.
// It is a fact about the invocation rather than about a workflow, and reporting it as a
// diagnostic would put "you typed the wrong path" in the same list as "this step
// references a step that does not exist" — one is fixed in the shell and the other in
// the file.
//
// A file that *parses* badly is the opposite: that is a fact about the workflow, so it
// becomes a diagnostic like any other.
func validateMachine(cmd *cobra.Command, args []string, format OutputFormat, catalog *v1.PluginCatalog) error {
	surface := newSurface(cmd)

	targets, err := collectValidateTargets(args, cmd.InOrStdin())
	if err != nil {
		return err
	}

	reports := make([]*v1.DiagnosticReport, 0, len(targets))
	for _, target := range targets {
		path := target.path

		if target.isTest {
			reports = append(reports, validateTestFile(target).Report(path))
			continue
		}

		diagnostics, err := validateWorkflowTarget(target)
		if err != nil {
			var parsed flowfile.Diagnostics
			if !errors.As(err, &parsed) {
				// Stdin ("-") names nothing on disk to stat: its bytes were
				// already read in full by [collectValidateTargets], so any
				// error here is the document's own problem, never a missing
				// or unreadable file.
				if target.data == nil {
					if _, statErr := os.Stat(path); statErr != nil {
						return fmt.Errorf("%s: %w", path, statErr)
					}
				}
				// Not a shape this can position — a document that is not YAML at
				// all. It is still the file's problem rather than the caller's, so
				// it is reported as an unpositioned diagnostic rather than dropped.
				parsed = flowfile.Diagnostics{{Message: err.Error()}}
			}
			diagnostics = parsed
		}

		// The same `plugins:` requirement check the text path runs, so the two
		// renderings of this verb agree — a machine consumer must not get a
		// green a person would not (#835 review). A no-op with no catalog, and
		// skipped for a file that did not parse (its diagnostics already say so).
		diagnostics = append(diagnostics, validatePluginRequirements(target, catalog)...)

		reports = append(reports, diagnostics.Report(path))
	}

	var failed bool
	for _, report := range reports {
		if len(report.GetDiagnostics()) > 0 {
			failed = true
		}
	}

	if format == FormatJSONL {
		// One line per file, which is what makes the shape worth having: a reader
		// consumes them as they arrive and each line names its own file.
		for _, report := range reports {
			if err := writeJSON(surface, format, report); err != nil {
				return err
			}
		}
	} else if err := writeJSON(surface, format, &v1.ValidationReport{Files: reports}); err != nil {
		// One document per invocation, which is what `json` means everywhere else in
		// this CLI. Checking three files is still one answer.
		return err
	}

	if failed {
		return errValidationFailed
	}

	return nil
}

// errValidationFailed reports that validation found problems. It carries no
// message of its own because the diagnostics have already been printed.
var errValidationFailed = errors.New("validation failed")

// loadWorkflow reads, compiles, and validates a Flowfile.
//
// Validation happens before execution so that a mistake is reported instead of
// partially performed. An unknown task name, for instance, used to fail only when
// its step was reached, by which point earlier steps had already made their
// requests.
func loadWorkflow(path string) (*v1.Workflow, error) {
	// File-aware rather than reading the bytes and calling [flowfile.Unmarshal]:
	// a `call:` step is resolved relative to this file's own directory, and only
	// the path-aware entry points know it.
	workflow, _, err := flowfile.ParseFile(path)
	if err != nil {
		// Positioned diagnostics get a line each naming this file, like every
		// other diagnostic surface. Wrapping the error instead put the filename on
		// a line of its own and left every position after the first unattributed
		// (#384). A failure that is not diagnostics is about the invocation rather
		// than the file, and keeps its own wrapping.
		var parsed flowfile.Diagnostics
		if errors.As(err, &parsed) {
			return nil, diagnosticsError(path, parsed)
		}
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	diagnostics, err := flowfile.ValidateSourceFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(diagnostics) > 0 {
		return nil, diagnosticsError(path, diagnostics)
	}

	return workflow, nil
}

// newRootCommand builds the whole CLI: every command, its flags, and the groups the
// help sorts them into.
//
// Extracted from main so a test can *have* it. Everything below used to be built
// inline, which meant nothing could ask the CLI what commands it has — and the
// README's table of them was therefore checked by nobody. A table of commands that
// drifts is worse than no table: it is the page a newcomer reads to find out what
// this tool does.
//
// It takes no arguments and reads no state, so the answer is the same one main gets.
// A constructor that needed a live server or a terminal to build would be a
// constructor a test was asserting something else about.
func newRootCommand() *cobra.Command {
	// Root command for the Flowstate CLI application (flow).
	rootCmd := &cobra.Command{
		Use:   "flow",
		Short: "Durable, policy-governed workload engine",
		Long: "Flowstate runs workloads that have to finish correctly despite crashes, network " +
			"failures, and long waits. You write one as a Flowfile (YAML with CEL expressions) " +
			"and run it in this process or durably on Temporal. The two behave the same, which is " +
			"what makes a local run worth rehearsing with.",
		Version: version,
		Example: `# Run a workflow locally (without Temporal):
flow run local examples/hello-world/workflow.yaml

# Run a workflow using Temporal via the server:
flow run examples/hello-world/workflow.yaml

# Start a Temporal worker:
flow worker

# Start the Flowstate API server:
flow server

# Start the LSP server for Flowfile editing:
flow lsp`,
	}

	// Bound to a local rather than to the package variable, and copied across when
	// the command actually runs.
	//
	// Two reasons, and the second one was silently broken. pflag writes the default
	// into the pointer the moment the flag is declared, so binding the package
	// variable directly meant *building* the CLI wrote to shared state — which is a
	// data race as soon as two tests build one at once, and there is no reason
	// constructing a command should be an observable event at all.
	//
	// And the default it wrote was `false`, over the top of the value
	// `FLOWSTATE_VERBOSE_LOGGING` had just put there. That variable is documented in
	// the README and did nothing: the environment set it, construction cleared it,
	// and nothing in between read it. The default is the environment's own value now,
	// so the flag overrides the variable rather than erasing it.
	//
	// Bound to nothing at all, and read off the command by whoever needs it — which
	// is what finally removes the copy-in-PersistentPreRun that stood in for a fix.
	rootCmd.PersistentFlags().BoolP("verbose", "v",
		os.Getenv("FLOWSTATE_VERBOSE_LOGGING") == "true", "enable verbose logging")

	// --no-color forces the same no-colour path NO_COLOR already takes, through the
	// same plumbing rather than a second one: [environForSurface] folds it into the
	// environment [ui.Detect] resolves from, so it degrades stdout, stderr, and the
	// `flow watch` TUI's styling exactly as NO_COLOR does — and, being the more
	// explicit ask, wins over NO_COLOR, CLICOLOR_FORCE, and everything else in the
	// environment.
	rootCmd.PersistentFlags().Bool("no-color", false,
		"disable colour on every stream, the same way NO_COLOR does; it is the most explicit ask, "+
			"so it wins over CLICOLOR_FORCE and the terminal's own capabilities")

	// Run command, which executes a workflow using the Flowstate service.
	runCmd := &cobra.Command{
		Use:   "run [workflow-file]",
		Short: "Run a workflow and follow it",
		Long: "Start a workload on a Flowstate server and follow the run until it finishes.\n\n" +
			"This verb always means the server, and it never falls back to running the " +
			"workload here when no server answers: a network failure must not turn a " +
			"deploy into a laptop run. `flow run local` is the other venue, and each run " +
			"says which one it is on before it starts, so the address a shell happens to " +
			"carry is never something to find out afterwards.\n\n" +
			"Following works exactly as `flow watch` does, because it is the same code: a " +
			"live view where there is a terminal, one line per change where there is not, " +
			"and the outputs on stdout when the run produced them. The exit code is the " +
			"run's, so `flow run x && ./promote.sh` behaves the way a shell reader expects.\n\n" +
			"Stopping watching does not stop the run. The workflow id is printed as soon as " +
			"the run starts, so `flow watch` can pick it up again afterwards.\n\n" +
			"A workflow that declares `inputs:` is given them with --input name=value or " +
			"--input-file inputs.json. The declaration decides how a value is read, so an " +
			"argument that does not fit is refused here, before the run starts." +
			runDocumentHelp,
		Args: cobra.ExactArgs(1),
		RunE: runWorkflow,
		Example: `# Run a workflow and watch it:
flow run examples/hello-world/workflow.yaml

# Run a workflow that takes arguments:
flow run examples/parameterized-deploy/workflow.yaml --input service=checkout --input replicas=3

# Or send the same arguments as a document:
flow run examples/parameterized-deploy/workflow.yaml --input-file examples/parameterized-deploy/inputs.json

# Run it and pipe the outputs, with the live view still on the terminal:
flow run examples/hello-world/workflow.yaml | jq .steps

# In CI: one line per change, exit code reports the outcome.
flow run examples/hello-world/workflow.yaml >/dev/null

# Check a workflow without running it:
flow validate examples/hello-world/workflow.yaml`,
	}

	addOutputFlag(runCmd)
	addRawOutputFlag(runCmd)
	addFollowFlags(runCmd)
	addInputFlags(runCmd)

	// Why a person is starting this run, recorded on it. Optional here and
	// required by the *workflow*: a file declaring `manual: {require_reason:
	// true}` is refused a start without one, by the server, where the rule lives.
	// Offered on every run rather than only where a file asks for one, because a
	// flag that appears and disappears with the file is a flag nobody learns —
	// and provenance is worth recording whether or not it was demanded.
	//
	// Not offered on `flow run local`: a rehearsal is never gated, so a reason
	// there would be a value recorded nowhere, for a check that does not run.
	runCmd.Flags().String("reason", "",
		"why this run is being started, recorded on it; required by a workflow whose "+
			"`manual:` block asks for one")

	// Run local command, which executes a workflow locally without using Temporal or the Flowstate service.
	runLocalCmd := &cobra.Command{
		Use:   "local [workflow-file]",
		Short: "Run a workflow locally without Temporal",
		Long: "Execute a workload in this process, without Temporal and without a Flowstate " +
			"server.\n\nThis is the rehearsal, and it is worth rehearsing with because the two " +
			"drivers are one execution model: conditions, retries, timeouts, loops and waits " +
			"behave here the way they behave in production, and the answer comes back in the " +
			"same document `flow run` writes, so a `jq` expression written against one works " +
			"against the other.\n\nWhat it cannot give you is durability. A local run is a " +
			"process: it has no run id, nothing can watch it, and it does not survive this " +
			"command being interrupted.\n\n" +
			"Arguments are given the same way `flow run` takes them (--input name=value or " +
			"--input-file inputs.json) and are bound against the workflow's `inputs:` by the " +
			"same function the server binds them with, so a rehearsal refuses what production " +
			"refuses.\n\n" +
			"Plugin tasks run here too, given --plugin-dir: the plugins are launched in this " +
			"process, through the discovery, handshake and catalog a worker uses, and the " +
			"file's `plugins:` requirements are resolved against what was launched, refused " +
			"in the words a server refuses a submission in. Without --plugin-dir there are no " +
			"plugins, and a step naming one is an unknown task, which is what a worker without " +
			"them would also say.\n\n" +
			"--as-subject and its siblings name the identity this rehearsal runs as, and every " +
			"surface that reads one reads that: the secret access rules, a credential the run " +
			"assumes, plugin tasks, `run.identity`, and the --task-policy and --egress-policy " +
			"rules a worker would enforce. So a rule keyed on identity.namespace answers here " +
			"the way it answers in production, which is what rehearsing under a policy is for.\n\n" +
			"What that does not do is make the run attested. Nothing verified these flags - they " +
			"are what you say you are - so `run.local` reads true, and a credential this run " +
			"assumes is minted under a subject carrying a `_local` component no server-attested " +
			"run can ever produce. A cloud trust policy written for your production subject will " +
			"not match a rehearsal's, deliberately: that refusal is the one divergence between " +
			"the two drivers that is a feature.\n\n" +
			"A gate is the one place that limit is lifted, because a gate is the thing worth " +
			"rehearsing. --signal-as-subject and its siblings name the approver a --signal " +
			"delivery stands in for, and the workflow's own `signals:` policy is then checked " +
			"here by the same function the server checks it with - so an approver a rule admits " +
			"in production opens the gate here, one it refuses is refused here, and an approver " +
			"who is this run's own starter is refused by `distinct_from_starter:` on both. It " +
			"remains a rehearsal, and says so: nothing attested it, and the gate's own " +
			"`sender.local` output reads true." + runDocumentHelp,
		Args: cobra.MinimumNArgs(1),
		RunE: runLocalWorkflow,
		Example: `# Run a workflow locally:
flow run local examples/hello-world/workflow.yaml

# Run a multi-step workflow:
flow run local examples/hello-world-multi-step/workflow.yaml

# Take one step's output, the same way you would from a durable run:
flow run local examples/hello-world/workflow.yaml | jq .steps.hello

# Ask for the whole run as one document, including how it went:
flow run local examples/hello-world/workflow.yaml -o json | jq -r .status

# Run a workflow with an approval gate, answering the gate up front:
flow run local examples/expense-approval/workflow.yaml --input-file examples/expense-approval/inputs.json --signal manager-approved='{"approved": true}'

# Rehearse a gate whose signals: policy names its approver, standing in for them:
flow run local examples/approval-gate/workflow.yaml --input-file examples/approval-gate/inputs.json --signal deploy-approved='{"approved": true}' --signal-as-subject sre-lead@example.com --signal-as-issuer https://issuer.example.com --signal-as-claim team=release-managers

# Run a workflow that takes arguments, and read what it answered with:
flow run local examples/computed-outputs/workflow.yaml --input release=2026.9.0 -o json | jq .runOutputs

# Rehearse a workflow whose steps use a plugin's tasks, launching the plugins here:
flow run local examples/plugins/greet/workflow.yaml --plugin-dir ./plugins --secret-env GREET_TOKEN --auth-policy auth.yaml`,
	}

	addOutputFlag(runLocalCmd)
	addRawOutputFlag(runLocalCmd)
	addInputFlags(runLocalCmd)
	addRevealSensitiveFlag(runLocalCmd)

	// Supplying signals up front is what makes an approval gate something an author
	// can exercise on their laptop rather than first meeting in production. A local
	// run is a process, so there is nobody to signal it after it starts; the local
	// waiter buffers what is given here, so a gate reached later still finds its
	// answer waiting — the same behavior the durable driver has because Temporal
	// buffers signals for a run.
	runLocalCmd.Flags().StringArray("signal", nil,
		`answer a wait_for_signal step, as name=json (repeatable), e.g. --signal deploy-approved='{"approved": true}'`)

	// Who those answers are from. A gate whose `signals:` policy names an
	// approver is unreachable without this: a delivery attesting nobody matches
	// no `allow:` rule, so the only rehearsal available was the refusal. These
	// name the approver every --signal of this run stands in for, and the same
	// check production runs then admits or refuses it here - including
	// `distinct_from_starter:`, compared against --as-subject/--as-issuer.
	//
	// Spelled to rhyme with --as-subject and its siblings, which name the
	// starter, because they answer the same shape of question about the other
	// party. Deliberately no --signal-as-deployment: no `signals:` rule can
	// match on a deployment, so a flag for it would rehearse nothing.
	runLocalCmd.Flags().String("signal-as-subject", "",
		"authenticated subject to deliver --signal as, with --signal-as-issuer (local runs only)")
	runLocalCmd.Flags().String("signal-as-issuer", "",
		"authenticated issuer to deliver --signal as, with --signal-as-subject (local runs only)")
	runLocalCmd.Flags().String("signal-as-namespace", "",
		"tenant namespace to deliver --signal as (local runs only)")
	runLocalCmd.Flags().StringArray("signal-as-claim", nil,
		"authenticated string claim NAME=VALUE to deliver --signal as (repeatable)")

	// Worker command, which starts a Temporal worker to process workflows and activities.
	workerCmd := &cobra.Command{
		Use:   "worker",
		Short: "Start a worker",
		Long: "Start a Temporal worker: the process that actually runs a workflow's steps. " +
			"The server submits work to Temporal and a worker polling its task queue is what picks " +
			"it up, so nothing a deployment accepts runs until at least one worker is up: the two " +
			"never talk to each other, they meet at Temporal. With --deployment-name and --build-id " +
			"it claims a Worker Deployment version, pinning every run already in flight to the " +
			"interpreter it started on: a later deploy changes what new runs compute, not what " +
			"in-flight ones do, until each reaches continue-as-new. With --tenant it executes one " +
			"namespace's runs and refuses every other outright, rather than running them with this " +
			"worker's secrets, egress policy and plugins, which needs a queue of its own, named by " +
			"--task-queue-prefix (the value the server was started with) or given as --task-queue.",
		RunE: runWorker,
		Example: `# Start a worker, pinned so a deploy does not change runs already in flight:
flow worker --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"

# Start one against a local dev server, accepting that nothing pins the interpreter:
flow worker --allow-unversioned-interpreter

# Start a worker with custom Temporal server:
flow worker --temporal-address localhost:7233 --deployment-name flowstate --build-id dev-1

# Start a worker with custom namespace:
flow worker --temporal-namespace production --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"`,
	}

	// These override Temporal's environment configuration when set; unset means
	// Server command, which starts a Flowstate server to handle workflow requests.
	serverCmd := &cobra.Command{
		Use:   "server",
		Short: "Start a server",
		Long: "Start the Flowstate control plane: the Connect (HTTP/gRPC) endpoint every CLI verb and " +
			"`flow mcp` reaches a deployment through. It authenticates each caller and maps them onto " +
			"a tenant, then submits accepted runs to Temporal, where the workers execute them: the " +
			"server schedules, lists and reports on runs, it does not run their steps itself. " +
			"Authentication is fail-closed: it serves only with a trust policy configured " +
			"(--auth-policy, naming the issuers and claims to accept) or with authentication waived " +
			"out loud (--insecure-no-auth, for local development), and never by defaulting open.",
		RunE: runServer,
		Example: `# Start the server with default settings:
flow server

# Start the server with verbose logging:
flow server --verbose`,
	}

	// use whatever TEMPORAL_* variables or the temporal.toml profile resolve to.
	//
	// Prefixed because they are Temporal's settings and this process has
	// settings of its own that answer the same questions: --listen is the
	// socket `flow server` binds, --tenant is a Flowstate tenant. Unprefixed,
	// `--address` named Temporal's frontend here and the Flowstate server on
	// every client verb (picatz/flowstate#580). addRenamedTemporalFlags keeps
	// the old spellings registered, hidden, and refusing.
	for _, c := range []*cobra.Command{workerCmd, serverCmd} {
		c.Flags().String("temporal-address", "", "Temporal frontend address to dial (overrides environment configuration)")
		c.Flags().String("temporal-namespace", "", "Temporal namespace (overrides environment configuration)")
		c.Flags().String("temporal-profile", "", "Temporal configuration profile to use")
		addRenamedTemporalFlags(c)
	}
	workerCmd.Flags().String("task-queue", cmp.Or(os.Getenv("TEMPORAL_TASK_QUEUE"), engine.RunTaskQueueName),
		"task queue for Temporal workflows and activities")

	// Per-tenant routing: the prefix on both sides, the tenant on the worker.
	//
	// The prefix is the same value on the server and the worker because it is
	// the same fact — how this deployment spells a tenant's queue — and a worker
	// that spelled it differently would poll a queue nothing submits to. Stated
	// twice rather than discovered, because a worker does not talk to the
	// Flowstate server at all: it dials Temporal, and there is nothing there to
	// ask.
	for _, c := range []*cobra.Command{workerCmd, serverCmd} {
		c.Flags().String("task-queue-prefix", os.Getenv("FLOWSTATE_TASK_QUEUE_PREFIX"),
			"route each tenant's runs to a task queue of their own, named <prefix>_<namespace>, "+
				"so a per-tenant worker fleet can be addressed; unset means every tenant shares "+
				"the single default queue, which is the zero-configuration behavior")
	}
	workerCmd.Flags().String("tenant", "",
		"execute only this Flowstate namespace's runs, refusing any other tenant's outright "+
			"rather than executing it with this worker's secrets, egress policy and plugins. "+
			"Pass an empty value (--tenant=) for the default tenant of an untenanted deployment. "+
			"Needs a queue of this worker's own: either --task-queue-prefix (the same value the "+
			"server was started with) or an explicit --task-queue")

	workerCmd.Flags().String("deployment-name", os.Getenv("FLOWSTATE_DEPLOYMENT_NAME"),
		"Worker Deployment this worker belongs to. With --build-id, pins every in-flight run to the "+
			"interpreter version it started on; a run moves to the current version only at continue-as-new")
	workerCmd.Flags().String("build-id", os.Getenv("FLOWSTATE_BUILD_ID"),
		"version identifier for this worker's binary, unique per build. Required with --deployment-name")
	workerCmd.Flags().Bool(allowUnversionedFlag, false,
		"start without a Worker Deployment version, accepting that deploying a different binary "+
			"changes what runs already in flight compute; for local development")

	workerCmd.Flags().String("identity", os.Getenv("FLOWSTATE_WORKER_IDENTITY"),
		"how this worker identifies itself to Temporal, shown in Event History and a Task Queue's "+
			"poller list (#752); a platform-native identifier (a Kubernetes pod name from the "+
			"downward API, an ECS task id) is the most useful value here. Unset builds one from "+
			"--deployment-name/--build-id, --tenant if set, and this process's hostname — still more "+
			"specific than the SDK's own pid@hostname default, but a real platform identifier beats it")

	// How long Stop() gives an in-flight activity or workflow task to finish
	// once SIGINT/SIGTERM arrives, before the SDK gives up waiting and returns.
	// Keep it under whatever grace period the deployment shape actually gives
	// this process (Docker's `stop` default is 10s; Kubernetes'
	// terminationGracePeriodSeconds is commonly 30s) — a drain window longer
	// than the grace period never finishes; it just moves where the hard kill
	// lands. See docs/DEPLOYMENT.md.
	//
	// A string flag rather than cobra's Duration type, matching every other
	// FLOWSTATE_*-defaulted flag in this file: pflag reprints a Duration
	// default in its own canonical form, which is indistinguishable from a
	// literal constant, so `flow docs generate`'s environment-mirror detection
	// (cmd/flow/internal/docsgen/cli.go) — which works by setting a variable to
	// a sentinel string and checking whether it survives unchanged into a
	// flag's default — could never see this one. A plain string default passes
	// the sentinel through untouched. Parsed once in runWorker, with
	// v1.ParseDuration for the same duration grammar the DSL itself accepts.
	workerCmd.Flags().String("worker-stop-timeout",
		cmp.Or(os.Getenv("FLOWSTATE_WORKER_STOP_TIMEOUT"), v1.DefaultWorkerStopTimeout.String()),
		"how long to wait for in-flight activities and workflow tasks to finish once a shutdown "+
			"signal (SIGINT or SIGTERM) arrives, before this worker exits regardless")

	// Worker capacity (#783): the two execution sizes Temporal's slot-exhaustion
	// runbook names first, plus the two rate limits #785 folded into this same
	// issue. All four are string flags for the identical reason
	// --worker-stop-timeout above is one, and all four take 0 to mean "the
	// Temporal SDK's own default" — see workerCapacityOptions for the full
	// reasoning and the SDK line numbers this is verified against.
	workerCmd.Flags().String("max-concurrent-activities",
		cmp.Or(os.Getenv("FLOWSTATE_WORKER_MAX_CONCURRENT_ACTIVITIES"), "0"),
		"maximum number of activity tasks executing at once in this process; 0 takes the Temporal "+
			"SDK default (1000). Raising this trades worker CPU/memory for throughput on a single "+
			"replica; see docs/DEPLOYMENT.md's capacity section for when to raise this versus "+
			"scaling out")
	workerCmd.Flags().String("max-concurrent-workflow-tasks",
		cmp.Or(os.Getenv("FLOWSTATE_WORKER_MAX_CONCURRENT_WORKFLOW_TASKS"), "0"),
		"maximum number of workflow tasks executing at once in this process; 0 takes the Temporal "+
			"SDK default (1000). The value 1 is refused: a worker with a single workflow-task slot "+
			"never polls its regular queue, which the SDK enforces by panicking")
	workerCmd.Flags().String("max-activities-per-second",
		cmp.Or(os.Getenv("FLOWSTATE_WORKER_MAX_ACTIVITIES_PER_SECOND"), "0"),
		"maximum rate, per second, at which this worker process starts activity tasks; 0 takes "+
			"the Temporal SDK default (effectively unlimited). Enforced locally, per worker process "+
			"— see --task-queue-activities-per-second for the server-enforced, per-queue limit")
	workerCmd.Flags().String("task-queue-activities-per-second",
		cmp.Or(os.Getenv("FLOWSTATE_WORKER_TASK_QUEUE_ACTIVITIES_PER_SECOND"), "0"),
		"maximum rate, per second, at which the Temporal server dispatches activity tasks from "+
			"this worker's task queue, shared across every worker polling that queue; 0 takes the "+
			"Temporal SDK default (effectively unlimited). Per queue, not per worker: setting this "+
			"differently on two workers sharing a queue is last-writer-wins on the server, and "+
			"setting it disables eager activity execution for this worker (DisableEagerActivities)")

	addPluginFlags(workerCmd)
	addPluginFlags(serverCmd)
	addSecretFlags(serverCmd)
	addWebhookFlags(serverCmd)

	// And the local rehearsal, which for a long time was the one execution verb
	// without them: a Flowfile using a plugin task could be validated, run
	// durably, and invoked one task at a time through `flow task run`, and the
	// only answer `flow run local` had for it was that the task did not exist
	// (#436). That is a refusal production does not give, from the driver whose
	// entire purpose is to say what production will do.
	addPluginFlags(runLocalCmd)

	// The worker and the local rehearsal, and deliberately not the server — see
	// egress.go for why a policy registered on the server would change nothing
	// the server answers.
	addEgressPolicyFlag(workerCmd)
	addEgressPolicyFlag(runLocalCmd)
	addTaskPolicyFlag(workerCmd)
	addTaskPolicyFlag(runLocalCmd)
	addSecretFlags(workerCmd)
	addSecretFlags(runLocalCmd)
	addLocalRehearsalFlags(runLocalCmd)
	workerCmd.Flags().String("auth-policy", os.Getenv("FLOWSTATE_AUTH_POLICY"),
		"path to an access policy whose secrets rules authorize worker-side resolution")
	workerCmd.Flags().StringArray("identity-key", identityKeyDefault(), identityKeyUsage)

	serverCmd.Flags().String("auth-policy",
		os.Getenv("FLOWSTATE_AUTH_POLICY"),
		"path to an OIDC/workload-identity trust policy (YAML) describing which issuers to accept")
	serverCmd.Flags().Bool("insecure-no-auth", false,
		"allow unauthenticated access; for local development only")
	serverCmd.Flags().StringArray("identity-key",
		identityKeyDefault(),
		"path to a PKCS#8 PEM private key Flowstate signs its own assertions with, "+
			"required when the trust policy configures federation; the file's base name "+
			"becomes the published key id, so 2026-07.pem publishes as \"2026-07\". "+
			"Repeatable: the first occurrence signs and every later one is published for "+
			"verification only, so a restart that rotates keys does not reject assertions "+
			"the previous process signed")

	// The server's deployment name is not the worker's Worker Deployment pair: it
	// names this Flowstate installation in the identity every run carries, so an
	// assertion presented to an external system distinguishes staging from
	// production. Same spelling and same environment default as the worker's flag
	// on purpose, because they describe the same installation.
	serverCmd.Flags().String("deployment-name", os.Getenv("FLOWSTATE_DEPLOYMENT_NAME"),
		"name of this Flowstate deployment, recorded in each run's workload identity "+
			"and in every assertion subject it mints")
	serverCmd.Flags().StringArray("identity-claim", nil,
		"caller token claim to carry into each run's workload identity (repeatable), "+
			"such as repository or email; only named claims are carried, and they are "+
			"what workload.claims[...] policy rules read")

	// The public listener's own address. Until now this was the one setting in
	// the tree configured by environment variable with no flag beside it:
	// FLOWSTATE_ADDRESS remains the default, so nothing that works today stops
	// working, but `flow server --help` now says how to change it instead of
	// sending an operator to the deployment docs. Named --listen rather than
	// --address for the same reason `flow server dev` is: a bare host:port for
	// net.Listen is not the client's --address, which takes a URL and may carry
	// a scheme. It is also what `--address` on this command is now refused in
	// favour of, alongside --temporal-address (picatz/flowstate#580).
	serverCmd.Flags().String("listen", cmp.Or(os.Getenv("FLOWSTATE_ADDRESS"), defaultServerAddress),
		"address this server listens on, as a bare host:port for net.Listen (default "+
			"$FLOWSTATE_ADDRESS); not a URL, and not the client's --address — off loopback, "+
			"refusePlaintextListener requires --tls-cert-file/--tls-key-file or "+
			"--tls-terminated-upstream")

	// The public listener's TLS configuration, its ACME automatic-certificate
	// alternative, and the internal listener's own address — see
	// cmd/flow/tls.go, cmd/flow/acme.go and cmd/flow/internallistener.go.
	// Server only: a worker makes no listener of its own to protect.
	addTLSFlags(serverCmd)
	addACMEFlags(serverCmd)
	addMTLSFlags(serverCmd)
	addInternalListenerFlags(serverCmd)

	// RFC 9728 protected resource metadata for the MCP surface — see
	// cmd/flow/protectedresource.go. Server only, for the same reason as the
	// listener flags above: a worker serves no HTTP surface to advertise.
	addProtectedResourceFlags(serverCmd, "Unset (the default): the route does not exist "+
		"and every challenge reads exactly as it does today")

	// Validate command, which checks Flowfiles without executing them.
	validateCmd := &cobra.Command{
		Use:   "validate [workflow-file...]",
		Short: "Check workflows for problems without running them",
		Long: "Check one or more Flowfiles for problems without executing them. " +
			"Reports unknown tasks, duplicate or unusable step ids, and references to " +
			"steps that do not exist or have not run yet, with the line each problem is on.\n\n" +
			"A file naming a plugin's task is checked against that plugin given " +
			"--plugin-dir: the plugins there are launched here, through the same " +
			"discovery, handshake and catalog a worker uses, and their tasks and input " +
			"schemas are then what this command checks against — so a misspelled input " +
			"to a plugin task is a diagnostic at your terminal rather than a failure at " +
			"the worker. It launches third-party binaries, which is why it takes this " +
			"flag rather than looking anywhere by default, and a plugin that will not " +
			"start fails this command outright: carrying on without it would report " +
			"every one of its tasks as unknown, which is a false report about the file.\n\n" +
			"Without --plugin-dir a step naming a plugin task is reported as not " +
			"registered here, which is what it is: whether a plugin is installed is the " +
			"deployment's decision and this process has not been told about one.",
		Args:          cobra.MinimumNArgs(1),
		RunE:          runValidate,
		SilenceErrors: true,
		// A file with a problem in it is not a command someone invoked wrongly, and
		// the usage block after the diagnostics reads as though it were — sending
		// the reader to check their flags instead of the line they were just told
		// about. `flow fix` has said so since it was written; this said it by
		// accident, because the error report absorbs the usage on the way out.
		//
		// Which stopped being harmless with `--output json`: cobra writes usage to
		// the same stream, so a consumer parsing the answer got a JSON document with
		// a usage block appended to it.
		SilenceUsage: true,
		Example: `# Check a single workflow:
flow validate examples/hello-world/workflow.yaml

# Check every example:
flow validate examples/*/workflow.yaml

# Ask for the diagnostics as data, one line per file:
flow validate examples/*/workflow.yaml -o jsonl | jq 'select(.diagnostics | length > 0)'

# Check a file whose steps name a plugin's tasks, against that plugin:
flow validate --plugin-dir ./plugins examples/plugins/greet/workflow.yaml

# The same check against a saved catalog, launching nothing:
flow plugins --plugin-dir ./plugins -o json > plugins.lock.json
flow validate --plugin-catalog plugins.lock.json examples/plugins/greet/workflow.yaml`,
	}

	// Diagnostics are a schema message, so `-o json` means here what it means on
	// `get` and `list`: the fields are the schema's and addressable by name.
	addOutputFlag(validateCmd)

	// The same flags every other surface that reads a task name takes, doing the
	// same thing through the same [startPlugins] (#724, #710).
	//
	// This was the gap the landing rule fell into: CLAUDE.md says a capability
	// lands when a Flowfile expresses it, `flow validate` accepts it, and an
	// example exercises it in CI — and `flow validate` was the one verb in the
	// tree that could never say yes to a plugin task, because it was the one
	// authoring surface with no way to be told what a plugin provides. `flow
	// lsp`, `flow mcp`, `flow run local`, `flow task run`, the worker and the
	// server all had it.
	//
	// Opt-in, exactly as it is on `flow lsp` and for the identical reason: this
	// executes the binaries on the search path, and nothing but a command line
	// somebody typed turns that on. See [runValidate] for what a plugin that
	// fails to launch does here, which is the decision that matters on this verb.
	addPluginFlags(validateCmd)

	// And the same question answered without executing anything, from a saved
	// catalog (#710). It is the mechanism the surfaces that cannot exec need —
	// a browser authoring surface (#102, #242), a server-side Validate RPC —
	// and the one a CI job wants, since a checked-in catalog validates plugin
	// examples with no plugin binaries in the runner.
	addPluginCatalogFlag(validateCmd)

	// Get command, which asks a server what a run is doing.
	//
	// `flow run` polls for this while it waits, which serves the case where the
	// person who started the workload is still watching. A durable workload's
	// whole point is outliving that terminal, so it has to be askable about later.
	getCmd := &cobra.Command{
		Use:   "get [workflow-id]",
		Short: "Report what a run is doing",
		Long: "Report the status of a run, and its outputs if it has finished. The status is " +
			"written to stderr and the outputs to stdout, so the outputs can be piped. A run " +
			"that failed is reported as a failure, so `flow get id && ...` behaves as expected." +
			runDocumentHelp,
		Args: cobra.ExactArgs(1),
		RunE: runGet,
		Example: `# Ask what a run is doing:
flow get flowstate-workflow-3f7c

# Keep only the outputs:
flow get flowstate-workflow-3f7c | jq .steps

# Ask about one attempt rather than the current one:
flow get flowstate-workflow-3f7c --run-id 0198f1e2-...`,
	}

	addOutputFlag(getCmd)
	addRawOutputFlag(getCmd)
	addRevealSensitiveFlag(getCmd)

	getCmd.Flags().String("run-id", "",
		"ask about one attempt of the workload; unset asks about whichever is current")

	// Signal command, which answers a gate on a run that is already waiting.
	//
	// The counterpart to `flow run local --signal`: there the answers are given
	// before the run starts, because a local run is a process with nobody to
	// signal it; here the run is durable and the answer arrives whenever the
	// person gets to it, which is the case an approval gate exists for.
	signalCmd := &cobra.Command{
		Use:   "signal [workflow-id] [signal-name]",
		Short: "Send a signal to a waiting run",
		Long: "Deliver a signal to a run waiting for one, which is how a human approval reaches " +
			"a workload. The payload becomes the waiting step's outputs, so its keys are what " +
			"later steps read as ${step_id.key}.\n\n" +
			// The numbers are the constants, not a prose copy of them: a limit
			// documented by hand is a limit that drifts the day it changes.
			fmt.Sprintf("Two limits, both worth knowing before designing a payload. A payload over "+
				"%d KiB is refused synchronously, with the size and the limit named; send a "+
				"reference to something large rather than the thing itself, since the payload "+
				"travels with the run from then on. And a signal that arrives before its gate is "+
				"reached is held for it, at most %d across all names with the earliest kept: "+
				"sending does not fail when the run is elsewhere, it waits.",
				v1.MaxSignalPayloadBytes/1024, v1.MaxPendingSignals) + mutationFlagHelp +
			"\n\n`result` is \"delivered\" once the server has taken the signal, and `signalName` is " +
			"which one: two signals to one run are two acts, so the name is part of the result " +
			"rather than only of the request. \"delivered\" rather than \"applied\" because it is a " +
			"claim about the server and not about the workflow: being held for a gate not reached " +
			"yet counts as delivered, and a signal still held when the run continues as new is " +
			"dropped once the pending limit above is full, so a workflow that never sees it is a " +
			"possible ending of a delivery that succeeded.",
		Args: cobra.ExactArgs(2),
		RunE: runSignal,
		Example: `# Approve a deploy waiting on a gate:
flow signal deploy-abc123 deploy-approved --data '{"approved": true, "by": "someone@example.com"}'

# Decline it; the workload can tell this apart from nobody answering:
flow signal deploy-abc123 deploy-approved --data '{"approved": false}'

# Send a signal that carries nothing:
flow signal deploy-abc123 deploy-approved

# A local run is given its answers up front instead, the same idea for a
# workflow with no signals: policy to attest a sender against:
flow run local examples/expense-approval/workflow.yaml --input-file examples/expense-approval/inputs.json --signal manager-approved='{"approved": true}'

# Confirm the delivery from a script, which gets a document rather than a sentence:
flow signal deploy-abc123 deploy-approved -o json | jq -r '.signalName, .result'`,
	}

	addOutputFlag(signalCmd)

	signalCmd.Flags().String("data", "",
		`signal payload as a JSON object, whose keys become the waiting step's outputs, e.g. --data '{"approved": true}'`)

	// The commands that talk to a Flowstate server can say which one.
	//
	// FLOWSTATE_ADDRESS was the only way to point these somewhere until now, which
	// meant addressing a second deployment took an exported variable rather than a
	// flag. Both the plain-HTTP warning and the no-server error already told people
	// to use --address, so this is the flag they were being sent to look for.
	//
	// `run local` deliberately does not get it: it contacts nothing.
	//
	// Built from one list so a verb added later cannot be given a group and left
	// without an address — the way `get` and `signal` were first written.
	lifecycleCmds := lifecycleCommands()
	watchCmd := newWatchCommand()

	// The schedule verbs add their own server flags to each sub-command, because
	// they are one level down and a flag on the group would not reach them.
	scheduleCmd := newScheduleCommand()

	serverCmds := append([]*cobra.Command{runCmd, getCmd, signalCmd, watchCmd}, lifecycleCmds...)

	for _, c := range serverCmds {
		addServerFlags(c)
	}
	signalCmd.Flags().String("run-id", "",
		"pin the signal to one run of the workload; unset addresses whichever run is current, "+
			"which is what approving a workload means")

	// Tasks command: the index of what a step can name, and the description of any
	// one of them.
	//
	// An index and a detail view rather than one page, per #379. Every task's full
	// input and output tables plus the whole expression reference printed in one
	// breath is a page nobody can hold, and it grew in the one dimension a
	// [cobra.NoArgs] command cannot subset: the catalog, which plugins extend.
	tasksCmd := &cobra.Command{
		Use:   "tasks [name]",
		Short: "List the tasks workflows can use, or describe one",
		Long: "List the tasks available to workflow steps, one line each. Name one to see " +
			"it in full: every input with what may be written in it, what the task evaluates " +
			"itself, what it hands back, and a step to copy.\n\n" +
			"What a plugin provides is listed too, given --plugin-dir: the plugins there " +
			"are launched and their tasks join the catalog, each marked with the plugin it " +
			"came from, so this is the whole of what a step could name on a worker " +
			"configured the same way. Without it this is what this binary alone can run, " +
			"and `flow plugins` is the other half.",
		Args: cobra.MaximumNArgs(1),
		RunE: runTasks,
		// Completed from the registry, which is the same place the listing comes
		// from, so a name this offers is a name this build can run.
		ValidArgsFunction: func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
			return v1.TaskNames(), cobra.ShellCompDirectiveNoFileComp
		},
		Example: `# Every task a step can name, one line each:
flow tasks

# One task in full: its inputs, their bounds, and a step to copy:
flow tasks http

# What every expression in a Flowfile can say:
flow tasks --expressions

# The whole catalog as a document, for a script or an agent:
flow tasks --output json

# One task as a document:
flow tasks http --output json | jq '.inputs'

# Every task a worker with these plugins could run, built-in and provided:
flow tasks --plugin-dir ./plugins

# One plugin's task in full, the same page a built-in gets:
flow tasks example.greet --plugin-dir ./plugins

# What a worker holding those plugins would run, read from a saved catalog:
flow tasks --plugin-catalog plugins.lock.json`,
	}
	addOutputFlag(tasksCmd)
	tasksCmd.Flags().Bool(expressionsFlag, false,
		"describe what every expression can say: the CEL functions, the duration "+
			"constructors, `now` inside a wait, and where a value comes from")

	// The other half of #724. This command's own doc comment has said since it was
	// written that the catalog is the dimension "plugins already extend" — true of
	// `flow plugins`, and not of this command, which launched nothing and so
	// listed the built-ins and called them the catalog. Naming a plugin's task
	// here answered that no task by that name exists in this build, which was a
	// statement about the invocation rather than about the task.
	addPluginFlags(tasksCmd)

	// A catalog *is* a listing, which is why this verb takes the offline form
	// too (#710): the document holds every task a plugin provides, with the
	// descriptors this page renders, so `flow tasks --plugin-catalog` prints
	// what a worker holding those plugins would run without holding them. The
	// provenance lines then describe the machine the catalog was written on,
	// which is what the document actually records.
	addPluginCatalogFlag(tasksCmd)

	// Task command, which runs one task without a workflow around it. Built in
	// taskrun.go, beside the code it drives. See [newTaskCommand] for why the
	// singular verb is not folded into the plural listing above.
	taskCmd := newTaskCommand()

	// Plugins command, which reports what a plugin directory adds to this build.
	//
	// Beside `tasks` rather than under `worker`, because the question it answers is
	// the same one: what can a step in my file name? The registry a worker runs
	// with is the built-ins plus whatever its plugins provide, and until this
	// existed the second half was visible only in a worker's log.
	pluginsCmd := &cobra.Command{
		Use:   "plugins",
		Short: "List the plugins a worker would load, and the tasks they add",
		Long: "List the plugins discovered on a search path, along with the tasks and " +
			"secret schemes each one provides. A plugin is an executable named " +
			"flowstate-plugin-<name>; this launches each one it finds and asks it, " +
			"which is the only way to know what a plugin does.",
		Args: cobra.NoArgs,
		RunE: runPlugins,
		Example: `# What would a worker with this plugin directory be able to run?
flow plugins --plugin-dir /usr/local/lib/flowstate/plugins

# The same thing as a document:
flow plugins --plugin-dir /usr/local/lib/flowstate/plugins --output json

# Which plugin provides a given task?
flow plugins -o json | jq -r '.plugins[] | select(.tasks[].name == "example.greet") | .name'`,
	}
	addOutputFlag(pluginsCmd)
	addPluginFlags(pluginsCmd)

	// Keys and JWT commands, for admin debugging of workload identity: what a
	// generated key publishes, and what a token actually claims and verifies
	// against, without needing a throwaway Go program to find out.
	keysCmd := newKeysCommand()
	jwtCmd := newJWTCommand()

	// Version command, answering "which build" the way a bug report or an
	// agent transcript needs to: see version.go for why this is a verb rather
	// than only the `--version` line cobra already prints.
	versionCmd := newVersionCommand()

	// MCP command, which serves the control plane to an AI agent as tools.
	mcpCmd := &cobra.Command{
		Use:   "mcp",
		Short: "Serve Flowstate to an AI agent over the Model Context Protocol",
		Long: "Serve every workflow-service RPC as an MCP tool over stdin and stdout, " +
			"with input schemas derived from the same protobuf schema the API speaks. " +
			"Validation, compilation and local execution always answer in this process; " +
			"the run-lifecycle tools call the configured server. The task catalog answers " +
			"in this process too, unless --address (or FLOWSTATE_ADDRESS) explicitly names " +
			"a deployment, in which case it answers from that deployment instead, and " +
			"refuses rather than falling back here if it cannot be reached.\n\n" +
			"flowstate_run_local executes a submitted Flowfile here, the way `flow run local` " +
			"does. What such a run may reach is decided by the flags this process is started " +
			"with and by nothing a client sends: with no flags, egress is denied and no secret " +
			"scheme is registered.\n\n" +
			"Beside the tools, the server publishes read-only resources: the whole DSL " +
			"reference at flowstate://docs/dsl, the task catalog as JSON at " +
			"flowstate://catalog/tasks, and every example Flowfile under " +
			"flowstate://docs/examples/, embedded at build time, so an agent can read the " +
			"language and working references without a checkout nearby. See docs/CLI.md " +
			"for client configuration.\n\n" +
			"An agent host launches this and speaks to it over the same stdin and stdout " +
			"this process already has; typing `flow mcp` yourself waits for a host to " +
			"connect rather than doing anything. Claude Code: " +
			"`claude mcp add flowstate -- flow mcp`. A host that reads the JSON config " +
			"MCP servers conventionally use instead: " +
			`{"mcpServers":{"flowstate":{"command":"flow","args":["mcp"]}}}`,
		Args: cobra.NoArgs,
		RunE: runMCP,
		Example: `# What an agent host runs; typing this yourself waits for one to connect:
flow mcp

# Against a specific server for the run-lifecycle tools:
flow mcp --address flowstate.internal:9233

# Permit local runs to reach what an egress policy names, and nothing else:
flow mcp --egress-policy examples/egress-policy.yaml

# Let local runs resolve one environment secret, under an access policy:
flow mcp --secret-env API_KEY --auth-policy policy.yaml

# Teach the catalog, flowstate_validate and flowstate_run_local a plugin's
# tasks, so a file naming one stops reading as "unknown task":
flow mcp --plugin-dir ./plugins`,
	}
	addServerFlags(mcpCmd)

	// The posture flowstate_run_local executes under, taken at start-up because a
	// long-lived process serving a model cannot take it per call: an opt-in a
	// caller can send is not an opt-in. See mcp.go.
	addLocalRunFlags(mcpCmd)

	// `flow mcp` was, until #241, the only plugin-relevant command without this:
	// worker, server, plugins and lsp all call addPluginFlags, and an agent asked
	// to author a `codex.exec:` workflow — the flagship agentic story — was told
	// `unknown task` by the surface built for agents, with no way to validate,
	// catalog, or rehearse it. The precedent this follows, exactly, is
	// [runLSP]'s: an explicit operator flag, launched once at start-up rather
	// than reached for per call, registering into the same [v1.DefaultRegistry]
	// every lookup in the engine already consults (see [startPlugins]). Never
	// auto-discovery, for the reason runLSP's own comment gives — a cloned
	// repository must not choose what an agent's editor or this process executes
	// — and never per call, because a tool call is not a moment to launch a
	// process any more than a keystroke is.
	addPluginFlags(mcpCmd)

	// The HTTP half, as its own verb rather than a flag on the command above:
	// picatz/flowstate#558's decision 2, and see cmd/flow/mcpserve.go's header
	// for the reasoning. `flow mcp` with no subcommand still runs runMCP over
	// stdio, byte for byte as it did.
	mcpServeCmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve Flowstate to an AI agent over MCP on HTTP, as an OAuth 2.1 protected resource",
		Long: "Serve the Model Context Protocol over streamable HTTP, requiring every caller to " +
			"present a bearer token this deployment's own trust policy accepts and whose audience " +
			"names this resource specifically (RFC 8707 section 2). A request with no token is " +
			"answered 401 with a WWW-Authenticate header naming the RFC 9728 protected resource " +
			"metadata document, which this command also serves, so a compliant MCP client can " +
			"bootstrap from the refusal alone.\n\n" +
			"This is a different surface from `flow mcp`, not a transport switch on it. Over stdio " +
			"there is exactly one caller and it is the process that spawned this one, which is what " +
			"makes every posture flag there a decision taken once at start-up; over HTTP there are " +
			"many callers and those flags would silently change meaning. So the tools that execute " +
			"or dispatch anything are not served here: flowstate_run_local is absent, because over " +
			"HTTP it is remote code execution as a feature, and the run-lifecycle tools are absent " +
			"because they would spend this process's own credential on a caller's behalf. What is " +
			"served is what answers in this process and reaches nothing — flowstate_validate, " +
			"flowstate_compile, flowstate_get_catalog — plus flowstate_test, whose stubbed runs " +
			"replace every task implementation before a step executes.\n\n" +
			"Flowstate is not an authorization server: it issues no tokens, runs no authorization " +
			"or token endpoint, and verifies nothing it did not receive from the identity provider " +
			"an operator configured. No scope vocabulary is advertised or challenged for yet, and a " +
			"token carrying an RFC 8693 `act` or `may_act` delegation claim is refused rather than " +
			"read as its bare subject. See docs/MCP_AUTHORIZATION.md.",
		Args: cobra.NoArgs,
		RunE: runMCPServe,
		Example: `# Behind a TLS-terminating proxy, advertising one identity provider:
flow mcp serve --listen 127.0.0.1:8617 \
  --auth-policy /etc/flowstate/policy.yaml \
  --protected-resource https://flowstate.example.com/mcp \
  --authorization-server https://acme.okta.com

# Terminating TLS here instead:
flow mcp serve --listen :8617 \
  --tls-cert-file /etc/flowstate/tls.crt --tls-key-file /etc/flowstate/tls.key \
  --auth-policy /etc/flowstate/policy.yaml \
  --protected-resource https://flowstate.example.com/mcp \
  --authorization-server https://acme.okta.com`,
	}
	addMCPServeFlags(mcpServeCmd)
	mcpCmd.AddCommand(mcpServeCmd)

	// LSP command, which starts a Language Server Protocol (LSP) server for Flowfile files.
	lspCmd := &cobra.Command{
		Use:   "lsp",
		Short: "Start a Flowfile Language Server Protocol (LSP) server",
		Long: "Start a language server for Flowfile editing in text editors and IDEs, " +
			"serving the Language Server Protocol over stdin and stdout. It reports " +
			"Flowfile problems as diagnostics as you type.\n\n" +
			"This is not something you run and watch: an editor launches it and talks " +
			"to it over the same stdin and stdout this process already has, so there is " +
			"no address or port to configure. In VS Code, point a generic LSP extension " +
			"(or an extension you write) at the command; in Neovim's built-in client, " +
			"`cmd = {\"flow\", \"lsp\"}` (add `\"--plugin-dir\", \"/opt/flowstate/plugins\"` to the table " +
			"if a plugin's tasks should stop reading as unknown) with `filetypes` set to " +
			"Flowfile's, typically YAML.",
		RunE: runLSP,
		Example: `# What an editor runs; typing this yourself waits for one to connect:
flow lsp

# Teach the editor the tasks a plugin provides, so a file that names one
# stops reading as a mistake:
flow lsp --plugin-dir /opt/flowstate/plugins`,
	}

	// The same flags `flow worker` takes, doing the same thing — one discovery
	// path, so a directory that brings a plugin up on a worker brings the same one
	// up here.
	//
	// Opt-in, and only from this command line. Without it the server answers from
	// the built-in task set and a plugin's task reads as unknown, which is the
	// honest report from a process that launched nothing. Turning it on means
	// executing the binaries on the search path, and the two ways that could
	// happen by itself are both refused: it is not read from workspace
	// configuration, because a cloned repository would then choose what an
	// author's editor runs, and it is not done per request, because a keystroke
	// is not a moment to launch a process. Somebody types this flag for their own
	// machine, once, in the editor configuration that starts the server.
	//
	// [addEditorPluginFlags] rather than [addPluginFlags]: the same four flags,
	// with the two things a workspace must not decide taken away — the
	// environment default and a relative path. It carries the narrower help
	// text with it, so what `flow lsp --help` prints and what the command does
	// are one decision.
	addEditorPluginFlags(lspCmd)

	// Add command groups for better organization
	rootCmd.AddGroup(&cobra.Group{
		ID:    "workflow",
		Title: "Workflow Commands",
	})

	rootCmd.AddGroup(&cobra.Group{
		ID:    "infrastructure",
		Title: "Infrastructure Commands",
	})

	rootCmd.AddGroup(&cobra.Group{
		ID:    "development",
		Title: "Development Commands",
	})

	// Set command groups
	runCmd.GroupID = "workflow"
	validateCmd.GroupID = "workflow"
	tasksCmd.GroupID = "workflow"
	taskCmd.GroupID = "workflow"
	getCmd.GroupID = "workflow"
	watchCmd.GroupID = "workflow"
	signalCmd.GroupID = "workflow"
	scheduleCmd.GroupID = "workflow"
	for _, c := range lifecycleCmds {
		c.GroupID = "workflow"
	}
	workerCmd.GroupID = "infrastructure"
	serverCmd.GroupID = "infrastructure"
	lspCmd.GroupID = "development"
	keysCmd.GroupID = "development"
	jwtCmd.GroupID = "development"
	versionCmd.GroupID = "development"

	// Add commands to root.
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(validateCmd)

	// First in the group because it is first in the order somebody does things:
	// `init` writes the file every other command in this group reads, and a
	// newcomer scanning the list for where to start finds it above `validate`
	// rather than under a heading that assumes they already have a Flowfile.
	initCmd := newInitCommand()
	initCmd.GroupID = "workflow"
	rootCmd.AddCommand(initCmd)

	// Grouped with the other commands that read a Flowfile without running one.
	// Left out, it lands under the bare "Commands" heading beside `help` and
	// `completion`, which is where an author stops looking.
	fixCmd := newFixCommand()
	fixCmd.GroupID = "workflow"
	rootCmd.AddCommand(fixCmd)

	fmtCmd := newFmtCommand()
	fmtCmd.GroupID = "workflow"
	rootCmd.AddCommand(fmtCmd)

	// Beside `run local`, which is the other verb answering a question about a
	// workflow's own behavior rather than about its file — `run local` answers
	// "what does this do", this answers "does it do what it promised" (#155).
	testCmd := newTestCommand()
	testCmd.GroupID = "workflow"
	rootCmd.AddCommand(testCmd)

	// Beside `validate`, which is the command it is most often confused with and
	// the one an author reaches for first: same file, same compiler, different
	// question — whether it is correct, and what it becomes.
	compileCmd := newCompileCommand()
	compileCmd.GroupID = "workflow"
	rootCmd.AddCommand(compileCmd)

	// Beside `validate` and `test`, the other two commands that read a Flowfile
	// without running it. `buf breaking` guards the proto contract; this guards
	// the contract one level up, a workflow's declared inputs and outputs, in the
	// callee author's own CI at the moment of the change (#419).
	breakingCmd := newBreakingCommand()
	breakingCmd.GroupID = "workflow"
	rootCmd.AddCommand(breakingCmd)

	// Beside the other three that read a Flowfile without running one, because
	// that is the walk it shares with them, and nowhere near them in what it
	// says: `validate`, `test` and `breaking` each tell an author something is
	// wrong with a file, and this one tells a language designer what the language
	// costs the author (#411). See [newAuditCommand] for why that distinction is
	// worth being careful about.
	auditCmd := newAuditCommand()
	auditCmd.GroupID = "workflow"
	rootCmd.AddCommand(auditCmd)

	// Beside `validate` and `audit`, between which it sits exactly: `validate`
	// refuses a file, `audit` measures the language, and this suggests a better
	// spelling of a file that is already correct. That is tier 4 of the style
	// charter (docs/STYLE.md, Part II), which named the tier and had no tool for
	// it (#646). See [newLintCommand] for why it is a verb rather than a flag on
	// `validate`, and what that choice costs.
	lintCmd := newLintCommand()
	lintCmd.GroupID = "workflow"
	rootCmd.AddCommand(lintCmd)

	rootCmd.AddCommand(tasksCmd)
	rootCmd.AddCommand(taskCmd)
	rootCmd.AddCommand(pluginsCmd)
	rootCmd.AddCommand(mcpCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(watchCmd)
	rootCmd.AddCommand(signalCmd)
	rootCmd.AddCommand(scheduleCmd)
	for _, c := range lifecycleCmds {
		rootCmd.AddCommand(c)
	}
	rootCmd.AddCommand(workerCmd)
	rootCmd.AddCommand(serverCmd)

	// The whole stack in one command, under `server` because that is where
	// somebody looking for a server looks. Everything it is lives in
	// serverdev.go; this is the registration. See [newServerDevCommand].
	serverCmd.AddCommand(newServerDevCommand())

	runCmd.AddCommand(runLocalCmd)
	rootCmd.AddCommand(lspCmd)
	rootCmd.AddCommand(keysCmd)
	rootCmd.AddCommand(jwtCmd)
	rootCmd.AddCommand(versionCmd)

	return rootCmd
}

func main() {
	rootCmd := newRootCommand()

	// We can use a context to handle OS signals like Ctrl+C gracefully, and —
	// this is the one every documented deployment shape actually sends —
	// SIGTERM: what `docker stop`, a Kubernetes pod termination, and
	// `systemctl stop` all send before SIGKILL. os.Kill (SIGKILL) used to be
	// listed here too; the kernel handles it directly, so no process can ever
	// catch it, and registering for it was a no-op that read as coverage this
	// context never had.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// signal.NotifyContext keeps intercepting SIGINT/SIGTERM until cancel is
	// called — that is the whole point while ctx is still live, so the first
	// signal is the one that actually cancels it rather than being swallowed.
	// But a plain `defer cancel()` only runs once execute (and whatever it
	// blocked on) returns, and `flow worker` blocks exactly here: the first
	// signal cancels cmd.Context(), which sends w.Stop() into a drain that can
	// legitimately take up to --worker-stop-timeout. Every signal an operator
	// sends during that drain would otherwise be caught and silently
	// discarded by this same context — canceling it again is a no-op — leaving
	// SIGKILL as the only way to force an unresponsive drain down, which is
	// the exact hard-kill outcome this whole context exists to give an
	// alternative to. So cancel is called the moment the first signal lands,
	// not only at process exit: everything already reading ctx sees exactly
	// the same cancellation it would have anyway, and every signal after the
	// first reaches the process's default disposition (terminate) instead of
	// this handler.
	go func() {
		<-ctx.Done()
		cancel()
	}()

	// The help and every error report are drawn by this binary, in the same palette
	// as everything else it prints. A binary whose help is one colour and whose
	// output is another reads as two tools — and the reason it is drawn here rather
	// than by fang is a terminal query fang's options could not reach. See execute.go.
	err := execute(ctx, rootCmd)

	// Every command's last act, and the only one a client command gets: `flow
	// run`, `flow get`, `flow watch` and the rest live for a second or two,
	// which is shorter than a batch exporter's window, so without this their
	// spans are built, recorded, and thrown away at exit. Not deferred, because
	// the failing path below leaves through os.Exit and a deferred flush would
	// be the one skipped exactly when a trace is being read to find out why.
	//
	// Costs nothing when telemetry was never started, which is the default.
	flushTelemetry()

	if err != nil {
		os.Exit(exitCodeFor(err))
	}
}
