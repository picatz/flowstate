package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// `flow server dev` is the whole stack in one command, and it exists because the
// honest minimum before it was three terminals:
//
//	temporal server start-dev
//	flow server --insecure-no-auth
//	flow worker --allow-unversioned-interpreter
//
// Every one of those flags is a lesson somebody has not had yet, and a person
// evaluating this tool met all three of them before they had run anything. This
// assembles the same three processes into one, takes the same postures, and says
// every one of them out loud at start-up rather than leaving an operator to
// discover which flag they were spared.
//
// Three decisions carry the design.
//
// **Where Temporal comes from.** The dev server is the `temporal` CLI, execed as
// a child through [testsuite.StartDevServer]: the machinery this repository's
// own integration tests have used since they existed, version-pinned by the SDK,
// with sqlite, in-memory and the web UI already wired. Importing
// `github.com/temporalio/cli/temporalcli/devserver` instead would remove the
// first-run download at the cost of pulling `go.temporal.io/server` into this
// module: a very large dependency tree, every one of whose advisories becomes
// this repository's `govulncheck` problem, on a binary that is also meant to
// stay portable. The SDK path costs three packages (`testsuite`, `archive/tar`
// and `archive/zip`) and not one line of go.mod, because everything else it
// needs was already in this binary's graph. The download happens once, is cached
// by the SDK, and is stated in the help text.
//
// **What co-hosting means.** One process cannot hold two postures, so this holds
// the worker's, on purpose and in writing. `flow server` starts plugins with no
// secret providers ([runServer]) and installs no egress or task policy at all
// (see egress.go for why a policy on the server would change nothing it
// answers); `flow worker` installs both process-wide and sets [engine.UseCodec],
// which is process-global. Co-hosted, the worker's set wins, because it is the
// superset and because it is the half that actually executes steps. The server
// half is unaffected: it consults the task registry only for Validate and
// GetCatalog, so a policy registered here narrows what runs, never what the
// server reports.
//
// **What it refuses.** Invariant 6 is fail-closed, and a dev stack is a pile of
// postures that are defensible only because everything is on loopback and
// nothing outlives the terminal. So this refuses to start whenever one of those
// premises is not true: an off-loopback listen address, a trust policy the
// anonymous posture would silently ignore, or an operator's own Temporal cluster
// this command would quietly not use. See [devRefusals].

// devPostureAnonymous and devPostureUnversioned are the two sentences this
// command has to say because of the two flags it takes on the operator's behalf.
//
// They are the exact sentences `flow server --insecure-no-auth` and `flow worker
// --allow-unversioned-interpreter` log on every start (main.go). Verbatim rather
// than paraphrased, so that somebody who graduates from this command to the three
// real ones reads the same words and recognizes the posture as the one they
// already had. TestDevBannerSaysWhatTheReplacedFlagsSay pins them against
// main.go's source, because a paraphrase that drifted would leave two
// descriptions of one posture and no way to tell which is current.
const (
	devPostureAnonymous   = "authentication is disabled; every caller is anonymous and can start workflows"
	devPostureUnversioned = "starting worker unversioned; deploying this binary changes every run in flight"
)

// devTemporalNamespace is the namespace the dev server registers at start-up and
// the one everything here is pointed at. Stated rather than left to Temporal's
// environment configuration, because [devRefusals] has already established that
// no such configuration is in play.
const devTemporalNamespace = "default"

// devDefaultUIPort is where `temporal server start-dev` serves its web UI, and
// therefore where somebody who has met that command already looks for it.
const devDefaultUIPort = 8233

// devStartTimeout bounds bringing the Temporal child up, including the download
// on a first run. Generous, because the first run of this command on a fresh
// machine is fetching an archive over somebody else's network, and a person
// watching a quiet terminal for two minutes is a better outcome than one told it
// failed while it was working.
const devStartTimeout = 2 * time.Minute

// devShutdownTimeout bounds draining in-flight requests on the way out. Short,
// because Ctrl-C is a person asking for their terminal back.
const devShutdownTimeout = 10 * time.Second

// newServerDevCommand builds `flow server dev`.
func newServerDevCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Run the whole stack in one command: Temporal, the server, and a worker",
		Long: "Start everything a durable run needs, in one process: a Temporal dev server, the " +
			"Flowstate control plane, and a worker polling the run queue. Everything binds loopback " +
			"and everything is ephemeral unless --db names a file, so a session leaves nothing " +
			"behind. Ctrl-C stops all three, the Temporal child process included.\n\n" +
			"It takes two postures on your behalf and states both at start-up: callers are " +
			"anonymous (what `flow server --insecure-no-auth` does) and the interpreter is " +
			"unversioned (what `flow worker --allow-unversioned-interpreter` does). Both are " +
			"acceptable here only because nothing is reachable off this machine, which is why the " +
			"command refuses to start when that stops being true.\n\n" +
			"The Temporal dev server is the `temporal` CLI, downloaded on first use and cached " +
			"afterwards, so the first run needs network and later ones do not. Telemetry composes " +
			"rather than being contained: set OTEL_EXPORTER_OTLP_ENDPOINT and traces, metrics and " +
			"logs flow to it exactly as they do from `flow server` and `flow worker`, which is how " +
			"this points at examples/observability.",
		Args:          cobra.NoArgs,
		RunE:          runServerDev,
		SilenceErrors: true,
		// A refused posture is a finding about the environment this was started
		// in, not a command typed wrongly, and a usage block under it sends the
		// reader to check their flags instead of the variable they were just
		// told about.
		SilenceUsage: true,
		Example: `# The whole stack, ephemeral, on loopback:
flow server dev

# Keep the runs: Temporal persists to sqlite at this path.
flow server dev --db ./flowstate.db

# Somewhere else, and without the web UI:
flow server dev --listen localhost:9999 --ui-port 0

# Compose with the observability lab: export OTEL_EXPORTER_OTLP_ENDPOINT
# (examples/observability serves a collector at http://localhost:4317) and
# telemetry flows to it, as it does from flow server and flow worker.
flow server dev

# Resolved endpoints, for a script that starts the stack and then addresses it:
flow server dev -o json`,
	}

	// Deliberately not spelled --address. On `flow server` and `flow worker`
	// that flag means *Temporal's* address, while on every verb that talks to
	// Flowstate it means the Flowstate server's. Two meanings for one spelling,
	// split by which command declares it. This command holds both halves at
	// once, so it may not inherit the ambiguity: --listen is where Flowstate
	// listens, and there is no flag for Temporal's address because this command
	// is what starts Temporal.
	cmd.Flags().String("listen", cmp.Or(os.Getenv("FLOWSTATE_ADDRESS"), defaultServerAddress),
		"address the Flowstate server listens on (default $FLOWSTATE_ADDRESS); "+
			"loopback only, and a port of 0 takes a free one")

	cmd.Flags().String("db", "",
		"persist Temporal to a sqlite file at this path, so runs survive a restart; "+
			"unset keeps everything in memory and nothing outlives the process")

	cmd.Flags().Int("ui-port", devDefaultUIPort,
		"port for Temporal's web UI, where a run's history is readable; 0 serves no UI")

	// The worker's posture, taken on purpose: plugins, the egress policy, the
	// task-shape policy and the secret providers are all process-global, and
	// this process is the worker. See this file's package comment.
	addPluginFlags(cmd)
	addEgressPolicyFlag(cmd)
	addTaskPolicyFlag(cmd)
	addSecretFlags(cmd)
	cmd.Flags().String("auth-policy", os.Getenv("FLOWSTATE_AUTH_POLICY"),
		"path to an access policy whose secrets rules authorize worker-side resolution. Only its "+
			"secrets section is read: this command serves every caller anonymously, so the policy's "+
			"issuers go unused, and inheriting the path from $FLOWSTATE_AUTH_POLICY is refused rather "+
			"than silently ignoring the authentication a deployment configured")
	cmd.Flags().String("identity-key", os.Getenv("FLOWSTATE_IDENTITY_KEY"),
		"PKCS#8 PEM key used to mint short-lived workload assertions for federation targets")

	// The resolved endpoints as data, so a script or an agent can start the
	// stack and then address it without parsing prose.
	addOutputFlag(cmd)

	return cmd
}

// devFlags is what the command was asked for, read once before anything starts.
//
// The two "given" fields distinguish a value that arrived on this command line
// from one that arrived in the environment, which is the whole of what
// [devRefusals] decides about auth: the same path means different things
// depending on who put it there.
type devFlags struct {
	listen      string
	listenGiven bool
	db          string
	uiPort      int

	authPolicy      string
	authPolicyGiven bool
}

// devFlagsOf reads them off the command being run.
func devFlagsOf(cmd *cobra.Command) devFlags {
	listen, _ := cmd.Flags().GetString("listen")
	db, _ := cmd.Flags().GetString("db")
	uiPort, _ := cmd.Flags().GetInt("ui-port")
	authPolicy, _ := cmd.Flags().GetString("auth-policy")

	return devFlags{
		listen:          listen,
		listenGiven:     cmd.Flags().Changed("listen"),
		db:              db,
		uiPort:          uiPort,
		authPolicy:      authPolicy,
		authPolicyGiven: cmd.Flags().Changed("auth-policy"),
	}
}

// devEnv reads the environment the refusals are decided from.
//
// A parameter rather than os.Getenv reached for directly, so every refusal can
// be exercised from a table without a process-wide t.Setenv per case, and so
// that the set of variables this command's decision depends on is visible as
// arguments rather than buried in calls.
type devEnv func(string) string

// devRefusals reports why this stack must not start, or nil.
//
// Invariant 6 says a component denies by default and denies on error, and this
// command is a bundle of postures that hold only because of what is true around
// them: everything is on loopback, nobody authenticates, nothing is versioned,
// and the cluster is a child process that dies with the terminal. Each refusal
// below is one of those premises being contradicted by the environment the
// command was started in: the case where the postures would compose into
// something that is no longer a dev stack.
//
// One function, called before any of the start-up, rather than checks scattered
// through it: a refusal that arrives after a Temporal server has been
// downloaded, a plugin launched and a port bound has already done most of what
// it was refusing to do.
//
// Every message names the flag or variable that caused it and the command line
// that does what the operator evidently meant, because a fail-closed refusal
// with no way forward is the dead end invariant 8 forbids.
func devRefusals(flags devFlags, getenv devEnv) error {
	// Loopback, or the anonymous posture stops being defensible. Checked on the
	// resolved value rather than on the variable, so --listen and
	// FLOWSTATE_ADDRESS are covered by one rule, with the source named so the
	// operator knows which one to change.
	if err := devCheckLoopback(flags, getenv); err != nil {
		return err
	}

	// An operator whose environment configures a trust policy configured
	// authentication, and this command serves everyone anonymously. Honouring
	// the file is not on offer (a third auth mode would break the verifier
	// contract invariant 6 keeps binary), so the only honest answers are to
	// refuse or to ignore it, and ignoring a security configuration is the
	// fail-open this exists to prevent.
	if err := devCheckAuthPolicy(flags, getenv); err != nil {
		return err
	}

	// This command *is* the Temporal cluster. An address, a profile, or an
	// explicit configuration file pointing at somebody else's would go unused,
	// and the silence is the problem, because the operator who exported it
	// believes their runs are landing there. TEMPORAL_CONFIG_FILE is in the
	// list because the client configuration loader reads it exactly as it
	// reads the other two; leaving it out would be the same misrouting
	// through a different spelling.
	for _, name := range []string{"TEMPORAL_ADDRESS", "TEMPORAL_PROFILE", "TEMPORAL_CONFIG_FILE"} {
		value := getenv(name)
		if value == "" {
			continue
		}

		return fmt.Errorf(
			"refusing to start: %s=%s points this process at Temporal configuration of your own, and "+
				"`flow server dev` starts a cluster of its own: every run would land in the ephemeral "+
				"server this command creates rather than where you configured, and nothing would say "+
				"so. Unset %s to use this command's own server, or run the stack against yours with "+
				"`flow server --insecure-no-auth` and `flow worker --%s`",
			name, value, name, allowUnversionedFlag)
	}

	return nil
}

// devCheckLoopback refuses a listen address anything off this machine can reach.
//
// The whole justification for anonymous access is that the only callers are on
// this machine, so an address that is not loopback turns a development
// convenience into an unauthenticated workflow engine on a network. The empty
// host (":9233", or "0.0.0.0:9233") is every interface, which is the case that
// matters most and the one a permissive check would miss, so an address with no
// host is refused rather than read as a local default.
func devCheckLoopback(flags devFlags, getenv devEnv) error {
	host, _, err := net.SplitHostPort(flags.listen)
	if err != nil {
		return fmt.Errorf("--listen %q is not a host:port address: %w", flags.listen, err)
	}

	if devLoopbackHost(host) {
		return nil
	}

	source := "--listen " + flags.listen
	if !flags.listenGiven {
		source = "FLOWSTATE_ADDRESS=" + getenv("FLOWSTATE_ADDRESS")
	}

	return fmt.Errorf(
		"refusing to start: %s reaches past this machine, and `flow server dev` accepts every "+
			"caller anonymously, which is defensible only while every caller is local. "+
			"Bind loopback (localhost, 127.0.0.1, ::1), or serve a network with authentication "+
			"configured: `flow server --auth-policy <file>` beside `flow worker`",
		source)
}

// devLoopbackHost reports whether a listen host is this machine and nothing else.
//
// By name as well as by address, because "localhost" is what somebody types and
// what this command's own banner prints back.
func devLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}

	ip := net.ParseIP(host)

	return ip != nil && ip.IsLoopback()
}

// devCheckAuthPolicy refuses ambient authentication configuration this command
// would ignore.
//
// One file, two readers. `flow server --auth-policy` reads its `issuers:` and
// authenticates every caller against them; `flow worker --auth-policy` reads its
// `secrets:` and decides which workloads may resolve which secrets. A policy
// always carries issuers ([auth.ParsePolicy] refuses one that does not), so
// there is no such thing as a file this command could accept "because it only
// governs secrets". Both halves are always present, and this command runs the
// server half anonymously.
//
// So the distinction that decides it is who put the path there. FLOWSTATE_AUTH_POLICY
// is a deployment's ambient configuration, exported once for `flow server` and
// `flow worker` alike, and starting an anonymous server in an environment that
// says how callers are authenticated is exactly the silent fail-open invariant 6
// forbids. `--auth-policy` typed on *this* command line is somebody choosing,
// for this dev stack, to authorize worker-side secret resolution. The flag's own
// help says the issuers go unused, and the banner says it again at start-up.
func devCheckAuthPolicy(flags devFlags, getenv devEnv) error {
	if flags.authPolicyGiven {
		return nil
	}

	path := getenv("FLOWSTATE_AUTH_POLICY")
	if path == "" {
		return nil
	}

	return fmt.Errorf(
		"refusing to start: FLOWSTATE_AUTH_POLICY=%s configures how callers are authenticated, and "+
			"`flow server dev` authenticates nobody: it would accept callers that policy rejects, "+
			"and verify no token at all. Run the two commands the policy is for, "+
			"`flow server --auth-policy %s` and `flow worker --%s`, or pass --auth-policy %s on this "+
			"command line to use only its secrets rules, for this dev stack, with the issuers unused",
		path, path, allowUnversionedFlag, path)
}

// devStack is what came up, and what the banner and the JSON document describe.
type devStack struct {
	// flowstate is the address the control plane actually bound, which is not
	// necessarily the one asked for: --listen with a port of 0 takes a free one.
	flowstate string

	// temporal is the dev server's frontend, chosen by the SDK from the free
	// ports, and uiURL is empty when no UI was asked for.
	temporal string
	uiURL    string

	// database is the sqlite file Temporal was pointed at, empty for in-memory.
	database string

	// otlp is the endpoint telemetry was pointed at, empty when none was
	// configured. The one line that says whether this session composes with an
	// observability stack.
	otlp string

	// loopbackEgress records whether the http task may reach this machine,
	// which is an opt-in this command does not take on anybody's behalf.
	loopbackEgress bool

	// egressPolicy and taskPolicy name files the operator supplied, empty for
	// the built-in defaults, and authPolicy the access policy whose secrets
	// rules this stack's worker resolves under, whose issuers it does not use,
	// which is the part the banner has to say out loud.
	egressPolicy string
	taskPolicy   string
	authPolicy   string
}

// runServerDev starts Temporal, the server and a worker, and stops all three.
func runServerDev(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	flags := devFlagsOf(cmd)

	// Before anything is downloaded, launched or bound. See [devRefusals].
	if err := devRefusals(flags, os.Getenv); err != nil {
		return err
	}

	// The worker's posture, installed process-wide before anything can poll, so
	// a policy file that does not load refuses the command rather than
	// governing some steps and not others: the same ordering [runWorker] uses,
	// for the same reason.
	if err := applyEgressPolicy(cmd); err != nil {
		return err
	}
	if err := applyTaskPolicy(cmd); err != nil {
		return err
	}

	secretProviders, secretsConfigured, closeSecretProviders, err := secretRegistry(cmd)
	if err != nil {
		return err
	}
	defer closeSecretProviders()

	logger := infraLogger()
	surface := newSurface(cmd)

	// The connection this stack will use, resolved before the server it
	// connects to exists, because the client the dev server builds for itself
	// is the client this stack then uses, and it has to be built with this
	// deployment's payload codec and tracing interceptors. A second client
	// dialed afterwards would work equally well and would be a second answer to
	// "which converter does this process run": the codec decides whether a memo
	// this server writes is readable by the server that reads it back, and one
	// configuration cannot disagree with itself.
	cfg, err := temporalConfig(cmd.Context(), temporalFlags{namespace: devTemporalNamespace})
	if err != nil {
		return err
	}

	clientOpts, err := cfg.Options()
	if err != nil {
		return err
	}

	// Cleared on purpose. [testsuite.DevServerOptions] reads HostPort as *where
	// to bind*, and [temporalclient.Config.Options] fills it with the
	// conventional local address when nothing configured one, so leaving it
	// would make this command demand port 7233 and fail against the
	// `temporal server start-dev` somebody already had running. Empty means the
	// dev server takes a free port and reports it.
	clientOpts.HostPort = ""

	// And the credentials with it. [devRefusals] has already refused an explicit
	// TEMPORAL_ADDRESS or TEMPORAL_PROFILE, but a `temporal.toml` default profile
	// on this machine can still contribute TLS material and an API key meant for
	// the cluster it describes. The server on the far end of this connection is a
	// plaintext child process this command started thirty milliseconds ago, so
	// TLS would simply fail to handshake, and an API key would be handed to a
	// process that has no business holding one.
	clientOpts.ConnectionOptions.TLS = nil
	clientOpts.Credentials = nil

	// The SDK's default logger writes INFO to *stdout*, which is the stream
	// `-o json` promises holds one document and nothing else. Warnings and
	// errors only, on the account stream, where the rest of this command's
	// commentary already goes.
	clientOpts.Logger = log.NewStructuredLogger(slog.New(slog.NewTextHandler(
		surface.Err, &slog.HandlerOptions{Level: slog.LevelWarn})))

	// Bounds start-up only: the SDK uses this context to download the executable
	// if it is not cached and to wait for the server to answer. The process it
	// starts outlives the context, which is exactly why stopping it is a
	// deferred call rather than a cancellation.
	startCtx, cancelStart := context.WithTimeout(cmd.Context(), devStartTimeout)
	defer cancelStart()

	devServer, err := testsuite.StartDevServer(startCtx, testsuite.DevServerOptions{
		ClientOptions: &clientOpts,
		DBFilename:    flags.db,
		EnableUI:      flags.uiPort != 0,
		UIPort:        devUIPort(flags.uiPort),
		LogLevel:      "warn",

		// The child writes its own start-up banner (its version, its ports,
		// its metrics endpoint) and without these it inherits this process's
		// streams and puts that banner on stdout, beside the JSON document.
		// Onto the account stream, where a second opinion about the ports is
		// welcome and harmless.
		Stdout: surface.Err,
		Stderr: surface.Err,
	})
	if err != nil {
		return devStartError(err, flags)
	}

	// Every path out of this function passes here, the ones returning an error
	// included, because a `flow server dev` that failed to bind its port and
	// left a Temporal server running is precisely the leak this command exists
	// to make impossible.
	defer func() {
		if err := devServer.Stop(); err != nil {
			logger.Warn("the Temporal dev server did not stop cleanly", "error", err)
		}
	}()

	temporal := devServer.Client()
	defer temporal.Close()

	// Process-global, and the worker's. See this file's package comment.
	engine.UseCodec(cfg.Codec)

	// With the worker's argument rather than the server's nil: this process
	// executes steps, so a plugin task here needs the secret providers a worker
	// would have given it. The server half loses nothing by it: it reads the
	// same registry for Validate and GetCatalog either way.
	pluginCatalog, closePlugins, err := startPlugins(cmd, secretProviders)
	if err != nil {
		return err
	}
	defer closePlugins()

	// This process is both halves of the plugin contract, so the catalog is
	// installed on both. The worker's half admits a run against what this
	// process can actually execute, before it polls; the server's half is what
	// makes a `plugins:` requirement resolvable at submission at all. Either one
	// alone fails closed against the other: a dev stack with only the server's
	// pin admits a run its own worker then refuses, and one with only the
	// worker's refuses every submission as "not installed".
	engine.UsePluginCatalog(pluginCatalog)

	runtime, err := workerRuntime(cmd, secretProviders, secretsConfigured)
	if err != nil {
		return err
	}

	// Idempotent, once, before anything serves, and a warning rather than a
	// refusal, exactly as in [runServer]: a dev server with no operator setup
	// lists and filters correctly either way, and this only decides whether the
	// visibility store additionally carries the index.
	serverOpts := []server.Option{
		server.WithDataConverter(cfg.Codec.DataConverter()),
		server.WithPluginCatalog(pluginCatalog),
	}
	if err := server.EnsureSearchAttributesRegistered(cmd.Context(), temporal, devTemporalNamespace); err != nil {
		logger.Warn("could not register Flowstate's search attributes; "+
			"`flow list --filter` still works, scanning executions rather than querying an index",
			"error", err)
	} else {
		serverOpts = append(serverOpts, server.WithSearchAttributesRegistered())
	}

	w := worker.New(temporal, engine.RunTaskQueueName, worker.Options{
		Interceptors:             temporalWorkerInterceptors(),
		DeadlockDetectionTimeout: v1.WorkerDeadlockDetectionTimeout,
	})
	engine.Register(w, runtime)

	if err := w.Start(); err != nil {
		return fmt.Errorf("starting the worker: %w", err)
	}
	defer w.Stop()

	httpServer, listener, err := devHTTPServer(flags, serverOpts, temporal)
	if err != nil {
		return err
	}

	// Bound before the banner is written rather than inside the goroutine that
	// serves, so the address printed is one a client can already connect to. A
	// banner promising an endpoint that is not listening yet is a race the
	// two-command happy path would lose about as often as it won.
	stack := devStack{
		flowstate:      listener.Addr().String(),
		temporal:       devServer.FrontendHostPort(),
		uiURL:          devUIURL(flags.uiPort),
		database:       flags.db,
		otlp:           devOTLPEndpoint(),
		loopbackEgress: os.Getenv(v1.AllowLoopbackEgressEnv) == "true",
	}
	stack.egressPolicy, _ = cmd.Flags().GetString("egress-policy")
	stack.taskPolicy, _ = cmd.Flags().GetString("task-policy")
	stack.authPolicy = flags.authPolicy

	serveErr := make(chan error, 1)
	go func() {
		// Reported rather than terminating the process from a goroutine, so
		// shutdown still runs and the error reaches the caller.
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("serving on %s: %w", stack.flowstate, err)

			return
		}
		serveErr <- nil
	}()

	if format.Machine() {
		if err := writeDevStackJSON(surface, stack); err != nil {
			return err
		}
	}
	writeDevBanner(surface, stack)

	select {
	case err := <-serveErr:
		return err
	case <-cmd.Context().Done():
	}

	fmt.Fprintln(surface.Err)
	logger.Info("stopping the dev stack")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), devShutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("the server was forced down with requests still in flight", "error", err)
	}

	// The deferred calls above finish the rest, in the order that keeps each
	// half alive for as long as the half above it can still use it: the worker
	// stops, then the client it polls through closes, then the plugins it
	// dispatched to, then the Temporal child process. Telemetry is flushed here,
	// before any of that, so the last spans belong to a stack that was still
	// whole when they were recorded.
	flushTelemetry()

	return nil
}

// devUIPort renders the UI port the way [testsuite.DevServerOptions] wants it,
// with 0 meaning "no UI" rather than "pick one". The SDK's own encoding of "pick
// a free port" is the empty string, and a free port nobody can be told about is
// not a UI anybody can open: DevServer reports its frontend address and not its
// UI's.
func devUIPort(port int) string {
	if port == 0 {
		return ""
	}

	return strconv.Itoa(port)
}

// devUIURL is where the Temporal web UI can be opened, or empty when none runs.
func devUIURL(port int) string {
	if port == 0 {
		return ""
	}

	return "http://localhost:" + strconv.Itoa(port)
}

// devOTLPEndpoint is the collector telemetry was pointed at, for the banner.
//
// The general endpoint first and the signal-specific ones after, in the order
// [telemetryConfigured] reads them, so what the banner names is the variable
// that actually turned telemetry on.
//
// Written as four literal reads rather than a loop over a slice of names, which
// is what [telemetryConfigured] does and for the same reason: the env-var
// reference is kept honest by a test that reads every os.Getenv call site in the
// tree, and a name that arrives as a variable is a read it cannot resolve. A
// loop here would have to be exempted from that check, which is a hole in the
// shape of the thing the check defends.
func devOTLPEndpoint() string {
	return cmp.Or(
		os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
		os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"),
		os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"),
	)
}

// devStartError explains a Temporal dev server that would not come up.
//
// The two failures worth translating are the ones this command's own flags
// cause: a UI port somebody else holds, and a sqlite file that cannot be opened.
// Everything else is passed through with the context of what was being
// attempted, since the SDK's own message is usually the honest one.
func devStartError(err error, flags devFlags) error {
	switch {
	case flags.uiPort != 0 && strings.Contains(err.Error(), "address already in use"):
		return fmt.Errorf("starting the Temporal dev server: port %d is already in use, which is "+
			"usually another `temporal server start-dev` or another `flow server dev`; "+
			"pass --ui-port with a free port, or --ui-port 0 to serve no UI: %w", flags.uiPort, err)
	case flags.db != "":
		return fmt.Errorf("starting the Temporal dev server with --db %s: %w", flags.db, err)
	default:
		return fmt.Errorf("starting the Temporal dev server: %w", err)
	}
}

// devHTTPServer builds the Flowstate control plane this stack serves, and binds
// its listener.
//
// The listener is opened here rather than by ListenAndServe so that the address
// the banner prints is one already accepting connections, and so that a port of
// 0 resolves to a real port this command can report.
func devHTTPServer(flags devFlags, opts []server.Option, temporal client.Client) (*http.Server, net.Listener, error) {
	otelInterceptor, err := otelconnect.NewInterceptor()
	if err != nil {
		return nil, nil, fmt.Errorf("error creating OpenTelemetry interceptor: %w", err)
	}

	rpcMux := http.NewServeMux()
	rpcMux.Handle(
		flowstatev1connect.NewWorkflowServiceHandler(
			server.New(temporal, opts...),
			connect.WithInterceptors(validate.NewInterceptor(), otelInterceptor),
			// The same bound `flow server` sets: connect-go defaults to
			// unlimited, and an anonymous caller must not choose how much this
			// process allocates.
			connect.WithReadMaxBytes(maxRequestBytes),
		),
	)

	listener, err := net.Listen("tcp", flags.listen)
	if err != nil {
		return nil, nil, fmt.Errorf("listening on %s: %w", flags.listen, err)
	}

	// The anonymous verifier and no broker: this command's whole authentication
	// posture in one line, and the same one `flow server --insecure-no-auth`
	// installs.
	httpServer := &http.Server{
		Handler: serverHandler(infraLogger(), auth.InsecureAnonymousVerifier(), nil, nil, rpcMux, nil),

		// The same timeouts `flow server` sets, for the same reason: Go's zero
		// values mean no timeout at all, and a dev stack on loopback still has a
		// browser and an editor's language server talking to it.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       2 * time.Minute,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}

	return httpServer, listener, nil
}

// writeDevBanner says what is running, what it is not protecting, and what to
// type next.
//
// To the account stream, in the account stream's theme, because none of it is an
// answer a pipe reads. The answer, when one was asked for, is the JSON document
// written above it.
//
// The posture section is the reason this command is allowed to exist. Two flags
// were taken on the operator's behalf, so both are named as flags and both carry
// the sentence the flag itself would have caused to be logged (see
// [devPostureAnonymous]). The egress line is the third posture and reads the
// opposite way: this command takes no opt-in there, so the line reports what is
// denied and names the variable that would change it.
func writeDevBanner(surface *ui.UI, stack devStack) {
	out, theme := surface.Err, surface.ErrTheme

	fmt.Fprintf(out, "\n%s\n\n", theme.Accent.Render("flow server dev"))

	row := func(label, value string) {
		fmt.Fprintf(out, "  %-14s %s\n", label, theme.Strong.Render(value))
	}

	row("flowstate", "http://"+stack.flowstate)
	row("temporal", stack.temporal)
	if stack.uiURL != "" {
		row("temporal ui", stack.uiURL)
	}
	if stack.database == "" {
		row("storage", "in memory; nothing here survives this process")
	} else {
		row("storage", "sqlite at "+stack.database+"; runs survive a restart")
	}
	if stack.otlp != "" {
		row("telemetry", stack.otlp)
	}

	fmt.Fprintf(out, "\n%s\n", theme.Accent.Render("POSTURE"))

	posture := func(as, sentence string, style lipgloss.Style) {
		fmt.Fprintf(out, "  %s\n    %s\n", theme.Strong.Render(as), style.Render(sentence))
	}
	warn, muted := theme.Warning, theme.Muted

	posture("--insecure-no-auth", devPostureAnonymous, warn)
	posture("--"+allowUnversionedFlag, devPostureUnversioned, warn)

	if stack.loopbackEgress {
		posture(v1.AllowLoopbackEgressEnv+"=true",
			"the http task may reach this machine, including services this stack does not own", warn)
	} else {
		posture(v1.AllowLoopbackEgressEnv+" unset",
			"the http task refuses this machine, as it does everywhere else", muted)
	}
	if stack.egressPolicy != "" {
		posture("--egress-policy "+stack.egressPolicy,
			"this file is the whole egress policy, replacing the built-in one", muted)
	}
	if stack.taskPolicy != "" {
		posture("--task-policy "+stack.taskPolicy,
			"this file governs which task shapes may be dispatched", muted)
	}
	if stack.authPolicy != "" {
		// Said as a warning rather than a note, because the half that is *not*
		// in force is the surprising one: a file named on the command line
		// reads as configuration that took effect.
		posture("--auth-policy "+stack.authPolicy,
			"only its secrets rules are in force; its issuers are unused, because nothing here "+
				"authenticates a caller", warn)
	}

	fmt.Fprintf(out, "\n%s\n", theme.Accent.Render("NEXT"))
	fmt.Fprintf(out, "  %s\n", theme.Strong.Render("flow run <file>"))
	fmt.Fprintf(out, "  %s\n", theme.Strong.Render("flow list"))

	fmt.Fprintf(out, "\n%s\n\n", muted.Render(
		"Production differs in three ways: a trust policy instead of anonymous callers, "+
			"a Worker Deployment version instead of an unversioned interpreter, and a Temporal "+
			"cluster that outlives this terminal. Ctrl-C stops all of it."))
}

// devStackJSON is the resolved stack as data.
//
// Hand-written rather than a schema message, and deliberately: nothing here
// crosses the Flowstate wire or is stored by anything. It describes one
// process's own local endpoints to whoever started it, which is the same job
// `flow keys` and `flow jwt` do with their own hand-written documents.
type devStackJSON struct {
	FlowstateAddress  string `json:"flowstateAddress"`
	FlowstateURL      string `json:"flowstateUrl"`
	TemporalAddress   string `json:"temporalAddress"`
	TemporalNamespace string `json:"temporalNamespace"`
	TemporalUIURL     string `json:"temporalUiUrl,omitempty"`
	Database          string `json:"database,omitempty"`
	Persistence       string `json:"persistence"`
	OTLPEndpoint      string `json:"otlpEndpoint,omitempty"`
	AnonymousAuth     bool   `json:"anonymousAuth"`
	Unversioned       bool   `json:"unversionedInterpreter"`
	LoopbackEgress    bool   `json:"loopbackEgress"`
}

// writeDevStackJSON writes the resolved endpoints to the answer stream, on one
// line and before the stack begins serving, so a script can read a line and go.
//
// The three posture fields travel with the endpoints on purpose. A caller
// starting this programmatically is exactly the caller who will never read the
// banner, and "this endpoint accepts anonymous callers" is not a detail such a
// caller should have to infer from the command's name.
func writeDevStackJSON(surface *ui.UI, stack devStack) error {
	persistence := "memory"
	if stack.database != "" {
		persistence = "sqlite"
	}

	encoded, err := json.Marshal(devStackJSON{
		FlowstateAddress:  stack.flowstate,
		FlowstateURL:      "http://" + stack.flowstate,
		TemporalAddress:   stack.temporal,
		TemporalNamespace: devTemporalNamespace,
		TemporalUIURL:     stack.uiURL,
		Database:          stack.database,
		Persistence:       persistence,
		OTLPEndpoint:      stack.otlp,
		AnonymousAuth:     true,
		Unversioned:       true,
		LoopbackEgress:    stack.loopbackEgress,
	})
	if err != nil {
		return fmt.Errorf("rendering the resolved endpoints: %w", err)
	}

	fmt.Fprintf(surface.Out, "%s\n", encoded)

	return nil
}
