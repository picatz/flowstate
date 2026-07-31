package main

import (
	"cmp"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile/lsp"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
	"github.com/picatz/flowstate/pkg/flowstate/v1/temporalclient"
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
	// "address" rather than "temporal-address": on `worker` and `server` that flag
	// means Temporal's address, while on the verbs that talk to Flowstate it means
	// the Flowstate server's. Two meanings for one spelling, split by which command
	// declares it — pre-existing, and renaming either would break a command line
	// somebody has written down.
	address, _ := cmd.Flags().GetString("address")
	namespace, _ := cmd.Flags().GetString("namespace")
	profile, _ := cmd.Flags().GetString("profile")
	taskQueue, _ := cmd.Flags().GetString("task-queue")
	deploymentName, _ := cmd.Flags().GetString("deployment-name")
	buildID, _ := cmd.Flags().GetString("build-id")
	verbose, _ := cmd.Flags().GetBool("verbose")

	return temporalFlags{
		address:        address,
		namespace:      namespace,
		profile:        profile,
		taskQueue:      taskQueue,
		deploymentName: deploymentName,
		buildID:        buildID,
		verbose:        verbose,
	}
}

// authFlags is how `flow server` decides who it will accept.
//
// There is no default that accepts callers: either a trust policy is configured, or
// anonymous access is requested explicitly.
type authFlags struct {
	policyPath string
	insecure   bool

	// identityKeyPath holds the private key Flowstate signs its own assertions
	// with, when the trust policy configures federation. Unset means the server
	// verifies callers but issues nothing, which is the inbound-only deployment.
	identityKeyPath string
}

// authFlagsOf reads them off the command being run.
func authFlagsOf(cmd *cobra.Command) authFlags {
	policyPath, _ := cmd.Flags().GetString("auth-policy")
	insecure, _ := cmd.Flags().GetBool("insecure-no-auth")
	identityKeyPath, _ := cmd.Flags().GetString("identity-key")

	return authFlags{policyPath: policyPath, insecure: insecure, identityKeyPath: identityKeyPath}
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
	cfg := temporalclient.Config{
		Address:   flags.address,
		Namespace: flags.namespace,
		Profile:   flags.profile,
	}

	if flags.verbose {
		if opts, err := cfg.Options(); err == nil {
			log.Printf("Temporal: %s", temporalclient.Describe(opts))
		}
	}

	return temporalclient.Dial(ctx, cfg)
}

// runWorker implements the worker sub-command to start a Temporal worker
// to process Flowstate workflows and activities.
func runWorker(cmd *cobra.Command, args []string) error {
	flags := temporalFlagsOf(cmd)

	c, err := initTemporalClient(cmd.Context(), flags)
	if err != nil {
		return err
	}
	defer c.Close()

	// Before the worker starts polling, because a worker that accepted a step for
	// a plugin task it has not registered yet would answer `unknown task` for a
	// workflow that is correct — and Open is strict, so a plugin that cannot come
	// up fails the command here rather than one step at a time later.
	closePlugins, err := startPlugins(cmd)
	if err != nil {
		return err
	}
	defer closePlugins()

	deployment := engine.DeploymentOptions(flags.deploymentName, flags.buildID)

	w := worker.New(c, flags.taskQueue, worker.Options{
		DeploymentOptions: deployment,
	})

	engine.Register(w)

	if deployment.UseVersioning {
		log.Printf("Starting worker on task queue %s as %s/%s",
			flags.taskQueue, deployment.Version.DeploymentName, deployment.Version.BuildID)
	} else {
		// Said out loud rather than left to be inferred from silence. Without a
		// version, deploying this binary changes the behaviour of every run
		// already in flight — which is the default Temporal has always had, and
		// is a choice an operator should know they are making.
		log.Printf("Starting worker on task queue %s, unversioned "+
			"(set FLOWSTATE_DEPLOYMENT_NAME and FLOWSTATE_BUILD_ID, or --deployment-name and --build-id, "+
			"to pin in-flight runs to the interpreter they started on)", flags.taskQueue)
	}

	// Start worker (non-blocking) such that it can run in the background
	// while we wait for shutdown signals.
	err = w.Start()
	if err != nil {
		return fmt.Errorf("unable to start worker: %w", err)
	}

	// Listen for shutdown signals to gracefully stop the worker.
	<-cmd.Context().Done()
	log.Println("Shutting down worker...")
	w.Stop()
	log.Println("Worker stopped")

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
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	workflow, err := loadWorkflow(args[0])
	if err != nil {
		return err
	}

	surface := newSurface(cmd)

	started, err := newWorkflowServiceClient(serverFlagsOf(cmd)).Run(cmd.Context(),
		connect.NewRequest(&v1.RunRequest{Workflow: workflow}))
	if err != nil {
		return fmt.Errorf("starting %s: %w", workflow.GetName(), err)
	}

	workflowID := started.Msg.GetWorkflowId()

	// Said before the following begins, because it is the one fact somebody needs in
	// order to come back to this run later, and following is where they might stop
	// paying attention. Only to a person: the machine formats carry the id in every
	// document they emit, so saying it again in prose would be something a reader has
	// to parse past.
	if format == FormatText {
		fmt.Fprintf(surface.Err, "started %s; come back to it with `flow watch %s`\n",
			workflowID, workflowID)
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

	return watchRun(cmd.Context(), surface, format,
		clientPoller{workflowID: workflowID, server: serverFlagsOf(cmd)},
		clampWatchInterval(interval), plain, workflowID, startedRun(started.Msg))
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
	// Resolve configuration before doing any I/O, so a misconfiguration is
	// reported immediately rather than after waiting on a connection attempt.
	authCfg := authFlagsOf(cmd)

	verifier, policy, err := authVerifier(authCfg)
	if err != nil {
		return err
	}

	broker, err := identityBroker(authCfg, policy)
	if err != nil {
		return err
	}

	c, err := initTemporalClient(cmd.Context(), temporalFlagsOf(cmd))
	if err != nil {
		return err
	}
	defer c.Close()

	// The server answers Validate and GetCatalog from the process-wide registry,
	// so a deployment whose workers load plugins points the server at the same
	// directory — otherwise the capability it reports is the built-ins alone, and
	// a caller authoring against GetCatalog would be told a task its workers run
	// does not exist. The plugins launched here serve descriptors and health
	// checks; execution still happens on the workers.
	closePlugins, err := startPlugins(cmd)
	if err != nil {
		return err
	}
	defer closePlugins()

	interceptor, err := validate.NewInterceptor()
	if err != nil {
		return fmt.Errorf("error creating validation interceptor: %w", err)
	}

	otelInterceptor, err := otelconnect.NewInterceptor()
	if err != nil {
		return fmt.Errorf("error creating OpenTelemetry interceptor: %w", err)
	}

	rpcMux := http.NewServeMux()
	rpcMux.Handle(
		flowstatev1connect.NewWorkflowServiceHandler(
			server.New(c),
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
		// Where this server *listens*, which was the same package variable a client
		// used to decide where to *connect*. One default served both, so nothing
		// broke — but they are different facts, and `flow server` never declared an
		// --address flag, so the variable it shared could only ever hold the
		// environment's value anyway. Read directly, which is what it meant.
		Addr:    cmp.Or(os.Getenv("FLOWSTATE_ADDRESS"), defaultServerAddress),
		Handler: serverHandler(verifier, broker, rpcMux),

		// Without these a client that opens a connection and sends bytes
		// slowly, or never, occupies a connection indefinitely. Go's zero
		// values mean no timeout at all, so they must be set explicitly.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       2 * time.Minute,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}

	log.Printf("Starting Flowstate server on %s", httpServer.Addr)
	if authCfg.insecure {
		log.Printf("WARNING: authentication is disabled; every caller is anonymous " +
			"and can start workflows. Do not use this outside local development.")
	}
	if broker != nil {
		// Log the discovery URL rather than the fact of federation: an operator
		// configuring a relying party needs this exact string, and finding it by
		// reading source is the sort of friction that gets solved by guessing.
		log.Printf("Issuing workload identity assertions; discovery at %s%s",
			broker.Issuer().URL(), auth.DiscoveryPath)
	}

	serveErr := make(chan error, 1)
	go func() {
		// Report a listen failure instead of terminating the process from a
		// goroutine, so shutdown still runs and the error reaches the caller.
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("listening on %s: %w", httpServer.Addr, err)
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		return err
	case <-cmd.Context().Done():
	}

	log.Println("Shutting down server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server forced to shutdown: %w", err)
	}
	log.Println("Server stopped")

	return nil
}

// maxRequestBytes bounds a single RPC request body.
//
// A workflow specification is text and a large one is still small; this leaves
// generous room while keeping an unauthenticated caller from choosing how much
// memory the server allocates.
const maxRequestBytes = 4 << 20 // 4 MiB

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
// rotation self-documenting: a verifier that has cached "2026-07" can be handed
// "2026-08" without a coordinated restart.
func identityBroker(flags authFlags, policy *auth.Policy) (*auth.Broker, error) {
	if policy == nil || policy.Federation == nil {
		if flags.identityKeyPath != "" {
			return nil, fmt.Errorf("--identity-key was given but the trust policy configures no federation: " +
				"add a federation section, or drop the key")
		}
		return nil, nil
	}

	if flags.identityKeyPath == "" {
		return nil, fmt.Errorf("the trust policy configures federation but no signing key was given: " +
			"pass --identity-key with a PKCS#8 PEM private key, since Flowstate cannot issue an " +
			"assertion it cannot sign")
	}

	pem, err := os.ReadFile(flags.identityKeyPath)
	if err != nil {
		return nil, fmt.Errorf("reading identity key: %w", err)
	}
	key, err := parseSigningKey(flags.identityKeyPath, pem)
	if err != nil {
		return nil, err
	}

	broker, err := policy.Federation.Broker(key)
	if err != nil {
		return nil, fmt.Errorf("configuring identity federation: %w", err)
	}
	return broker, nil
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

	conn := jsonrpc2.NewConn(
		cmd.Context(),
		jsonrpc2.NewBufferedStream(stdio{}, jsonrpc2.VSCodeObjectCodec{}),
		jsonrpc2.AsyncHandler(&lsp.FlowfileServer{}),
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
func runValidate(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}
	if format.Machine() {
		return validateMachine(cmd, args, format)
	}

	// Through the surface, and with its theme, for the reason renderHelp and
	// renderError no longer take a writer at all: a styled byte written past the
	// surface skips the layer that degrades the palette to what the stream carries.
	surface := newSurface(cmd)
	out, theme := surface.Out, surface.Theme

	var failed bool

	for _, path := range args {
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("error reading workflow file: %w", err)
		}

		diagnostics, err := flowfile.ValidateSource(data)
		if err != nil {
			// A parse failure already carries its own line and column.
			failed = true
			fmt.Fprintf(out, "%s: %v\n", theme.Muted.Render(path), err)
			continue
		}
		if len(diagnostics) == 0 {
			// The one word worth finding in a run over nineteen files: everything
			// else on the line is the path, and a reader scanning for the failure
			// among them is scanning for this.
			fmt.Fprintf(out, "%s: %s\n", theme.Muted.Render(path), theme.Success.Render("ok"))
			continue
		}

		failed = true
		for _, d := range diagnostics {
			fmt.Fprintf(out, "%s:%s\n", theme.Muted.Render(path), d.Error())
		}
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
func validateMachine(cmd *cobra.Command, args []string, format OutputFormat) error {
	surface := newSurface(cmd)

	reports := make([]*v1.DiagnosticReport, 0, len(args))
	for _, path := range args {
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("error reading workflow file: %w", err)
		}

		diagnostics, err := flowfile.ValidateSource(data)
		if err != nil {
			var parsed flowfile.Diagnostics
			if !errors.As(err, &parsed) {
				// Not a shape this can position — a document that is not YAML at
				// all. It is still the file's problem rather than the caller's, so
				// it is reported as an unpositioned diagnostic rather than dropped.
				parsed = flowfile.Diagnostics{{Message: err.Error()}}
			}
			diagnostics = parsed
		}
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
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading workflow file: %w", err)
	}

	workflow, err := flowfile.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	diagnostics, err := flowfile.ValidateSource(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(diagnostics) > 0 {
		lines := make([]string, 0, len(diagnostics))
		for _, d := range diagnostics {
			lines = append(lines, fmt.Sprintf("%s:%s", path, d.Error()))
		}
		return nil, errors.New(strings.Join(lines, "\n"))
	}

	return workflow, nil
}

// runTasks implements the tasks sub-command, listing the tasks a workflow may
// use along with the expression libraries available to them.
//
// The listing is derived from the task registry rather than maintained by hand,
// so it cannot drift from what the engine will actually execute.
// writeFields prints one task's inputs or outputs, aligned under a label.
//
// A block per task rather than a row per task, which is what this was. `http` has
// eleven inputs; on one line they run past any terminal and take the table's
// alignment with them, so the shape that fits four tasks today would be unusable
// the moment somebody registers a fifth with a real schema. A block is the same
// width whatever the task holds.
//
// The writer is passed in so that a task's inputs and outputs share one, and
// therefore share a column layout. Two tabwriters would align each block against
// itself and against nothing else, which reads as a mistake even when every
// number in it is right.
//
// Required inputs are marked with `*` rather than only sorted first, because a
// mark survives being piped, logged, and read by somebody who cannot see colour.
// Errors are returned rather than discarded, which the tabwriter this replaced did
// by way of Flush. A full disk or a pipe that has gone away makes every write fail,
// and a listing that stopped halfway while reporting success is worse than one that
// says it could not finish.
func writeFields(w io.Writer, theme ui.Theme, groups []fieldGroup) error {
	// Laid out here rather than by tabwriter, which measures the bytes it is given.
	// A styled cell is mostly escape sequences, so tabwriter counted them as width
	// and every column after the first shifted — the terminal rendering came out
	// visibly ragged while the piped one, being unstyled, looked fine. Widths are
	// measured with lipgloss, which counts displayed columns, the same way the help
	// page's own two-column lists are laid out.
	const gutter = 2

	var labels, names int
	for _, group := range groups {
		if len(group.fields) > 0 {
			labels = max(labels, lipgloss.Width(group.label))
		}
		for _, field := range group.fields {
			names = max(names, lipgloss.Width(fieldName(field)))
		}
	}

	for _, group := range groups {
		for i, field := range group.fields {
			// The label sits beside the first row and the rest align under it, so
			// the eye reads down a column rather than hunting for where one list
			// ends.
			// Only the first row of a group carries the label, and an empty one is
			// left unstyled: rendering "" through a style emits escape sequences
			// around nothing, which a terminal ignores and a reader of the raw
			// bytes has to skip past.
			label, styledLabel := "", ""
			if i == 0 {
				label = group.label
				styledLabel = theme.Header.Render(label)
			}

			name := fieldName(field)

			if _, err := fmt.Fprintf(w, "  %s%s%s%s%s",
				styledLabel, pad(labels-lipgloss.Width(label)+gutter),
				theme.Strong.Render(name), pad(names-lipgloss.Width(name)+gutter),
				theme.Muted.Render(field.Type)); err != nil {
				return err
			}

			if field.Deferred {
				// Worth saying, because it changes what an author may write here.
				// The engine resolves an expression before scheduling the step;
				// these the task evaluates itself, against a scope the workflow
				// does not have — which is why `http`'s `outputs` can name
				// `status_code` and an ordinary input cannot.
				if _, err := fmt.Fprintf(w, "  %s",
					theme.Muted.Render("the task evaluates this itself, in its own scope")); err != nil {
					return err
				}
			}

			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
	}

	return nil
}

// fieldGroup is one labelled list of a task's fields.
type fieldGroup struct {
	label  string
	fields []v1.InputField
}

// fieldName is how a field is written, with the marker a required one carries.
func fieldName(field v1.InputField) string {
	if field.Required {
		return field.Name + "*"
	}

	return field.Name
}

// pad returns n spaces, never fewer than none.
func pad(n int) string {
	if n < 0 {
		return ""
	}

	return strings.Repeat(" ", n)
}

func runTasks(cmd *cobra.Command, args []string) error {
	surface := newSurface(cmd)

	// Through the surface rather than cmd.OutOrStdout(), which matters now that
	// this is styled. A theme resolves to the palette's own colours, and it is
	// [ui.New]'s colorprofile writer that degrades those to what the stream can
	// carry — 24-bit down to 256, to 16, to none. Writing styled text past it sent
	// truecolor sequences to a terminal that had told us it has 256 colours.
	out := surface.Out

	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	// The registry, as a document, for everything that is not a person: an agent
	// driving this CLI as a tool, a generator producing documentation, an editor
	// that is not this project's language server. All of them previously had to
	// parse the columns below, and column positions are not a contract.
	//
	// One document rather than one per line even for jsonl, because a catalog is
	// one answer — a consumer wants `.tasks[] | select(.name=="http")`, not a
	// stream it has to reassemble before it can index into it.
	if format.Machine() {
		return writeJSON(surface, FormatJSON, v1.Catalog())
	}

	// What a task takes, not just that it exists.
	//
	// This listed a name and a one-line summary, which tells a reader that `http`
	// exists and nothing about how to write one — so the next stop was the
	// README's hand-maintained table, which is exactly the drift the registry
	// exists to prevent. The schema knows; printing it here means the answer
	// cannot go stale.
	theme := surface.Theme

	for i, def := range v1.DefaultRegistry().All() {
		if i > 0 {
			fmt.Fprintln(out)
		}
		fmt.Fprintf(out, "%s\n  %s\n", theme.Accent.Render(def.Name), def.Summary)

		if err := writeFields(out, theme, []fieldGroup{
			{label: "inputs", fields: v1.Inputs(def)},
			{label: "outputs", fields: v1.Outputs(def)},
		}); err != nil {
			return fmt.Errorf("writing the task catalog: %w", err)
		}
	}

	fmt.Fprintf(out, "\n%s\n", theme.Muted.Render("* marks an input the task cannot run without."))

	// "every expression", not "the cel task", and that is the change worth
	// spelling out here. These used to be opt-in per `cel` step, which meant this
	// listing was accurate for one step kind and misleading for the rest of the
	// file — an author reading it to find out what an `if:` could say got the
	// wrong answer.
	// Named, rather than only counted. This printed the library names and stopped,
	// which says what is switched on and nothing about what any of it offers — so
	// somebody who wanted to sort a list had no way to find out that `sortBy`
	// exists. A profile is a *membership*, and one nobody can enumerate is one
	// nobody can write against.
	//
	// A macro is marked because the difference reaches an author: it is expanded
	// when the file compiles, so it is frozen into the compiled workflow, where a
	// function is looked up by whichever worker evaluates the run.
	if functions := v1.ProfileFunctions(v1.CurrentProfile); len(functions) > 0 {
		fmt.Fprintf(out, "\n%s\n",
			theme.Accent.Render("CEL functions available to every expression:"))

		width := 0
		for _, fn := range functions {
			width = max(width, len(fn.Library))
		}

		for _, lib := range v1.ExtensionLibraries() {
			names := make([]string, 0, len(functions))
			for _, fn := range functions {
				if fn.Library != lib {
					continue
				}
				if fn.Macro {
					names = append(names, fn.Name+" (macro)")

					continue
				}
				names = append(names, fn.Name)
			}
			if len(names) == 0 {
				continue
			}

			fmt.Fprintf(out, "  %s  %s\n",
				theme.Muted.Render(fmt.Sprintf("%-*s", width, lib)),
				strings.Join(names, ", "))
		}

		// Said once rather than inlined per entry. A macro's name is not its call
		// form — cel-go reports `greatest` for `math.greatest(1, 2)` — so a reader
		// needs an example, and a list of ninety names with two long expressions
		// spliced into it is harder to scan than a list of names plus one line.
		//
		// The examples come from the catalog rather than being written here, so this
		// line cannot describe a spelling the schema does not carry. Two of them,
		// because the two shapes are the whole point: one goes on a namespace and
		// one on a value, and showing only either would imply macros are all alike.
		if written := macroExamplesFor(functions, "greatest", "sortBy"); written != "" {
			fmt.Fprintf(out, "  %s\n",
				theme.Muted.Render("a macro goes on something — "+written+
					" — and is expanded when the file compiles"))
		}
	}

	fmt.Fprintf(out, "\n%s\n  %s\n",
		theme.Accent.Render("Duration constructors available to every expression:"),
		strings.Join(v1.DurationUnits(), ", "))
	fmt.Fprintf(out, "\nInside wait_until, %s is the moment the wait is evaluated,\n"+
		"so a deadline can be written as ${%s + days(3)}.\n", v1.NowIdentifier, v1.NowIdentifier)

	// Where a value comes from, which this listing otherwise leaves somebody to
	// guess at.
	//
	// A task is an *effect*. Two are listed above and one of them produces nothing,
	// so a reader who arrives here asking "how do I compute something" would
	// reasonably conclude the answer is almost nothing. It is an expression, named
	// under `vars:`, and saying so is the difference between a task list and an
	// answer to the question people run this command to ask.
	fmt.Fprintf(out, "\n%s\n"+
		"  at the top of a file, read everywhere as ${%s.<name>}\n"+
		"  on a step, read inside it as a bare ${<name>}\n"+
		"A step's outputs are what it learned from outside, read as ${%s.<id>.<output>}.\n",
		theme.Accent.Render(fmt.Sprintf(
			"Values come from expressions rather than from tasks. Name one with %s:", v1.VarsRoot)),
		v1.VarsRoot, v1.StepsRoot)

	return nil
}

// macroExamplesFor renders the catalog's example calls for the named macros.
//
// Read out of the catalog rather than written into the sentence, so the one line
// explaining how a macro is written cannot name a spelling the schema does not
// carry — the two would drift the first time an example changed, and this line is
// the only place a reader of the terminal listing learns the call form at all.
//
// Silently skips a name with no example, and returns empty if none of them have
// one, which the caller treats as "say nothing". A sentence promising examples and
// then listing none is worse than its absence.
func macroExamplesFor(functions []v1.LibraryFunction, names ...string) string {
	var out []string
	for _, name := range names {
		for _, fn := range functions {
			if fn.Name == name && fn.Example != "" {
				out = append(out, fn.Example)

				break
			}
		}
	}

	return strings.Join(out, ", ")
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
			"failures, and long waits. You write one as a Flowfile — YAML with CEL expressions — " +
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

	// Run command, which executes a workflow using the Flowstate service.
	runCmd := &cobra.Command{
		Use:   "run [workflow-file]",
		Short: "Run a workflow and follow it",
		Long: "Start a workload on a Flowstate server and follow the run until it finishes.\n\n" +
			"Following works exactly as `flow watch` does, because it is the same code: a " +
			"live view where there is a terminal, one line per change where there is not, " +
			"and the outputs on stdout when the run produced them. The exit code is the " +
			"run's, so `flow run x && ./promote.sh` behaves the way a shell reader expects.\n\n" +
			"Stopping watching does not stop the run. The workflow id is printed as soon as " +
			"the run starts, so `flow watch` can pick it up again afterwards.",
		Args: cobra.ExactArgs(1),
		RunE: runWorkflow,
		Example: `# Run a workflow and watch it:
flow run examples/hello-world/workflow.yaml

# Run it and pipe the outputs, with the live view still on the terminal:
flow run examples/hello-world/workflow.yaml | jq .stepValues

# In CI: one line per change, exit code reports the outcome.
flow run examples/hello-world/workflow.yaml >/dev/null

# Check a workflow without running it:
flow validate examples/hello-world/workflow.yaml`,
	}

	addOutputFlag(runCmd)
	addFollowFlags(runCmd)

	// Run local command, which executes a workflow locally without using Temporal or the Flowstate service.
	runLocalCmd := &cobra.Command{
		Use:   "local [workflow-file]",
		Short: "Run a workflow locally without Temporal",
		Long: "Execute a workload in this process, without Temporal and without a Flowstate " +
			"server.\n\nThis is the rehearsal, and it is worth rehearsing with because the two " +
			"drivers are one execution model: conditions, retries, timeouts, loops and waits " +
			"behave here the way they behave in production, and the answer comes back in the " +
			"same document `flow run` writes — so a `jq` expression written against one works " +
			"against the other.\n\nWhat it cannot give you is durability. A local run is a " +
			"process: it has no run id, nothing can watch it, and it does not survive this " +
			"command being interrupted.",
		Args: cobra.MinimumNArgs(1),
		RunE: runLocalWorkflow,
		Example: `# Run a workflow locally:
flow run local examples/hello-world/workflow.yaml

# Run a multi-step workflow:
flow run local examples/hello-world-multi-step/workflow.yaml

# Take one step's output, the same way you would from a durable run:
flow run local examples/hello-world/workflow.yaml | jq .stepValues.hello.namedValues

# Ask for the whole run as one document, including how it went:
flow run local examples/hello-world/workflow.yaml -o json | jq -r .status

# Run a workflow with an approval gate, answering the gate up front:
flow run local examples/approval-gate/workflow.yaml --signal deploy-approved='{"approved": true}'`,
	}

	addOutputFlag(runLocalCmd)

	// Supplying signals up front is what makes an approval gate something an author
	// can exercise on their laptop rather than first meeting in production. A local
	// run is a process, so there is nobody to signal it after it starts; the local
	// waiter buffers what is given here, so a gate reached later still finds its
	// answer waiting — the same behavior the durable driver has because Temporal
	// buffers signals for a run.
	runLocalCmd.Flags().StringArray("signal", nil,
		`answer a wait_for_signal step, as name=json (repeatable), e.g. --signal deploy-approved='{"approved": true}'`)

	// Worker command, which starts a Temporal worker to process workflows and activities.
	workerCmd := &cobra.Command{
		Use:   "worker",
		Short: "Start a worker",
		Long:  "Start a Temporal worker to process workflows and activities. The worker connects to the Temporal server and processes tasks from the specified task queue.",
		RunE:  runWorker,
		Example: `# Start a worker with default settings:
flow worker

# Start a worker with custom Temporal server:
flow worker --address localhost:7233

# Start a worker with custom namespace:
flow worker --namespace production

# Start a versioned worker, so a deploy does not change runs already in flight:
flow worker --deployment-name flowstate --build-id "$(git rev-parse --short HEAD)"`,
	}

	// These override Temporal's environment configuration when set; unset means
	// Server command, which starts a Flowstate server to handle workflow requests.
	serverCmd := &cobra.Command{
		Use:   "server",
		Short: "Start a server",
		Long:  "Start a Flowstate API server to handle workflow requests. The server provides HTTP/gRPC endpoints for managing workflows and integrates with Temporal for execution.",
		RunE:  runServer,
		Example: `# Start the server with default settings:
flow server

# Start the server with verbose logging:
flow server --verbose`,
	}

	// use whatever TEMPORAL_* variables or the temporal.toml profile resolve to.
	for _, c := range []*cobra.Command{workerCmd, serverCmd} {
		c.Flags().String("address", "", "Temporal server address (overrides environment configuration)")
		c.Flags().String("namespace", "", "Temporal namespace (overrides environment configuration)")
		c.Flags().String("profile", "", "Temporal configuration profile to use")
	}
	workerCmd.Flags().String("task-queue", cmp.Or(os.Getenv("TEMPORAL_TASK_QUEUE"), engine.RunTaskQueueName),
		"task queue for Temporal workflows and activities")

	workerCmd.Flags().String("deployment-name", os.Getenv("FLOWSTATE_DEPLOYMENT_NAME"),
		"Worker Deployment this worker belongs to. With --build-id, pins every in-flight run to the "+
			"interpreter version it started on; a run moves to the current version only at continue-as-new")
	workerCmd.Flags().String("build-id", os.Getenv("FLOWSTATE_BUILD_ID"),
		"version identifier for this worker's binary, unique per build. Required with --deployment-name")

	addPluginFlags(workerCmd)
	addPluginFlags(serverCmd)

	serverCmd.Flags().String("auth-policy", "",
		"path to an OIDC/workload-identity trust policy (YAML) describing which issuers to accept")
	serverCmd.Flags().Bool("insecure-no-auth", false,
		"allow unauthenticated access; for local development only")
	serverCmd.Flags().String("identity-key",
		os.Getenv("FLOWSTATE_IDENTITY_KEY"),
		"path to a PKCS#8 PEM private key Flowstate signs its own assertions with, "+
			"required when the trust policy configures federation; the file's base name "+
			"becomes the published key id, so 2026-07.pem publishes as \"2026-07\"")

	// Validate command, which checks Flowfiles without executing them.
	validateCmd := &cobra.Command{
		Use:   "validate [workflow-file...]",
		Short: "Check workflows for problems without running them",
		Long: "Check one or more Flowfiles for problems without executing them. " +
			"Reports unknown tasks, duplicate or unusable step ids, and references to " +
			"steps that do not exist or have not run yet, with the line each problem is on.",
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
flow validate examples/*/workflow.yaml -o jsonl | jq 'select(.diagnostics | length > 0)'`,
	}

	// Diagnostics are a schema message, so `-o json` means here what it means on
	// `get` and `list`: the fields are the schema's and addressable by name.
	addOutputFlag(validateCmd)

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
			"that failed is reported as a failure, so `flow get id && ...` behaves as expected.",
		Args: cobra.ExactArgs(1),
		RunE: runGet,
		Example: `# Ask what a run is doing:
flow get flowstate-workflow-3f7c

# Keep only the outputs:
flow get flowstate-workflow-3f7c | jq .stepValues

# Ask about one attempt rather than the current one:
flow get flowstate-workflow-3f7c --run-id 0198f1e2-...`,
	}

	addOutputFlag(getCmd)

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
			"later steps read as ${step_id.key}.",
		Args: cobra.ExactArgs(2),
		RunE: runSignal,
		Example: `# Approve a deploy waiting on a gate:
flow signal deploy-abc123 deploy-approved --data '{"approved": true, "by": "someone@example.com"}'

# Decline it; the workload can tell this apart from nobody answering:
flow signal deploy-abc123 deploy-approved --data '{"approved": false}'

# Send a signal that carries nothing:
flow signal deploy-abc123 deploy-approved

# Answer the same gate on a local run, which is given its answers up front:
flow run local examples/approval-gate/workflow.yaml --signal deploy-approved='{"approved": true}'`,
	}

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
	serverCmds := append([]*cobra.Command{runCmd, getCmd, signalCmd, watchCmd}, lifecycleCmds...)

	for _, c := range serverCmds {
		addServerFlags(c)
	}
	signalCmd.Flags().String("run-id", "",
		"pin the signal to one run of the workload; unset addresses whichever run is current, "+
			"which is what approving a workload means")

	// Tasks command, which lists the available tasks.
	tasksCmd := &cobra.Command{
		Use:   "tasks",
		Short: "List the tasks workflows can use",
		Long:  "List the tasks available to workflow steps, along with the CEL libraries every expression reaches.",
		Args:  cobra.NoArgs,
		RunE:  runTasks,
		Example: `# List available tasks, their inputs, and their outputs:
flow tasks

# The same thing as a document, for a script or an agent:
flow tasks --output json

# What inputs does the http task take, and which are required?
flow tasks --output json | jq '.tasks[] | select(.name == "http") | .inputs'`,
	}
	addOutputFlag(tasksCmd)

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

	// MCP command, which serves the control plane to an AI agent as tools.
	mcpCmd := &cobra.Command{
		Use:   "mcp",
		Short: "Serve Flowstate to an AI agent over the Model Context Protocol",
		Long: "Serve every workflow-service RPC as an MCP tool over stdin and stdout, " +
			"with input schemas derived from the same protobuf schema the API speaks. " +
			"Validation and the task catalog answer locally; the run-lifecycle tools " +
			"call the configured server.",
		Args: cobra.NoArgs,
		RunE: runMCP,
		Example: `# Serve the MCP tools on stdio (an MCP client launches this):
flow mcp

# Against a specific server for the run-lifecycle tools:
flow mcp --address flowstate.internal:9233`,
	}
	addServerFlags(mcpCmd)

	// LSP command, which starts a Language Server Protocol (LSP) server for Flowfile files.
	lspCmd := &cobra.Command{
		Use:   "lsp",
		Short: "Start a Flowfile Language Server Protocol (LSP) server",
		Long: "Start a language server for Flowfile editing in text editors and IDEs, " +
			"serving the Language Server Protocol over stdin and stdout. It reports " +
			"Flowfile problems as diagnostics as you type.",
		RunE: runLSP,
		Example: `# Start the LSP server:
flow lsp`,
	}

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
	getCmd.GroupID = "workflow"
	watchCmd.GroupID = "workflow"
	signalCmd.GroupID = "workflow"
	for _, c := range lifecycleCmds {
		c.GroupID = "workflow"
	}
	workerCmd.GroupID = "infrastructure"
	serverCmd.GroupID = "infrastructure"
	lspCmd.GroupID = "development"

	// Add commands to root.
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(validateCmd)

	// Grouped with the other commands that read a Flowfile without running one.
	// Left out, it lands under the bare "Commands" heading beside `help` and
	// `completion`, which is where an author stops looking.
	fixCmd := newFixCommand()
	fixCmd.GroupID = "workflow"
	rootCmd.AddCommand(fixCmd)
	rootCmd.AddCommand(tasksCmd)
	rootCmd.AddCommand(pluginsCmd)
	rootCmd.AddCommand(mcpCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(watchCmd)
	rootCmd.AddCommand(signalCmd)
	for _, c := range lifecycleCmds {
		rootCmd.AddCommand(c)
	}
	rootCmd.AddCommand(workerCmd)
	rootCmd.AddCommand(serverCmd)
	runCmd.AddCommand(runLocalCmd)
	rootCmd.AddCommand(lspCmd)

	return rootCmd
}

func main() {
	rootCmd := newRootCommand()

	// We can use a context to handle OS signals like Ctrl+C gracefully.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	// The help and every error report are drawn by this binary, in the same palette
	// as everything else it prints. A binary whose help is one colour and whose
	// output is another reads as two tools — and the reason it is drawn here rather
	// than by fang is a terminal query fang's options could not reach. See execute.go.
	if err := execute(ctx, rootCmd); err != nil {
		os.Exit(exitCode)
	}
}
