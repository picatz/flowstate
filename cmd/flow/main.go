package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/authn"
	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	"github.com/charmbracelet/fang"
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
	"google.golang.org/protobuf/encoding/protojson"
)

// Set by the build system, e.g. using -ldflags="-X main.version=1.0.0"
var version = "dev"

// Configuration for the Flowstate CLI, resolved from the environment at startup
// and overridable by flags.
//
// TODO(kent): consider refactoring to avoid global state, e.g. by passing
// configuration structs to command handlers or using a context-based approach.
var (
	flowstateAddress  string = cmp.Or(os.Getenv("FLOWSTATE_ADDRESS"), "localhost:9233")
	temporalTaskQueue string = cmp.Or(os.Getenv("TEMPORAL_TASK_QUEUE"), engine.RunTaskQueueName)
	verboseLogging    bool   = os.Getenv("FLOWSTATE_VERBOSE_LOGGING") == "true"

	// Temporal connection settings are deliberately empty by default. Unset
	// means "use Temporal's own environment configuration" — the standard
	// TEMPORAL_* variables and the TOML profile the `temporal` CLI reads — so
	// these flags override that configuration rather than replacing it.
	temporalAddressFlag   string
	temporalNamespaceFlag string
	temporalProfileFlag   string

	// Authentication settings for `flow server`. There is no default that
	// accepts callers: either a trust policy is configured, or anonymous access
	// is requested explicitly.
	authPolicyPath string
	insecureNoAuth bool

	temporalClient client.Client = nil
)

// initTemporalClient connects to Temporal.
//
// Configuration comes from Temporal's own environment configuration — the
// standard TEMPORAL_* variables and the same TOML profile file the `temporal` CLI
// reads — so a self-hosted cluster, Temporal Cloud, and a local development
// server differ only in configuration. Flags override whatever that resolves to.
func initTemporalClient(ctx context.Context) (client.Client, error) {
	if temporalClient != nil {
		return temporalClient, nil
	}

	cfg := temporalclient.Config{
		Address:   temporalAddressFlag,
		Namespace: temporalNamespaceFlag,
		Profile:   temporalProfileFlag,
	}

	if verboseLogging {
		if opts, err := cfg.Options(); err == nil {
			log.Printf("Temporal: %s", temporalclient.Describe(opts))
		}
	}

	c, err := temporalclient.Dial(ctx, cfg)
	if err != nil {
		return nil, err
	}

	temporalClient = c
	return c, nil
}

// runWorker implements the worker sub-command to start a Temporal worker
// to process Flowstate workflows and activities.
func runWorker(cmd *cobra.Command, args []string) error {
	// Initialize a Temporal client using the configured address
	// and namespace global variables (yuck, amiright).
	c, err := initTemporalClient(cmd.Context())
	if err != nil {
		return err
	}
	defer c.Close()

	// Create a new Temporal worker for the specified task queue.
	w := worker.New(c, temporalTaskQueue, worker.Options{
		// TODO(kent): consider making this configurable using flags or env vars.
	})

	// Register workflow and activities
	w.RegisterWorkflow(engine.Run)
	w.RegisterActivity(engine.Task)
	w.RegisterActivity(engine.TaskInScope)
	// Registered so a run started before scopes existed can still complete.
	w.RegisterActivity(engine.TaskWithPrev)

	log.Printf("Starting worker on task queue: %s", temporalTaskQueue)

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

// runWorkflow implements the run sub-command to execute a workflow
// using the Flowstate service. It reads a workflow definition from a file,
// sends it to the Flowstate server, and polls for updates until completion.
func runWorkflow(cmd *cobra.Command, args []string) error {
	// Check for workflow file
	if len(args) < 1 {
		return fmt.Errorf("workflow file path required")
	}

	workflowFilePath := args[0]

	workflow, err := loadWorkflow(workflowFilePath)
	if err != nil {
		return err
	}

	// TODO(kent): support HTTPs connections.
	flowstateClient := flowstatev1connect.NewWorkflowServiceClient(http.DefaultClient, fmt.Sprintf("http://%s", flowstateAddress))

	runResp, err := flowstateClient.Run(cmd.Context(), &connect.Request[v1.RunRequest]{
		Msg: &v1.RunRequest{
			Workflow: workflow,
		},
	})
	if err != nil {
		return fmt.Errorf("error running workflow: %w", err)
	}

	// Poll for updates every 2 seconds until completed.
	for cmd.Context().Err() == nil {
		time.Sleep(2 * time.Second)
		resp, err := flowstateClient.Get(cmd.Context(), &connect.Request[v1.GetRequest]{
			Msg: &v1.GetRequest{
				WorkflowId: runResp.Msg.WorkflowId,
			},
		})
		if err != nil {
			return fmt.Errorf("error getting workflow run status: %w", err)
		}
		if resp.Msg.Status == v1.RunResponse_STATUS_COMPLETED {
			log.Printf("Workflow completed successfully! WorkflowID: %s, RunID: %s", resp.Msg.WorkflowId, resp.Msg.RunId)

			b, err := protojson.Marshal(resp.Msg.GetOutputs())
			if err != nil {
				return fmt.Errorf("error marshaling result to JSON: %w", err)
			}
			cmd.OutOrStdout().Write(b)
			cmd.OutOrStdout().Write([]byte("\n"))

			break
		} else if resp.Msg.Status == v1.RunResponse_STATUS_FAILED {
			return fmt.Errorf("workflow execution failed: %s", resp.Msg.GetError())
		} else {
			log.Printf("Workflow is still running. Status: %s", resp.Msg.Status)
		}
	}

	return nil
}

// runServer implements the server sub-command to start a Flowstate server
// that listens for incoming workflow requests and serves them using the
// Flowstate service implementation over HTTP (via Connect RPC).
func runServer(cmd *cobra.Command, args []string) error {
	// Resolve configuration before doing any I/O, so a misconfiguration is
	// reported immediately rather than after waiting on a connection attempt.
	verifier, err := authVerifier()
	if err != nil {
		return err
	}

	c, err := initTemporalClient(cmd.Context())
	if err != nil {
		return err
	}
	defer c.Close()

	interceptor, err := validate.NewInterceptor()
	if err != nil {
		return fmt.Errorf("error creating validation interceptor: %w", err)
	}

	otelInterceptor, err := otelconnect.NewInterceptor()
	if err != nil {
		return fmt.Errorf("error creating OpenTelemetry interceptor: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle(
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

	authMiddleware := authn.NewMiddleware(auth.NewAuthenticator(verifier).Authenticate)

	httpServer := &http.Server{
		Addr:    flowstateAddress,
		Handler: authMiddleware.Wrap(mux),

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
	if insecureNoAuth {
		log.Printf("WARNING: authentication is disabled; every caller is anonymous " +
			"and can start workflows. Do not use this outside local development.")
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
func authVerifier() (auth.Verifier, error) {
	if insecureNoAuth {
		return auth.InsecureAnonymousVerifier(), nil
	}

	if authPolicyPath == "" {
		return nil, fmt.Errorf("no authentication configured: pass --auth-policy with a trust policy, " +
			"or --insecure-no-auth to allow anonymous access for local development")
	}

	data, err := os.ReadFile(authPolicyPath)
	if err != nil {
		return nil, fmt.Errorf("reading auth policy: %w", err)
	}
	policy, err := auth.ParsePolicy(data)
	if err != nil {
		return nil, fmt.Errorf("parsing auth policy %s: %w", authPolicyPath, err)
	}
	verifier, err := auth.NewOIDCVerifier(policy)
	if err != nil {
		return nil, fmt.Errorf("configuring token verification: %w", err)
	}
	return verifier, nil
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
	out := cmd.OutOrStdout()
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
			fmt.Fprintf(out, "%s: %v\n", path, err)
			continue
		}
		if len(diagnostics) == 0 {
			fmt.Fprintf(out, "%s: ok\n", path)
			continue
		}

		failed = true
		for _, d := range diagnostics {
			fmt.Fprintf(out, "%s:%s\n", path, d.Error())
		}
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
func runTasks(cmd *cobra.Command, args []string) error {
	out := cmd.OutOrStdout()

	tw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "TASK\tDESCRIPTION")
	for _, def := range v1.DefaultRegistry().All() {
		fmt.Fprintf(tw, "%s\t%s\n", def.Name, def.Summary)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	fmt.Fprintf(out, "\nCEL libraries available to the cel task via the libs input:\n  %s\n",
		strings.Join(v1.ExtensionLibraries(), ", "))
	return nil
}

// TODO(kent): consider making the commands their own package with additional tests.
func main() {
	// Root command for the Flowstate CLI application (flow).
	rootCmd := &cobra.Command{
		Use:     "flow",
		Short:   "Flowstate workflow engine",
		Long:    "Flowstate is a workflow engine that uses Temporal for durable execution and CEL expressions for dynamic workflows.",
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

	rootCmd.PersistentFlags().BoolVarP(&verboseLogging, "verbose", "v", false, "enable verbose logging")

	// Run command, which executes a workflow using the Flowstate service.
	runCmd := &cobra.Command{
		Use:   "run [workflow-file]",
		Short: "Run a workflow",
		Long:  "Execute a workflow using the Flowstate service. The workflow file should be a YAML file containing step definitions.",
		Args:  cobra.MinimumNArgs(1),
		RunE:  runWorkflow,
		Example: `# Run a workflow using the Flowstate server:
flow run examples/hello-world/workflow.yaml

# Check a workflow without running it:
flow validate examples/hello-world/workflow.yaml`,
	}

	// Run local command, which executes a workflow locally without using Temporal or the Flowstate service.
	runLocalCmd := &cobra.Command{
		Use:   "local [workflow-file]",
		Short: "Run a workflow locally without Temporal",
		Long:  "Execute a workflow locally without using Temporal or the Flowstate service. This is useful for testing and development.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			workflowFilePath := args[0]
			workflow, err := loadWorkflow(workflowFilePath)
			if err != nil {
				return err
			}
			result, err := v1.Run(cmd.Context(), workflow)
			if err != nil {
				return fmt.Errorf("error running workflow locally: %w", err)
			}
			b, err := protojson.Marshal(result)
			if err != nil {
				return fmt.Errorf("error marshaling result to JSON: %w", err)
			}
			log.Printf("Workflow completed successfully! Result: %s", string(b))
			cmd.OutOrStdout().Write(b)
			cmd.OutOrStdout().Write([]byte("\n"))
			return nil
		},
		Example: `# Run a workflow locally:
flow run local examples/hello-world/workflow.yaml

# Run a multi-step workflow:
flow run local examples/hello-world-multi-step/workflow.yaml`,
	}

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
flow worker --namespace production`,
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
		c.Flags().StringVar(&temporalAddressFlag, "address", "", "Temporal server address (overrides environment configuration)")
		c.Flags().StringVar(&temporalNamespaceFlag, "namespace", "", "Temporal namespace (overrides environment configuration)")
		c.Flags().StringVar(&temporalProfileFlag, "profile", "", "Temporal configuration profile to use")
	}
	workerCmd.Flags().StringVar(&temporalTaskQueue, "task-queue", temporalTaskQueue, "task queue for Temporal workflows and activities")

	serverCmd.Flags().StringVar(&authPolicyPath, "auth-policy", "",
		"path to an OIDC/workload-identity trust policy (YAML) describing which issuers to accept")
	serverCmd.Flags().BoolVar(&insecureNoAuth, "insecure-no-auth", false,
		"allow unauthenticated access; for local development only")

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
		Example: `# Check a single workflow:
flow validate examples/hello-world/workflow.yaml

# Check every example:
flow validate examples/*/workflow.yaml`,
	}

	// Tasks command, which lists the available tasks.
	tasksCmd := &cobra.Command{
		Use:   "tasks",
		Short: "List the tasks workflows can use",
		Long:  "List the tasks available to workflow steps, along with the CEL libraries expressions can enable.",
		Args:  cobra.NoArgs,
		RunE:  runTasks,
		Example: `# List available tasks:
flow tasks`,
	}

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
	workerCmd.GroupID = "infrastructure"
	serverCmd.GroupID = "infrastructure"
	lspCmd.GroupID = "development"

	// Add commands to root.
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(validateCmd)
	rootCmd.AddCommand(tasksCmd)
	rootCmd.AddCommand(workerCmd)
	rootCmd.AddCommand(serverCmd)
	runCmd.AddCommand(runLocalCmd)
	rootCmd.AddCommand(lspCmd)

	// We can use a context to handle OS signals like Ctrl+C gracefully.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	// Execute the CLI command with Fang for enhanced styling and features.
	if err := fang.Execute(ctx, rootCmd, fang.WithNotifySignal(os.Interrupt, os.Kill)); err != nil {
		os.Exit(1)
	}
}
