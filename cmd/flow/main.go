package main

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"connectrpc.com/authn"
	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	"github.com/charmbracelet/fang"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile/lsp"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
	"github.com/sourcegraph/jsonrpc2"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"
)

// Set by the build system, e.g. using -ldflags="-X main.version=1.0.0"
var version = "dev"

// must is a utility function that panics if the error is not nil.
func must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

// maybe is a utility function that returns the value if no error,
// otherwise returns the zero value of T. We don't actually use the error
// parameter here, but it is included to match a common pattern
// where we might want to ignore the error in a more complex scenario,
// like when there's a fallback default value or similar logic.
//
// It's generally not a good idea to ignore errors, of course.
func maybe[T any](value T, _ error) T {
	return value
}

// Global configuration variables for the Flowstate CLI.
//
// Since these are global, they are inherently cursed and should be used
// with caution. They are initialized from environment variables or
// default values, and are used throughout the CLI commands to configure
// the Flowstate service, Temporal client, and other settings.
//
// TODO(kent): consider refactoring to avoid global state, e.g. by passing
// configuration structs to command handlers or using a context-based approach.
var (
	flowstateAddress   string        = cmp.Or(os.Getenv("FLOWSTATE_ADDRESS"), "localhost:9233")
	temporalAddress    string        = cmp.Or(os.Getenv("TEMPORAL_ADDRESS"), "localhost:7233")
	temporalNamespace  string        = cmp.Or(os.Getenv("TEMPORAL_NAMESPACE"), "default")
	temporalTaskQueue  string        = cmp.Or(os.Getenv("TEMPORAL_TASK_QUEUE"), engine.RunTaskQueueName)
	workflowRunTimeout time.Duration = cmp.Or(
		maybe(time.ParseDuration(os.Getenv("WORKFLOW_RUN_TIMEOUT"))),
		must(time.ParseDuration("10m")),
	)
	verboseLogging bool          = cmp.Or(os.Getenv("FLOWSTATE_VERBOSE_LOGGING") == "true", false)
	temporalClient client.Client = nil
)

// initTemporalClient initializes a Temporal client using config settings
func initTemporalClient() (client.Client, error) {
	if temporalClient != nil {
		return temporalClient, nil
	}

	clientOptions := client.Options{
		Namespace: temporalNamespace,
	}

	if temporalAddress != "" {
		clientOptions.HostPort = temporalAddress
	}

	c, err := client.Dial(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to create Temporal client: %w", err)
	}

	temporalClient = c
	return c, nil
}

// runWorker implements the worker sub-command to start a Temporal worker
// to process Flowstate workflows and activities.
func runWorker(cmd *cobra.Command, args []string) error {
	// Initialize a Temporal client using the configured address
	// and namespace global variables (yuck, amiright).
	c, err := initTemporalClient()
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

	log.Printf("Starting worker on task queue: %s", temporalTaskQueue)
	if verboseLogging {
		log.Printf("Temporal configuration: address=%s namespace=%s", temporalAddress, temporalNamespace)
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

// runWorkflow implements the run sub-command to execute a workflow
// using the Flowstate service. It reads a workflow definition from a file,
// sends it to the Flowstate server, and polls for updates until completion.
func runWorkflow(cmd *cobra.Command, args []string) error {
	// Check for workflow file
	if len(args) < 1 {
		return fmt.Errorf("workflow file path required")
	}

	workflowFilePath := args[0]

	// Read workflow definition from file.
	workflowDFileData, err := os.ReadFile(workflowFilePath)
	if err != nil {
		return fmt.Errorf("error reading workflow file: %w", err)
	}

	workflow, err := flowfile.Unmarshal(workflowDFileData)
	if err != nil {
		return fmt.Errorf("error parsing workflow file: %w", err)
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
	c, err := initTemporalClient()
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
		),
	)

	authMiddleware := authn.NewMiddleware(func(ctx context.Context, req *http.Request) (any, error) {
		// TODO(kent): implement authentication logic here.
		// For now, we just return nil to allow all requests.
		return nil, nil
	})

	// TODO(kent): add more flags and secure defaults for the server.
	// It's generally insecure to run an HTTP server without TLS in production,
	// and even more so to expose it publicly without additional limits like timeouts,
	// rate limiting, etc.
	httpServer := &http.Server{
		Addr:    flowstateAddress,
		Handler: authMiddleware.Wrap(mux),
	}

	log.Printf("Starting Flowstate server on %s", httpServer.Addr)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not listen on %s: %v\n", httpServer.Addr, err)
		}
	}()
	<-cmd.Context().Done()
	log.Println("Shutting down server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	log.Println("Server stopped")

	return nil
}

// TODO(kent): this is very experimental and a work in progress.
func runLSP(cmd *cobra.Command, args []string) error {
	log.SetFlags(0)
	log.SetOutput(os.Stderr)
	server := &lsp.FlowfileServer{}
	jsonrpc2.NewConn(cmd.Context(), jsonrpc2.NewBufferedStream(os.Stdin, jsonrpc2.VSCodeObjectCodec{}), server)
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

# Run with a specific workflow ID:
flow run examples/hello-world/workflow.yaml --workflow-id my-workflow

# Run with a custom timeout:
flow run examples/hello-world/workflow.yaml --timeout 5m`,
	}

	runCmd.Flags().StringP("workflow-id", "w", "", "Workflow ID (auto-generated if not specified)")
	runCmd.Flags().DurationVar(&workflowRunTimeout, "timeout", 10*time.Minute, "Workflow execution timeout")

	// Run local command, which executes a workflow locally without using Temporal or the Flowstate service.
	runLocalCmd := &cobra.Command{
		Use:   "local [workflow-file]",
		Short: "Run a workflow locally without Temporal",
		Long:  "Execute a workflow locally without using Temporal or the Flowstate service. This is useful for testing and development.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			workflowFilePath := args[0]
			workflowData, err := os.ReadFile(workflowFilePath)
			if err != nil {
				return fmt.Errorf("error reading workflow file: %w", err)
			}
			workflow, err := flowfile.Unmarshal(workflowData)
			if err != nil {
				return fmt.Errorf("error parsing workflow file: %w", err)
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

	workerCmd.Flags().StringVar(&temporalAddress, "address", temporalAddress, "service address for Temporal server")
	workerCmd.Flags().StringVar(&temporalNamespace, "namespace", temporalNamespace, "namespace for Temporal workflows")
	workerCmd.Flags().StringVar(&temporalTaskQueue, "task-queue", temporalTaskQueue, "task queue for Temporal workflows and activities")

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

	// LSP command, which starts a Language Server Protocol (LSP) server for Flowfile files.
	lspCmd := &cobra.Command{
		Use:   "lsp",
		Short: "Start a Flowfile Language Server Protocol (LSP) server",
		Long:  "Start an LSP server for Flowfile editing support in text editors and IDEs. This provides syntax highlighting, completion, and validation for Flowfile YAML files.",
		RunE:  runLSP,
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
	workerCmd.GroupID = "infrastructure"
	serverCmd.GroupID = "infrastructure"
	lspCmd.GroupID = "development"

	// Add commands to root.
	rootCmd.AddCommand(runCmd)
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
