package main

import (
	"cmp"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	"github.com/charmbracelet/fang"
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

	// Worker Deployment Versioning, off unless both halves are configured.
	//
	// A version is the pair, so honouring one without the other would produce a
	// worker claiming a version nothing can address. Defaulted from the
	// environment because the build id is a property of the artifact and belongs
	// in whatever built it — a CI job knows the commit; a person typing `flow
	// worker` does not.
	//
	// See pkg/flowstate/v1/engine/versioning.go for what turning this on buys.
	workerDeploymentName = os.Getenv("FLOWSTATE_DEPLOYMENT_NAME")
	workerBuildID        = os.Getenv("FLOWSTATE_BUILD_ID")

	// Authentication settings for `flow server`. There is no default that
	// accepts callers: either a trust policy is configured, or anonymous access
	// is requested explicitly.
	authPolicyPath string
	insecureNoAuth bool

	// identityKeyPath holds the private key Flowstate signs its own assertions
	// with, when the trust policy configures federation. Unset means the server
	// verifies callers but issues nothing, which is the inbound-only deployment.
	identityKeyPath string

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

	deployment := engine.DeploymentOptions(workerDeploymentName, workerBuildID)

	w := worker.New(c, temporalTaskQueue, worker.Options{
		DeploymentOptions: deployment,
	})

	engine.Register(w)

	if deployment.UseVersioning {
		log.Printf("Starting worker on task queue %s as %s/%s",
			temporalTaskQueue, deployment.Version.DeploymentName, deployment.Version.BuildID)
	} else {
		// Said out loud rather than left to be inferred from silence. Without a
		// version, deploying this binary changes the behaviour of every run
		// already in flight — which is the default Temporal has always had, and
		// is a choice an operator should know they are making.
		log.Printf("Starting worker on task queue %s, unversioned "+
			"(set FLOWSTATE_DEPLOYMENT_NAME and FLOWSTATE_BUILD_ID, or --deployment-name and --build-id, "+
			"to pin in-flight runs to the interpreter they started on)", temporalTaskQueue)
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

	workflow, err := loadWorkflow(workflowFilePath)
	if err != nil {
		return err
	}

	flowstateClient := newWorkflowServiceClient()

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
			log.Printf("run completed: workflow %s run %s", resp.Msg.GetWorkflowId(), resp.Msg.GetRunId())

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
			// statusLabel, not the raw enum: `flow get` and `flow list` both say
			// RUNNING, and a third spelling of one status is a third thing for a
			// reader to reconcile.
			log.Printf("run is still going; %s", statusLabel(resp.Msg.GetStatus()))
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
	verifier, policy, err := authVerifier()
	if err != nil {
		return err
	}

	broker, err := identityBroker(policy)
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
		Addr:    flowstateAddress,
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
	if insecureNoAuth {
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
func authVerifier() (auth.Verifier, *auth.Policy, error) {
	if insecureNoAuth {
		return auth.InsecureAnonymousVerifier(), nil, nil
	}

	if authPolicyPath == "" {
		return nil, nil, fmt.Errorf("no authentication configured: pass --auth-policy with a trust policy, " +
			"or --insecure-no-auth to allow anonymous access for local development")
	}

	data, err := os.ReadFile(authPolicyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("reading auth policy: %w", err)
	}
	policy, err := auth.ParsePolicy(data)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing auth policy %s: %w", authPolicyPath, err)
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
func identityBroker(policy *auth.Policy) (*auth.Broker, error) {
	if policy == nil || policy.Federation == nil {
		if identityKeyPath != "" {
			return nil, fmt.Errorf("--identity-key was given but the trust policy configures no federation: " +
				"add a federation section, or drop the key")
		}
		return nil, nil
	}

	if identityKeyPath == "" {
		return nil, fmt.Errorf("the trust policy configures federation but no signing key was given: " +
			"pass --identity-key with a PKCS#8 PEM private key, since Flowstate cannot issue an " +
			"assertion it cannot sign")
	}

	pem, err := os.ReadFile(identityKeyPath)
	if err != nil {
		return nil, fmt.Errorf("reading identity key: %w", err)
	}
	key, err := parseSigningKey(identityKeyPath, pem)
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
func writeFields(tw *tabwriter.Writer, label string, fields []v1.InputField) {
	for i, field := range fields {
		name := field.Name
		if field.Required {
			name += "*"
		}

		// The label sits beside the first row and the rest align under it, so the
		// eye reads down a column rather than hunting for where one list ends.
		heading := ""
		if i == 0 {
			heading = label
		}

		note := ""
		if field.Deferred {
			// Worth saying, because it changes what an author may write here. The
			// engine resolves an expression before scheduling the step; these the
			// task evaluates itself, against a scope the workflow does not have —
			// which is why `http`'s `outputs` can name `status_code` and an
			// ordinary input cannot.
			note = "\tthe task evaluates this itself, in its own scope"
		}

		fmt.Fprintf(tw, "  %s\t%s\t%s%s\n", heading, name, field.Type, note)
	}
}

func runTasks(cmd *cobra.Command, args []string) error {
	out := cmd.OutOrStdout()

	format, err := resolveOutputFormat()
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
		return writeJSON(newSurface(cmd), FormatJSON, v1.Catalog())
	}

	// What a task takes, not just that it exists.
	//
	// This listed a name and a one-line summary, which tells a reader that `http`
	// exists and nothing about how to write one — so the next stop was the
	// README's hand-maintained table, which is exactly the drift the registry
	// exists to prevent. The schema knows; printing it here means the answer
	// cannot go stale.
	for i, def := range v1.DefaultRegistry().All() {
		if i > 0 {
			fmt.Fprintln(out)
		}
		fmt.Fprintf(out, "%s\n  %s\n", def.Name, def.Summary)

		tw := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
		writeFields(tw, "inputs", v1.Inputs(def))
		writeFields(tw, "outputs", v1.Outputs(def))
		if err := tw.Flush(); err != nil {
			return err
		}
	}

	fmt.Fprintf(out, "\n* marks an input the task cannot run without.\n")

	// "every expression", not "the cel task", and that is the change worth
	// spelling out here. These used to be opt-in per `cel` step, which meant this
	// listing was accurate for one step kind and misleading for the rest of the
	// file — an author reading it to find out what an `if:` could say got the
	// wrong answer.
	fmt.Fprintf(out, "\nCEL libraries available to every expression:\n  %s\n",
		strings.Join(v1.ExtensionLibraries(), ", "))

	fmt.Fprintf(out, "\nDuration constructors available to every expression:\n  %s\n",
		strings.Join(v1.DurationUnits(), ", "))
	fmt.Fprintf(out, "\nInside wait_until, %s is the moment the wait is evaluated,\n"+
		"so a deadline can be written as ${%s + days(3)}.\n", v1.NowIdentifier, v1.NowIdentifier)

	return nil
}

// TODO(kent): consider making the commands their own package with additional tests.
func main() {
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
			// A workload that waits for a signal needs something able to deliver
			// one, or it blocks with nothing that could ever release it.
			ctx, err := withLocalSignals(cmd.Context(), localSignals)
			if err != nil {
				return err
			}
			reportUnansweredGates(cmd.ErrOrStderr(), workflow, localSignals)

			result, err := v1.Run(ctx, workflow)
			if err != nil {
				return fmt.Errorf("error running workflow locally: %w", err)
			}
			b, err := protojson.Marshal(result)
			if err != nil {
				return fmt.Errorf("error marshaling result to JSON: %w", err)
			}
			// The result itself goes to stdout below. Logging it here as well
			// meant a person read it twice and a pipe read it once, and the two
			// copies were free to drift.
			log.Println("run completed")
			cmd.OutOrStdout().Write(b)
			cmd.OutOrStdout().Write([]byte("\n"))
			return nil
		},
		Example: `# Run a workflow locally:
flow run local examples/hello-world/workflow.yaml

# Run a multi-step workflow:
flow run local examples/hello-world-multi-step/workflow.yaml

# Run a workflow with an approval gate, answering the gate up front:
flow run local examples/approval-gate/workflow.yaml --signal deploy-approved='{"approved": true}'`,
	}

	// Supplying signals up front is what makes an approval gate something an author
	// can exercise on their laptop rather than first meeting in production. A local
	// run is a process, so there is nobody to signal it after it starts; the local
	// waiter buffers what is given here, so a gate reached later still finds its
	// answer waiting — the same behavior the durable driver has because Temporal
	// buffers signals for a run.
	runLocalCmd.Flags().StringArrayVar(&localSignals, "signal", nil,
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
		c.Flags().StringVar(&temporalAddressFlag, "address", "", "Temporal server address (overrides environment configuration)")
		c.Flags().StringVar(&temporalNamespaceFlag, "namespace", "", "Temporal namespace (overrides environment configuration)")
		c.Flags().StringVar(&temporalProfileFlag, "profile", "", "Temporal configuration profile to use")
	}
	workerCmd.Flags().StringVar(&temporalTaskQueue, "task-queue", temporalTaskQueue, "task queue for Temporal workflows and activities")

	workerCmd.Flags().StringVar(&workerDeploymentName, "deployment-name", workerDeploymentName,
		"Worker Deployment this worker belongs to. With --build-id, pins every in-flight run to the "+
			"interpreter version it started on; a run moves to the current version only at continue-as-new")
	workerCmd.Flags().StringVar(&workerBuildID, "build-id", workerBuildID,
		"version identifier for this worker's binary, unique per build. Required with --deployment-name")

	serverCmd.Flags().StringVar(&authPolicyPath, "auth-policy", "",
		"path to an OIDC/workload-identity trust policy (YAML) describing which issuers to accept")
	serverCmd.Flags().BoolVar(&insecureNoAuth, "insecure-no-auth", false,
		"allow unauthenticated access; for local development only")
	serverCmd.Flags().StringVar(&identityKeyPath, "identity-key",
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
		Example: `# Check a single workflow:
flow validate examples/hello-world/workflow.yaml

# Check every example:
flow validate examples/*/workflow.yaml`,
	}

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

	getCmd.Flags().StringVar(&getRunID, "run-id", "",
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

	signalCmd.Flags().StringVar(&signalData, "data", "",
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
	serverCmds := append([]*cobra.Command{runCmd, getCmd, signalCmd}, lifecycleCmds...)

	for _, c := range serverCmds {
		c.Flags().StringVar(&flowstateAddress, "address", flowstateAddress,
			"address of the Flowstate server (overrides FLOWSTATE_ADDRESS); "+
				"an explicit https:// scheme is honored")

		// A path, never the token. A credential in argv is a credential in `ps`
		// and in shell history — and the file form is the one federated identity
		// arrives in anyway, since Kubernetes projects a service account token to
		// a path and rotates it there. Read per request for that reason.
		c.Flags().StringVar(&tokenFilePath, "token-file", tokenFilePath,
			"file holding the bearer token to authenticate with (overrides FLOWSTATE_TOKEN_FILE); "+
				"re-read per request, so a rotating token keeps working. "+
				"Without it, FLOWSTATE_TOKEN is used, and neither means anonymous")
	}
	signalCmd.Flags().StringVar(&signalRunID, "run-id", "",
		"pin the signal to one run of the workload; unset addresses whichever run is current, "+
			"which is what approving a workload means")

	// Tasks command, which lists the available tasks.
	tasksCmd := &cobra.Command{
		Use:   "tasks",
		Short: "List the tasks workflows can use",
		Long:  "List the tasks available to workflow steps, along with the CEL libraries expressions can enable.",
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
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(signalCmd)
	for _, c := range lifecycleCmds {
		rootCmd.AddCommand(c)
	}
	rootCmd.AddCommand(workerCmd)
	rootCmd.AddCommand(serverCmd)
	runCmd.AddCommand(runLocalCmd)
	rootCmd.AddCommand(lspCmd)

	// We can use a context to handle OS signals like Ctrl+C gracefully.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	// Execute the CLI command with Fang for enhanced styling and features.
	// The help and every error report are drawn by fang, so they are dressed in
	// the same palette as everything the CLI prints itself. A binary whose help is
	// one colour and whose output is another reads as two tools.
	//
	// The version is handed over explicitly because fang sets root.Version for
	// itself otherwise, which silently overwrote the value the build stamps in via
	// -ldflags — so `flow --version` reported "unknown (built from source)" on
	// every release binary.
	err := fang.Execute(ctx, rootCmd,
		fang.WithNotifySignal(os.Interrupt, os.Kill),
		fang.WithColorSchemeFunc(ui.FangColorScheme),
		fang.WithVersion(version),
	)
	if err != nil {
		os.Exit(1)
	}
}
