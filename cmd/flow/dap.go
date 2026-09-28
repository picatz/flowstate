package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/signal"
	"slices"
	"sync"
	"syscall"

	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdap"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile/lsp"
)

// `flow dap`, the debugger an editor drives.
//
// It is the fourth front over one session core, after the CLI's console, the
// MCP tool and the Go test walker — and, like them, it implements none of the
// debugging. Stepping, breakpoints, scope and evaluation are the session's;
// this command supplies a run and a stream and gets out of the way.

// dapBanner is what a person sees if they run `flow dap` at a terminal, for the
// reason [lspBanner] exists: an adapter speaks nothing until a client writes to
// it, and silence is indistinguishable from a hang.
const dapBanner = "flow dap speaks the Debug Adapter Protocol over stdio and is waiting for an\n" +
	"editor to connect. It is not meant to be run by hand — point your editor's debug\n" +
	"configuration at it, or use `flow run local --debug` for a terminal debugger.\n"

// newDAPCommand builds `flow dap`.
func newDAPCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dap",
		Short: "Debug a workflow from an editor, over the Debug Adapter Protocol",
		Long: "Speak the Debug Adapter Protocol on stdin and stdout, so an editor's step and " +
			"continue buttons drive a Flowstate run.\n\n" +
			"A `launch` request runs the Flowfile named as `program` locally. Breakpoints can be set " +
			"on its lines, or as *function* breakpoints named after a step (`build`, " +
			"`pages/page`, `pages[2]/page`), with conditions, hit counts and log messages.\n\n" +
			"An `attach` request with a `workflowId` (and optionally `runId`) debugs a durable run " +
			"through the server named by --address and this command's credentials, which need " +
			"`workload.debug` (and `workload.debug_inspect` to inspect values or set conditions). " +
			"A durable run holds only at step boundaries, has no logpoints or failure stops, and " +
			"shows step addresses rather than source lines; the editor is told which.",
		Args: cobra.NoArgs,
		RunE: runDAP,
		Example: `# What an editor's launch configuration runs, rather than a person:
flow dap

# An adapter that can also attach to durable runs on a server:
flow dap --address https://flowstate.example.com

# The terminal debugger, for a person:
flow run local --debug examples/hello-world/workflow.yaml`,
	}

	addEditorPluginFlags(cmd)

	// The same two the worker and `flow run local` take. This adapter runs the
	// workflow its client points it at, with this operator's secret providers
	// and plugins behind it, so it is a real local execution surface and not a
	// reader — a rehearsal under a different egress or task-shape policy
	// rehearses a different production, and an operator who set those for the
	// worker had no way to set them here (#1119).
	addEgressPolicyFlag(cmd)
	addTaskPolicyFlag(cmd)

	addSecretFlags(cmd)
	addLocalRehearsalFlags(cmd)
	addRevealSensitiveFlag(cmd)

	// For an `attach` request: the server a durable run is reached through,
	// with the caller's own credentials.
	addServerFlags(cmd)

	return cmd
}

// runDAP serves one debug session.
func runDAP(cmd *cobra.Command, _ []string) error {
	// A policy the operator configured and the adapter cannot load refuses
	// the adapter before anything is served, launch or attach: that is the
	// flag's contract, and failing closed on it is not a local-run detail.
	if err := applyEgressPolicy(cmd); err != nil {
		return err
	}
	if err := applyTaskPolicy(cmd); err != nil {
		return err
	}

	// What only a local run uses — secret providers and plugins, which start
	// processes and open connections — is started by the launch that needs
	// it. An attach reaches a durable run through the server alone, and must
	// not fail on a plugin it never runs.
	var local localRunResources
	defer local.close()

	// An editor that dies takes the read end of this process's standard output
	// with it, and a write to fd 1 after that is a SIGPIPE that kills the
	// process under a run the session detached from, with its plugins never
	// closed. Ignored, the write fails with EPIPE, which the adapter discards;
	// it also stops writing once it sees the client gone, but a message
	// already in flight when the editor dies would still meet the pipe.
	signal.Ignore(syscall.SIGPIPE)

	writeStdioBanner(cmd.ErrOrStderr(), stdinIsInteractive(cmd), dapBanner)

	// The console the session narrates through. Attached once the adapter
	// exists, because the adapter is what the text goes to.
	var console dapConsole

	var server *flowdap.Server
	server = flowdap.NewServer(nil, lsp.NewBoundedStream(stdio{}),
		flowdap.WithLaunch(func(ctx context.Context, args flowdap.LaunchArguments) (*flowdap.Launch, error) {
			catalog, providers, err := local.open(cmd)
			if err != nil {
				return nil, err
			}

			return launchDebuggedRun(cmd, args, catalog, providers, &console, server)
		}),
		flowdap.WithAttach(func(ctx context.Context, args flowdap.AttachArguments) (*flowdap.Attachment, error) {
			return attachDebuggedRun(ctx, cmd, args)
		}),
	)
	console.attach(server)

	err := server.Serve(cmd.Context())
	// A disconnect that did not terminate a launched run detached from it,
	// and it goes on: the plugins and secret providers deferred above stay
	// open until it returns, rather than failing it midway.
	server.Wait()

	return err
}

// localRunResources is what a local debugged run needs from its process,
// opened on the first launch and closed when the adapter exits.
type localRunResources struct {
	opened    bool
	catalog   *v1.PluginCatalog
	providers *localSecrets
	closers   []func()
}

// open starts the secret providers and plugins, once.
func (r *localRunResources) open(cmd *cobra.Command) (*v1.PluginCatalog, *localSecrets, error) {
	if r.opened {
		return r.catalog, r.providers, nil
	}

	providers, err := localSecretProviders(cmd)
	if err != nil {
		return nil, nil, fmt.Errorf("flowdap: configuring secrets for the debug adapter: %w", err)
	}
	r.closers = append(r.closers, providers.close)

	catalog, closePlugins, err := startPlugins(cmd, providers.registry)
	if err != nil {
		return nil, nil, fmt.Errorf("flowdap: starting plugins for the debug adapter: %w", err)
	}
	r.closers = append(r.closers, closePlugins)

	r.opened, r.catalog, r.providers = true, catalog, providers

	return catalog, providers, nil
}

// close releases what open started, last first.
func (r *localRunResources) close() {
	for _, closer := range slices.Backward(r.closers) {
		closer()
	}
}

// launchDebuggedRun prepares a local run of the program a launch names: the
// workflow, its disclosure decision, its plugins, and a session built with the
// program and its source map. The run starts when the adapter calls Start.
func launchDebuggedRun(
	cmd *cobra.Command, args flowdap.LaunchArguments, catalog *v1.PluginCatalog,
	providers *localSecrets, console *dapConsole, server *flowdap.Server,
) (*flowdap.Launch, error) {
	program := args.Program
	reveal := revealSensitiveRequested(cmd) || args.RevealSensitive
	if program == "" {
		return nil, errors.New("flowdap: the launch configuration named no `program`, so there is no workflow to run")
	}

	workflow, source, err := loadDebuggedWorkflow(program)
	if err != nil {
		var diagnostics flowfile.Diagnostics
		if !errors.As(err, &diagnostics) || decideCarriedValues(nil, reveal) == carriedValuesShown {
			return nil, fmt.Errorf("flowdap: %w", err)
		}

		return nil, errors.New("flowdap: workflow diagnostics withheld because the invalid file has no " +
			"trusted sensitive-value declarations; run `flow validate` outside the adapter, or " +
			"explicitly authorize disclosure with --reveal-sensitive or \"revealSensitive\": true")
	}

	// A debugger is a reveal, so a workflow whose declarations would make the
	// final render withhold values does not get one without saying so.
	switch decideCarriedValues(workflow, reveal) {
	case carriedValuesShown:
	case carriedValuesDeclared:
		return nil, errors.New("flowdap: the workflow declares sensitive inputs or outputs whose " +
			"values the debugger would expose; add --reveal-sensitive to the adapter command " +
			"or \"revealSensitive\": true to the launch configuration to debug it with values shown")
	default:
		return nil, errors.New("flowdap: the workflow's sensitive-value declarations could not be fully " +
			"inspected, so the debugger will not start without explicit disclosure authorization; " +
			"add --reveal-sensitive to the adapter command or \"revealSensitive\": true to the " +
			"launch configuration")
	}
	if err := v1.ResolvePlugins(workflow, catalog); err != nil {
		return nil, fmt.Errorf("flowdap: resolving plugins before this run: %w", err)
	}

	sourceMap := source.sourceMap(workflow)
	session, err := flowdebug.New(flowdebug.Options{
		Controlled: true,
		Out:        io.Discard,
		Emit:       func(text string, _ flowdebug.Tone) { console.write(text) },
		Workflow:   workflow,
		SourceMap:  sourceMap,
	})
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(cmd.Context())

	return &flowdap.Launch{
		Target:    session,
		SourceMap: sourceMap,
		Terminate: cancel,
		Start: func() {
			defer cancel()

			exit := 0
			defer func() {
				server.Exited(exit)
				_ = session.Close()
				server.Finished()
			}()

			ctx := v1.NewContextWithDebugger(runCtx, session)
			ctx = v1.NewContextWithRunObserver(ctx, session)
			ctx, err := withLocalTaskRuntimeUsing(cmd, ctx, workflow, providers)
			// The exit code is recorded before the session reads as ended: a
			// movement answered ENDED reports the run's end at once, and must
			// report this code rather than the default.
			if err != nil {
				exit = 1
				server.Exited(exit)
				session.Finished(err)
				server.Output(fmt.Sprintf("flowdap: configuring the local task runtime: %v\n", err))

				return
			}

			_, runErr := v1.RunWithInputs(ctx, workflow, nil)
			if runErr != nil {
				exit = 1
				server.Exited(exit)
			}
			session.Finished(runErr)
			if runErr != nil {
				server.Output(session.RedactText(fmt.Sprintf("run failed: %v\n", runErr)))
			}
		},
	}, nil
}

// attachDebuggedRun attaches to a durable run through the server this command
// was pointed at, with the caller's own credentials.
func attachDebuggedRun(ctx context.Context, cmd *cobra.Command, args flowdap.AttachArguments) (*flowdap.Attachment, error) {
	// No source map on a durable attach. A map is bound to its program by the
	// IR digest, and the IR carries no positions: a file whose lines moved
	// since the run was submitted compiles to the same digest, and would put
	// frames and line breakpoints on the wrong lines. The run records no digest
	// of its source to check a local file against, so an attach shows step
	// addresses and answers line breakpoints unverified rather than guess.
	remote, _, err := flowdebug.AttachRemote(ctx, newWorkflowServiceClient(serverFlagsOf(cmd)),
		args.WorkflowID, args.RunID, flowdebug.RemoteOptions{SessionID: args.SessionID})
	if err != nil {
		return nil, fmt.Errorf("flowdap: attaching to %s: %w", args.WorkflowID, err)
	}

	return &flowdap.Attachment{Target: remote}, nil
}

type dapConsole struct {
	mu sync.Mutex
	to *flowdap.Server
}

func (c *dapConsole) attach(server *flowdap.Server) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.to = server
}

// write drops the fragment where there is no client, which is every fragment
// produced before one connects — there is nowhere else for it to go, and this
// process's standard output is the protocol stream.
func (c *dapConsole) write(text string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.to == nil {
		return
	}

	c.to.Output(text)
}
