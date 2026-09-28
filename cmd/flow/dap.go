package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

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
		Long: "Speak the Debug Adapter Protocol on stdin and stdout, so an editor's step, " +
			"continue and pause buttons drive a real local run, or a durable one.\n\n" +
			"A `launch` request runs the workflow its configuration names as `program`, so " +
			"one adapter serves whatever the editor points it at. An `attach` request names " +
			"a durable run's `workflowId` and reaches it through the server flags below, as " +
			"their identity; the run's `debug:` policy must allow it.\n\n" +
			"Line breakpoints resolve to the innermost step written at that line. Function " +
			"breakpoints name a step id or an address such as `orders/charge`. A breakpoint " +
			"that resolves to nothing is answered unverified, saying why.",
		Args: cobra.NoArgs,
		RunE: runDAP,
		Example: `# What an editor's launch configuration runs, rather than a person:
flow dap

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
	if err := applyEgressPolicy(cmd); err != nil {
		return err
	}
	if err := applyTaskPolicy(cmd); err != nil {
		return err
	}

	providers, err := localSecretProviders(cmd)
	if err != nil {
		return fmt.Errorf("configuring secrets for the debug adapter: %w", err)
	}
	defer providers.close()

	catalog, closePlugins, err := startPlugins(cmd, providers.registry)
	if err != nil {
		return fmt.Errorf("starting plugins for the debug adapter: %w", err)
	}
	defer closePlugins()

	writeStdioBanner(cmd.ErrOrStderr(), stdinIsInteractive(cmd), dapBanner)

	// The console the session narrates through. Attached once the adapter
	// exists, because the adapter is what the text goes to.
	var console dapConsole

	var server *flowdap.Server
	server = flowdap.NewServer(nil, lsp.NewBoundedStream(stdio{}),
		flowdap.WithLaunch(func(ctx context.Context, args flowdap.LaunchArguments) (*flowdap.Launch, error) {
			return launchDebuggedRun(cmd, args, catalog, providers, &console, server)
		}),
		flowdap.WithAttach(func(ctx context.Context, args flowdap.AttachArguments) (*flowdap.Attachment, error) {
			return attachDebuggedRun(ctx, cmd, args)
		}),
	)
	console.attach(server)

	return server.Serve(cmd.Context())
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

	workflow, err := loadWorkflow(program)
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

	sourceMap := debugSourceMap(program, workflow)
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
			if err != nil {
				exit = 1
				session.Finished(err)
				server.Output(fmt.Sprintf("flowdap: configuring the local task runtime: %v\n", err))

				return
			}

			_, runErr := v1.RunWithInputs(ctx, workflow, nil)
			session.Finished(runErr)
			if runErr != nil {
				exit = 1
				server.Output(session.RedactText(fmt.Sprintf("run failed: %v\n", runErr)))
			}
		},
	}, nil
}

// attachDebuggedRun attaches to a durable run through the server this command
// was pointed at, with the caller's own credentials. A program, when named, is
// compiled for its source map, which is used only if it matches the program
// the run executes.
func attachDebuggedRun(ctx context.Context, cmd *cobra.Command, args flowdap.AttachArguments) (*flowdap.Attachment, error) {
	var sourceMap *v1.DebugSourceMap
	if args.Program != "" {
		if workflow, err := loadWorkflow(args.Program); err == nil {
			sourceMap = debugSourceMap(args.Program, workflow)
		}
	}

	remote, _, err := flowdebug.AttachRemote(ctx, newWorkflowServiceClient(serverFlagsOf(cmd)),
		args.WorkflowID, args.RunID, flowdebug.RemoteOptions{SessionID: args.SessionID, SourceMap: sourceMap})
	if err != nil {
		return nil, fmt.Errorf("flowdap: attaching to %s: %w", args.WorkflowID, err)
	}
	if sourceMap != nil && !remote.SourceMapVerified() {
		sourceMap = nil
	}

	return &flowdap.Attachment{Target: remote, SourceMap: sourceMap}, nil
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
