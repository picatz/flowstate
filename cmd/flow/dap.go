package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
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
			"A `launch` request runs the Flowfile named as `program` locally, with its `inputs` object as the " +
			"run's arguments. Breakpoints can be set " +
			"on its lines, or as *function* breakpoints named after a step (`build`, " +
			"`pages/page`, `pages[2]/page`), with conditions, hit counts and log messages.\n\n" +
			"An `attach` request with a `workflowId` (and optionally `runId`) debugs a durable run " +
			"through the server named by --address and this command's credentials, which need " +
			"`workload.debug` (and `workload.debug_inspect` to inspect values or to set or read conditions). " +
			"A durable run holds only at step boundaries and has no logpoints or failure stops; the " +
			"editor is told which. It shows source lines, and takes line breakpoints, when the attach's " +
			"`program` is the file the run executes, and step addresses otherwise.",
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
	// closed. Handled, the write fails with EPIPE, which the adapter discards;
	// it also stops writing once it sees the client gone, but a message
	// already in flight when the editor dies would still meet the pipe.
	//
	// Notify rather than Ignore: an ignored signal stays ignored across exec,
	// so every plugin and secret command this adapter starts would inherit
	// it and behave differently under the debugger than under a worker. A
	// handled one is reset to its default in each child.
	signal.Notify(make(chan os.Signal, 1), syscall.SIGPIPE)

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
	// An interrupt is how an editor or an operator stops the adapter, not a
	// failure of it: the session detached, and the command exits cleanly.
	if errors.Is(err, context.Canceled) && cmd.Context().Err() != nil {
		err = nil
	}
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

	catalog, closePlugins, err := startPlugins(cmd, providers.registry)
	if err != nil {
		// Released now rather than at exit: a client that retries the launch
		// would otherwise open another set of providers each time.
		providers.close()

		return nil, nil, fmt.Errorf("flowdap: starting plugins for the debug adapter: %w", err)
	}
	r.closers = append(r.closers, providers.close, closePlugins)

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
	// The run's arguments, bound before anything runs and after the
	// disclosure decision, so a refusal quoting one is shown only where
	// values may be. A launch missing a required input could only fail, and
	// the launch's own response is where an editor shows why.
	inputs, err := jsonRunInputs(workflow, args.Inputs, "the launch configuration's inputs",
		"arguments go in the `inputs` object of the launch configuration, keyed by the name the workflow declares under `inputs:`")
	if err != nil {
		return nil, fmt.Errorf("flowdap: %w", err)
	}

	sourceMap := source.sourceMap(workflow)
	build := debuggedRunBuilder{
		cmd: cmd, workflow: workflow, inputs: inputs, sourceMap: sourceMap, reveal: reveal,
		providers: providers, console: console, server: server,
	}
	if args.Reverse {
		return build.reversible(args)
	}

	return build.once(args)
}

// debuggedRunBuilder is what a local debugged run is made of, whether it runs
// once or can run again to step back.
type debuggedRunBuilder struct {
	cmd       *cobra.Command
	workflow  *v1.Workflow
	inputs    map[string]*v1.Value
	sourceMap *v1.DebugSourceMap
	reveal    bool
	providers *localSecrets
	console   *dapConsole
	server    *flowdap.Server
}

// session is a controlled session for one run of the program.
func (b debuggedRunBuilder) session(emit func(string, flowdebug.Tone), continueAtEntry bool) (*flowdebug.Session, error) {
	return flowdebug.New(flowdebug.Options{
		Controlled: true,
		Out:        io.Discard,
		Emit:       emit,
		Workflow:   b.workflow,
		SourceMap:  b.sourceMap,
		// Authorized above, or the program declares nothing to withhold.
		RevealSensitive: b.reveal,
		// Held at the first step only when the editor asked to be: the
		// breakpoints are set before the run starts, so a run that is not
		// to stop on entry need not hold there only to be released at once,
		// narrating a stop the editor never shows.
		Continue: continueAtEntry,
	})
}

// execute runs the program under session and says how it ended. reports says
// whether this run's outcome is the session's, and is asked each time
// something would be said about it: a run a rewind replaced, or that is still
// being replayed, says nothing.
//
// The editor is told the run ended by the returned func, not here, so a caller
// that must wait for the run itself (a rewind stopping the one it replaced,
// while the adapter's ordering lock is held) never waits on the adapter's own
// report of an end, which needs that lock.
//
// The exit code is recorded before the session reads as ended: a movement
// answered ENDED reports the run's end at once, and must report this code
// rather than the default.
func (b debuggedRunBuilder) execute(
	runCtx context.Context, session *flowdebug.Session, reports func() bool,
) (finish func()) {
	exit := 0
	func() {
		ctx := v1.NewContextWithDebugger(runCtx, session)
		ctx = v1.NewContextWithRunObserver(ctx, session)
		ctx, err := withLocalTaskRuntimeUsing(b.cmd, ctx, b.workflow, b.providers)
		if err != nil {
			exit = 1
			if reports() {
				b.server.Exited(exit)
			}
			session.Finished(err)
			if reports() {
				b.server.Output(fmt.Sprintf("flowdap: configuring the local task runtime: %v\n", err))
			}

			return
		}

		_, runErr := v1.RunWithInputs(ctx, b.workflow, b.inputs)
		if runErr != nil {
			exit = 1
			if reports() {
				b.server.Exited(exit)
			}
		}
		session.Finished(runErr)
		if runErr != nil && reports() {
			b.server.Output("run failed: " + session.FailureText(runErr) + "\n")
		}
	}()

	if reports() {
		b.server.Exited(exit)
	}
	_ = session.Close()

	return func() {
		if reports() {
			b.server.Finished()
		}
	}
}

// once is a launch that runs the program one time.
func (b debuggedRunBuilder) once(args flowdap.LaunchArguments) (*flowdap.Launch, error) {
	session, err := b.session(func(text string, _ flowdebug.Tone) { b.console.write(text) },
		args.StopOnEntry != nil && !*args.StopOnEntry)
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(b.cmd.Context())

	return &flowdap.Launch{
		Target:    session,
		SourceMap: b.sourceMap,
		Terminate: cancel,
		Start: func() {
			defer cancel()
			b.execute(runCtx, session, func() bool { return true })()
		},
	}, nil
}

// debuggedRun is one run a reversible launch has started.
type debuggedRun struct {
	cancel context.CancelFunc
	// done closes when the run has ended and its session is released, before
	// the editor is told: stopping a run waits on this and nothing the adapter
	// holds a lock for. reported closes after the editor has been told.
	done     chan struct{}
	reported chan struct{}
	live     atomic.Bool
	stopped  atomic.Bool
}

// reports is whether this run's end is the session's: it is the one shown, and
// no rewind has stopped it. The editor's terminate cancels without stopping, so
// it reports. A replay that ends early, or that diverges, was never shown and
// says nothing.
func (r *debuggedRun) reports() bool { return r.live.Load() && !r.stopped.Load() }

// reversible is a launch that can step back. Going back runs the program
// again from its start, so it is a choice the launch configuration makes with
// `reverse: true`, and a replay that does not reproduce what the editor was
// shown is refused rather than shown as if it had.
func (b debuggedRunBuilder) reversible(args flowdap.LaunchArguments) (*flowdap.Launch, error) {
	// The entry stop is the first stop in the history a step back returns to.
	// Running past it unseen would leave a history whose first visible stop
	// has a hidden one behind it, and stepping back would land there only to
	// be sent forward again.
	if args.StopOnEntry != nil && !*args.StopOnEntry {
		return nil, errors.New("flowdap: \"reverse\" keeps the run's first stop in its history, " +
			"so it cannot also run past it unseen; leave \"stopOnEntry\" out or set it true, " +
			"or drop \"reverse\"")
	}

	var (
		mu      sync.Mutex
		current *debuggedRun
		// started releases the first run, at configurationDone like any
		// other launch: a client that disconnects before then has run nothing.
		// Every later run is a replay, and starts at once.
		started = make(chan struct{})
		first   atomic.Bool
	)
	shown := func() *debuggedRun {
		mu.Lock()
		defer mu.Unlock()

		return current
	}

	target, err := flowdebug.NewReversible(b.cmd.Context(), func(context.Context) (*flowdebug.Run, error) {
		run := &debuggedRun{done: make(chan struct{}), reported: make(chan struct{})}
		// A replay is silent: the editor was shown that account the first time.
		session, err := b.session(func(text string, _ flowdebug.Tone) {
			if run.reports() {
				b.console.write(text)
			}
		}, false)
		if err != nil {
			return nil, err
		}
		var runCtx context.Context
		runCtx, run.cancel = context.WithCancel(b.cmd.Context())
		initial := !first.Swap(true)
		go func() {
			defer close(run.reported)
			if initial {
				select {
				case <-started:
				case <-runCtx.Done():
					close(run.done)

					return
				}
			}
			finish := b.execute(runCtx, session, run.reports)
			close(run.done)
			finish()
		}()

		return &flowdebug.Run{
			Session: session,
			Live: func() {
				run.live.Store(true)
				mu.Lock()
				current = run
				mu.Unlock()
			},
			Stop: func() {
				// Cancelled before the session is released, which would
				// otherwise let the run carry on through its remaining steps.
				run.stopped.Store(true)
				run.cancel()
				_ = session.Close()
				<-run.done
			},
		}, nil
	})
	if err != nil {
		return nil, err
	}

	return &flowdap.Launch{
		Target:    target,
		SourceMap: b.sourceMap,
		// Releases the first run, and then waits for the run in front to finish
		// if the editor detaches, as a launch that ran once does.
		Start: func() {
			close(started)
			for {
				run := shown()
				<-run.reported
				if shown() == run {
					return
				}
			}
		},
		// Ends the run without waiting for it: the caller holds the adapter's
		// ordering lock, which the run's own report of its end needs.
		Terminate: func() {
			if run := shown(); run != nil {
				run.cancel()
			}
		},
	}, nil
}

// attachDebuggedRun attaches to a durable run through the server this command
// was pointed at, with the caller's own credentials.
//
// The configuration's `program`, when it names one, is compiled for its
// source map alone, and the map is used only when it names the program the
// run executes ([flowdebug.Remote.SourceMapVerified]). The compiled program
// records the digest of the bytes it came from ([v1.Workflow.SourceDigest]),
// so a file whose lines moved since the run was submitted names a different
// program, and an attach shows step addresses and answers line breakpoints
// unverified rather than put them on the wrong lines.
func attachDebuggedRun(ctx context.Context, cmd *cobra.Command, args flowdap.AttachArguments) (*flowdap.Attachment, error) {
	var sourceMap *v1.DebugSourceMap
	if args.Program != "" {
		workflow, source, err := loadMappedWorkflow(args.Program)
		if err != nil {
			// Diagnostics withheld, as a launch withholds them without a
			// reveal: the file is read only for its lines, and an invalid
			// one has no trusted declarations saying which of its values may
			// be shown. Any other failure, a file that cannot be read, says
			// what it is.
			if _, diagnostics := errors.AsType[flowfile.Diagnostics](err); !diagnostics {
				return nil, fmt.Errorf("flowdap: reading the attach configuration's `program`: %w", err)
			}
			return nil, fmt.Errorf("flowdap: the attach configuration's `program` %s does not compile; "+
				"run `flow validate` on it, or leave `program` out to attach without lines", args.Program)
		}
		sourceMap = source.sourceMap(workflow)
	}

	remote, _, err := flowdebug.AttachRemote(ctx, newWorkflowServiceClient(serverFlagsOf(cmd)),
		args.WorkflowID, args.RunID, flowdebug.RemoteOptions{SessionID: args.SessionID, SourceMap: sourceMap})
	if err != nil {
		return nil, fmt.Errorf("flowdap: attaching to %s: %w", args.WorkflowID, err)
	}

	attachment := &flowdap.Attachment{Target: remote}
	if remote.SourceMapVerified() {
		attachment.SourceMap = sourceMap
	}

	return attachment, nil
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
