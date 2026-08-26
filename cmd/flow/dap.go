package main

import (
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
	return &cobra.Command{
		Use:   "dap",
		Short: "Debug a workflow from an editor, over the Debug Adapter Protocol",
		Long: "Speak the Debug Adapter Protocol on stdin and stdout, so an editor's step and " +
			"continue buttons drive a real local run.\n\n" +
			"The workflow to run comes from the client's launch configuration, as `program`, " +
			"so one adapter serves whatever the editor points it at.\n\n" +
			"Breakpoints are step ids rather than source lines. The debugger is handed steps " +
			"and not files, so there is no line to break on — set them as *function* " +
			"breakpoints named after a step. A line breakpoint is answered, unverified, " +
			"saying so.",
		Args: cobra.NoArgs,
		RunE: runDAP,
		Example: `# What an editor's launch configuration runs, rather than a person:
flow dap

# The terminal debugger, for a person:
flow run local --debug examples/hello-world/workflow.yaml`,
	}
}

// runDAP serves one debug session.
func runDAP(cmd *cobra.Command, _ []string) error {
	writeStdioBanner(cmd.ErrOrStderr(), stdinIsInteractive(cmd), dapBanner)

	// Where the session's prose goes once there is a client to send it to.
	//
	// The indirection is the construction order rather than a preference: the
	// server needs the session, so the session cannot be handed the server. A
	// mutex rather than a bare variable because the session emits from the
	// run's goroutine, and "it cannot emit before the run starts" is an
	// argument, not a synchronization edge.
	var console dapConsole

	// Controlled, and with nowhere to type: this session has no console and no
	// stream of commands, because the client is the only thing driving it.
	session, err := flowdebug.New(flowdebug.Options{
		Controlled: true,
		// Discarded rather than written: this process's standard output *is*
		// the protocol stream, and a debugger printing its account onto it
		// would corrupt the conversation. Emit below sends the same fragments
		// to the client's debug console instead, which is where a person is
		// looking anyway.
		Out:  io.Discard,
		Emit: func(text string, _ flowdebug.Tone) { console.write(text) },
	})
	if err != nil {
		return err
	}
	defer func() { _ = session.Close() }()

	server := flowdap.NewServer(session,
		// The same bounded framing the language server reads with. DAP frames
		// exactly as LSP does, and the bound there was measured rather than
		// guessed — see lsp.MaxFrameBytes for the 512 MiB an unbounded header
		// parse cost. A second framer here would be a second place to get that
		// wrong.
		lsp.NewBoundedStream(stdio{}))

	console.attach(server)

	// The run starts when the client has finished configuring and not before.
	// Breakpoints arrive after launch in DAP's own order, so a run started at
	// the launch request is already past the step somebody set one on.
	go func(server *flowdap.Server) {
		select {
		case <-server.Launched():
		case <-cmd.Context().Done():
			return
		}

		defer server.Finished()
		// Releases anything still waiting for a stop that will not come: the
		// adapter cannot see a run end, so this is what says so.
		defer func() { _ = session.Close() }()

		program := server.Program()
		if program == "" {
			server.Output("flowdap: the launch configuration named no `program`, so there is " +
				"no workflow to run\n")

			return
		}

		workflow, _, err := flowfile.ParseFile(program)
		if err != nil {
			// The client's console is the only place a person will look, and
			// the parse error is the whole answer to why nothing ran.
			server.Output(fmt.Sprintf("flowdap: %v\n", err))

			return
		}

		ctx := v1.NewContextWithDebugger(cmd.Context(), session)
		ctx = v1.NewContextWithRunObserver(ctx, session)

		if _, err := v1.RunWithInputs(ctx, workflow, nil); err != nil {
			server.Output(fmt.Sprintf("run failed: %v\n", err))
		}
	}(server)

	return server.Serve(cmd.Context())
}

// dapConsole carries the session's prose to a client that does not exist yet
// when the session is built.
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
