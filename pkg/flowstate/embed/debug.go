package embed

import (
	"context"
	"fmt"
	"sync"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// DebugOptions configures one [Debug] call: everything [RunOptions] does, and
// how the session starts.
type DebugOptions struct {
	RunOptions

	// Breakpoints are armed before the run's first step, with the same
	// grammar and rules [flowdebug.Target.ReplaceBreakpoints] applies.
	Breakpoints []*v1.DebugBreakpoint

	// FailureMode selects which step failures stop the run.
	FailureMode v1.DebugFailureMode

	// Continue starts the run running: it stops only at a breakpoint, a
	// failure stop, or a pause. The zero value holds it at its first step.
	Continue bool

	// SourceMap relates the workflow's steps to their source, for frames and
	// line breakpoints. Optional.
	SourceMap *v1.DebugSourceMap

	// Output receives the session's narration: each stop, each step's
	// outcome, logpoints, and task notes. Nil discards it; the same account
	// is in each snapshot's observations.
	Output func(text string)
}

// Debugging is one local run under a debugger: a [flowdebug.Target] an
// embedding program drives in-process — no listener, no CLI, no
// serialization — and the run's own result once it finishes.
//
// It is the same session `flow run local --debug`, `flow dap` and the MCP
// sessions drive, so everything a snapshot says here means what it says
// there.
type Debugging struct {
	// Session is the debug session. It is a [flowdebug.Target].
	*flowdebug.Session

	cancel context.CancelFunc
	done   chan struct{}

	mu      sync.Mutex
	outputs *v1.Workflow_StepOutputs
	err     error
}

// Debug starts workflow under a debugger, in this process, with the same
// registry, egress, secret and clock rules as [RunLocal], and returns once the
// run is under way. Unless opts.Continue is set, the run holds at its first
// step until told to move.
//
// Cancel ctx, or call [Debugging.Cancel], to end the run; [Debugging.Close]
// detaches the debugger and lets it finish on its own.
func Debug(ctx context.Context, workflow *Workflow, opts DebugOptions) (*Debugging, error) {
	runCtx, err := localContext(ctx, workflow, opts.RunOptions, "Debug")
	if err != nil {
		return nil, err
	}

	session, err := flowdebug.New(flowdebug.Options{
		Controlled: true,
		Continue:   opts.Continue,
		Workflow:   workflow,
		SourceMap:  opts.SourceMap,
		Emit: func(text string, _ flowdebug.Tone) {
			if opts.Output != nil {
				opts.Output(text)
			}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("flowstate/embed: Debug: %w", err)
	}

	if len(opts.Breakpoints) > 0 || opts.FailureMode != v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNSPECIFIED {
		response, err := session.ReplaceBreakpoints(ctx, &v1.DebugSetBreakpointsRequest{
			Breakpoints: opts.Breakpoints, FailureMode: opts.FailureMode,
		})
		if err != nil {
			_ = session.Close()

			return nil, fmt.Errorf("flowstate/embed: Debug: %w", err)
		}
		for i, state := range response.GetBreakpoints() {
			if !state.GetVerified() {
				_ = session.Close()

				return nil, fmt.Errorf("flowstate/embed: Debug: breakpoint %d (%s): %s", i, state.GetId(), state.GetMessage())
			}
		}
	}

	runCtx, cancel := context.WithCancel(runCtx)
	debugging := &Debugging{Session: session, cancel: cancel, done: make(chan struct{})}

	go func() {
		defer close(debugging.done)
		defer cancel()

		runCtx = v1.NewContextWithDebugger(runCtx, session)
		runCtx = v1.NewContextWithRunObserver(runCtx, session)
		outputs, err := v1.RunWithInputs(runCtx, workflow, v1.NewNamedValues(opts.Inputs))
		session.Finished(err)

		debugging.mu.Lock()
		debugging.outputs, debugging.err = outputs, err
		debugging.mu.Unlock()
	}()

	return debugging, nil
}

// Driver returns a [flowdebug.Driver] over this session, for driving it with
// the debugger's command lines.
func (d *Debugging) Driver() *flowdebug.Driver {
	return flowdebug.NewDriver(d.Session)
}

// Wait blocks until the run finishes or ctx ends, and returns what [RunLocal]
// would have. A run held at a stop does not finish until something moves it.
func (d *Debugging) Wait(ctx context.Context) (*v1.Workflow_StepOutputs, error) {
	select {
	case <-d.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	return d.outputs, d.err
}

// Cancel ends the run, as cancelling the context [Debug] was given does.
func (d *Debugging) Cancel() {
	d.cancel()
}

// Close detaches the debugger: breakpoints stop holding and the run finishes
// on its own. It does not wait for the run; [Debugging.Wait] does.
func (d *Debugging) Close() error {
	return d.Session.Close()
}
