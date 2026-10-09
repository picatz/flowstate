package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/debugtui"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The full-screen debugger over a run in this process: `flow run local
// --debug`, with or without `--reverse`, and `flow test --debug`.
//
// It is the same screen `flow debug attach` opens, through the same
// [showScreen]. What differs is only who owns the run: here this process does,
// so leaving the screen decides how the run ends, and what the run says while
// the screen owns the terminal has to wait for the screen to close.

// maxScreenNarrationBytes bounds what a run may say while the screen is open. A
// workflow that logs in a loop would otherwise grow this process by as much as
// it likes behind a view that does not show the text; past the bound the rest
// is counted rather than kept.
const maxScreenNarrationBytes = 1 << 20

// screenNarration holds the account of a run, its `log:` steps and its
// narration, while the screen owns the terminal. Writing it straight to stderr
// would draw over the screen; the screen shows the run through the target, so
// the text is kept and printed when the terminal is the shell's again.
type screenNarration struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	dropped int
}

// Write keeps p up to the bound and never fails: a run does not stop because
// nobody is reading what it says.
func (n *screenNarration) Write(p []byte) (int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	written := len(p)
	room := max(0, maxScreenNarrationBytes-n.buf.Len())
	if len(p) > room {
		n.dropped += len(p) - room
		p = p[:room]
	}
	n.buf.Write(p)

	// Every byte was accepted, kept or counted, so a caller is not told of a
	// short write.
	return written, nil
}

// flushTo writes what was kept to w, and says how much was not. It empties the
// buffer, so calling it again says nothing twice.
func (n *screenNarration) flushTo(w io.Writer) {
	n.mu.Lock()
	kept, dropped := n.buf.Bytes(), n.dropped
	n.buf, n.dropped = bytes.Buffer{}, 0
	n.mu.Unlock()

	_, _ = w.Write(kept)
	if dropped > 0 {
		fmt.Fprintf(w, "(%d more bytes of the run's account were not kept while the screen was open)\n", dropped)
	}
}

// runUnderScreen runs a program in this process under a [flowdebug.Session] the
// screen drives, and returns what the run returned.
//
// The run is on its own goroutine because the screen holds this one. How the
// person leaves decides how the run ends, as it does at the line editor: ctrl-C
// ends the run the way `quit` does, and every other exit (`detach`, `q`, ctrl-D,
// a lost terminal) lets it finish unattended without the debugger. A run that
// is already over is simply waited for.
func runUnderScreen(
	ctx context.Context,
	in io.Reader,
	surface *ui.UI,
	session *flowdebug.Session,
	over screenOver,
	run func(context.Context) (*v1.Workflow_StepOutputs, error),
) (*v1.Workflow_StepOutputs, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type finished struct {
		outputs *v1.Workflow_StepOutputs
		err     error
	}
	done := make(chan finished, 1)
	go func() {
		outputs, err := run(runCtx)
		session.Finished(err)
		done <- finished{outputs, err}
	}()

	over.Target, over.Driver = session, flowdebug.NewDriver(session)
	over.Capabilities, over.Local = session.Capabilities(), true

	outcome, err := showScreen(runCtx, in, surface, over)
	switch {
	case err != nil:
		cancel()
		<-done

		return nil, err
	case outcome == debugtui.OutcomeInterrupt:
		endRun(session, cancel)
	default:
		_ = session.Close()
	}

	result := <-done

	return result.outputs, result.err
}

// endRun ends the run the debugger holds, as `quit` does at the prompt. The
// session takes the command at its next boundary; a run that is mid-step and
// does not reach one in time is cancelled instead, so ctrl-C cannot wait on a
// task.
func endRun(session *flowdebug.Session, cancel context.CancelFunc) {
	quitCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()

	if err := session.Control(quitCtx, "quit"); err != nil {
		cancel()
	}
}

// screenFront is the reversible front `flow test --debug` plays a case under
// when the full-screen debugger is open, and the function that prints what the
// case said once the screen has closed.
//
// The workflow at path names the steps the screen offers. A case's stubs replace
// what a step does, never which steps there are, so the inventory is the one
// the run reaches; a workflow that does not parse offers none, and the run is
// about to say why.
func screenFront(cmd *cobra.Command, surface *ui.UI, path string) (*reversibleFront, func()) {
	account := &screenNarration{}
	steps := workflowStepList(path)

	front := &reversibleFront{
		Steps:  steps,
		Out:    account,
		Emit:   debugEmitter(account, surface.Theme),
		Prompt: flowdebug.Prompt,
		Theme:  surface.Theme,
	}
	front.Screen = func(ctx context.Context, shown screenOver) (debugtui.Outcome, error) {
		shown.Frames = flowdebug.FrameOptions{Inventory: steps}
		shown.Recording = front.Record
		if shown.Recording == nil {
			shown.Recording = &attachRecording{}
		}

		return showScreen(ctx, cmd.InOrStdin(), surface, shown)
	}

	return front, func() { account.flushTo(surface.Out) }
}
