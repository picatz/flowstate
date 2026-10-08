package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// reversibleFront drives one stubbed test case from a terminal through a
// [flowdebug.Driver] over a [flowdebug.Reversible], which is what makes `back`
// and `reverse-continue` real there.
//
// The prompt of [flowdebug.Session] cannot do it. The run is parked inside the
// session, waiting for the line the prompt is about to read, and a rewind ends
// with stopping that run and waiting for it: executed from the prompt's own
// goroutine, it would wait for itself. So the loop lives outside every run,
// the way the editor's and the model's fronts already do.
type reversibleFront struct {
	// Path is the test file, and Run its options without a debugger: each run
	// the case makes, the first and every replay, gets its own.
	Path string
	Run  flowtest.RunOptions

	// Steps are the ids `break` and `until` complete over.
	Steps []flowdebug.Step

	// Next reads one line: [io.EOF] at the end of input, and
	// [flowdebug.ErrConsoleInterrupted] when the person interrupted. Out is
	// where the account of the shown run and the answer to each line are
	// written. With a Console, lines are read from it and Next is ignored.
	Next func() (string, error)
	Out  io.Writer

	// Console, when set, is the terminal line editor the lines come from, and
	// Panes the scope and step panes drawn beside it.
	Console *debugConsole
	Panes   *debugPanes
	Theme   ui.Theme

	// Emit receives the shown run's account. Defaults to writing it to Out.
	Emit func(text string, tone flowdebug.Tone)

	// Record, when set, collects the lines the run accepted.
	Record *attachRecording

	// Prompt is written before each read.
	Prompt string

	// mu serialises writes to Out: the shown run narrates from its own
	// goroutine while the prompt loop answers from this one.
	mu sync.Mutex

	// The line reader: one goroutine asked for one line at a time, so a
	// cancelled run leaves at most the read the terminal itself cannot abandon.
	readerOnce sync.Once
	wants      chan struct{}
	lines      chan readLine
	stop       chan struct{}
	readErr    error
}

// readLine is one answer from the reader goroutine.
type readLine struct {
	text string
	err  error
}

// write is the one way text reaches Out.
func (f *reversibleFront) write(text string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, _ = io.WriteString(f.Out, text)
}

// run plays the case at the prompt and returns its result.
func (f *reversibleFront) run(ctx context.Context) (flowtest.RunResult, error) {
	var (
		results  sync.Map // *v1.TestReport -> flowtest.RunResult
		verdict  = make(chan flowtest.RunResult, 1)
		shown    atomic.Pointer[flowdebug.Session]
		settleAt sync.Once
		first    atomic.Bool
	)
	emit := f.Emit
	if emit == nil {
		emit = func(text string, _ flowdebug.Tone) { f.write(text) }
	}

	launch := stubbedCase{
		Steps: f.Steps,
		Speak: emit,
		Shown: func(session *flowdebug.Session) {
			shown.Store(session)
			// A replay that was stopped will never be asked for its verdict, and
			// each step or rewind makes another: only the shown run's is kept.
			results.Clear()
			f.Panes.setSession(session)
		},
		Run: func(ctx context.Context, debugger v1.Debugger) flowtest.RunResult {
			// The first run's panes are wired before it can reach its first
			// stop: it is the shown run from the start, and the host is told so
			// only after it is already running.
			if session, ok := debugger.(*flowdebug.Session); ok && first.CompareAndSwap(false, true) {
				f.Panes.setSession(session)
			}
			opts := f.Run
			opts.Debugger = debugger
			result := flowtest.RunPath(ctx, f.Path, opts)
			results.Store(result.Report, result)

			return result
		},
		Finish: func(report *v1.TestReport) {
			settleAt.Do(func() {
				if result, ok := results.LoadAndDelete(report); ok {
					verdict <- result.(flowtest.RunResult)
				} else {
					verdict <- flowtest.RunResult{Report: report}
				}
			})
		},
	}.launcher(ctx)

	reversible, err := flowdebug.NewReversible(ctx, launch)
	if err != nil {
		return flowtest.RunResult{}, err
	}
	defer reversible.Stop()
	f.stop = make(chan struct{})
	defer close(f.stop)

	driver := flowdebug.NewDriver(reversible)
	driver.Wait = 30 * time.Second
	if f.Console != nil {
		f.Console.SetCompleter(attachCompleter(ctx, driver))
	}

	// The first stop, or the end of a case with no steps to hold at. after is
	// the revision the run ended at, once it has: what follows is the case's
	// verdict, or an autopsy, which holds a failed case for questions.
	var after uint64
	ended := false
	for {
		snapshot, err := reversible.WaitSnapshot(ctx, after)
		if err != nil {
			return flowtest.RunResult{}, err
		}
		after = snapshot.GetRevision()
		if terminalDebugState(snapshot.GetState()) {
			ended = true

			break
		}
		if snapshot.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_RUNNING {
			break
		}
	}
	for {
		if ended {
			result, autopsy, err := awaitEnd(ctx, reversible, after, verdict)
			if err != nil || !autopsy {
				return result, err
			}
			ended = false
		}
		if f.Console == nil && f.Prompt != "" {
			f.write(f.Prompt)
		}
		text, err := f.read(ctx)
		if ctx.Err() != nil {
			return flowtest.RunResult{}, ctx.Err()
		}
		if errors.Is(err, flowdebug.ErrConsoleInterrupted) {
			// ctrl-C ends the run exactly as `quit` does.
			text = "quit"
		} else if err != nil {
			// The end of input releases the run, as a session with no console
			// does: every stop is resumed until the case ends.
			after, ended = f.release(ctx, driver, after)

			continue
		}
		line := strings.TrimSpace(text)
		if line == "" {
			line = "step"
		}
		if line == "quit" || line == "q" || line == "exit" {
			// The verdict stays the one the prompt has always given: the case
			// ended by the person, which is the session's own to say.
			if session := shown.Load(); session != nil {
				if session.Control(ctx, "quit") == nil && f.Record != nil {
					f.Record.add("quit")
				}
			}
			after, ended = reversibleRevision(ctx, reversible), true

			continue
		}

		result, err := driver.Do(ctx, line)
		if err != nil {
			f.write(fmt.Sprintf("%v\n", err))

			continue
		}
		if notDone(result) == nil && f.Record != nil {
			f.Record.add(line)
		}
		// The shown run narrates a forward movement itself, as it always has;
		// repeating that from the answer would say each stop twice. A rewind is
		// not narrated, because the run it lands on was replayed in silence.
		// A forward verb that was refused (`until nosuch`) narrates nothing, so
		// its explanation is only in the answer.
		if !flowdebug.MovesForward(line) || notDone(result) != nil {
			f.write(result.Text)
			if f.Panes != nil && flowdebug.StepsBack(line) && notDone(result) == nil {
				f.Panes.paint()
			}
		}
		if terminalDebugState(result.Snapshot.GetState()) {
			after, ended = result.Snapshot.GetRevision(), true
		} else if notDone(result) == nil && strings.HasPrefix(line, "detach") {
			// An accepted detach carries no snapshot: the case runs on unattended
			// and its verdict is what is waited for.
			after, ended = reversibleRevision(ctx, reversible), true
		}
	}
}

// awaitEnd waits for what follows a run that ended at revision after: the
// case's verdict, or an autopsy, which holds a failed case at its end so it can
// be questioned (and stepped back from) before the verdict is given. It reports
// autopsy true when the person has a prompt again.
func awaitEnd(ctx context.Context, reversible *flowdebug.Reversible, after uint64, verdict <-chan flowtest.RunResult) (flowtest.RunResult, bool, error) {
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	held := make(chan *v1.DebugSnapshot, 1)
	go func() {
		if snapshot, err := reversible.WaitSnapshot(waitCtx, after); err == nil {
			held <- snapshot
		}
	}()

	select {
	case result := <-verdict:
		return result, false, nil
	case snapshot := <-held:
		if snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD {
			return flowtest.RunResult{}, true, nil
		}
	case <-ctx.Done():
		return flowtest.RunResult{}, false, ctx.Err()
	}

	select {
	case result := <-verdict:
		return result, false, nil
	case <-ctx.Done():
		return flowtest.RunResult{}, false, ctx.Err()
	}
}

// reversibleRevision is the revision the run is at, for waiting on what comes
// after it.
func reversibleRevision(ctx context.Context, reversible *flowdebug.Reversible) uint64 {
	snapshot, err := reversible.Snapshot(ctx)
	if err != nil {
		return 0
	}

	return snapshot.GetRevision()
}

// release resumes the run at every stop until it ends, which is what the end of
// input means to a session, and reports the revision it ended at. Bounded,
// since a program may hold at a loop's every iteration.
func (f *reversibleFront) release(ctx context.Context, driver *flowdebug.Driver, after uint64) (uint64, bool) {
	for range 100_000 {
		result, err := driver.Do(ctx, "continue")
		if err != nil {
			return after, true
		}
		if terminalDebugState(result.Snapshot.GetState()) {
			return result.Snapshot.GetRevision(), true
		}
	}

	return after, true
}

// read is one line of input.
func (f *reversibleFront) read(ctx context.Context) (string, error) {
	if f.readErr != nil {
		return "", f.readErr
	}
	f.readerOnce.Do(func() {
		f.wants = make(chan struct{}, 1)
		f.lines = make(chan readLine, 1)
		go func() {
			for {
				select {
				case <-f.wants:
				case <-f.stop:
					return
				}
				var line readLine
				if f.Console != nil {
					line.text, line.err = f.Console.Prompt()
				} else {
					line.text, line.err = f.Next()
				}
				f.lines <- line
				if line.err != nil && !errors.Is(line.err, flowdebug.ErrConsoleInterrupted) {
					return
				}
			}
		}()
	})
	f.wants <- struct{}{}

	select {
	case line := <-f.lines:
		if line.err != nil && !errors.Is(line.err, flowdebug.ErrConsoleInterrupted) {
			f.readErr = line.err
		}

		return line.text, line.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// play runs the case in the file at path under the front.
func (f *reversibleFront) play(ctx context.Context, path string, opts flowtest.RunOptions) (flowtest.RunResult, error) {
	f.Path, f.Run = path, opts

	return f.run(ctx)
}
