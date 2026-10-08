package main

import (
	"context"
	"sync"
	"sync/atomic"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// stubbedCase is one stubbed test case a host runs under a
// [flowdebug.Reversible]: the launch every such host shares, so a rewind
// re-executes the case the same way whether a model, an editor or a terminal
// asked for it.
type stubbedCase struct {
	// Program and Steps are what the session judges a target against.
	Program *v1.Workflow
	Steps   []flowdebug.Step

	// RevealSensitive is the explicit opt-in every session of the case is
	// built with, replays included.
	RevealSensitive bool

	// Speak receives the account of the run being shown. A replay is silent
	// until it replaces the run before it: the caller was shown that account
	// the first time.
	Speak func(text string, tone flowdebug.Tone)

	// Shown, when set, is told which session is the one shown each time a run
	// becomes it: the first run at once, a replay when it replaces the run
	// before it. A host that sends a line to the session the person is looking
	// at (`quit`) keeps the latest.
	Shown func(*flowdebug.Session)

	// Run executes the case with the debugger it is handed.
	Run func(ctx context.Context, debugger v1.Debugger) flowtest.RunResult

	// Failure, when set, words why a report that did not pass failed, for the
	// session's account of the end of the run. Defaults to [caseFailure].
	Failure func(*v1.TestReport) error

	// Finish settles the case with the verdict of the run that is shown. It is
	// called at most once by the host's own guard, and may be called from the
	// run's goroutine.
	Finish func(*v1.TestReport)
}

// launcher is the [flowdebug.Launcher] for the case. Each call starts one run
// in its own goroutine under runCtx; the first is the initial run.
func (c stubbedCase) launcher(runCtx context.Context) flowdebug.Launcher {
	var launched atomic.Bool

	return func(context.Context) (*flowdebug.Run, error) {
		run := &stubbedRun{done: make(chan struct{}), initial: !launched.Swap(true), finish: c.Finish}
		session, err := flowdebug.New(flowdebug.Options{
			Controlled: true, Workflow: c.Program, Steps: c.Steps, RevealSensitive: c.RevealSensitive,
			Emit: func(text string, tone flowdebug.Tone) {
				if run.speaks() {
					c.Speak(text, tone)
				}
			},
		})
		if err != nil {
			return nil, err
		}
		caseCtx, cancelCase := context.WithCancel(runCtx)
		go func() {
			defer close(run.done)
			defer cancelCase()

			result := c.Run(caseCtx, session)
			if testReportFailed(result.Report) {
				failure := caseFailure
				if c.Failure != nil {
					failure = c.Failure
				}
				session.Finished(failure(result.Report))
			} else {
				session.Finished(nil)
			}
			_ = session.Close()
			run.ended(result.Report)
		}()

		return &flowdebug.Run{
			Session: session,
			Live: func() {
				run.shown()
				if c.Shown != nil {
					c.Shown(session)
				}
			},
			Stop: func() {
				// Cancelled before the session is released, which would
				// otherwise let the run carry on through its remaining steps.
				run.stopped.Store(true)
				cancelCase()
				_ = session.Close()
				<-run.done
			},
		}, nil
	}
}

// stubbedRun is one run of a stubbed case under a [flowdebug.Reversible]: the
// first, or a replay that a rewind has made, or is making, the one shown.
type stubbedRun struct {
	done    chan struct{}
	initial bool
	stopped atomic.Bool
	finish  func(*v1.TestReport)

	mu       sync.Mutex
	live     bool
	finished bool
	report   *v1.TestReport
}

// speaks is whether this run's account is the caller's: the first run's
// from its first word, a replay's only once it has replaced the run before it.
func (r *stubbedRun) speaks() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return (r.live || r.initial) && !r.stopped.Load()
}

// shown is [flowdebug.Run.Live]. A run that had already ended when it became
// the one shown ends the case now.
func (r *stubbedRun) shown() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.live = true
	if r.finished && !r.stopped.Load() {
		r.finish(r.report)
	}
}

// ended records the run's verdict, which is the case's only if the run is the
// one shown and no rewind has stopped it: a replay that ends before it is shown
// was never the caller's, and a run a rewind cancelled ends in the
// cancellation, not in a verdict.
func (r *stubbedRun) ended(report *v1.TestReport) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.finished, r.report = true, report
	if r.live && !r.stopped.Load() {
		r.finish(report)
	}
}
