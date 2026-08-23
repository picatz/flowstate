package flowstatev1

import (
	"context"
	"time"
)

// RunObserver receives the local driver's own account of a run as it happens:
// each step's outcome the moment it is recorded, each skip the moment its
// `if:` decides it, and each wait the moment it parks. It exists so a harness
// can show an author what a run *did* — `flow test`'s failure transcript
// (issue #929) is the first reader — without a second, parallel bookkeeping of
// facts the engine already decides.
//
// It is deliberately an account, not a control surface: an observer cannot
// pause, reorder, or change anything, and the engine never waits on one. The
// step-debugger design (issue #928) builds its record-and-replay on this same
// stream precisely because it is read-only — recording is safe to leave on.
//
// # Local driver only, like [Scheduler]
//
// Nothing here runs under the durable driver, and that is a boundary rather
// than a gap: a durable run's account of record is Temporal history, written
// by the server, and a per-process callback would see one worker's slice of a
// run that may hop workers across a Continue-As-New. The surfaces that read
// this observer (`flow test`, the local debugger) run the local driver by
// design (#155); the durable equivalents read history and queries. The
// both-drivers rule in CLAUDE.md governs what a *workflow* can observe, and no
// workflow can observe its observer.
//
// Callbacks arrive on the goroutine running the step, so an implementation
// that stores events must synchronize itself if the workflow has `parallel:`
// branches or `async:` steps — this driver may still interleave goroutines at
// yield points even though it runs branches in written order.
type RunObserver interface {
	// StepFinished reports one step's recorded outcome: outputs as they enter
	// the transcript (the failure record, for a failed step), err when the
	// step failed, and tolerated when `continue_on_error:` absorbed that
	// failure. It fires at the same single point the transcript itself is
	// written — recordStepOutcome and its loop-body equivalents — so the
	// account and the record cannot disagree about what a step produced.
	StepFinished(id string, outputs *Node_Outputs, err error, tolerated bool)

	// StepSkipped reports a step whose `if:` evaluated false. It is the one
	// fact the transcript cannot carry — a skipped step records nothing — and
	// the reason `expect.skipped` claims are otherwise checked by absence.
	StepSkipped(id string)

	// WaitStarted reports a wait the moment it parks: the signal name it
	// waits for, or "" for a plain timer (`sleep:`/`wait_until:`), with the
	// resolved timeout. bounded is false for a signal wait with no timeout —
	// a wait that only a delivery can end. A wait that resolves without
	// parking (a non-positive duration) reports nothing.
	WaitStarted(id string, signal string, timeout time.Duration, bounded bool)
}

type runObserverKey struct{}

// NewContextWithRunObserver installs an observer for every step the local
// driver runs under this context — including loop bodies, parallel branches,
// switch bodies, and called workflows, which all descend from it.
func NewContextWithRunObserver(ctx context.Context, observer RunObserver) context.Context {
	return context.WithValue(ctx, runObserverKey{}, observer)
}

// RunObserverFromContext returns the context's observer, or nil when none is
// installed — the ordinary case for every run outside a harness.
func RunObserverFromContext(ctx context.Context) RunObserver {
	observer, _ := ctx.Value(runObserverKey{}).(RunObserver)
	return observer
}

// observeStepFinished, observeStepSkipped and observeWaitStarted are the
// engine's call sites' spelling: nil-safe, so the hot path pays one context
// lookup and nothing else when no harness is listening.
func observeStepFinished(ctx context.Context, id string, outputs *Node_Outputs, err error, tolerated bool) {
	if observer := RunObserverFromContext(ctx); observer != nil {
		observer.StepFinished(id, outputs, err, tolerated)
	}
}

func observeStepSkipped(ctx context.Context, id string) {
	if observer := RunObserverFromContext(ctx); observer != nil {
		observer.StepSkipped(id)
	}
}

func observeWaitStarted(ctx context.Context, id, signal string, timeout time.Duration, bounded bool) {
	if observer := RunObserverFromContext(ctx); observer != nil {
		observer.WaitStarted(id, signal, timeout, bounded)
	}
}
