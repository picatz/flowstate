package flowstatev1

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"
)

// RunObserver receives the local driver's own account of a run as it happens:
// each step's outcome the moment it is recorded, each skip the moment its
// `if:` decides it, and each wait the moment it parks. It exists so a harness
// can show an author what a run *did* — `flow test`'s failure transcript
// (issue #929) is the first reader — without a second, parallel bookkeeping of
// facts the engine already decides.
//
// It is deliberately an account, not a control surface: an observer returns
// nothing, so it cannot pause, reorder, or change what the run does. The
// step-debugger design (issue #928) builds its record-and-replay on this same
// stream precisely because it is read-only — recording is safe to leave on.
//
// What it can still cost is time and a panic, because the callbacks are
// synchronous and run on the step's own goroutine (below). This type is
// exported, so the implementation may be an embedder's rather than this
// repository's, and an account of the work must never be the reason the work
// does not happen — the same rule [telemetryResource] states for a resource
// detector whose entropy source is unavailable. A panic in an observer is
// therefore recovered and dropped rather than unwinding a run that was
// otherwise succeeding. Time is not recovered from and deliberately so: a
// callback that blocks forever is a bug in the observer that a silent timeout
// would hide, and this driver has no clock of its own to bound it against.
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
	//
	// outputs is the observer's own copy, cloned before the callback, and err
	// is a snapshot carrying the failure's rendered text rather than the live
	// value the run propagates: the run's transcript holds the originals, and
	// an account that could write back into either — mutating cloned outputs,
	// or type-asserting a *TaskError and editing its fields — would not be
	// read-only, it would be a second author of the run's own record.
	StepFinished(id string, outputs *Node_Outputs, err error, tolerated bool)

	// StepSkipped reports a step whose `if:` evaluated false. It is the one
	// fact the transcript cannot carry — a skipped step records nothing — and
	// the reason `expect.skipped` claims are otherwise checked by absence.
	StepSkipped(id string)

	// WaitStarted reports a wait at the moment the driver commits to
	// waiting: the signal name it waits for, or "" for a plain timer
	// (`sleep:`/`wait_until:`), with the resolved timeout. bounded is false
	// for a signal wait with no timeout — a wait that only a delivery can
	// end. A wait that resolves without parking reports nothing: a
	// non-positive duration, or a delivery already in hand where the
	// [SignalWaiter] can report one preflight — which [LocalSignals], the
	// waiter every `flow test` case runs under, always can.
	//
	// The boundary of that claim is the waiter's, not this contract's: a
	// custom [SignalWaiter] holding a buffered delivery this driver cannot
	// see may answer the instant after this fires, and then the "wait" it
	// reported ended at once — the same boundary the local wait announcement
	// beside it has always had, since neither can ask a waiter what it will
	// do without an interface for asking.
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

// observeSafely runs one observer callback, dropping a panic it raises.
//
// The recover is the whole of the isolation, and it is here rather than at
// each call site so that no future observation point can forget it. See
// [RunObserver] for why an account may not take down the run it describes;
// the value is discarded rather than logged because this package has no
// logger of its own on this path, and the alternative — a diagnostic emitted
// from inside a diagnostic — is how one bad observer becomes two problems.
func observeSafely(call func()) {
	defer func() { _ = recover() }()

	call()
}

func observeStepFinished(ctx context.Context, id string, outputs *Node_Outputs, err error, tolerated bool) {
	observer := RunObserverFromContext(ctx)
	if observer == nil {
		return
	}

	// Cloned so the read-only contract is structural rather than polite: the
	// pointer recordStepOutcome just stored IS what later expressions and the
	// run's final outputs read, and an observer that edited it would make an
	// observed run differ from an unobserved one — the one thing an account
	// must never do. Paid only when someone is listening.
	//
	// The error snapshots for the same reason: the live value is commonly a
	// mutable *TaskError the run is about to propagate, and an observer that
	// type-asserted and edited its fields would be editing the run's own
	// verdict. The snapshot carries the rendered text — the whole of what an
	// account renders; a future reader needing the classification gets it as
	// its own immutable parameter, never the live object.
	var copied *Node_Outputs
	if outputs != nil {
		copied = proto.Clone(outputs).(*Node_Outputs)
	}
	snapshot := err
	if err != nil {
		snapshot = errors.New(err.Error())
	}
	observeSafely(func() { observer.StepFinished(id, copied, snapshot, tolerated) })
}

func observeStepSkipped(ctx context.Context, id string) {
	if observer := RunObserverFromContext(ctx); observer != nil {
		observeSafely(func() { observer.StepSkipped(id) })
	}
}

func observeWaitStarted(ctx context.Context, id, signal string, timeout time.Duration, bounded bool) {
	if observer := RunObserverFromContext(ctx); observer != nil {
		observeSafely(func() { observer.WaitStarted(id, signal, timeout, bounded) })
	}
}
