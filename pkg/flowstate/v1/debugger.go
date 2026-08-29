package flowstatev1

import (
	"context"
	"errors"
)

// ErrDebugSessionEnded is returned by a [Debugger] that abandoned the run it
// was holding, rather than by a run that failed on its own.
//
// Here rather than in the debugger's own package, for the reason CLAUDE.md
// gives about a value with one meaning written down twice: a harness judging a
// run and a session ending one must agree about what "abandoned" is, and both
// already import this package — a harness importing the debugger to learn it
// would be the coupling capability discovery exists to avoid.
//
// The distinction is load-bearing. An abandoned run and a failed one read
// identically as "the run returned an error", so a case declaring
// `expect.failed: true` would otherwise be *satisfied* by a debugger quitting
// before the run reached the failure it named — a debugger turning a red case
// green, which is the one thing a debugger must never do (Codex, #1109).
var ErrDebugSessionEnded = errors.New("the debug session ended this run")

// Debugger decides when the local driver may run a step, one step boundary at
// a time.
//
// It is the control seam the step debugger (issue #928) runs through, and the
// deliberate counterpart to [RunObserver]: an observer is handed an account of
// what already happened and can change nothing, while a debugger is asked
// before anything happens and can hold the run there for as long as it likes.
// Every interactive verb slice 1 names — break, step, continue, until — is
// that one power plus bookkeeping the engine never sees. The engine knows
// nothing about breakpoints; it asks whether it may proceed, and waits for the
// answer.
//
// Nothing installs one outside a debugging session, so the boundary pays one
// context lookup and a nil check per step, against work the driver already
// does per step. That is the whole performance story, and it is why this lives
// at the boundary rather than inside the step: a debugger must never be a tax
// on running a step.
//
// # Why the interface is named for its only implementer
//
// The obvious name for "the thing that decides whether a step may proceed" is
// a gate, and this package already spends that word: an approval gate is a
// `wait_for_signal:` a person releases, and [Signal.Prompt] is the sentence it
// asks (see waitprompt.go). A second meaning of gate here would make "the gate
// held the run" ambiguous between a person deciding and a debugger pausing.
//
// Naming the interface after the one thing that may legitimately implement it
// also says what it is not: a general-purpose interception hook. #928's own
// constraint is that a debugger must not become a second execution model, and
// an interface called Debugger is harder to quietly grow into one than an
// interface called Hook.
//
// # Local driver only, like [Scheduler] and [RunObserver]
//
// Pausing a durable run is a different mechanism for a different reason — the
// durable interpreter suspends against its step budget and resumes from
// history, and a per-process callback would hold one worker's goroutine while
// the run itself is free to continue on another. Slice 2 of #928 is that
// design; this seam is not it, and must not be made to look like it.
//
// # What an implementation may and may not do
//
// It may block, and it may refuse: returning a non-nil error from BeforeStep
// stops the run there, which is how a session quits without running the rest
// of the workflow. What it must not do is edit the scope it is handed. That is
// a contract rather than a structure, and the difference from [RunObserver] —
// which clones what it hands out — is a cost stated rather than an oversight:
//
//   - `inspect <expr>` has to answer about *this* run, so it evaluates against
//     the run's own activation. A cloned scope would answer, accurately, about
//     a copy, which is the one thing a debugger may not do.
//   - A structural guarantee would be theatre besides. Something that can
//     abort the run outright is already a control surface; protecting the
//     scope from it while handing it that power would describe a boundary that
//     is not there.
type Debugger interface {
	// BeforeStep is called at each step boundary in the written position of
	// the step, after its `if:` has decided the step runs and before any of
	// its work happens — including before an `async:` step launches, so a
	// session sees the step where its author wrote it rather than where its
	// result is heard.
	//
	// It blocks for as long as the session holds the run. Under a
	// [VirtualClock] that pause also holds virtual time, but not by
	// accident and not by anything this seam does: the run's whole-run
	// participant ([EnterClockForWholeRun]) stays registered while blocked
	// here and is not parked on a timer, and [VirtualClock] advances only
	// once every registered participant is parked. So a scripted delivery
	// due at t=5m does not arrive at a run paused at t=0s — provided the
	// run is a participant, which is the same precondition
	// [NewVirtualClock] states for any first waiter.
	//
	// A non-nil error stops the run at this step. ctx is the step's own
	// context, so an implementation that wants to be interruptible should
	// respect its cancellation.
	BeforeStep(ctx context.Context, node *Node, scope *Scope) error
}

type debuggerKey struct{}

// NewContextWithDebugger installs a debugger for every step the local driver
// runs under this context — including loop bodies, parallel branches, switch
// bodies, and called workflows, which all descend from it.
func NewContextWithDebugger(ctx context.Context, debugger Debugger) context.Context {
	return context.WithValue(ctx, debuggerKey{}, debugger)
}

// DebuggerFromContext returns the context's debugger, or nil when none is
// installed — the ordinary case for every run that is not being debugged.
func DebuggerFromContext(ctx context.Context) Debugger {
	debugger, _ := ctx.Value(debuggerKey{}).(Debugger)
	return debugger
}

// debuggerBeforeStep is the engine's call-site spelling: nil-safe, so a run
// nobody is debugging pays one context lookup and nothing else.
//
// A panic is deliberately *not* recovered here, which is the opposite of
// [observeSafely] and for the opposite reason. An observer is a bystander, so
// a bug in one must not take down a run that was otherwise succeeding; a
// debugger is the thing deciding whether the run proceeds, and a run whose
// debugger has panicked has no answer to that question. Continuing as though
// it had said yes would silently run the steps a session was holding back.
func debuggerBeforeStep(ctx context.Context, node *Node, scope *Scope) error {
	debugger := DebuggerFromContext(ctx)
	if debugger == nil {
		return nil
	}

	return debugger.BeforeStep(ctx, node, scope)
}
