package flowstatev1

import (
	"context"
	"errors"
	"slices"
)

// Which workflow's steps are running, carried on the run's own context.
//
// # Why this is the engine's and not the task runtime's
//
// The fact already existed, and it rode on the wrong thing. `runCall` moves a
// position across a call so that "a consumer of the runtime position [cannot
// confuse] equal step ids in two different workflow files" — but it moved it by
// rewriting [TaskRuntime.Step], and a [TaskRuntime] exists only where secrets
// or workload identity are configured. `cmd/flow` returns the context untouched
// when neither is (`secrets.go`, the `!configured && broker == nil` arm), and
// `flowtest` builds one with an empty `Step`. So on an ordinary `flow run local
// --debug` and on every `flow test --debug` — which is to say on both paths a
// person actually uses — the answer was "no workflow", and the consumer that
// needs it most, the step debugger's pane, could not tell a caller's `build`
// from a callee's (Codex, #1186).
//
// That is a value with one meaning written down in a place only one
// configuration reaches. The workflow being executed is a property of the run,
// so the engine stamps it: [eval] at the run's start and [runCall] at each call
// boundary, with no condition on either. Every driver and every configuration
// therefore has it, and no command has to remember to seed it — which is the
// part that matters, because two commands seeding the same fact is exactly the
// drift this repository has paid for before.
//
// The secret policy's copy is left where it is and set from the same value on
// the adjacent line. One source, two audiences.

// executingWorkflowKey carries the name of the workflow whose steps are
// running.
type executingWorkflowKey struct{}

type executingPosition struct {
	workflow string
	callers  []*DebugStackFrame

	// segments is the dynamic nesting a debugger addresses occurrences by,
	// outermost first. Only recorded while a [Debugger] is installed; see
	// [contextWithSegment].
	segments []*DebugSegment

	// root and here are what a debugger holding a step on this context
	// withholds: the root workflow's declared-sensitive inputs, and those of
	// every workflow called on the way here, the one whose steps are running
	// included. Only recorded while something reads them ([withholdingRead]);
	// see [ExecutingSensitiveFromContext].
	root, here SensitiveValues

	// all is root and here merged, once, where the position is made: every
	// step on it asks, and one set per position — rather than one built per
	// step — is what lets a reader gathering them recognize a set it has
	// already heard ([SensitiveAccumulator], Copilot on #2215).
	all SensitiveValues
}

// contextWithExecutingWorkflow returns ctx carrying name as the workflow whose
// steps are running on it.
//
// Unexported deliberately: this is the engine's own bookkeeping, and a caller
// able to set it could tell a debugger the run is somewhere it is not. The
// engine sets the root here and moves calls through
// [contextWithExecutingCall], both immediately before interpreting that
// workflow.
//
// sensitive is the root's declared-sensitive inputs, as bound, when a
// [Debugger] is installed; see [ExecutingSensitiveFromContext].
func contextWithExecutingWorkflow(ctx context.Context, name string, sensitive SensitiveValues) context.Context {
	return context.WithValue(ctx, executingWorkflowKey{}, executingPosition{workflow: name, root: sensitive, all: sensitive})
}

// contextWithExecutingCall moves execution into callee and records the caller
// frame that reached it. The list is bounded by CheckCallDepth before this is
// called, and copied so a nested call cannot mutate its parent's context.
// sensitive is the callee's own declared-sensitive inputs, as bound, when a
// [Debugger] is installed.
func contextWithExecutingCall(ctx context.Context, callerStep, callerKind, callee string, sensitive SensitiveValues) context.Context {
	position, _ := ctx.Value(executingWorkflowKey{}).(executingPosition)
	callers := append([]*DebugStackFrame(nil), position.callers...)
	callers = append(callers, &DebugStackFrame{
		Workflow: position.workflow,
		StepId:   callerStep,
		Kind:     callerKind,
	})

	segments := position.segments
	if DebuggerFromContext(ctx) != nil && len(segments) < MaxDebugSegments {
		segments = append(slices.Clip(segments), &DebugSegment{
			Kind:     DebugSegmentKind_DEBUG_SEGMENT_KIND_CALL,
			StepId:   callerStep,
			Workflow: position.workflow,
			Callee:   callee,
		})
	}

	// Every caller's as well as the callee's own: a value a middle workflow
	// declared sensitive and forwarded under a plain name stays withheld
	// below it (Codex, #2209), as the durable driver withholds it.
	here := position.here.Merge(sensitive)

	return context.WithValue(ctx, executingWorkflowKey{}, executingPosition{
		workflow: callee,
		callers:  callers,
		segments: segments,
		root:     position.root,
		here:     here,
		all:      position.root.Merge(here),
	})
}

// ExecutingSensitiveFromContext is what a debugger holding a step on ctx
// withholds from what it shows: the root workflow's declared-sensitive inputs
// and those of every workflow called on the way there, the one whose steps
// are running included (#2208). It is the durable driver's rule
// (engine/debugsession.go, sensitiveAt): sensitivity belongs to a value's
// origin, so a value the root or a middle workflow declared sensitive is
// withheld in whatever it is passed to, and a callee's own declarations reach
// no caller's redactor any other way.
//
// Empty where no [Debugger] was installed when the workflow began, or the
// engine never ran.
func ExecutingSensitiveFromContext(ctx context.Context) SensitiveValues {
	position, _ := ctx.Value(executingWorkflowKey{}).(executingPosition)

	return position.all
}

// WithFailureSensitiveValues returns err carrying sensitive beside it: what a
// called workflow, and every workflow on the way to it, declared sensitive,
// for a debugger rendering the failure where the callee's position is gone
// (#2210). A failure raised inside a callee is reported by its caller, in the
// caller's step outcome and in the run's final message, and its text can quote
// a value only the callee declared sensitive: `no such key: <api_key>`. The
// caller's position knows nothing of the callee's declarations, so the failure
// has to say what its own text may hold.
//
// The error's text, and everything [errors.Is] and [errors.As] find through
// it, are err's: a run carrying the set behaves exactly as one that does not.
// What a failure already carries is kept, so wrapping at each call boundary
// accumulates the chain's. An empty set returns err itself.
func WithFailureSensitiveValues(err error, sensitive SensitiveValues) error {
	if err == nil {
		return nil
	}
	sensitive = sensitive.Merge(FailureSensitiveValues(err))
	if sensitive.Empty() {
		return err
	}

	return &sensitiveFailure{err: err, sensitive: sensitive}
}

// FailureSensitiveValues is what err carries of the sensitive values its text
// may quote ([WithFailureSensitiveValues]), or the empty set. The outermost
// carrier answers, since each one holds what the failures it wraps carried.
//
// A driver's own failure type joins by implementing
// `FailureSensitiveValues() SensitiveValues`, as the durable driver's
// ErrRunFailed does: that driver rebuilds the failure at each level rather
// than wrapping it, and a wrapper would not survive that.
func FailureSensitiveValues(err error) SensitiveValues {
	var carrier interface{ FailureSensitiveValues() SensitiveValues }
	if errors.As(err, &carrier) {
		return carrier.FailureSensitiveValues()
	}

	return SensitiveValues{}
}

// callReturnKey carries, while one call step runs under a debugger, the slot
// [runCall] fills with what its callee withholds once it returns (#2212). A
// callee's outputs can hand back a value only the callee declared sensitive,
// and the call step's account renders them from the caller's position, which
// knows nothing of the callee's declarations. The failure path carries the
// same set on the error ([WithFailureSensitiveValues]); this is its success
// path.
type callReturnKey struct{}

// contextWithCallReturn installs a fresh slot for one call step while
// something reads it ([withholdingRead]), and returns it; nil, and ctx
// unchanged, otherwise.
func contextWithCallReturn(ctx context.Context) (context.Context, *SensitiveValues) {
	if !withholdingRead(ctx) {
		return ctx, nil
	}
	slot := new(SensitiveValues)

	return context.WithValue(ctx, callReturnKey{}, slot), slot
}

// returnCallSensitive fills the slot of the call step running on ctx, if it
// has one.
func returnCallSensitive(ctx context.Context, sensitive SensitiveValues) {
	if slot, ok := ctx.Value(callReturnKey{}).(*SensitiveValues); ok && slot != nil {
		*slot = sensitive
	}
}

// returnedCallSensitive is what a call step's slot holds, or nothing.
func returnedCallSensitive(slot *SensitiveValues) SensitiveValues {
	if slot == nil {
		return SensitiveValues{}
	}

	return *slot
}

// sensitiveFailure is [WithFailureSensitiveValues]'s carrier.
type sensitiveFailure struct {
	err       error
	sensitive SensitiveValues
}

func (f *sensitiveFailure) Error() string { return f.err.Error() }

func (f *sensitiveFailure) Unwrap() error { return f.err }

// FailureSensitiveValues implements the carrier [FailureSensitiveValues]
// discovers.
func (f *sensitiveFailure) FailureSensitiveValues() SensitiveValues { return f.sensitive }

// debugSensitiveInputs is what [ExecutingSensitiveFromContext] records for
// one workflow's bound inputs: only while something reads it
// ([withholdingRead]), so an ordinary run does not pay for it.
func debugSensitiveInputs(ctx context.Context, wf *Workflow, inputs map[string]*Value) SensitiveValues {
	if !withholdingRead(ctx) {
		return SensitiveValues{}
	}

	return SensitiveInputValues(inputs, SensitiveInputNames(wf))
}

// withholdingRead reports whether anything on ctx reads what a position
// withholds: a [Debugger], which renders at holds and arrivals, a
// [WithholdingRunObserver], which renders each step's outcome — `flow test`'s
// transcript among them (#2211) — or a [WithholdingOnlyRunObserver], which
// gathers the sets for a rendering made after the run. None is installed on
// an ordinary run.
func withholdingRead(ctx context.Context) bool {
	if DebuggerFromContext(ctx) != nil {
		return true
	}
	switch RunObserverFromContext(ctx).(type) {
	case WithholdingRunObserver, WithholdingOnlyRunObserver:
		return true
	}

	return false
}

// ExecutingWorkflowFromContext reports which workflow's steps are running on
// this context.
//
// The answer a step boundary needs and the one [TaskStepRefFromContext] cannot
// give it. That function requires a *step* stamp as well, and the two callers
// differ in when they ask: a task asks from inside a step, where both halves
// are set, and a boundary asks before the node's own stamp exists — [runNodes]
// builds that context for the node's work and hands the enclosing one to the
// debugger seam.
//
// It reports ("", false) only where the engine never ran: a [Debugger] driven
// directly by an embedder, which is a real case and the honest answer for it.
// A consumer must have one — the position is a fact the run reports, not one it
// can require — and "not said" must not be read as a name, because two steps
// that answer to no workflow are not thereby the same step.
func ExecutingWorkflowFromContext(ctx context.Context) (string, bool) {
	position, ok := ctx.Value(executingWorkflowKey{}).(executingPosition)
	if !ok || position.workflow == "" {
		return "", false
	}

	return position.workflow, true
}

// ExecutingBacktraceFromContext returns the current frame followed by the
// caller chain, innermost first. An embedder that drives a Debugger directly
// still gets the current step, with an unsaid workflow.
func ExecutingBacktraceFromContext(ctx context.Context, step, kind string) *DebugBacktrace {
	position, _ := ctx.Value(executingWorkflowKey{}).(executingPosition)
	frames := make([]*DebugStackFrame, 0, 1+len(position.callers))
	frames = append(frames, &DebugStackFrame{Workflow: position.workflow, StepId: step, Kind: kind})
	for i := len(position.callers) - 1; i >= 0; i-- {
		frame := position.callers[i]
		frames = append(frames, &DebugStackFrame{Workflow: frame.GetWorkflow(), StepId: frame.GetStepId(), Kind: frame.GetKind()})
	}

	return &DebugBacktrace{Frames: frames}
}
