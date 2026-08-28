package flowstatev1

import "context"

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

// contextWithExecutingWorkflow returns ctx carrying name as the workflow whose
// steps are running on it.
//
// Unexported deliberately: this is the engine's own bookkeeping, and a caller
// able to set it could tell a debugger the run is somewhere it is not. The
// engine sets it at exactly two places, and both are a workflow it is about to
// interpret.
func contextWithExecutingWorkflow(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, executingWorkflowKey{}, name)
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
	name, ok := ctx.Value(executingWorkflowKey{}).(string)
	if !ok || name == "" {
		return "", false
	}

	return name, true
}
