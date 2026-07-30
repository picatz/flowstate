package engine

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/workflow"
)

// A run reports RUNNING and, until this, nothing else. `flow get` could say how long
// a workload had been going and not what it was doing, which makes the difference
// between a slow step and a wedged one invisible — and those want opposite responses.
//
// Nothing outside the run can answer it. A listing and a DescribeWorkflowExecution
// both know the run is RUNNING; neither knows what it is running, because the
// position is in the interpreter's own call stack rather than in anything the service
// records. A Temporal query is the one way to ask: it runs against live state, writes
// no history event, and cannot make the run do anything.

// ProgressQuery is the query name a client asks for a run's position by.
//
// Namespaced, because a query name is a public identifier on every workflow this
// engine runs and Temporal's own tooling puts its built-ins (`__stack_trace`,
// `__enhanced_stack_trace`) in the same namespace.
const ProgressQuery = "flowstate.progress"

// progress is the run's position, shared by pointer with every nested executor.
//
// A pointer for the same reason [signalCarry] is one: a nested executor is a
// different struct running the same run, and a copy per level would leave the query
// answering from whichever copy the root happened to hold — which is the one that is
// not moving.
//
// No lock. Workflow coroutines are scheduled cooperatively, so only one of them runs
// at a time, and a query handler runs on that same scheduler rather than on a
// separate goroutine. This is the same reasoning [signalCarry] records, and it is the
// only reason a mutable value can be shared across a parallel block at all.
type progress struct {
	// stepID is the top-level step, and path is the position inside it. Kept apart
	// rather than as one slice because they have different guarantees: the top-level
	// step is always known, and the path is only meaningful outside concurrent work.
	stepID string
	path   []string

	// completed counts steps finished in this segment, which is what the step budget
	// counts too.
	completed int
}

// snapshot copies the position into the message a query answers with.
//
// A copy, because the slice underneath keeps being appended to and truncated as the
// run walks. Handing the caller the live slice would let the answer change under
// serialization — and the failure would be a rare, unreproducible wrong path rather
// than an error.
func (p *progress) snapshot() *v1.RunProgress {
	if p == nil {
		return nil
	}

	out := &v1.RunProgress{
		StepId:         p.stepID,
		CompletedSteps: int32(p.completed),
	}
	if len(p.path) > 0 {
		out.Path = append(make([]string, 0, len(p.path)), p.path...)
	}

	return out
}

// enter records that the run has reached a step at some depth.
//
// Depth zero names the top-level step and resets the path, since arriving at a new
// top-level step means whatever was inside the last one is over. Deeper levels append
// — a body step under its loop — and re-entering the same depth replaces rather than
// stacks, which is what makes the second iteration of a loop overwrite the first.
//
// Truncating to the entered depth is also what keeps the path honest without any
// separate bookkeeping: arriving at a step one level up drops whatever was recorded
// below it, so a stale deeper entry cannot survive into a later query.
func (p *progress) enter(depth int, stepID string) {
	if p == nil {
		return
	}

	if depth <= 0 {
		p.stepID = stepID
		p.path = p.path[:0]

		return
	}

	// A path entry per level below the top, so index depth-1 is this level's.
	for len(p.path) < depth {
		p.path = append(p.path, "")
	}
	p.path = p.path[:depth]
	p.path[depth-1] = stepID
}

// finished records that a step completed.
func (p *progress) finished() {
	if p == nil {
		return
	}

	p.completed++
}

// setProgressQuery installs the handler that answers [ProgressQuery].
//
// Installed before anything else runs, including the vars activity, so a query that
// arrives in the first moments of a run is answered with an empty position rather
// than refused. Temporal fails a query for a handler that is not registered yet, and
// "the run had not got anywhere" is a better answer than an error that reads like the
// worker being broken.
//
// Registering a handler is replay-safe: it schedules nothing and writes no history
// event, so it cannot diverge a run already in flight. A run pinned to an interpreter
// built before this simply has no handler, which is why the server treats a failed
// query as unknown rather than as a failure.
func setProgressQuery(ctx workflow.Context, p *progress) error {
	return workflow.SetQueryHandler(ctx, ProgressQuery, func() (*v1.RunProgress, error) {
		return p.snapshot(), nil
	})
}
