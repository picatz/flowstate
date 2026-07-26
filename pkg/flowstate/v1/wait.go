package flowstatev1

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// TimedOutOutput is the output every wait produces, reporting whether it ended
// because what it waited for happened or because it ran out of time.
//
// It is a normal output rather than an error because a lapsed approval is a
// normal outcome. An author writes `if: ${!approval.timed_out}` and decides what
// it means; forcing them through `continue_on_error` would make a wait that timed
// out indistinguishable from a wait that broke, and would swallow the second.
//
// The name is reserved: a signal payload carrying it does not get to set it, or a
// sender could report that a wait they timed out had in fact succeeded.
const TimedOutOutput = "timed_out"

// MaxPendingSignals bounds how many early-arriving signals a run carries across
// Continue-As-New.
//
// The names come from the specification, so their variety is bounded by the
// workload — but nothing stops a sender delivering the same signal a million
// times, and every one of them would otherwise be carried in the run's state.
// Beyond this many the oldest are kept and the rest dropped, because a wait
// consumes one signal and the first to arrive is the one that approved it.
const MaxPendingSignals = 128

// WaitOutputs builds the outputs of a completed wait.
//
// A signal's payload becomes the step's outputs directly, which is what lets
// `${approval.approved}` resolve like any other step reference rather than
// needing a form of its own.
func WaitOutputs(payload *Node_Outputs, timedOut bool) *Node_Outputs {
	out := &Node_Outputs{NamedValues: map[string]*Value{}}

	for name, value := range payload.GetNamedValues() {
		out.NamedValues[name] = value
	}

	// Set last, so a payload cannot claim otherwise.
	out.NamedValues[TimedOutOutput] = NewLiteral(timedOut)

	return out
}

// NowIdentifier is the name a wait expression uses for the moment it is being
// evaluated, so `wait_until: ${now + days(1)}` means what it reads as.
//
// # Why this is bound here and nowhere else
//
// The value comes from the caller, which is workflow code holding the driver's
// own clock: `workflow.Now` under Temporal, the wall clock locally. Both are
// deterministic for their driver — Temporal's replays to the same instant — so a
// wait computed from it survives replay and a worker restart.
//
// A task input is the other case, and it is the reason this is not simply bound
// everywhere. Input expressions are resolved inside an activity, so a `now` there
// would be read afresh on every attempt: a retried step would compute a different
// value than the one that failed, and two steps in the same run would disagree
// about what time it is. Neither is a bug anybody would find quickly. Making the
// name resolvable in exactly the place with a clock behind it keeps the awkward
// version from being expressible at all.
//
// It is reserved as a step id for the same reason: a step named `now` would be
// silently shadowed inside a wait expression, and `flowfile` refuses it rather
// than letting a reference quietly mean something else.
const NowIdentifier = "now"

// evalWaitExpr evaluates a wait's expression with [NowIdentifier] bound.
func evalWaitExpr(ctx context.Context, v *Value, scope *Scope, now time.Time) (ref.Val, error) {
	switch kind := v.GetKind().(type) {
	case *Value_Literal:
		return cel.ValueToRefValue(TypeAdapter, kind.Literal)
	case *Value_Expr:
		activation := scope.ActivationWith(ctx, map[string]ref.Val{
			NowIdentifier: types.DefaultTypeAdapter.NativeToValue(now),
		})
		return DefaultEvaluator().EvalParsedBase(ctx, kind.Expr, activation)
	default:
		return nil, fmt.Errorf("unsupported value kind %T", kind)
	}
}

// EvalWaitDeadline resolves a `wait_until` expression to the moment it names.
//
// # Why this is a time and not a condition
//
// A condition polled inside a workflow can only read what the workflow can see,
// and while the workflow is blocked in a wait, nothing it can see changes: no
// step is running to produce an output. A condition over step outputs is
// therefore either already true or never going to be, so polling one would be a
// way to spell "wait forever" by accident. Waiting for something outside the
// workload is what a signal is for.
//
// What is genuinely useful, and what the roadmap groups this with, is a durable
// timer to an absolute moment: "wait until 09:00 on Monday", "wait until the
// maintenance window opens". So the expression names a time, and the wait is a
// timer to it.
//
// now is passed in rather than read here, because the caller is workflow code and
// must use the workflow's own clock — reading the wall clock would make the same
// history replay to a different answer.
func EvalWaitDeadline(ctx context.Context, until *Value, scope *Scope, now time.Time) (time.Time, error) {
	if until == nil {
		return time.Time{}, fmt.Errorf("wait_until has no expression")
	}

	value, err := evalWaitExpr(ctx, until, scope, now)
	if err != nil {
		return time.Time{}, fmt.Errorf("evaluating wait_until: %w", err)
	}

	switch resolved := value.Value().(type) {
	case time.Time:
		return resolved, nil

	case time.Duration:
		// A relative wait spelled as a duration. `sleep:` says the same thing
		// more plainly, but an expression that computes one is reasonable.
		return now.Add(resolved), nil

	case string:
		parsed, err := time.Parse(time.RFC3339, resolved)
		if err != nil {
			return time.Time{}, fmt.Errorf(
				"wait_until produced %q, which is not an RFC 3339 time: %w", truncateForError(resolved), err)
		}
		return parsed, nil

	case int64:
		return time.Unix(resolved, 0).UTC(), nil

	case bool:
		// The mistake this is most likely to be, so it gets the answer rather
		// than a type error: whatever the author meant, a boolean cannot say
		// when to stop waiting.
		return time.Time{}, fmt.Errorf(
			"wait_until produced a boolean, but it must produce a time; a condition over step outputs cannot change while the workload waits — use `sleep:` for a delay, or `wait_for_signal:` to wait for something outside the workload")

	default:
		return time.Time{}, fmt.Errorf(
			"wait_until produced %s, but it must produce a time (an RFC 3339 string, a timestamp, or Unix seconds)", value.Type())
	}
}

// truncateForError bounds expression output on its way into a message.
func truncateForError(s string) string {
	const max = 64
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// SignalNames returns every signal a workload can wait for, in the order they
// appear, without repeats.
//
// The set is static, which is what makes carrying early-arriving signals across
// Continue-As-New possible at all: the run can drain exactly the channels the
// specification declares, rather than having to guess which names something might
// send.
func SignalNames(spec *Workflow) []string {
	var (
		names []string
		seen  = map[string]struct{}{}
	)

	var walk func(nodes []*Node)
	walk = func(nodes []*Node) {
		for _, node := range nodes {
			switch kind := node.GetKind().(type) {
			case *Node_Wait:
				name := kind.Wait.GetSignal().GetName()
				if name == "" {
					continue
				}
				if _, dup := seen[name]; dup {
					continue
				}
				seen[name] = struct{}{}
				names = append(names, name)

			case *Node_ForEach:
				walk(kind.ForEach.GetBody())

			case *Node_Parallel:
				for _, branch := range kind.Parallel.GetBranches() {
					walk(branch.GetSteps())
				}
			}
		}
	}

	walk(spec.GetSteps())

	return names
}

// ValidateWait reports whether a wait can be executed as written.
//
// It covers what the schema's own rules cannot: that a timeout is meaningless on
// a `sleep`, where the duration is already the bound. Reporting it is worth more
// than ignoring it, because an author who wrote one believes it does something.
func ValidateWait(wait *Wait) error {
	if wait == nil {
		return fmt.Errorf("wait is missing")
	}

	switch kind := wait.GetKind().(type) {
	case *Wait_Duration:
		if d := kind.Duration.AsDuration(); d < 0 {
			return fmt.Errorf("sleep is negative (%s)", d)
		}
		if wait.GetTimeout() != nil {
			return fmt.Errorf("sleep has a timeout, which does nothing: the duration is already how long it waits")
		}
		return nil

	case *Wait_Until:
		if kind.Until == nil {
			return fmt.Errorf("wait_until has no expression")
		}
		return nil

	case *Wait_Signal:
		if kind.Signal.GetName() == "" {
			return fmt.Errorf("wait_for_signal has no signal name")
		}
		return nil

	default:
		return fmt.Errorf("wait must be one of sleep, wait_until, or wait_for_signal")
	}
}

// WaitDescription renders a wait for a log line or a progress display, such as
// the local driver's report of what a run is blocked on.
func WaitDescription(wait *Wait) string {
	switch kind := wait.GetKind().(type) {
	case *Wait_Duration:
		return "sleeping for " + kind.Duration.AsDuration().String()
	case *Wait_Until:
		return "waiting until a time"
	case *Wait_Signal:
		return "waiting for signal " + strconv.Quote(kind.Signal.GetName())
	default:
		return "waiting"
	}
}
