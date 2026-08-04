package flowstatev1

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
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

// PayloadOutput is where a signal sender's data lands: `${approval.payload.approved}`.
//
// Rooted rather than spread, and the reason is integrity rather than tidiness.
// A signal's payload used to become the step's outputs *directly*, so whoever
// sent the signal chose names in the namespace that every later expression
// resolves against. A sender who guessed a step id could introduce any name they
// liked into it, and expressions elsewhere in the workload would silently start
// resolving against a value they picked.
//
// `timed_out` was protected from that, but only by ordering — it was written
// last, so a payload carrying it lost. That is a defence that works for exactly
// the names somebody thought to write down, and this engine will grow more
// wait outputs. Under one root there is nothing to think of: a sender can only
// ever name things inside `payload`, by grammar, and no future output can
// collide with one.
const PayloadOutput = "payload"

// SenderOutput is where the server-attested sender lands: `${approval.sender.identity.subject}`.
//
// Rooted for the identical reason [PayloadOutput] is: a sender's own payload must
// never be able to write anything outside `payload`, and `sender` is exactly the
// name that would matter most for a sender to forge. It never comes from
// anything the payload contains — [SignalOutputs] takes it as its own argument,
// built by the engine from a [SignalSender] the server attested, not from
// whatever the sender happened to name a key.
const SenderOutput = "sender"

// TimerOutputs builds the outputs of a wait that nobody sends anything to: a
// `sleep`, or a `wait_until` reaching its moment.
//
// No [PayloadOutput], because there is no sender. An empty mapping would be
// truthful and misleading — it would invite `${pause.payload.x}` on a step where
// a payload can never arrive, and the answer to that should be a diagnostic, not
// an empty map.
func TimerOutputs(timedOut bool) *Node_Outputs {
	return &Node_Outputs{
		NamedValues: map[string]*Value{
			TimedOutOutput: NewLiteral(timedOut),
		},
	}
}

// SignalOutputs builds the outputs of a wait for a signal.
//
// The wait's own outputs sit at the top — [TimedOutOutput], [SenderOutput], and
// whatever else is added later — and everything the sender supplied sits under
// [PayloadOutput]. The separation is the point: `payload` is what somebody else
// asserted, `sender` is who the server established that somebody to be, and
// neither an expression nor a sender should have to know which is which by
// remembering a list of reserved names.
//
// The payload mapping is present even when it is empty, which is the case for a
// wait that timed out. `has(gate.payload.approved)` is then answerable either
// way, rather than failing on a missing `payload` — the point of a root is that
// there is always something to look inside.
//
// sender is nil for a wait that timed out with nothing pending, and for any
// signal recorded before this field existed — both read as unattested, via
// [signalSenderValue].
func SignalOutputs(payload *Node_Outputs, sender *SignalSender, timedOut bool) *Node_Outputs {
	out := TimerOutputs(timedOut)

	named := payload.GetNamedValues()
	entries := make([]*expr.MapValue_Entry, 0, len(named))

	// Sorted, because a protobuf map has no order and this value is serialized
	// into the run's state and carried across every Continue-As-New. Two encodings
	// of the same payload would differ for no reason a reader could see, and a
	// workflow's persisted state is exactly the wrong place for that.
	for _, name := range slices.Sorted(maps.Keys(named)) {
		literal := named[name].GetLiteral()
		if literal == nil {
			// Not a literal, so it did not come from a sender — a signal carries
			// data, not expressions. Skipped rather than guessed at.
			continue
		}
		entries = append(entries, &expr.MapValue_Entry{
			Key:   NewLiteral(name).GetLiteral(),
			Value: literal,
		})
	}

	out.NamedValues[PayloadOutput] = &Value{
		Kind: &Value_Literal{
			Literal: &expr.Value{
				Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: entries}},
			},
		},
	}

	// Never built from payload — sender is the engine's own attestation, passed
	// in by the caller rather than read out of what the sender sent, which is
	// what keeps a payload key named "sender" from ever being confused with it.
	out.NamedValues[SenderOutput] = signalSenderValue(sender)

	return out
}

// signalSenderValue renders a [SignalSender] as the map an expression reads
// under [SenderOutput].
//
// A nil sender renders with every identity field empty and `local: true` —
// never the same shape a genuinely attested-but-anonymous caller produces
// (`local: false` with an empty subject, which is what an unattested identity
// provider still yields through a real server). Nil means the engine has
// nothing at all to say about who sent this, and three cases produce it: a
// timed-out wait with nothing pending, a [PendingSignal] carried from before
// this field existed, and a signal that arrived in the pre-#194 wire shape (a
// bare [Node_Outputs], decoded by the engine's compatibility fallback — see
// engine/signal_compat.go). All three are the same fact from a workflow
// author's point of view — nothing here was attested — so they read the same
// way, and none of them may be mistaken for a real identity that merely
// happens to be blank.
func signalSenderValue(sender *SignalSender) *Value {
	local := sender.GetLocal()
	if sender == nil {
		local = true
	}

	identity := sender.GetIdentity()

	identityEntries := []*expr.MapValue_Entry{
		{Key: NewLiteral("subject").GetLiteral(), Value: NewLiteral(identity.GetSubject()).GetLiteral()},
		{Key: NewLiteral("issuer").GetLiteral(), Value: NewLiteral(identity.GetIssuer()).GetLiteral()},
		{Key: NewLiteral("namespace").GetLiteral(), Value: NewLiteral(identity.GetNamespace()).GetLiteral()},
		{Key: NewLiteral("deployment").GetLiteral(), Value: NewLiteral(identity.GetDeployment()).GetLiteral()},
	}

	acceptedAt := ""
	if at := sender.GetAcceptedAt(); at.IsValid() {
		acceptedAt = at.AsTime().Format(time.RFC3339)
	}

	entries := []*expr.MapValue_Entry{
		{
			Key: NewLiteral("identity").GetLiteral(),
			Value: &expr.Value{
				Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: identityEntries}},
			},
		},
		{Key: NewLiteral("accepted_at").GetLiteral(), Value: NewLiteral(acceptedAt).GetLiteral()},
		{Key: NewLiteral("local").GetLiteral(), Value: NewLiteral(local).GetLiteral()},
	}

	return &Value{
		Kind: &Value_Literal{
			Literal: &expr.Value{
				Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{Entries: entries}},
			},
		},
	}
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
// everywhere — though not for the reason it first looks like. Most task inputs are
// resolved in workflow code too, where the same replay-safe clock is available; the
// activity is taken only by a task declaring NeedsPrevOutputs, which today is `http`
// and plugins asking for a scope.
//
// That split is the problem. A `now` resolved in an activity is read afresh on every
// attempt: a retried step would compute a different value than the one that failed,
// and two steps in the same run would disagree about what time it is. So binding the
// name in task inputs would make it replay-safe for most tasks and not for `http` —
// one spelling with two behaviours, decided by a property of the task an author has
// no reason to know. Neither failure is one anybody would find quickly. Making the
// name resolvable in exactly the place that has a clock behind it in *every* case
// keeps the awkward version from being expressible at all.
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
		return DefaultEvaluator().EvalParsedBase(ctx, scope.GetProfile(), kind.Expr, activation)
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
// a `sleep`, where the duration is already the bound, and on a `wait_until`, where
// the moment is. Reporting it is worth more than ignoring it, because an author who
// wrote one believes it does something.
//
// A `wait_until` carrying one is not reachable from a Flowfile — the parser sets a
// timeout only under `wait_for_signal:` — but a spec submitted to the Run RPC is
// built by hand, and both drivers ignored the field there: `timed_out` stayed false
// however long the wait ran. A caller who set it and branched on `timed_out` was
// waiting on something that could never happen.
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
		if wait.GetTimeout() != nil {
			return fmt.Errorf("wait_until has a timeout, which does nothing: the moment is already how long it waits")
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
