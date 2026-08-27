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

// MaxPendingSignals names how many early-arriving signals a well-behaved run
// should ever be carrying across Continue-As-New at once.
//
// It is not a hard cap: [engine]'s drainSignals carries every acknowledged
// delivery unconditionally, however many accumulate, because a sender who was
// told a signal was delivered must never find out later that it silently
// wasn't (#1013). Beyond this many, the honest reading is not "too many to
// keep" but "something is wrong" — a wait that should have consumed them is
// missing, or a sender is retrying into a run that already answered — and
// crossing it is logged for an operator to act on. The bound that actually
// protects a run is [CheckRunStateSize], weighed at every Continue-As-New: a
// carry too large to fit fails the run loudly rather than being silently
// truncated, which is why [MaxSignalPayloadBytes] bounds what one delivery can
// weigh — with the count itself unbounded, a payload's size is what stands
// between an ordinary backlog and one that blows the carry.
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
// # Where it is bound
//
// In a wait, and nowhere else in the language. Every expression a [Wait] carries
// sees it: `until`, `duration_expr`, and `timeout_expr`. That is one rule about a
// node kind rather than three about fields, and it is the rule the reasoning below
// actually supports — a wait is evaluated in workflow code holding the driver's own
// clock whichever arm it takes, so `${deadline - now}` is as replay-safe as
// `${now + days(3)}` is.
//
// It was narrower than that for one release, bound only inside `until`, and the
// narrowness was an artifact rather than a decision: `until` was the only arm that
// took an expression at all. When `sleep:` and `timeout:` learned to as well, the
// binding followed the clock rather than the field, because the alternative is a
// name that resolves in one expression of a message and not in its sibling — which
// an author has no way to predict and no reason to expect.
//
// # Why not everywhere
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
//
// bound carries any further names the position sees — the wait's own result,
// inside [ShapeSignalOutputs] — and is empty everywhere else. They join `now`
// rather than replacing it, because every expression a wait holds sees the
// clock and a shaping expression is no exception.
func evalWaitExpr(ctx context.Context, v *Value, scope *Scope, now time.Time, bound map[string]ref.Val) (ref.Val, error) {
	switch kind := v.GetKind().(type) {
	case *Value_Literal:
		return cel.ValueToRefValue(TypeAdapter, kind.Literal)
	case *Value_Expr:
		extra := make(map[string]ref.Val, len(bound)+1)
		for name, value := range bound {
			extra[name] = value
		}
		extra[NowIdentifier] = types.DefaultTypeAdapter.NativeToValue(now)

		activation := scope.ActivationWith(ctx, extra)
		return DefaultEvaluator().EvalParsedBase(ctx, scope.GetProfile(), kind.Expr, activation)
	default:
		return nil, fmt.Errorf("unsupported value kind %T", kind)
	}
}

// ShapeSignalOutputs applies a `wait_for_signal:`'s own `outputs:` to the result
// the wait produced, and is what makes a gate expressible once.
//
// # Where this runs
//
// At the moment the wait resolves, in workflow code, on both drivers, against
// exactly the outputs that were about to be recorded — the same moment
// [SignalOutputs] built them. That is not a convenience: it is the whole reason
// this shape was chosen over a lazy binding. There is no new evaluation position
// for a reader to hold in their head, nothing to re-evaluate on a later read, and
// nothing new at the Continue-As-New seam — a shaped output is recorded like any
// other step output and carried like one.
//
// # What the expressions see
//
// The wait's own result bound bare — [PayloadOutput], [SenderOutput],
// [TimedOutOutput] — plus [NowIdentifier], over the enclosing scope. So
// `${has(payload.approved) && payload.approved}` reads the sender's data, and
// `${steps.request.id}` reads an earlier step, in the one expression.
//
// The bare spelling is deliberate and is the reason `flow fix` has to know about
// this position: a file may legitimately contain a step called `payload`, and a
// rewriter that rooted the name here would silently turn the gate into a
// reference to that step. See the rewriter's own note.
//
// # Replace, not extend
//
// The returned outputs are *only* what was shaped. An empty or absent mapping
// leaves the wait's own outputs untouched, so nothing changes for a wait that
// does not ask.
//
// A failure here fails the step rather than producing an empty value, because the
// values being shaped are the ones later steps branch on: an expression naming a
// field the payload does not carry has to say so, at the step that would have
// hidden it.
func ShapeSignalOutputs(ctx context.Context, signal *Signal, raw *Node_Outputs, scope *Scope, now time.Time) (*Node_Outputs, error) {
	shaping := signal.GetOutputs()
	if len(shaping) == 0 {
		return raw, nil
	}

	bound := make(map[string]ref.Val, len(raw.GetNamedValues()))
	for name, value := range raw.GetNamedValues() {
		converted, err := cel.ValueToRefValue(TypeAdapter, value.GetLiteral())
		if err != nil {
			return nil, fmt.Errorf("binding %q for outputs shaping: %w", name, err)
		}
		bound[name] = converted
	}

	out := &Node_Outputs{NamedValues: make(map[string]*Value, len(shaping))}

	// Sorted, for the reason [SignalOutputs] sorts its payload entries: this
	// result is serialized into the run's state and carried across every
	// Continue-As-New, and evaluation order is observable through a cost limit
	// shared by the whole set. A protobuf map has no order of its own, so
	// without this two runs of one file could spend their budget differently.
	for _, name := range slices.Sorted(maps.Keys(shaping)) {
		value, err := evalWaitExpr(ctx, shaping[name], scope, now, bound)
		if err != nil {
			return nil, fmt.Errorf("evaluating outputs.%s: %w", name, err)
		}

		literal, err := cel.RefValueToValue(value)
		if err != nil {
			return nil, fmt.Errorf("converting outputs.%s: %w", name, err)
		}

		out.NamedValues[name] = &Value{Kind: &Value_Literal{Literal: literal}}
	}

	return out, nil
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

	value, err := evalWaitExpr(ctx, until, scope, now, nil)
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

// EvalWaitDuration resolves how long a `sleep:` waits, whether it was written as
// a literal or computed.
//
// The single reader of the two fields that can carry it, which is what keeps them
// from becoming the "one value written down twice" disagreement CLAUDE.md is
// about: neither driver knows which one answered, so neither can answer
// differently. Both call this, at the same point, with the same clock.
//
// Deterministic under replay for the same reason [EvalWaitDeadline] is. CEL here
// is a pure function of the run's already-resolved scope plus now, and now is the
// driver's own clock — under Temporal `workflow.Now`, which replays to the instant
// it first returned. Nothing is read from the world, so a replay computes what the
// original execution computed.
//
// It returns an error for a wait that is not a sleep at all, because a caller
// asking a signal how long it sleeps has confused two things.
func EvalWaitDuration(ctx context.Context, wait *Wait, scope *Scope, now time.Time) (time.Duration, error) {
	switch kind := wait.GetKind().(type) {
	case *Wait_Duration:
		return kind.Duration.AsDuration(), nil

	case *Wait_DurationExpr:
		d, err := evalDuration(ctx, kind.DurationExpr, scope, now, "sleep")
		if err != nil {
			return 0, err
		}
		if d < 0 {
			return 0, fmt.Errorf(
				"sleep computed %s, and a wait cannot run backwards; guard the expression, as in ${until > now ? until - now : duration('0s')}", d)
		}

		return d, nil

	default:
		return 0, fmt.Errorf("wait is not a sleep")
	}
}

// EvalWaitTimeout resolves the bound on a wait, whether it was written as a
// literal or computed.
//
// bounded reports whether there is a timeout at all, and it is a separate result
// rather than a zero duration because those are two different facts that the wire
// spells the same way. An absent `timeout:` means "wait as long as the run does";
// a computed `${deadline - now}` that lands exactly on zero means "the deadline is
// now". Collapsing them would make the second read as the first, which is the
// difference between a gate that times out immediately and one that never does.
//
// A negative computed timeout is refused rather than clamped, and this is the
// reason both this and the schema say so twice. `timeout <= 0` is how "unbounded"
// is encoded everywhere below here — `engine.waitForSignal` and
// `waitForSignalLocally` both gate their timer on it — so a sign error in an
// author's expression would not produce a short wait, it would produce an approval
// gate that blocks until the run itself times out. Failing the run names the
// mistake while somebody can still fix it; the alternative is a workload that
// looks like it is waiting patiently.
func EvalWaitTimeout(ctx context.Context, wait *Wait, scope *Scope, now time.Time) (timeout time.Duration, bounded bool, err error) {
	if wait.GetTimeoutExpr() != nil {
		d, err := evalDuration(ctx, wait.GetTimeoutExpr(), scope, now, "wait_for_signal timeout")
		if err != nil {
			return 0, false, err
		}
		if d < 0 {
			return 0, false, fmt.Errorf(
				"wait_for_signal timeout computed %s; a negative timeout is how this engine spells \"no timeout\", so it is refused rather than silently making the wait unbounded — guard the expression, as in ${deadline > now ? deadline - now : duration('0s')}", d)
		}

		return d, true, nil
	}

	if literal := wait.GetTimeout(); literal != nil {
		return literal.AsDuration(), true, nil
	}

	return 0, false, nil
}

// evalDuration evaluates one expression and reads a duration out of what it
// produced.
//
// A string is accepted and read with [ParseDuration], which is what makes
// `sleep: ${inputs.grace}` work against a declared input: [InputDeclaration.Type]
// has no duration member — it is the six types a caller can *send*, and a duration
// is not one of them — so a string is how a duration arrives from outside, and it
// has to mean there exactly what the same characters mean written literally.
//
// An integer is refused, and that refusal is the point rather than an omission.
// Nothing in `${inputs.grace * 2}` says whether the number counts seconds or
// nanoseconds, and CEL's own answer (nanoseconds, since that is what a duration
// holds) is the one an author is least likely to mean. The message names both
// spellings that are unambiguous.
func evalDuration(ctx context.Context, v *Value, scope *Scope, now time.Time, label string) (time.Duration, error) {
	value, err := evalWaitExpr(ctx, v, scope, now, nil)
	if err != nil {
		return 0, fmt.Errorf("evaluating %s: %w", label, err)
	}

	switch resolved := value.Value().(type) {
	case time.Duration:
		return resolved, nil

	case string:
		parsed, err := ParseDuration(resolved)
		if err != nil {
			return 0, fmt.Errorf(
				"%s produced %q, which is not a duration; write it as 30s, 5m, 1h, or 7d", label, truncateForError(resolved))
		}

		return parsed, nil

	case int64:
		return 0, fmt.Errorf(
			"%s produced the number %d, and a number does not say what unit it counts; write duration('30s'), or seconds(30), minutes(5), hours(2), days(7)", label, resolved)

	default:
		return 0, fmt.Errorf(
			"%s produced %s, but it must produce a duration — duration('720h'), days(30), or a string like 30s, 5m, 1h, 7d", label, value.Type())
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

			case *Node_Loop:
				// A `wait_for_signal:` inside a loop body declares a channel like one
				// anywhere else. Missing it here fails twice over: a `signals:` policy
				// for that name is rejected as naming a wait nobody wrote, and — because
				// this list is what a run drains before it suspends — an early signal on
				// that channel is lost across a Continue-As-New and the resumed loop
				// blocks on it forever.
				walk(kind.Loop.GetBody())

			case *Node_Parallel:
				for _, branch := range kind.Parallel.GetBranches() {
					walk(branch.GetSteps())
				}

			case *Node_Switch:
				// A `wait_for_signal:` inside a case body declares a channel
				// like one anywhere else — every body, not only the branch a
				// run happens to take, because the signal policy and the
				// pre-suspend drain are properties of the specification.
				for _, body := range SwitchBodies(kind.Switch) {
					walk(body)
				}

			case *Node_Call:
				// Calls are embedded workflows, so their declared signal channels belong
				// to this durable run and must be drained before Continue-As-New too.
				walk(kind.Call.GetWorkflow().GetSteps())
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

	// Two spellings of one bound is not something a spec gets to say, and it is
	// checked before the arms because it is wrong on every one of them. A caller
	// building a message by hand is the only way to produce it — the compiler
	// writes one field or the other — which is exactly the caller this function
	// exists for.
	if wait.GetTimeout() != nil && wait.GetTimeoutExpr() != nil {
		return fmt.Errorf("wait has both a literal timeout and a computed one; set one")
	}

	switch kind := wait.GetKind().(type) {
	case *Wait_Duration:
		if d := kind.Duration.AsDuration(); d < 0 {
			return fmt.Errorf("sleep is negative (%s)", d)
		}
		if wait.hasTimeout() {
			return fmt.Errorf("sleep has a timeout, which does nothing: the duration is already how long it waits")
		}
		return nil

	case *Wait_DurationExpr:
		// How long it sleeps is not knowable here — that is what makes it a
		// computed one — so what is checkable is that there is something to
		// evaluate. The value's own sign is checked by [EvalWaitDuration], at the
		// only moment it exists.
		if kind.DurationExpr == nil {
			return fmt.Errorf("sleep has no expression")
		}
		if wait.hasTimeout() {
			return fmt.Errorf("sleep has a timeout, which does nothing: the duration is already how long it waits")
		}
		return nil

	case *Wait_Until:
		if kind.Until == nil {
			return fmt.Errorf("wait_until has no expression")
		}
		if wait.hasTimeout() {
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

// hasTimeout reports whether a bound was written at all, in either spelling.
//
// The two checks above that refuse a timeout on a `sleep:` and a `wait_until:`
// read `GetTimeout()` alone for as long as that was the only field. Left that way
// they would have gone quiet for the computed spelling — the same misuse, no
// longer reported, which is how a diagnostic rots.
func (w *Wait) hasTimeout() bool {
	return w.GetTimeout() != nil || w.GetTimeoutExpr() != nil
}

// WaitDescription renders a wait for a log line or a progress display, such as
// the local driver's report of what a run is blocked on.
func WaitDescription(wait *Wait) string {
	switch kind := wait.GetKind().(type) {
	case *Wait_Duration:
		return "sleeping for " + kind.Duration.AsDuration().String()
	case *Wait_DurationExpr:
		// Not the duration, because this is written into a log line *before* the
		// wait is evaluated and there is nothing to say yet. "for a computed
		// duration" is the honest version; a number here would have to be invented.
		return "sleeping for a computed duration"
	case *Wait_Until:
		return "waiting until a time"
	case *Wait_Signal:
		return "waiting for signal " + strconv.Quote(kind.Signal.GetName())
	default:
		return "waiting"
	}
}
