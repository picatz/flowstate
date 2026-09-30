package flowstatev1

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/google/cel-go/cel"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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
// # Local driver only, like Scheduler
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

// A WithholdingRunObserver is a [RunObserver] that renders a step's outcome
// for a person, and so is told, with it, what that rendering must withhold
// (#2210): the declared-sensitive inputs of the workflow the step belongs to
// and of every workflow on the way to it ([ExecutingSensitiveFromContext]),
// and, for a failure raised inside a callee, what that failure carries
// ([FailureSensitiveValues]), and, for a call that returned, what its callee
// withholds of the outputs it handed back. A step id says none of that, and an observer's
// own redactor knows only what its caller gave it — never a callee's
// declarations.
//
// It is called in place of StepFinished. Installing one is what makes the
// engine compute the sets at all, as a [Debugger] does; an ordinary run
// computes nothing.
type WithholdingRunObserver interface {
	RunObserver

	StepFinishedWithholding(id string, outputs *Node_Outputs, err error, tolerated bool, withhold SensitiveValues)
}

// WithholdingOnlyRunObserver is a [RunObserver] that reads, of each finished
// step, only what a rendering of it must withhold: the set a
// [WithholdingRunObserver] is told, without the outputs and the error. A
// reader gathering the sets for a rendering made elsewhere wants nothing
// else, and the engine then copies no step's outputs for it (Codex, #2215).
//
// StepWithheld is called in place of StepFinished. Installing one makes the
// engine compute the sets, as a [WithholdingRunObserver] does.
type WithholdingOnlyRunObserver interface {
	RunObserver

	StepWithheld(id string, withhold SensitiveValues)
}

// A GuardRunObserver is a [RunObserver] told how a step's `if:` decided
// against it (#2124): the condition a skip came from, and a condition that
// raised an error instead of an answer. The second otherwise reaches no
// observer at all, because the step never ran and so records no outcome.
//
// Both are the deciding evaluation's own result, reported where the driver
// reads it. Nothing is evaluated again to explain it.
type GuardRunObserver interface {
	RunObserver

	// StepSkippedBy is called in place of StepSkipped, with the condition
	// that decided, the observer's own copy, for the account both drivers give
	// of the skip ([SkippedText]). withhold is what a rendering of it must
	// withhold, as for [WithholdingRunObserver]: the condition is the
	// author's text, and an author can write a value there that the workflow
	// declares sensitive.
	StepSkippedBy(id string, condition *Value, withhold SensitiveValues)

	// GuardFailed reports a step whose `if:` could not be evaluated. The step
	// did not run, and err, a snapshot of the failure the run propagates, ends
	// it. withhold is what a rendering must withhold, as for
	// [WithholdingRunObserver].
	GuardFailed(id string, err error, withhold SensitiveValues)
}

// SkippedText is the account of a step whose `if:` evaluated false, in the one
// sentence both drivers' debuggers give (#2124). It quotes the condition that
// decided, rendered from the compiled expression, so someone whose breakpoint
// never stopped reads why beside the skip. A condition the renderer cannot
// write back, such as one using a comprehension macro, is not quoted.
//
// Every constant withheld reports true for is written as [SensitiveMarker]
// before the condition is rendered. By value, because the renderer's spelling
// of a constant is its own: a bytes literal is written in octal escapes, which
// no match for the value's text finds (Codex, #2227). withheld may be nil.
//
// The quote is whole. Each driver then withholds what the sentence must not
// show by text too, and only then bounds it, as it does every observation: a
// sentence cut first could keep the start of a sensitive value too long to
// fit, which nothing matching the whole value would then find (Codex, #2227).
func SkippedText(id string, condition *Value, withheld func(any) bool) string {
	switch text := conditionText(condition, withheld); text {
	case "":
		return id + " skipped (`if:` was false)"
	case "false":
		// Nothing to explain beyond the condition itself.
		return id + " skipped (`if: false`)"
	default:
		return id + " skipped: `if: " + text + "` was false"
	}
}

// conditionText renders condition as an author would write it, with what
// withheld reports withheld, or "" when it cannot be rendered. The rendering is
// linear in the compiled expression, whose source the compiler already bounds.
func conditionText(condition *Value, withheld func(any) bool) string {
	var text string
	switch kind := condition.GetKind().(type) {
	case *Value_Literal:
		b, ok := kind.Literal.GetKind().(*expr.Value_BoolValue)
		if !ok {
			return ""
		}
		text = fmt.Sprint(b.BoolValue)
		// Asked about as an expression's constants are, since a sensitive
		// structure can hold a bool leaf (Codex, #2227).
		if withheld != nil && withheld(b.BoolValue) {
			text = strconv.Quote(SensitiveMarker)
		}
	case *Value_Expr:
		parsed := kind.Expr
		if withheld != nil {
			parsed = proto.CloneOf(parsed)
			withholdConstants(parsed.ProtoReflect(), withheld)
		}
		rendered, err := cel.AstToString(cel.ParsedExprToAst(parsed))
		if err != nil {
			return ""
		}
		text = rendered
	default:
		return ""
	}

	return text
}

// withholdConstants writes [SensitiveMarker] in place of every constant, field
// name and field key in m that withheld reports true for, at any depth: the
// expression, a map entry's key, and the macro calls the renderer writes back
// from. Every message is
// visited rather than each expression kind, so a kind this walk does not name
// is not a place a constant can hide.
func withholdConstants(m protoreflect.Message, withheld func(any) bool) {
	switch node := m.Interface().(type) {
	case *expr.Constant:
		if slices.ContainsFunc(constantForms(node), withheld) {
			node.ConstantKind = &expr.Constant_StringValue{StringValue: SensitiveMarker}
		}

		return
	case *expr.Expr_Select:
		// A field name is a string of the author's too, and a sensitive
		// structure's keys are what its set withholds, one rune long
		// included, which no text match looks for (Codex, #2227).
		if withheld(node.GetField()) {
			node.Field = SensitiveMarker
		}
	case *expr.Expr_CreateStruct_Entry:
		if key, ok := node.GetKeyKind().(*expr.Expr_CreateStruct_Entry_FieldKey); ok && withheld(key.FieldKey) {
			key.FieldKey = SensitiveMarker
		}
	}
	m.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap():
			if field.MapValue().Message() != nil {
				value.Map().Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
					withholdConstants(entry.Message(), withheld)
					return true
				})
			}
		case field.IsList():
			if field.Message() != nil {
				for i := range value.List().Len() {
					withholdConstants(value.List().Get(i).Message(), withheld)
				}
			}
		case field.Message() != nil:
			withholdConstants(value.Message(), withheld)
		}

		return true
	})
}

// constantForms is a constant as every native value a sensitive set may hold
// it as. A set compares by value and type, and an author can write a value in
// a literal of another type, which the renderer then spells its own way: a
// string in a bytes literal (in octal), an int as a double (in exponent
// form). So bytes are also their text and text is also its bytes, and a
// number is also each other numeric type that holds it exactly (#2227).
func constantForms(constant *expr.Constant) []any {
	switch kind := constant.GetConstantKind().(type) {
	case *expr.Constant_StringValue:
		return []any{kind.StringValue, []byte(kind.StringValue)}
	case *expr.Constant_BytesValue:
		return []any{kind.BytesValue, string(kind.BytesValue)}
	case *expr.Constant_Int64Value:
		return numericForms(float64(kind.Int64Value), kind.Int64Value)
	case *expr.Constant_Uint64Value:
		return numericForms(float64(kind.Uint64Value), kind.Uint64Value)
	case *expr.Constant_DoubleValue:
		return numericForms(kind.DoubleValue, kind.DoubleValue)
	case *expr.Constant_BoolValue:
		return []any{kind.BoolValue}
	case *expr.Constant_NullValue:
		// A sensitive structure's null leaf is held as nil (Codex, #2227).
		return []any{nil}
	default:
		return nil
	}
}

// numericForms is value, and f as each of float64, int64 and uint64 that
// holds it. Each integer type is asked about only inside its own range, since
// converting a float64 outside it is not defined, and separately, since the
// unsigned range runs on past the signed one (Codex, #2227).
func numericForms(f float64, value any) []any {
	forms := []any{value, f}
	if f != math.Trunc(f) {
		return forms
	}
	if f >= -(1<<63) && f < 1<<63 {
		forms = append(forms, int64(f))
	}
	if f >= 0 && f < 1<<64 {
		forms = append(forms, uint64(f))
	}

	return forms
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

// returned is what a call step's callee withholds of the outputs it handed
// back ([WithholdingRunObserver]).
func observeStepFinished(ctx context.Context, id string, outputs *Node_Outputs, err error, tolerated bool, returned SensitiveValues) {
	observer := RunObserverFromContext(ctx)
	if observer == nil {
		return
	}
	if only, ok := observer.(WithholdingOnlyRunObserver); ok {
		// Before the copies below, which it never reads.
		withhold := ExecutingSensitiveFromContext(ctx).Merge(FailureSensitiveValues(err)).Merge(returned)
		observeSafely(func() { only.StepWithheld(id, withhold) })

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
	if withholding, ok := observer.(WithholdingRunObserver); ok {
		// Taken from the live error, before the snapshot drops its chain.
		withhold := ExecutingSensitiveFromContext(ctx).Merge(FailureSensitiveValues(err)).Merge(returned)
		observeSafely(func() { withholding.StepFinishedWithholding(id, copied, snapshot, tolerated, withhold) })

		return
	}
	observeSafely(func() { observer.StepFinished(id, copied, snapshot, tolerated) })
}

func observeStepSkipped(ctx context.Context, node *Node) {
	observer := RunObserverFromContext(ctx)
	if observer == nil {
		return
	}
	if guard, ok := observer.(GuardRunObserver); ok {
		// The observer's own copy, as a finished step's outputs are: the
		// condition is the run's, and an account must not be able to edit it.
		condition := proto.CloneOf(node.GetCondition())
		withhold := ExecutingSensitiveFromContext(ctx)
		observeSafely(func() { guard.StepSkippedBy(node.GetId(), condition, withhold) })

		return
	}
	observeSafely(func() { observer.StepSkipped(node.GetId()) })
}

// observeGuardFailed reports a step whose `if:` raised err
// ([GuardRunObserver]). An observer that is not told about guards hears
// nothing, as it always has.
func observeGuardFailed(ctx context.Context, id string, err error) {
	guard, ok := RunObserverFromContext(ctx).(GuardRunObserver)
	if !ok {
		return
	}
	withhold := ExecutingSensitiveFromContext(ctx).Merge(FailureSensitiveValues(err))
	snapshot := errors.New(err.Error())
	observeSafely(func() { guard.GuardFailed(id, snapshot, withhold) })
}

func observeWaitStarted(ctx context.Context, id, signal string, timeout time.Duration, bounded bool) {
	if observer := RunObserverFromContext(ctx); observer != nil {
		observeSafely(func() { observer.WaitStarted(id, signal, timeout, bounded) })
	}
}
