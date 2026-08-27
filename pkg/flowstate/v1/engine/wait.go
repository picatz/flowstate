package engine

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Waiting is the primitive that makes this engine worth building on the
// substrate it is built on.
//
// A run blocked in a wait is holding nothing: no worker, no goroutine, no
// connection. The wait is durable state on the server, so a worker that is
// redeployed, crashed, or moved machines mid-wait resumes exactly where it was —
// and a workload can therefore wait a week for a person without anything having
// to stay up for a week.
//
// All of it is workflow-side code, and that constrains how it may be written:
// nothing here may read the wall clock, generate a random number, or touch the
// network. Timers and signal channels are the substrate's own, so they replay
// correctly; `time.Now` would not.

// cancelSignalWaitTimerChange is the [workflow.GetVersion] changeID guarding
// the #770 fix: cancelling a bounded wait_for_signal's timeout timer once a
// signal (or the run's own cancellation) answers the gate first. See the
// comment where it is used, in [executor.waitForSignal].
const cancelSignalWaitTimerChange = "engine.wait.cancelSignalTimeoutTimer"

// runWait blocks until what the node waits for happens, then records the
// outcome as the step's outputs.
func (e *executor) runWait(node *v1.Node, wait *v1.Wait) error {
	if err := v1.ValidateWait(wait); err != nil {
		return nodeFailed(err)
	}

	logger := workflow.GetLogger(e.ctx)
	logger.Info("step is waiting", "id", node.GetId(), "wait", v1.WaitDescription(wait))

	switch kind := wait.GetKind().(type) {
	case *v1.Wait_Duration, *v1.Wait_DurationExpr:
		// Both spellings of a sleep go through one reader, so the durable driver
		// cannot resolve a computed one differently from the local driver — see
		// [v1.EvalWaitDuration]. The clock is `workflow.Now`, which replays to the
		// instant it first returned, which is what makes an expression naming
		// `now` safe in workflow code.
		d, err := v1.EvalWaitDuration(context.Background(), wait, e.scope, workflow.Now(e.ctx))
		if err != nil {
			return nodeFailed(err)
		}
		return e.waitFor(node, d)

	case *v1.Wait_Until:
		deadline, err := v1.EvalWaitDeadline(context.Background(), kind.Until, e.scope, workflow.Now(e.ctx))
		if err != nil {
			return nodeFailed(err)
		}
		// A moment already past is not an error: a workload resumed after an
		// outage may reach a window that has opened, and refusing then would
		// fail a run for being late rather than letting it catch up.
		return e.waitFor(node, deadline.Sub(workflow.Now(e.ctx)))

	case *v1.Wait_Signal:
		timeout, bounded, err := v1.EvalWaitTimeout(context.Background(), wait, e.scope, workflow.Now(e.ctx))
		if err != nil {
			return nodeFailed(err)
		}

		outputs, err := e.waitForSignal(node, kind.Signal, timeout, bounded)
		if err != nil {
			return err
		}

		// Shaped here rather than at each of the four places the outcome is
		// built, so there is exactly one moment a wait's `outputs:` can be
		// evaluated — the moment the wait resolves, whichever way it resolved.
		// The local driver shapes at its own single point for the same reason,
		// through this same function, so neither driver can shape a lapsed gate
		// differently from an answered one.
		//
		// `workflow.Now` again rather than the value read before the wait: the
		// clock a shaping expression sees is the moment the wait *ended*, which
		// is the only reading of `now` that is true here. It replays to the same
		// instant, so this is deterministic like every other read of it.
		shaped, err := v1.ShapeSignalOutputs(
			context.Background(), kind.Signal, outputs, e.scope, workflow.Now(e.ctx))
		if err != nil {
			return nodeFailed(err)
		}

		e.recordOutputs(node, shaped)

		return nil

	case *v1.Wait_SignalBatch:
		// The bound is read through the same [v1.EvalWaitTimeout] the single
		// wait's arm reads it through, so the two spellings cannot disagree
		// about what a written-but-zero `timeout:` means. That equivalence is
		// why `timeout` stayed on [v1.Wait] rather than being restated on the
		// new message.
		timeout, bounded, err := v1.EvalWaitTimeout(context.Background(), wait, e.scope, workflow.Now(e.ctx))
		if err != nil {
			return nodeFailed(err)
		}

		outputs, err := e.waitForSignals(node, kind.SignalBatch, timeout, bounded)
		if err != nil {
			return err
		}

		// One shaping moment, as above, through the sibling of the function
		// above — see [v1.ShapeSignalBatchOutputs], which is literally the same
		// evaluator, so a driver cannot shape a batch differently from a single
		// wait.
		shaped, err := v1.ShapeSignalBatchOutputs(
			context.Background(), kind.SignalBatch, outputs, e.scope, workflow.Now(e.ctx))
		if err != nil {
			return nodeFailed(err)
		}

		e.recordOutputs(node, shaped)

		return nil

	default:
		return nodeFailed(fmt.Errorf("unsupported wait kind %T", wait.GetKind()))
	}
}

// waitFor sleeps on a durable timer.
func (e *executor) waitFor(node *v1.Node, d time.Duration) error {
	if d > 0 {
		// `NewTimerWithOptions` rather than `workflow.Sleep`, which is the same
		// command with a fixed `Sleep` summary — see [sleepSummary] for why the
		// step's own name is worth the extra line.
		//
		// The future returns an error only when the run is cancelled, which must
		// propagate: a cancelled run has to stop waiting, and swallowing this
		// would make a waiting step the one place cancellation does not reach.
		timer := workflow.NewTimerWithOptions(e.ctx, d,
			workflow.TimerOptions{Summary: sleepSummary(e.path, node.GetId())})
		if err := timer.Get(e.ctx, nil); err != nil {
			return nodeFailed(err)
		}
	}

	e.recordOutputs(node, v1.TimerOutputs(false))

	return nil
}

// waitForSignal blocks until a signal arrives or the wait times out.
//
// bounded says whether a `timeout:` was written at all, and it is carried
// separately from its value because the two facts used to be one and that was a
// latent bug. "No timeout" was spelled `timeout <= 0`, so a bound that computed to
// zero — `${deadline - now}` reached exactly on the deadline, or a hand-built spec
// setting the field to `0s` — read as an approval gate that blocks forever, which
// is the opposite of what it says. A written bound is honoured at its value now,
// and zero means the gate has already lapsed.
func (e *executor) waitForSignal(node *v1.Node, signal *v1.Signal, timeout time.Duration, bounded bool) (*v1.Node_Outputs, error) {
	name := signal.GetName()

	// A signal that arrived before this step was reached, drained from its
	// channel before an earlier run suspended and carried here in the run's
	// state. Without this, approving a gate early — which is ordinary behavior —
	// would leave the run waiting for something that already happened.
	if payload, sender, ok := e.takePendingSignal(name); ok {
		workflow.GetLogger(e.ctx).Info("step consumed a signal that arrived earlier",
			"id", node.GetId(), "signal", name)
		return v1.SignalOutputs(payload, sender, false), nil
	}

	// The same signal, one segment younger: delivered after this run began but
	// before this step was reached, so it sits buffered on the channel rather
	// than carried in the run's state. Consumed here, before the bound and the
	// prompt, for the reason the carried one is: this gate resolves without
	// ever parking. The local driver peeks its own delivered queue at exactly
	// this point, so without this peek the two drivers disagree about whether
	// the prompt of a gate nobody ever saw held was evaluated at all, which a
	// prompt that fails at runtime turns into a run failing on one driver and
	// finishing on the other.
	channel := workflow.GetSignalChannel(e.ctx, name)

	var early v1.SignalDelivery
	if channel.ReceiveAsync(&early) {
		workflow.GetLogger(e.ctx).Info("step consumed a signal that arrived earlier in this run",
			"id", node.GetId(), "signal", name)
		return v1.SignalOutputs(early.GetPayload(), early.GetSender(), false), nil
	}

	// A bound that has already lapsed. Answered before the selector rather than
	// through a zero-length timer, because a selector holding both a ready channel
	// and an already-fired timer may take either, and "which one" would then be a
	// property of the SDK's scheduling rather than of the workload — the two
	// drivers could not be made to agree on it, and neither could two replays.
	if bounded && timeout <= 0 {
		workflow.GetLogger(e.ctx).Info("wait timed out before it began",
			"id", node.GetId(), "signal", name, "timeout", timeout)
		return v1.SignalOutputs(nil, nil, true), nil
	}

	// Announced only from here down, which is the point after both ways a wait
	// can resolve without ever parking: a signal that arrived early and is
	// consumed above, and a bound that had already lapsed. Neither of those is
	// something an operator can act on, and reporting them would mean a run that
	// walked straight through a gate briefly claimed to be held at it. The local
	// driver announces at its own matching point, for the same reason.
	//
	// The deadline is computed from `workflow.Now`, which replays to the instant
	// it first returned, so a query answered before and after a replay reports
	// the same moment rather than one that slides forward.
	var deadline *timestamppb.Timestamp
	if bounded {
		deadline = timestamppb.New(workflow.Now(e.ctx).Add(timeout))
	}

	// The question this gate is asking, resolved here - at the same point the
	// wait announces itself, and after both ways above that a wait resolves
	// without ever parking. `workflow.Now` again, which replays to the instant it
	// first returned, so a prompt naming `now` reads the same on every replay.
	//
	// A prompt that fails to evaluate fails the step, exactly as a `timeout:`
	// that fails to evaluate does, and the local driver fails at the same point:
	// a gate that parks with no question would leave an approver looking at a
	// blank where the decision was meant to be.
	prompt, promptCut, err := v1.EvalSignalPrompt(context.Background(), signal, e.scope, workflow.Now(e.ctx))
	if err != nil {
		return nil, nodeFailed(err)
	}

	leave := e.waits.enter(e.pendingWait(node, name, deadline, prompt, promptCut))
	defer leave()

	var delivery v1.SignalDelivery

	// One selector for both shapes, and cancellation is a case in it.
	//
	// A bare channel.Receive cannot be the no-timeout path, which is what it was:
	// the SDK's signal channels are never closed, and Receive only returns false
	// on a closed empty channel, so it does not observe cancellation at all. A
	// gate with no timeout — the spelling this file recommends for an approval
	// that should block until somebody acts — therefore ignored `flow cancel`
	// entirely and stayed RUNNING until the run timeout, or forever without one.
	//
	// Selecting on ctx.Done() is what makes cancellation reach a waiting step,
	// and it is the same construction with or without a deadline; only the timer
	// case is conditional.
	var received bool
	selector := workflow.NewSelector(e.ctx)
	selector.AddReceive(channel, func(c workflow.ReceiveChannel, _ bool) {
		received = c.Receive(e.ctx, &delivery)
	})
	selector.AddReceive(e.ctx.Done(), func(workflow.ReceiveChannel, bool) {})

	// The timer, when there is one, is built on its own cancellable child
	// context rather than on e.ctx directly, so that whichever branch wins the
	// race can free it. Left uncancelled, an answered or cancelled gate leaves
	// a durable timer running: the server fires it into a run that no longer
	// cares, appending a TimerFired event and a whole workflow task to process
	// a no-op. See #770 and
	// https://docs.temporal.io/design-patterns/updatable-timer, the exact SDK
	// caveat this shape runs into.
	//
	// Gated behind [workflow.GetVersion] rather than done unconditionally,
	// because this is workflow code: an in-flight run may already have a
	// WorkflowTaskCompleted event recording exactly this decision — "the
	// signal won, and nothing was cancelled" — written by an engine that
	// never issued a CancelTimer here. Replaying that already-committed task
	// with code that now issues one is a different command sequence at a
	// point history has already fixed, which is a non-determinism error, not
	// a graceful upgrade: the run would wedge the first time a worker picked
	// it up after this deploys. GetVersion returns [workflow.DefaultVersion]
	// on exactly that replay — no marker for this changeID exists in that
	// history — so an old run keeps leaking its timer for the rest of its
	// life rather than failing to resume. Every run that starts, or reaches
	// this point for the first time, after the fix deploys records the
	// marker once and gets the cancelling behaviour from then on, including
	// its own later replays.
	var cancelTimer workflow.CancelFunc
	if bounded {
		timerCtx := e.ctx
		if workflow.GetVersion(e.ctx, cancelSignalWaitTimerChange, workflow.DefaultVersion, 1) != workflow.DefaultVersion {
			timerCtx, cancelTimer = workflow.WithCancel(e.ctx)
		}
		selector.AddFuture(workflow.NewTimerWithOptions(timerCtx, timeout,
			workflow.TimerOptions{Summary: waitTimeoutSummary(e.path, node.GetId())}),
			func(workflow.Future) {})
	}
	selector.Select(e.ctx)

	// Freed as soon as the selector resolves, whichever way it resolved. This
	// is a no-op when the timer branch itself is what woke the selector: the
	// SDK's own cancellation callback for a timer's context only issues
	// RequestCancelTimer when the timer's future is not already ready (see
	// NewTimerWithOptions in the SDK), so cancelling an already-fired timer
	// adds no command and changes nothing about the "timer wins" path — the
	// one case #770 says was already correct. It is also a no-op when the run
	// itself was cancelled: e.ctx.Done() closing already cancelled this timer
	// context too, as any context derived from e.ctx via [workflow.WithCancel]
	// does. What is left, and the only case this line changes, is a signal
	// that answered the gate before the timer did — where it is what stops
	// the abandoned timer from later firing into a run that no longer cares.
	if cancelTimer != nil {
		cancelTimer()
	}

	// Cancellation has to be distinguished from the timer before `received` is
	// read, because the two look identical here: a cancelled wait leaves
	// `received` false, which is exactly the shape of nobody having answered in
	// time.
	//
	// Treated as a timeout, a cancelled gate records `timed_out` and the run
	// walks on to the next step — so cancelling a workload would make it take
	// the "nobody approved" branch rather than stop. That is the one outcome a
	// cancellation must never produce, and it is invisible: the outputs are
	// well-formed and the run looks like it merely went unanswered.
	if err := e.ctx.Err(); err != nil {
		return nil, stepFailed(err, "cancelled while waiting for signal %q", name)
	}

	if !received {
		// Only a deadline can produce this now that cancellation is handled
		// above: with no timeout there is nothing else for the selector to have
		// woken on.
		if !bounded {
			return nil, nodeFailed(fmt.Errorf("stopped waiting for signal %q", name))
		}

		workflow.GetLogger(e.ctx).Info("wait timed out",
			"id", node.GetId(), "signal", name, "timeout", timeout)
		return v1.SignalOutputs(nil, nil, true), nil
	}

	return v1.SignalOutputs(delivery.GetPayload(), delivery.GetSender(), false), nil
}

// waitForSignals blocks until the first delivery arrives or the wait times out,
// then takes everything else already buffered for that name without blocking
// again.
//
// # It is [executor.waitForSignal] with a drain where the single take was
//
// Written beside it rather than folded into it, and that is the cost this
// feature chose to pay: a second signal-receiving path per driver, which has to
// stay in step with the single-wait one on four subtleties that are all already
// load-bearing. Each is called out where it happens below — the carried
// signals before the channel, the point the prompt is evaluated, the point the
// wait announces itself, and the [cancelSignalWaitTimerChange] version gate —
// and each is pinned by a conformance case that asserts the two spellings agree.
//
// # The drain is not a poll
//
// Nothing new waits. The blocking part is the *same* selector construction the
// single wait uses, over the same channel, `ctx.Done()` and the same optional
// cancellable timer, and it resolves on the first delivery exactly as before.
// Only once something has arrived does this take the rest, with `ReceiveAsync`
// against a channel that already holds them. So "process whatever arrived"
// falls out of the wait that exists rather than needing a settle window or a
// second timer — which is why the settle window is a separate question and not
// this one.
func (e *executor) waitForSignals(node *v1.Node, batch *v1.SignalBatch, timeout time.Duration, bounded bool) (*v1.Node_Outputs, error) {
	name := batch.GetName()
	limit := v1.SignalBatchSize(batch)

	channel := workflow.GetSignalChannel(e.ctx, name)

	// Subtlety one, carried-before-channel. Signals drained into the run's own
	// state before an earlier segment suspended are older than anything still
	// on the channel, and `deliveries` is ordered oldest first — so they are
	// taken first, exactly as [executor.takePendingSignal] takes the carried one
	// ahead of the channel peek in the single wait. Getting this backwards would
	// reorder a batch across a Continue-As-New and nowhere else, which is a bug
	// that only appears in a run long enough to suspend.
	deliveries := e.takePendingSignals(name, limit)

	// Then whatever is already buffered on the channel, up to the bound. This
	// is subtlety two's other half: a batch that finds anything here resolves
	// without ever parking, so it evaluates no prompt and announces no wait,
	// which is the single wait's own rule for its early-arrival peek.
	deliveries = drainInto(e.ctx, channel, deliveries, limit)

	if len(deliveries) > 0 {
		workflow.GetLogger(e.ctx).Info("step drained signals that had already arrived",
			"id", node.GetId(), "signal", name, "count", len(deliveries))

		return v1.SignalBatchOutputs(deliveries, false), nil
	}

	// A bound that has already lapsed, answered before the selector for
	// [executor.waitForSignal]'s reason: a selector holding a ready channel and
	// an already-fired timer may take either, and which one would then be a
	// property of the SDK's scheduling rather than of the workload.
	if bounded && timeout <= 0 {
		workflow.GetLogger(e.ctx).Info("batch wait timed out before it began",
			"id", node.GetId(), "signal", name, "timeout", timeout)

		return v1.SignalBatchOutputs(nil, true), nil
	}

	var deadline *timestamppb.Timestamp
	if bounded {
		deadline = timestamppb.New(workflow.Now(e.ctx).Add(timeout))
	}

	// Subtlety two, the prompt's evaluation point: here, after both ways this
	// wait could have resolved without parking, and at the same instant the
	// wait announces itself. [v1.EvalSignalBatchPrompt] is [v1.EvalSignalPrompt]
	// under another name for precisely this reason — the two must not be able
	// to drift in what they refuse or how they bound.
	prompt, promptCut, err := v1.EvalSignalBatchPrompt(context.Background(), batch, e.scope, workflow.Now(e.ctx))
	if err != nil {
		return nil, nodeFailed(err)
	}

	// Subtlety three, the announcement point. A batch parked on an empty
	// channel is a gate an operator can act on, and it is reported exactly as a
	// single wait is: same [v1.PendingWait], same `signal_name`, so every
	// surface that already lists parked gates lists this one with no change.
	leave := e.waits.enter(e.pendingWait(node, name, deadline, prompt, promptCut))
	defer leave()

	var (
		delivery v1.SignalDelivery
		received bool
	)

	selector := workflow.NewSelector(e.ctx)
	selector.AddReceive(channel, func(c workflow.ReceiveChannel, _ bool) {
		received = c.Receive(e.ctx, &delivery)
	})
	selector.AddReceive(e.ctx.Done(), func(workflow.ReceiveChannel, bool) {})

	// Subtlety four, the version gate. The same changeID as the single wait's,
	// deliberately: it names one decision — "this engine cancels an answered
	// gate's timer" — and a run that reached that decision through either
	// spelling must replay it the same way. A second changeID would record a
	// second marker for the same behaviour and give a replaying run two
	// answers to one question.
	var cancelTimer workflow.CancelFunc
	if bounded {
		timerCtx := e.ctx
		if workflow.GetVersion(e.ctx, cancelSignalWaitTimerChange, workflow.DefaultVersion, 1) != workflow.DefaultVersion {
			timerCtx, cancelTimer = workflow.WithCancel(e.ctx)
		}
		selector.AddFuture(workflow.NewTimerWithOptions(timerCtx, timeout,
			workflow.TimerOptions{Summary: waitTimeoutSummary(e.path, node.GetId())}),
			func(workflow.Future) {})
	}
	selector.Select(e.ctx)

	if cancelTimer != nil {
		cancelTimer()
	}

	// Cancellation before `received`, for [executor.waitForSignal]'s reason: a
	// cancelled wait and an unanswered one are the same shape here, and treating
	// a cancelled batch as an empty one would make `flow cancel` take the
	// "nothing arrived" branch instead of stopping the run.
	if err := e.ctx.Err(); err != nil {
		return nil, stepFailed(err, "cancelled while waiting for signal %q", name)
	}

	if !received {
		if !bounded {
			return nil, nodeFailed(fmt.Errorf("stopped waiting for signal %q", name))
		}

		workflow.GetLogger(e.ctx).Info("batch wait timed out",
			"id", node.GetId(), "signal", name, "timeout", timeout)

		return v1.SignalBatchOutputs(nil, true), nil
	}

	// The drain proper: the delivery that answered the wait, plus everything
	// that arrived alongside it and is sitting on the channel now. This is the
	// whole saving — a burst delivered while one workflow task was pending is
	// read here in that one task rather than in one task each.
	deliveries = drainInto(e.ctx, channel, []*v1.SignalDelivery{{
		Payload: delivery.GetPayload(),
		Sender:  delivery.GetSender(),
	}}, limit)

	workflow.GetLogger(e.ctx).Info("step drained a batch of signals",
		"id", node.GetId(), "signal", name, "count", len(deliveries))

	return v1.SignalBatchOutputs(deliveries, false), nil
}

// drainInto appends whatever is already buffered on channel to deliveries,
// without blocking, stopping at limit.
//
// The same bounded `ReceiveAsync` loop [drainSignals] runs at Continue-As-New,
// which is what makes this feature a spelling rather than a mechanism: Temporal
// offers no batch receipt — `ReceiveChannel` has `Receive`,
// `ReceiveWithTimeout`, `ReceiveAsync`, `ReceiveAsyncWithMoreFlag` and `Len`,
// and a drain is a loop over the third — so the primitive was already written
// here and only unreachable from a Flowfile.
//
// Stopping at the limit leaves the remainder on the channel rather than
// dropping it or re-buffering it. That is the behaviour a `loop:` around the
// step wants, and it is why reaching the bound costs an iteration rather than
// an approval. It is also why the bound is safe to be a count: the resource the
// peer controls is how many arrive, and what is not taken is not read into
// memory at all.
func drainInto(ctx workflow.Context, channel workflow.ReceiveChannel, deliveries []*v1.SignalDelivery, limit int) []*v1.SignalDelivery {
	for len(deliveries) < limit {
		var delivery v1.SignalDelivery
		if !channel.ReceiveAsync(&delivery) {
			break
		}

		deliveries = append(deliveries, &v1.SignalDelivery{
			Payload: delivery.GetPayload(),
			Sender:  delivery.GetSender(),
		})
	}

	return deliveries
}

// recordOutputs records a step's outputs in the scope later steps resolve
// against.
func (e *executor) recordOutputs(node *v1.Node, outputs *v1.Node_Outputs) {
	e.scope.Outputs.StepValues[node.GetId()] = outputs
}

// takePendingSignal consumes an early-arriving signal, if one is held for this
// name.
func (e *executor) takePendingSignal(name string) (*v1.Node_Outputs, *v1.SignalSender, bool) {
	if e.signals == nil {
		return nil, nil, false
	}

	for i, pending := range e.signals.pending {
		if pending.GetName() != name {
			continue
		}
		// Consumed, so a second wait on the same name blocks rather than being
		// satisfied twice by one signal.
		e.signals.pending = append(e.signals.pending[:i:i], e.signals.pending[i+1:]...)
		return pending.GetPayload(), pending.GetSender(), true
	}

	return nil, nil, false
}

// takePendingSignals consumes up to limit early-arriving signals held for this
// name, oldest first.
//
// The plural of [executor.takePendingSignal], and it keeps that function's two
// properties rather than reimplementing them: each carried signal is *consumed*,
// so a later wait on the same name is not satisfied a second time by one
// delivery; and the order is the order they arrived, which is the order
// `deliveries` reports.
func (e *executor) takePendingSignals(name string, limit int) []*v1.SignalDelivery {
	if e.signals == nil || limit <= 0 {
		return nil
	}

	var (
		taken []*v1.SignalDelivery
		kept  []*v1.PendingSignal
	)

	for _, pending := range e.signals.pending {
		if pending.GetName() != name || len(taken) >= limit {
			kept = append(kept, pending)

			continue
		}
		taken = append(taken, &v1.SignalDelivery{
			Payload: pending.GetPayload(),
			Sender:  pending.GetSender(),
		})
	}

	if len(taken) > 0 {
		e.signals.pending = kept
	}

	return taken
}

// drainSignals collects signals that have arrived but not been waited for, so
// that suspending the run does not lose them.
//
// This is the part of Continue-As-New that is easy to get wrong and expensive to
// get wrong. Temporal delivers a signal to whichever run is current when it
// arrives, and a run that continues as new drops whatever is still buffered on a
// channel it never read — the SDK warns about it and carries on. So a workload
// whose approval arrived while it was on an earlier step, and which then
// suspended before reaching the gate, would resume with the approval gone and
// wait forever.
//
// Draining is possible only because the specification declares every signal name
// statically: the run knows exactly which channels to check, without guessing at
// what someone might have sent.
func drainSignals(ctx workflow.Context, spec *v1.Workflow, carried []*v1.PendingSignal) []*v1.PendingSignal {
	pending := carried

	for _, name := range v1.SignalNames(spec) {
		channel := workflow.GetSignalChannel(ctx, name)

		for {
			if len(pending) >= v1.MaxPendingSignals {
				// The first to arrive is the one that approved the gate, so the
				// oldest are kept. A sender that delivers a million signals
				// cannot grow the run's state without limit.
				workflow.GetLogger(ctx).Warn(
					"dropping signals beyond the carry limit; a wait consumes one, and the earliest are kept",
					"signal", name, "limit", v1.MaxPendingSignals)
				return pending
			}

			var delivery v1.SignalDelivery
			if !channel.ReceiveAsync(&delivery) {
				break
			}

			workflow.GetLogger(ctx).Info(
				"carrying a signal that arrived before its step was reached", "signal", name)

			pending = append(pending, &v1.PendingSignal{
				Name:    name,
				Payload: delivery.GetPayload(),
				Sender:  delivery.GetSender(),
			})
		}
	}

	return pending
}
