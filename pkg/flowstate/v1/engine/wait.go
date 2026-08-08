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

	default:
		return nodeFailed(fmt.Errorf("unsupported wait kind %T", wait.GetKind()))
	}
}

// waitFor sleeps on a durable timer.
func (e *executor) waitFor(node *v1.Node, d time.Duration) error {
	if d > 0 {
		// Sleep returns an error only when the run is cancelled, which must
		// propagate: a cancelled run has to stop waiting, and swallowing this
		// would make a waiting step the one place cancellation does not reach.
		if err := workflow.Sleep(e.ctx, d); err != nil {
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

	leave := e.waits.enter(e.pendingWait(node, signal, deadline, prompt, promptCut))
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
	if bounded {
		selector.AddFuture(workflow.NewTimer(e.ctx, timeout), func(workflow.Future) {})
	}
	selector.Select(e.ctx)

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
