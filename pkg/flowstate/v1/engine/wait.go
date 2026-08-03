package engine

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/workflow"
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
	case *v1.Wait_Duration:
		return e.waitFor(node, kind.Duration.AsDuration())

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
		return e.waitForSignal(node, kind.Signal, wait.GetTimeout().AsDuration())

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
func (e *executor) waitForSignal(node *v1.Node, signal *v1.Signal, timeout time.Duration) error {
	name := signal.GetName()

	// A signal that arrived before this step was reached, drained from its
	// channel before an earlier run suspended and carried here in the run's
	// state. Without this, approving a gate early — which is ordinary behavior —
	// would leave the run waiting for something that already happened.
	if payload, ok := e.takePendingSignal(name); ok {
		workflow.GetLogger(e.ctx).Info("step consumed a signal that arrived earlier",
			"id", node.GetId(), "signal", name)
		e.recordOutputs(node, v1.SignalOutputs(payload, false))
		return nil
	}

	channel := workflow.GetSignalChannel(e.ctx, name)

	var payload v1.Node_Outputs

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
		received = c.Receive(e.ctx, &payload)
	})
	selector.AddReceive(e.ctx.Done(), func(workflow.ReceiveChannel, bool) {})
	if timeout > 0 {
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
		return stepFailed(err, "cancelled while waiting for signal %q", name)
	}

	if !received {
		// Only a deadline can produce this now that cancellation is handled
		// above: with no timeout there is nothing else for the selector to have
		// woken on.
		if timeout <= 0 {
			return nodeFailed(fmt.Errorf("stopped waiting for signal %q", name))
		}

		workflow.GetLogger(e.ctx).Info("wait timed out",
			"id", node.GetId(), "signal", name, "timeout", timeout)
		e.recordOutputs(node, v1.SignalOutputs(nil, true))
		return nil
	}

	e.recordOutputs(node, v1.SignalOutputs(&payload, false))

	return nil
}

// recordOutputs records a step's outputs in the scope later steps resolve
// against.
func (e *executor) recordOutputs(node *v1.Node, outputs *v1.Node_Outputs) {
	e.scope.Outputs.StepValues[node.GetId()] = outputs
}

// takePendingSignal consumes an early-arriving signal, if one is held for this
// name.
func (e *executor) takePendingSignal(name string) (*v1.Node_Outputs, bool) {
	if e.signals == nil {
		return nil, false
	}

	for i, pending := range e.signals.pending {
		if pending.GetName() != name {
			continue
		}
		// Consumed, so a second wait on the same name blocks rather than being
		// satisfied twice by one signal.
		e.signals.pending = append(e.signals.pending[:i:i], e.signals.pending[i+1:]...)
		return pending.GetPayload(), true
	}

	return nil, false
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

			var payload v1.Node_Outputs
			if !channel.ReceiveAsync(&payload) {
				break
			}

			workflow.GetLogger(ctx).Info(
				"carrying a signal that arrived before its step was reached", "signal", name)

			pending = append(pending, &v1.PendingSignal{
				Name:    name,
				Payload: &payload,
			})
		}
	}

	return pending
}
