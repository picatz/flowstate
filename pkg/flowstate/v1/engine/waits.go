package engine

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What a run is parked on, as against where it has got to.
//
// [ProgressQuery] could say a run was on the step `approval` and could not say
// that `approval` was a gate, which signal name would open it, or whether the
// gate lapses on its own. Those are the three things somebody looking at a
// stuck-looking run actually needs: a run parked on a `wait_for_signal:` is
// waiting on a person, and until this the person had no way to learn what to
// send.
//
// It is answered by the same query for the reason it is not derivable from
// outside at all: a wait lives in the interpreter's own stack, so nothing the
// service records knows it exists. See [v1.PendingWait] for why the projection
// carries no payload and is non-secret by construction.
//
// The bound on how many parked waits one answer reports is [v1.MaxPendingWaits],
// read from the package both drivers import rather than written down here as
// well.

// waitRegistry is the set of signal waits parked in this run right now.
//
// Shared by pointer with every nested executor, including the concurrent ones
// that deliberately do not carry [progress]. A position is singular, so no one
// parallel branch may claim it; a set of waits is plural by construction, so
// every branch may add to it and the answer stays true. That is the whole
// reason this is a second structure rather than a field on [progress].
//
// No lock, for [progress]'s reason: workflow coroutines are scheduled
// cooperatively, so only one runs at a time and a query handler runs on that
// same scheduler.
type waitRegistry struct {
	// entries are the parked waits, in the order they parked. Each is built
	// once, at the moment its wait blocks, and never mutated afterwards, which
	// is what lets [waitRegistry.snapshot] copy the slice rather than clone
	// every message in it.
	entries []*v1.PendingWait

	// refused counts waits that are parked right now and are *not* in entries,
	// because the bound was already spent when they arrived.
	//
	// A count of the live ones rather than a flag that has ever tripped, which
	// is a precision [progress.loopStateTruncated] cannot have: a refused loop
	// entry never announces that it stopped mattering, while a refused wait
	// runs its own leave when it unparks. So this can go back to zero honestly,
	// and an answer says it is incomplete exactly while it is.
	refused int
}

// enter registers a parked wait and returns the function that unregisters it.
//
// The returned function is always safe to call and always exactly once: a
// refused wait gets one that gives its refusal back rather than one that does
// nothing, so a run that briefly held more gates than the bound stops reporting
// itself truncated once it does not.
func (r *waitRegistry) enter(wait *v1.PendingWait) func() {
	if r == nil || wait == nil {
		return func() {}
	}

	if len(r.entries) >= v1.MaxPendingWaits {
		r.refused++

		return func() { r.refused-- }
	}

	r.entries = append(r.entries, wait)

	return func() {
		for i, entry := range r.entries {
			if entry == wait {
				r.entries = append(r.entries[:i:i], r.entries[i+1:]...)

				break
			}
		}
	}
}

// snapshot copies the parked waits into the answer a query is serialized from,
// and reports whether the copy is short of what the run is really holding.
//
// A copy of the slice for [progress.snapshot]'s reason: the underlying array is
// appended to and cut as waits park and unpark, and handing a caller the live
// one would let the answer change under serialization. The messages inside it
// are shared rather than cloned because nothing ever mutates one after it is
// built.
func (r *waitRegistry) snapshot() (waits []*v1.PendingWait, truncated bool) {
	if r == nil || len(r.entries) == 0 {
		return nil, r.isTruncated()
	}

	return append(make([]*v1.PendingWait, 0, len(r.entries)), r.entries...), r.isTruncated()
}

// isTruncated reports whether some wait parked right now went unrecorded.
func (r *waitRegistry) isTruncated() bool {
	return r != nil && r.refused > 0
}

// pendingWait describes the wait about to park, positioned by whatever the run
// knows about where it is.
//
// The step's own id is always exact, including inside concurrent work where a
// position is not: the id comes from the node being run rather than from
// [progress]. The path is the ancestry [progress] happens to be holding, which
// is empty in a parallel branch or a concurrent iteration because those
// deliberately carry no position at all. That asymmetry is the honest one: an
// operator needs the name of the gate to open it, and the ancestry only to find
// it in a file.
//
// deadline is nil for a wait the author wrote no `timeout:` for, which is a
// different fact from a deadline that has not been reached yet: that gate
// blocks until somebody acts.
//
// prompt is what the gate is asking for, already evaluated and already bounded
// by [v1.EvalSignalPrompt]. Taken as a value rather than evaluated here, because
// evaluating it can fail the step and this function is called from one place
// that is already inside the wait's own error handling - and because the local
// driver's matching function takes it the same way, which is what keeps the two
// evaluating at one point each rather than at whichever point their reporting
// happens to sit at.
func (e *executor) pendingWait(
	node *v1.Node,
	// The signal *name* rather than the message, because both wait spellings
	// announce through here and they carry different messages. A name is all a
	// [v1.PendingWait] ever held of either.
	signalName string,
	deadline *timestamppb.Timestamp,
	prompt string,
	promptTruncated bool,
) *v1.PendingWait {
	return &v1.PendingWait{
		StepId:          node.GetId(),
		Path:            e.progress.ancestors(),
		SignalName:      signalName,
		Deadline:        deadline,
		Prompt:          prompt,
		PromptTruncated: promptTruncated,
		// The top-level workflow's declarations, never the callee's: a
		// delivery to this run is authorized against the policy the server
		// recorded on it at submit, which is the root spec's, so reporting a
		// called workflow's own `signals:` here would tell an operator a gate
		// was policed by something that does not police it. See
		// server/lifecycle.go's authorizeSignal.
		Policed: e.spec.GetSignals()[signalName] != nil,
	}
}
