package engine

import (
	"sort"

	"google.golang.org/protobuf/proto"
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
// and on how many the run retains to look one up by name is [v1.MaxHeldWaits];
// both are read from the package both drivers import rather than written down
// here as well.

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
	// entries are the parked waits, in the order they parked, at most
	// [v1.MaxHeldWaits] of them. Each is built at
	// the moment its wait blocks, and only a quorum wait's approval count
	// changes afterwards, in place, so [waitRegistry.snapshot] clones what it
	// reports: a query serializes the copy and the wait goes on counting.
	entries []*v1.PendingWait

	// seqs is the arrival number of each entry, parallel to entries and
	// strictly increasing along it. A listing resumes after a number rather
	// than at an index, so gates that close between two pages cannot make it
	// skip one that is still open.
	seqs []uint64

	// issued is the last arrival number handed out. Counts every wait that
	// reached [waitRegistry.enter], retained or not, so a number is never
	// reused within a run.
	issued uint64

	// refused counts waits that are parked right now and are *not* in entries,
	// because [v1.MaxHeldWaits] was already spent when they arrived.
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

	r.issued++

	if len(r.entries) >= v1.MaxHeldWaits {
		r.refused++

		return func() { r.refused-- }
	}

	r.entries = append(r.entries, wait)
	r.seqs = append(r.seqs, r.issued)

	return func() {
		for i, entry := range r.entries {
			if entry == wait {
				r.entries = append(r.entries[:i:i], r.entries[i+1:]...)
				r.seqs = append(r.seqs[:i:i], r.seqs[i+1:]...)

				break
			}
		}
	}
}

// snapshot copies the first [v1.MaxPendingWaits] parked waits into the answer a
// query is serialized from, and reports whether the copy is short of what the
// run is really holding.
//
// A copy of the slice for [progress.snapshot]'s reason: the underlying array is
// appended to and cut as waits park and unpark, and handing a caller the live
// one would let the answer change under serialization. The messages inside it
// are cloned too, for the reason below.
func (r *waitRegistry) snapshot() (waits []*v1.PendingWait, truncated bool) {
	if r == nil {
		return nil, false
	}

	reported := r.entries[:min(len(r.entries), v1.MaxPendingWaits)]
	truncated = len(r.entries) > len(reported) || r.isTruncated()
	if len(reported) == 0 {
		return nil, truncated
	}

	// Cloned, because a quorum wait updates its approval count in place while it
	// is parked; see [executor.waitForQuorum].
	waits = make([]*v1.PendingWait, 0, len(reported))
	for _, entry := range reported {
		waits = append(waits, proto.Clone(entry).(*v1.PendingWait))
	}

	return waits, truncated
}

// find is the one-gate lookup [GateQuery] answers with: the first parked wait on
// signalName, wherever it sits among the held ones, and whether a miss is a real
// one. See [v1.PendingWaits.Find], the local driver's twin.
func (r *waitRegistry) find(signalName string) (wait *v1.PendingWait, complete bool) {
	if r == nil {
		return nil, true
	}

	for _, entry := range r.entries {
		if entry.GetSignalName() == signalName {
			return proto.Clone(entry).(*v1.PendingWait), true
		}
	}

	return nil, !r.isTruncated()
}

// page is the listing [GatesQuery] answers with: up to limit parked waits that
// arrived after the arrival number after (0 starts at the first), in the order
// they parked.
//
// The slice [waitRegistry.snapshot] cannot give, which stops at
// [v1.MaxPendingWaits]: this one walks everything the run retains,
// [v1.MaxHeldWaits], a page at a time. However large limit is, an answer holds
// no more than the run retains, so it is bounded by the work bound and not by
// what a caller asks for.
func (r *waitRegistry) page(after uint64, limit int) *v1.GatePage {
	out := &v1.GatePage{}
	if r == nil {
		return out
	}

	limit = max(limit, 0)
	// The first retained arrival number past the cursor; seqs is sorted.
	start := sort.Search(len(r.seqs), func(i int) bool { return r.seqs[i] > after })

	end := start + min(limit, len(r.entries)-start)
	for i := start; i < end; i++ {
		out.Waits = append(out.Waits, proto.Clone(r.entries[i]).(*v1.PendingWait))
		out.LastSeq = r.seqs[i]
	}

	out.More = end < len(r.entries)
	out.Incomplete = r.isTruncated()

	return out
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
