package flowstatev1

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// What a run is parked on, for the local driver, in the shape the durable
// driver answers a query with.
//
// The durable driver answers this from inside a running workflow, because
// nothing outside one knows what it is waiting for. A local run is a process,
// and its equivalent of "reach the worker and ask" is "hold a reference to the
// registry the run announces into". So the mechanism differs exactly as much as
// the two drivers' notions of "outside the run" differ, and the *answer* is the
// same [PendingWait] messages with the same rules about what is set and what is
// absent - which is the part an author is entitled to see agree.
//
// Local rehearsal is where an author first meets a gate that never opens. Before
// this, `flow run local` on a workflow parked on `wait_for_signal:` looked
// exactly like a hung process, and the name to send was findable only by reading
// the file back.

// MaxPendingWaits bounds how many parked waits one answer reports.
//
// Read by both drivers rather than written down twice: a bound that disagreed
// with itself would make the truncation flag mean two different things
// depending on which driver produced the answer, and CLAUDE.md's rule for a
// value with one meaning is that one constant cannot disagree with itself.
//
// The resource is "waits open at once", which no other bound in this repo
// covers: a `for_each` with `max_parallel:` over a list a task fetched can open
// one gate per concurrent iteration, and how long that list is was decided by a
// peer rather than by the author. Locally the count cannot exceed one today,
// because the local driver runs `parallel:` branches and `for_each` iterations
// sequentially, so this is the durable driver's bound that the local driver
// applies anyway rather than keeping a second rule.
const MaxPendingWaits = 64

// MaxHeldWaits bounds how many parked waits a run keeps findable by signal
// name, which is what lets the server's GetGate answer for a gate past the
// [MaxPendingWaits] the summary lists.
//
// The summary is a bound on what one answer *says*; this is the bound on what
// the run *retains* to look a single gate up in, and the two are different
// resources: the first is a response size, the second is memory held inside a
// workflow for as long as the gates stay open. Sized to a `for_each` over its
// largest list ([MaxForEachItems]) with all of its iterations parked at once,
// rounded up, so the shape that opens the most gates is findable; a gate past
// it is not held, and the lookup says it cannot tell rather than that the gate
// is closed. Read by both drivers from here for the reason [MaxPendingWaits] is.
const MaxHeldWaits = 1024

// PendingWaits is the set of signal waits a local run is parked on, and the
// local driver's answer to the question engine.ProgressQuery answers durably.
//
// Safe for concurrent use, unlike the durable driver's equivalent: workflow
// coroutines are cooperatively scheduled so only one runs at a time, while a
// local run executes on ordinary goroutines and whoever is watching it is on
// another one.
type PendingWaits struct {
	mu sync.Mutex

	// entries are the parked waits in the order they parked, at most
	// [MaxHeldWaits] of them, each built at the moment its wait blocks. The only field that changes afterwards is a quorum
	// wait's approval count, and only under mu through [PendingWaits.report],
	// which is why [PendingWaits.Snapshot] hands out copies.
	entries []*PendingWait

	// refused counts waits parked right now that are not in entries because
	// [MaxHeldWaits] was already spent when they arrived. A live count rather
	// than a flag that has ever tripped, so an answer calls itself incomplete
	// exactly while it is.
	refused int
}

// NewPendingWaits returns an empty registry. The zero value works too.
func NewPendingWaits() *PendingWaits { return &PendingWaits{} }

// Snapshot returns the waits the run is parked on right now, at most
// [MaxPendingWaits] of them, and whether more are parked than are reported.
//
// A copy of the slice, because the run keeps appending to and cutting the live
// one as gates open and close, and handing a caller that one would let the
// answer change while it is being read. The messages inside are cloned as well,
// because a quorum wait's approval count moves while it is parked: at most
// [MaxPendingWaits] small messages, copied only when somebody asks.
func (w *PendingWaits) Snapshot() (waits []*PendingWait, truncated bool) {
	if w == nil {
		return nil, false
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	reported := w.entries[:min(len(w.entries), MaxPendingWaits)]
	truncated = len(w.entries) > len(reported) || w.refused > 0
	if len(reported) == 0 {
		return nil, truncated
	}

	waits = make([]*PendingWait, 0, len(reported))
	for _, entry := range reported {
		waits = append(waits, proto.Clone(entry).(*PendingWait))
	}

	return waits, truncated
}

// Find returns a copy of the first parked wait on signalName, the answer to
// "is this one gate open" that does not depend on [MaxPendingWaits].
//
// complete says whether a miss is a real one: false means some parked wait was
// not retained because [MaxHeldWaits] was spent, so the name may be open and
// unfindable. A hit is always complete.
func (w *PendingWaits) Find(signalName string) (wait *PendingWait, complete bool) {
	if w == nil {
		return nil, true
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, entry := range w.entries {
		if entry.GetSignalName() == signalName {
			return proto.Clone(entry).(*PendingWait), true
		}
	}

	return nil, w.refused == 0
}

// report rewrites a parked quorum wait's approval count from its tally, under
// mu so a [PendingWaits.Snapshot] on another goroutine never sees it half
// written.
func (w *PendingWaits) report(wait *PendingWait, tally *QuorumTally) {
	w.mu.Lock()
	defer w.mu.Unlock()

	tally.Report(wait)
}

// enter registers a parked wait and returns the function that unregisters it.
//
// The returned function is always safe to call exactly once, including for a
// wait the bound refused: that one gives the refusal back, so a run that briefly
// held more gates than the bound stops calling itself truncated once it does
// not.
func (w *PendingWaits) enter(wait *PendingWait) func() {
	if w == nil || wait == nil {
		return func() {}
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.entries) >= MaxHeldWaits {
		w.refused++

		return func() {
			w.mu.Lock()
			defer w.mu.Unlock()
			w.refused--
		}
	}

	w.entries = append(w.entries, wait)

	return func() {
		w.mu.Lock()
		defer w.mu.Unlock()

		for i, entry := range w.entries {
			if entry == wait {
				w.entries = append(w.entries[:i:i], w.entries[i+1:]...)

				break
			}
		}
	}
}

// waitReporting is everything a local wait needs to describe itself, carried
// together in one context value.
//
// One value rather than three, so the whole of this bookkeeping is present or
// absent together: a run nobody is watching does none of it, which is what keeps
// the cost of a feature for observers off the path of a run that has none.
type waitReporting struct {
	// waits is where a parked wait announces itself. Never nil in a value that
	// is installed at all.
	waits *PendingWaits

	// policies is the top-level workflow's declared `signals:`, read once when
	// the run starts. The root's, never a callee's, because a delivery to this
	// run is authorized against the root's declarations - see
	// [PendingWait.Policed] and server/lifecycle.go's authorizeSignal.
	policies map[string]*SignalPolicy

	// ancestry is the steps enclosing the step about to run, outermost first.
	ancestry []string

	// unpositioned marks that execution has descended into concurrent work, so
	// there is no ancestry to report even though this driver happens to know
	// one.
	//
	// The local driver runs `parallel:` branches and concurrent `for_each`
	// iterations sequentially, so it *could* name the enclosing step where the
	// durable driver cannot - and reporting it would be a disagreement between
	// the drivers about a field an author reads. The rule the schema states is
	// the durable one ("no position inside concurrent work", see
	// [RunProgress.Path]), so this driver adopts it rather than being
	// gratuitously more precise about an accident of how it schedules.
	unpositioned bool
}

// waitReportingKey carries [waitReporting].
type waitReportingKey struct{}

// ContextWithPendingWaits installs the registry a local run's signal waits
// announce themselves into, and is how a caller watches what a local run is
// parked on.
//
// A context value for the reason [ContextWithProgress] is one: a driver's
// reporting mechanism does not belong in the signature every task and every
// wait is written against.
func ContextWithPendingWaits(ctx context.Context, waits *PendingWaits) context.Context {
	if waits == nil {
		return ctx
	}

	return context.WithValue(ctx, waitReportingKey{}, &waitReporting{waits: waits})
}

// waitReportingFromContext returns the bookkeeping a watcher installed, if one
// did.
func waitReportingFromContext(ctx context.Context) (*waitReporting, bool) {
	reporting, ok := ctx.Value(waitReportingKey{}).(*waitReporting)

	return reporting, ok && reporting != nil
}

// withWaitReporting replaces the bookkeeping, deriving one context per nesting
// level so that concurrent branches cannot see each other's ancestry.
func withWaitReporting(ctx context.Context, reporting *waitReporting) context.Context {
	return context.WithValue(ctx, waitReportingKey{}, reporting)
}

// contextWithWaitPolicies records the workflow whose `signals:` decide whether a
// wait reports itself policed, called once by the local driver's run entry
// point. A no-op where nobody is watching.
func contextWithWaitPolicies(ctx context.Context, policies map[string]*SignalPolicy) context.Context {
	reporting, ok := waitReportingFromContext(ctx)
	if !ok {
		return ctx
	}

	next := *reporting
	next.policies = policies

	return withWaitReporting(ctx, &next)
}

// pushWaitAncestor returns a context whose waits report stepID as their nearest
// enclosing step, for the steps nested inside it.
//
// A derived context rather than a mutated value, because the nesting it records
// is per branch of the tree: returning to an outer level is returning to the
// outer context.
func pushWaitAncestor(ctx context.Context, stepID string) context.Context {
	reporting, ok := waitReportingFromContext(ctx)
	if !ok || reporting.unpositioned {
		return ctx
	}

	next := *reporting
	// A fresh array rather than an append onto the parent's: two siblings
	// descending from one level would otherwise write into the same spare
	// capacity and read each other's step id.
	next.ancestry = append(append(make([]string, 0, len(reporting.ancestry)+1), reporting.ancestry...), stepID)

	return withWaitReporting(ctx, &next)
}

// enterConcurrentWait returns a context whose waits report no ancestry at all,
// for the steps inside concurrent work. See [waitReporting.unpositioned].
func enterConcurrentWait(ctx context.Context) context.Context {
	reporting, ok := waitReportingFromContext(ctx)
	if !ok || reporting.unpositioned {
		return ctx
	}

	next := *reporting
	next.ancestry = nil
	next.unpositioned = true

	return withWaitReporting(ctx, &next)
}

// announceLocalWait registers a wait that is about to park, and returns the
// function that unregisters it. Always safe to call, and a no-op where nobody is
// watching.
//
// deadline is nil where the author wrote no `timeout:`, which is a different
// fact from a deadline not yet reached: that gate blocks until somebody acts.
//
// prompt is what the gate is asking for, already evaluated and already bounded
// by [EvalSignalPrompt]. Evaluated by the caller rather than here, and that
// placement is load-bearing: a prompt that fails to evaluate fails the step, and
// this function does nothing at all when nobody is watching, so evaluating it
// here would make a run fail only while somebody was looking at it.
func announceLocalWait(
	ctx context.Context,
	node *Node,
	// The signal *name* rather than the message, because both wait spellings
	// announce through here and they carry different messages. A name is all a
	// [PendingWait] ever held of either.
	signalName string,
	deadline *timestamppb.Timestamp,
	prompt string,
	promptTruncated bool,
) func() {
	reporting, ok := waitReportingFromContext(ctx)
	if !ok {
		return func() {}
	}

	return reporting.waits.enter(reporting.pendingWait(node, signalName, deadline, prompt, promptTruncated))
}

// announceLocalQuorumWait is [announceLocalWait] for a `quorum:` wait: the
// parked wait also reports the tally's approval count, and update re-reports it
// after each delivery the wait takes.
//
// Both returned functions are always safe to call, and do nothing where nobody
// is watching.
func announceLocalQuorumWait(
	ctx context.Context,
	node *Node,
	signalName string,
	deadline *timestamppb.Timestamp,
	prompt string,
	promptTruncated bool,
	tally *QuorumTally,
) (update func(), leave func()) {
	reporting, ok := waitReportingFromContext(ctx)
	if !ok {
		return func() {}, func() {}
	}

	wait := reporting.pendingWait(node, signalName, deadline, prompt, promptTruncated)
	tally.Report(wait)

	return func() { reporting.waits.report(wait, tally) }, reporting.waits.enter(wait)
}

// pendingWait describes a wait about to park, from what the run knows about
// where it is.
func (r *waitReporting) pendingWait(
	node *Node,
	signalName string,
	deadline *timestamppb.Timestamp,
	prompt string,
	promptTruncated bool,
) *PendingWait {
	return &PendingWait{
		StepId:          node.GetId(),
		Path:            r.ancestry,
		SignalName:      signalName,
		Deadline:        deadline,
		Policed:         r.policies[signalName] != nil,
		Prompt:          prompt,
		PromptTruncated: promptTruncated,
	}
}
