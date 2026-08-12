package flowstatev1

import (
	"context"
	"math/rand/v2"
	"sync"
)

// The seam a deterministic simulation runs through (issue #477, slice 0).
//
// The local driver has always made every scheduling decision the same way: a
// `parallel:` block runs its branches in declaration order, and an `async:` step
// runs its work where it is written and holds the result until the join. Both
// choices are legal under the execution model and neither is the *only* legal
// one — the durable driver makes different ones for the same file, because its
// branches are coroutines and its async work genuinely overlaps.
//
// That is the whole difficulty with the promise #418 makes. "Completion order is
// never observable" is a claim about every schedule, and a driver that only ever
// takes one of them cannot check it: written order is the schedule least likely
// to expose a dependency on order, because it is the order the file is read in.
//
// So the decisions become a value. A [Scheduler] on the context answers the two
// questions this driver has — in what order should these independent units
// advance, and should this launched step's work happen now or at its join — and
// the default answers exactly what the driver answered before ([WrittenOrder]),
// so nothing outside a test changes shape. [NewSeededScheduler] answers from a
// seeded PRNG instead, which turns "the same file under a different interleaving"
// into a thing a test can ask for by name and reproduce by number.
//
// # What is deliberately not behind the seam
//
// Go's own goroutine scheduler. The engine's decision points are the schedule
// space that matters and the only one this repo can control without a runtime
// shim; data races in arbitrary Go code stay the race detector's job and the
// flowtest package's ordering claims stay the `-cpu=1` tier's job. The seam sits
// above both rather than replacing either.
//
// A `for_each` with `max_parallel:` is also deliberately out. Its iterations
// accumulate `results` and weigh them against [MaxLoopResultsBytes] *as they
// run*, so permuting iteration order moves which iteration first trips that
// bound — a real change to the accumulation path rather than a schedule the
// model already permits. Putting it behind the seam means deciding what that
// bound means under an arbitrary order, on both drivers, which is more than a
// seam.

// SchedulePoint names one decision site in the local driver, so a scheduler can
// answer differently at different kinds of junction and so a divergence can be
// attributed to the place that produced it.
type SchedulePoint string

const (
	// SchedulePointParallelBranches is a `parallel:` block choosing the order to
	// advance its branches in. Branches may not observe each other, so every
	// order is legal; what is merged, and the order compensations land in, is
	// declaration order regardless.
	SchedulePointParallelBranches SchedulePoint = "parallel.branches"

	// SchedulePointAsyncLaunch is one `async:` step choosing whether its work
	// happens where it is written or is held until the position that joins it.
	// Both are rehearsals of the same durable behaviour — work launched here,
	// heard there — and an author must not be able to tell which one ran.
	SchedulePointAsyncLaunch SchedulePoint = "async.launch"
)

// Scheduler decides, among things the execution model leaves free, which happens
// when.
//
// Implementations must be safe for concurrent use: the local driver is
// sequential today, but a scheduler is held on a context that a called workflow
// and every nested block share, and nothing about this interface should have to
// change when a caller runs two of them.
type Scheduler interface {
	// Order returns the order in which n independent units should advance, as a
	// permutation of [0,n). A returned slice that is not a permutation of that
	// range is a bug in the scheduler; callers in this package check, and fall
	// back to written order rather than skipping or repeating work.
	Order(point SchedulePoint, n int) []int

	// Interleave answers a yes/no scheduling choice at point, for the step named
	// by id. False is always the answer written order would give.
	Interleave(point SchedulePoint, id string) bool
}

// writtenOrder is the [Scheduler] every run uses unless a context says
// otherwise: identity permutations and no deferral, which is precisely what this
// driver did before the seam existed.
type writtenOrder struct{}

func (writtenOrder) Order(_ SchedulePoint, n int) []int {
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}

	return order
}

func (writtenOrder) Interleave(SchedulePoint, string) bool { return false }

// WrittenOrder is the [Scheduler] the local driver uses unless a caller injects
// another, and the only one anything outside a test ever gets: branches advance
// in declaration order and an async step's work happens where it is written.
//
// Production code never names it — it is what [SchedulerFromContext] returns
// when nothing was injected, the same relationship [RealClock] has to [Clock].
var WrittenOrder Scheduler = writtenOrder{}

// schedulerContextKey is the context key carrying a [Scheduler].
type schedulerContextKey struct{}

// NewContextWithScheduler returns a context carrying scheduler, so that
// everything the local driver runs underneath it — including a called workflow's
// own blocks, since a call stays on the same context tree — makes its scheduling
// choices the same way.
//
// A context value for the reason [Clock] is one: a schedule is a fact about
// *this run*, supplied by whoever started it, not a parameter of the workflow or
// of any node in it, and threading it through every frame between
// [RunWithInputs] and the two sites that read it would touch every one of them
// to plumb something two of them use.
func NewContextWithScheduler(ctx context.Context, scheduler Scheduler) context.Context {
	return context.WithValue(ctx, schedulerContextKey{}, scheduler)
}

// SchedulerFromContext returns the scheduler a context carries, or
// [WrittenOrder] when none was injected.
func SchedulerFromContext(ctx context.Context) Scheduler {
	if s, ok := ctx.Value(schedulerContextKey{}).(Scheduler); ok && s != nil {
		return s
	}

	return WrittenOrder
}

// ScheduleOrder is [Scheduler.Order] with the contract enforced: whatever the
// scheduler returns, this hands back a genuine permutation of [0,n).
//
// Fail closed, applied to a schedule (CLAUDE.md). A scheduler is the one thing in
// a simulation that decides how much of the workload runs, so a scheduler that
// returns a short slice, a repeated index or an out-of-range one would silently
// run a step twice or not at all — and the property built on top of it would
// report a divergence that is the harness's own doing, or, far worse, report
// agreement because half the workload never ran. The engine takes written order
// instead of trusting it, which is the answer that runs everything exactly once.
func ScheduleOrder(scheduler Scheduler, point SchedulePoint, n int) []int {
	if n <= 1 {
		return WrittenOrder.Order(point, n)
	}

	order := scheduler.Order(point, n)
	if len(order) != n {
		return WrittenOrder.Order(point, n)
	}

	seen := make([]bool, n)
	for _, i := range order {
		if i < 0 || i >= n || seen[i] {
			return WrittenOrder.Order(point, n)
		}
		seen[i] = true
	}

	return order
}

// MaxScheduleDecisions bounds how many decisions one [SeededScheduler] answers
// from its PRNG before it stops choosing and answers written order for the rest
// of the run.
//
// The resource is *decisions*, and a workflow is untrusted input like any other
// (CLAUDE.md): a specification decides how many junctions a run reaches, through
// nesting a `parallel:` inside a `loop:` inside a `call:`, and each junction
// costs a PRNG draw and a permutation slice. Time and memory both follow the
// count, so the count is what is bounded. It is generous because a legitimate
// workflow's step count is already bounded elsewhere (CheckSpecSize); this exists
// so a pathological one degrades to the driver's old behaviour visibly — see
// [SeededScheduler.Truncated] — rather than spending an unbounded amount of a
// machine to explore a schedule space nobody asked for.
const MaxScheduleDecisions = 100_000

// SeededScheduler answers every scheduling choice from a PRNG seeded with one
// number, which is the whole of what a failing simulation has to carry: the same
// seed replays the same interleaving, decision for decision, and a different seed
// explores a different one.
//
// Deterministic given the seed *and the run*: the PRNG is drawn from in the order
// the run reaches its decision points, so a replay reproduces the schedule only
// because the local driver reaches those points in the same order every time.
// That is exactly the property this driver already had and the reason the
// simulation tier lives here rather than over the durable driver, whose ordering
// is Temporal's to decide.
type SeededScheduler struct {
	seed uint64

	mu        sync.Mutex
	rng       *rand.Rand
	decisions int
	truncated bool
}

// NewSeededScheduler returns a [SeededScheduler] whose every choice follows from
// seed.
func NewSeededScheduler(seed uint64) *SeededScheduler {
	return &SeededScheduler{
		seed: seed,
		// Two words from one seed, split rather than reused, so that seeds one
		// apart do not produce PCG streams that are trivially related — a search
		// that walks seed, seed+1, seed+2 is the ordinary way this is driven.
		rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
}

// Seed is the number this scheduler's every choice follows from, and the whole
// of what a failure has to report for someone to reproduce it.
func (s *SeededScheduler) Seed() uint64 { return s.seed }

// Decisions is how many choices this scheduler has been asked for, which is what
// makes a bound that was never approached distinguishable from one that was hit.
func (s *SeededScheduler) Decisions() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.decisions
}

// Truncated reports whether this scheduler spent its [MaxScheduleDecisions]
// budget and answered written order for the rest of the run.
//
// Worth reporting rather than swallowing: a truncated schedule is a schedule that
// stopped exploring partway, so a run that agreed with the baseline under one is
// weaker evidence than a run that agreed under a schedule that chose all the way
// through. A harness that hides this reports a green it did not earn.
func (s *SeededScheduler) Truncated() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.truncated
}

// spend accounts for one decision and reports whether this scheduler may still
// make it. Called with mu held.
func (s *SeededScheduler) spendLocked() bool {
	if s.decisions >= MaxScheduleDecisions {
		s.truncated = true

		return false
	}
	s.decisions++

	return true
}

// Order implements [Scheduler] with a Fisher-Yates shuffle of [0,n).
func (s *SeededScheduler) Order(point SchedulePoint, n int) []int {
	order := WrittenOrder.Order(point, n)
	if n <= 1 {
		return order
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.spendLocked() {
		return order
	}

	for i := n - 1; i > 0; i-- {
		j := s.rng.IntN(i + 1)
		order[i], order[j] = order[j], order[i]
	}

	return order
}

// Interleave implements [Scheduler] with a coin flip.
func (s *SeededScheduler) Interleave(SchedulePoint, string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.spendLocked() {
		return false
	}

	return s.rng.Uint64()&1 == 1
}
