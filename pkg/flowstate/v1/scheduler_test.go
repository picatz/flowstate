package flowstatev1_test

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestWrittenOrderIsWhatANonSimulatedRunGets is the first thing the seam owes,
// and the reason nothing outside a simulation changed shape.
//
// Every run that does not inject a scheduler — which is every production run,
// every `flow run local`, and every test in this repository that predates this
// file, including all of pkg/flowstate/v1/flowtest — asks the context for a
// scheduler and gets identity permutations and no deferral back. That is exactly
// the two decisions the driver hard-coded before. A seam that quietly changed the
// default would be a seam that changed what the `-cpu=1` ordering tier is running
// against, which is not a thing to find out later.
func TestWrittenOrderIsWhatANonSimulatedRunGets(t *testing.T) {
	require.Equal(t, v1.WrittenOrder, v1.SchedulerFromContext(context.Background()))
	require.Equal(t, v1.WrittenOrder, v1.SchedulerFromContext(t.Context()))

	// A context carrying a typed nil, or something else under the key, still
	// answers written order rather than panicking mid-run.
	//lint:ignore SA1029 the key type is unexported, so this is the only way to
	// prove a foreign value under a colliding key cannot reach the driver.
	require.Equal(t, v1.WrittenOrder, v1.SchedulerFromContext(context.WithValue(context.Background(), "scheduler", 7)))
	require.Equal(t, v1.WrittenOrder, v1.SchedulerFromContext(v1.NewContextWithScheduler(context.Background(), nil)))

	require.Equal(t, []int{0, 1, 2, 3}, v1.WrittenOrder.Order(v1.SchedulePointParallelBranches, 4))
	require.Empty(t, v1.WrittenOrder.Order(v1.SchedulePointParallelBranches, 0))
	require.False(t, v1.WrittenOrder.Interleave(v1.SchedulePointAsyncLaunch, "a"))
}

// TestAdversarialOrderIsTheOppositeAndIsFixed pins the schedule the
// deterministic cases are written against.
//
// It has to be reached identically every run and by nothing resembling a choice,
// because that is the whole difference between it and the seeded search: a
// search proves a claim about the seeds it drew, and this proves one about a
// schedule every run takes.
func TestAdversarialOrderIsTheOppositeAndIsFixed(t *testing.T) {
	require.Equal(t, []int{3, 2, 1, 0}, v1.AdversarialOrder.Order(v1.SchedulePointParallelBranches, 4))
	require.Equal(t, []int{0}, v1.AdversarialOrder.Order(v1.SchedulePointParallelBranches, 1))
	require.Empty(t, v1.AdversarialOrder.Order(v1.SchedulePointParallelBranches, 0))
	require.True(t, v1.AdversarialOrder.Interleave(v1.SchedulePointAsyncLaunch, "a"))

	// A genuine permutation, so the engine's own guard passes it through rather
	// than substituting written order and quietly running the cases against the
	// schedule they exist to depart from.
	require.Equal(t, []int{2, 1, 0},
		v1.ScheduleOrder(v1.AdversarialOrder, v1.SchedulePointParallelBranches, 3))
}

// brokenScheduler answers with things that are not permutations — the shapes a
// buggy or hostile scheduler would produce.
type brokenScheduler struct{ order []int }

func (b brokenScheduler) Order(v1.SchedulePoint, int) []int        { return b.order }
func (b brokenScheduler) Interleave(v1.SchedulePoint, string) bool { return false }

// TestScheduleOrderTakesWrittenOrderRatherThanTrustAScheduler is fail-closed
// applied to a schedule.
//
// A scheduler decides how much of a workload runs, so the failure mode of
// trusting one is not a wrong order, it is a step run twice or not at all — and
// the property built on top would then report a divergence of the harness's own
// making, or agreement because half the workload never happened. Every shape
// that is not a permutation of [0,n) is answered with written order, which is the
// answer that runs everything exactly once.
func TestScheduleOrderTakesWrittenOrderRatherThanTrustAScheduler(t *testing.T) {
	for _, broken := range []struct {
		name  string
		order []int
	}{
		{name: "short", order: []int{0, 1}},
		{name: "long", order: []int{0, 1, 2, 3}},
		{name: "repeated", order: []int{0, 0, 2}},
		{name: "out of range", order: []int{0, 1, 3}},
		{name: "negative", order: []int{0, 1, -1}},
		{name: "nil", order: nil},
	} {
		t.Run(broken.name, func(t *testing.T) {
			require.Equal(t, []int{0, 1, 2},
				v1.ScheduleOrder(brokenScheduler{order: broken.order}, v1.SchedulePointParallelBranches, 3))
		})
	}

	// A genuine permutation is passed through untouched, or the guard above
	// would be a guard against the seam working at all.
	require.Equal(t, []int{2, 0, 1},
		v1.ScheduleOrder(brokenScheduler{order: []int{2, 0, 1}}, v1.SchedulePointParallelBranches, 3))
}

// TestASeedReplaysItsWholeSchedule is what a failing seed is worth.
//
// Two schedulers built from one seed, asked the same questions in the same order,
// must answer identically all the way down — and two built from adjacent seeds
// must not, or a search that walks seeds upward is a search that explores one
// schedule many times.
func TestASeedReplaysItsWholeSchedule(t *testing.T) {
	ask := func(seed uint64) []string {
		scheduler := v1.NewSeededScheduler(seed)
		answers := make([]string, 0, 200)
		for i := range 100 {
			answers = append(answers,
				fmt.Sprint(scheduler.Order(v1.SchedulePointParallelBranches, 4)),
				fmt.Sprint(scheduler.Interleave(v1.SchedulePointAsyncLaunch, strconv.Itoa(i))))
		}

		return answers
	}

	require.Equal(t, ask(4242), ask(4242), "one seed produced two different schedules")

	different := 0
	for seed := range uint64(8) {
		if !slices.Equal(ask(seed), ask(seed+1)) {
			different++
		}
	}
	require.Equal(t, 8, different, "adjacent seeds produced the same schedule")
}

// TestASeededSchedulerActuallyPermutes guards against the emptiest possible
// green: a scheduler that always answers written order explores nothing, and
// every property built on it agrees with itself forever.
func TestASeededSchedulerActuallyPermutes(t *testing.T) {
	scheduler := v1.NewSeededScheduler(1)

	seen := map[string]bool{}
	deferred, immediate := 0, 0
	for i := range 200 {
		seen[fmt.Sprint(scheduler.Order(v1.SchedulePointParallelBranches, 3))] = true
		if scheduler.Interleave(v1.SchedulePointAsyncLaunch, strconv.Itoa(i)) {
			deferred++
		} else {
			immediate++
		}
	}

	require.Len(t, seen, 6, "the shuffle did not reach every order of three branches")
	require.Positive(t, deferred, "no async step was ever held until its join")
	require.Positive(t, immediate, "no async step ever ran where it was written")
}

// TestASeededSchedulerIsBoundedByDecisions covers the bound and, just as much,
// that spending it is *visible*.
//
// The resource is decisions, because a specification is untrusted input and it is
// the specification that decides how many junctions a run reaches — through a
// `parallel:` inside a `loop:` inside a `call:`. Past the bound the scheduler
// stops choosing and answers written order, which degrades the simulation to the
// driver's ordinary behaviour rather than to an unbounded amount of a machine.
// A schedule that got there proved less than one that chose all the way through,
// so [v1.SeededScheduler.Truncated] says so and the harness prints it.
func TestASeededSchedulerIsBoundedByDecisions(t *testing.T) {
	scheduler := v1.NewSeededScheduler(1)
	require.False(t, scheduler.Truncated(), "a scheduler that has decided nothing reports truncation")
	require.Zero(t, scheduler.Decisions())

	for range v1.MaxScheduleDecisions {
		scheduler.Interleave(v1.SchedulePointAsyncLaunch, "a")
	}
	require.Equal(t, v1.MaxScheduleDecisions, scheduler.Decisions())
	require.False(t, scheduler.Truncated(), "the bound was reported spent one decision early")

	// Past the bound: written order, forever, and said out loud.
	require.False(t, scheduler.Interleave(v1.SchedulePointAsyncLaunch, "a"))
	require.Equal(t, []int{0, 1, 2}, scheduler.Order(v1.SchedulePointParallelBranches, 3))
	require.True(t, scheduler.Truncated())
	require.Equal(t, v1.MaxScheduleDecisions, scheduler.Decisions(),
		"a decision refused by the bound was counted as one taken")
}

// TestAOneWayChoiceCostsNothing keeps the bound pointed at the resource that
// runs away. A block with one branch has one order, so there is no choice to
// make and none is spent — otherwise a workflow of a thousand single-branch
// blocks would exhaust a budget that exists for genuine junctions.
func TestAOneWayChoiceCostsNothing(t *testing.T) {
	scheduler := v1.NewSeededScheduler(1)
	for range 1000 {
		require.Equal(t, []int{0}, scheduler.Order(v1.SchedulePointParallelBranches, 1))
		require.Empty(t, scheduler.Order(v1.SchedulePointParallelBranches, 0))
	}
	require.Zero(t, scheduler.Decisions())
}
