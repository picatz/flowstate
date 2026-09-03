package dst_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
)

// Proving the property can fail.
//
// A property that passes on every implementation is decoration, and this one is
// especially easy to write that way: run a deterministic engine N times, hash the
// answer, watch the hashes agree. So the mutation is not a mutation of the
// harness — it is a workflow whose observables genuinely depend on the order its
// steps ran in, driven through the real engine, the real seeded scheduler and the
// real property. If the seam did not actually vary the schedule, or if the
// comparison did not actually compare, this test would fail to fail.
//
// That is also what makes it the guard against the worst outcome available here,
// which is not a red job but a green one that explored nothing: a search over a
// space of size one agrees with itself forever.

// orderProbeTask is a task whose output is *when it ran* — the invocation index,
// counted across the whole run.
//
// Exactly the mistake a real engine defect would make. An engine that let
// completion order reach an author would be leaking a fact of this shape into
// some output, and this task states it outright rather than waiting for one to
// be introduced.
const orderProbeTask = "dst_order_probe"

// probeRegistry returns a registry whose only task answers with its own
// invocation index, and the func that puts the count back to zero.
//
// Reset at the start of every schedule, which is not a detail: a counter that
// carried across runs would make every schedule differ from every other for a
// reason that has nothing to do with ordering, and the mutation proof below
// would pass while proving nothing at all. What must vary between schedules is
// the order, and only the order.
func probeRegistry(t *testing.T) (*v1.Registry, func()) {
	t.Helper()

	var order atomic.Int64

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{
		Name: orderProbeTask,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return &v1.Node_Outputs{NamedValues: v1.NewNamedValues(map[string]any{
				"ran": order.Add(1),
			})}, nil
		},
	}))

	return registry, func() { order.Store(0) }
}

// probeStep is one step running [orderProbeTask].
func probeStep(id string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: orderProbeTask}}}
}

// parallelProbes is two branches, each recording when it ran. Under written
// order the first branch always runs first; under a schedule free to choose, it
// does not.
func parallelProbes() *v1.Workflow {
	return &v1.Workflow{
		Name:    "dst-mutation-parallel",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "both",
			Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
				{Steps: []*v1.Node{probeStep("left")}},
				{Steps: []*v1.Node{probeStep("right")}},
			}}},
		}},
	}
}

// asyncProbes is an async step whose work the schedule may hold until its join,
// written before a step that does not read it — so the two run in one order or
// the other depending on the choice made at the launch.
func asyncProbes() *v1.Workflow {
	first := probeStep("held")
	first.Async = true

	return &v1.Workflow{
		Name:    "dst-mutation-async",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{first, probeStep("after")},
	}
}

// TestThePropertyFailsWhenAnObservableDependsOnOrder is the mutation proof.
//
// Both workflows produce a transcript that says which step ran first. That is a
// legal thing for a *task* to do and an illegal thing for the engine to expose,
// so a property that cannot see it here could not see it if the engine did it.
func TestThePropertyFailsWhenAnObservableDependsOnOrder(t *testing.T) {
	for _, mutation := range []struct {
		name     string
		workflow *v1.Workflow
	}{
		{name: "parallel branches", workflow: parallelProbes()},
		{name: "an async step's launch", workflow: asyncProbes()},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			registry, reset := probeRegistry(t)

			report := dst.Explore(v1.NewContextWithRegistry(t.Context(), registry),
				dst.Budget{Schedules: 32, Seed0: 1},
				func(ctx context.Context) dst.Result {
					reset()
					outputs, err := v1.RunWithInputs(ctx, mutation.workflow, nil)

					return dst.Result{Transcript: outputs, Err: err}
				})

			require.NotNil(t, report.Divergence,
				"a workflow whose transcript states which step ran first agreed across every schedule, "+
					"which means the search explored one schedule wearing %d hats", report.Schedules())
			require.Positive(t, report.Decisions(),
				"the search never reached a scheduling junction")

			// The failure a person is handed has to carry the seed and the
			// command, or the seed is a random number.
			text := dst.FailureText("TestExample", report.Divergence)
			require.Contains(t, text, dst.SeedEnv+"=", "the failure does not name the replay switch")
			require.Contains(t, text, "REPRODUCE THIS EXACT SCHEDULE")
			require.Contains(t, text, dst.ReproducePackage)
			require.Contains(t, text, "-run \"^TestExample$\"")
		})
	}
}

// TestThePropertyPassesOnTheSameShapeWithoutTheOrderDependence is the negative
// direction of the mutation, and the reason the one above is evidence rather
// than a coincidence of shape.
//
// The identical workflows, with the identical junctions and the identical number
// of scheduling decisions — only the task no longer answers with when it ran.
// The property goes quiet. So what it reacted to was the order dependence and
// not the presence of a `parallel:` block or an `async:` marker.
func TestThePropertyPassesOnTheSameShapeWithoutTheOrderDependence(t *testing.T) {
	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{
		Name: orderProbeTask,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return &v1.Node_Outputs{NamedValues: v1.NewNamedValues(map[string]any{"ran": "always the same"})}, nil
		},
	}))

	for _, workflow := range []*v1.Workflow{parallelProbes(), asyncProbes()} {
		t.Run(workflow.GetName(), func(t *testing.T) {
			report := dst.Explore(v1.NewContextWithRegistry(t.Context(), registry),
				dst.Budget{Schedules: 32, Seed0: 1},
				func(ctx context.Context) dst.Result {
					outputs, err := v1.RunWithInputs(ctx, workflow, nil)

					return dst.Result{Transcript: outputs, Err: err}
				})

			require.Positive(t, report.Decisions(), "the search never reached a scheduling junction")
			if report.Divergence != nil {
				t.Fatalf("%s", dst.FailureText(t.Name(), report.Divergence))
			}
		})
	}
}

// TestAFailingSeedReplaysExactly is the claim the whole tier rests on: the seed
// a failure prints is the whole of its state.
//
// Found once over a search, then run again as the single pinned seed the failure
// told a reader to set — a different budget, a different number of runs before
// it, a fresh counter — and it has to produce the same two renderings. A
// divergence that only appears in the company of the schedules that preceded it
// is not reproducible, and a tier whose failures are not reproducible is a flake
// generator.
func TestAFailingSeedReplaysExactly(t *testing.T) {
	workflow := parallelProbes()

	explore := func(budget dst.Budget) *dst.Report {
		registry, reset := probeRegistry(t)

		return dst.Explore(v1.NewContextWithRegistry(t.Context(), registry), budget,
			func(ctx context.Context) dst.Result {
				reset()
				outputs, err := v1.RunWithInputs(ctx, workflow, nil)

				return dst.Result{Transcript: outputs, Err: err}
			})
	}

	found := explore(dst.Budget{Schedules: 32, Seed0: 1})
	require.NotNil(t, found.Divergence, "no schedule diverged, so there is nothing to replay")

	seed := found.Divergence.Diverged.Seed
	replay := explore(dst.Budget{Pinned: &seed})

	require.NotNil(t, replay.Divergence, "the seed the failure named did not diverge on its own")
	require.Equal(t, seed, replay.Divergence.Diverged.Seed)
	require.Equal(t, found.Divergence.Diverged.Digest, replay.Divergence.Diverged.Digest,
		"replaying the seed produced a different interleaving than the search did")
	require.Equal(t, found.Divergence.Baseline.Digest, replay.Divergence.Baseline.Digest,
		"the written-order baseline is not stable")
	require.Equal(t, found.Divergence.Diverged.Decisions, replay.Divergence.Diverged.Decisions,
		"the replay made a different number of scheduling decisions")
}

// TestTheBudgetIsVisibleAndRefusesNonsense covers the two ways a bounded search
// can lie: by exploring less than it was asked to without saying so, and by
// accepting a budget nobody can read.
func TestTheBudgetIsVisibleAndRefusesNonsense(t *testing.T) {
	t.Run("a malformed budget is refused rather than defaulted", func(t *testing.T) {
		t.Setenv(dst.ScheduleBudgetEnv, "1O")
		_, err := dst.DefaultBudget()
		require.ErrorContains(t, err, dst.ScheduleBudgetEnv)
	})

	t.Run("a budget above the cap is refused", func(t *testing.T) {
		t.Setenv(dst.ScheduleBudgetEnv, "10000000")
		_, err := dst.DefaultBudget()
		require.ErrorContains(t, err, "linear")
	})

	t.Run("a pinned seed replaces the whole search", func(t *testing.T) {
		t.Setenv(dst.SeedEnv, "77")
		budget, err := dst.DefaultBudget()
		require.NoError(t, err)
		require.NotNil(t, budget.Pinned)
		require.Equal(t, uint64(77), *budget.Pinned)

		registry, reset := probeRegistry(t)
		report := dst.Explore(v1.NewContextWithRegistry(t.Context(), registry), budget,
			func(ctx context.Context) dst.Result {
				reset()
				outputs, err := v1.RunWithInputs(ctx, parallelProbes(), nil)

				return dst.Result{Transcript: outputs, Err: err}
			})
		require.Equal(t, 1, report.Schedules(), "a pinned seed explored more than one schedule")
	})

	t.Run("the environment moves the search", func(t *testing.T) {
		t.Setenv(dst.ScheduleBudgetEnv, "3")
		t.Setenv(dst.Seed0Env, "1000")
		budget, err := dst.DefaultBudget()
		require.NoError(t, err)
		require.Equal(t, 3, budget.Schedules)
		require.Equal(t, uint64(1000), budget.Seed0)
	})
}

// TestARunThatExploredNothingSaysSo pins the reporting a caller asserts on.
//
// A workflow with no junctions has one schedule, and every seed takes it. The
// report has to say that plainly — zero decisions — because "every schedule
// agreed" over a space of size one is the exact shape of a green nobody earned,
// and the corpus tests above refuse to pass on it.
func TestARunThatExploredNothingSaysSo(t *testing.T) {
	registry, reset := probeRegistry(t)
	sequential := &v1.Workflow{
		Name:    "dst-no-junctions",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{probeStep("one"), probeStep("two")},
	}

	report := dst.Explore(v1.NewContextWithRegistry(t.Context(), registry),
		dst.Budget{Schedules: 4, Seed0: 1},
		func(ctx context.Context) dst.Result {
			reset()
			outputs, err := v1.RunWithInputs(ctx, sequential, nil)

			return dst.Result{Transcript: outputs, Err: err}
		})

	require.Nil(t, report.Divergence)
	require.Equal(t, 4, report.Schedules())
	require.Zero(t, report.Decisions(),
		"a workflow with no parallel block and no async step reported scheduling decisions")
	require.False(t, report.Truncated())
}

// TestTheRenderingIsStableForOneResult guards the comparison itself.
//
// Two renderings of one result must be the same string, or the harness reports
// divergences that are the encoder's rather than the engine's. protojson would
// fail this — it varies its whitespace deliberately — which is why the digest is
// taken over a deterministic proto encoding instead.
func TestTheRenderingIsStableForOneResult(t *testing.T) {
	registry, reset := probeRegistry(t)
	ctx := v1.NewContextWithRegistry(t.Context(), registry)

	sequential := &v1.Workflow{
		Name:    "dst-stable",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{probeStep("one")},
	}

	report := dst.Explore(ctx, dst.Budget{Schedules: 0}, func(ctx context.Context) dst.Result {
		reset()
		outputs, err := v1.RunWithInputs(ctx, sequential, nil)

		return dst.Result{Transcript: outputs, Err: err}
	})
	require.Len(t, report.Observations, 1)
	require.NotEmpty(t, report.Observations[0].Digest)
	require.True(t, strings.HasPrefix(report.Observations[0].Rendering, "transcript: "))
}

// TestEffectsBeyondTheUnorderedPrefixAreAClaim pins the one place this harness
// decides what counts as observable, in both directions.
//
// The prefix is a set: two schedules that did the same concurrent work in
// different orders agree. Everything after it is a sequence: two schedules that
// compensated in different orders do not. Getting this backwards in either
// direction breaks the tier — too strict and no correct engine passes, too loose
// and the undo-order claim stops being checked at all.
func TestEffectsBeyondTheUnorderedPrefixAreAClaim(t *testing.T) {
	explore := func(effects func(seeded bool) []string, prefix int) *dst.Report {
		return dst.Explore(t.Context(), dst.Budget{Schedules: 1, Seed0: 1}, func(ctx context.Context) dst.Result {
			return dst.Result{
				Effects:         effects(v1.SchedulerFromContext(ctx) != v1.WrittenOrder),
				UnorderedPrefix: prefix,
			}
		})
	}

	reordered := func(seeded bool) []string {
		if seeded {
			return []string{"b", "a", "undo-a"}
		}

		return []string{"a", "b", "undo-a"}
	}

	require.Nil(t, explore(reordered, 2).Divergence,
		"work inside the unordered prefix was compared as a sequence")
	require.NotNil(t, explore(reordered, 0).Divergence,
		"effects outside the unordered prefix were compared as a set")

	unwound := func(seeded bool) []string {
		if seeded {
			return []string{"a", "b", "undo-a", "undo-b"}
		}

		return []string{"a", "b", "undo-b", "undo-a"}
	}
	require.NotNil(t, explore(unwound, 2).Divergence,
		"a compensation order that changed with the schedule was not reported")
}
