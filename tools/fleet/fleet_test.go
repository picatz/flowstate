package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const gib = 1 << 30

// TestPlanForNamesTheBoundThatDecided checks the arithmetic over machines this
// one will never be, which is the point of [PlanFor] taking a [Machine] rather
// than reading /proc itself: the interesting cases are a laptop, a build box
// and a container three commits from ENOSPC, and a test that could only
// describe the machine it runs on would assert nothing about any of them.
//
// Every case names the bound as well as the count, because the count alone is
// not actionable — an agent told "2" learns nothing, and an agent told
// "2, memory is the bound" knows what to change.
func TestPlanForNamesTheBoundThatDecided(t *testing.T) {
	for name, test := range map[string]struct {
		machine   Machine
		wantLanes int
		wantBound string
	}{
		// The box this was written on, idle. Three of four cores are
		// dispatchable, so one lane; the fourth is the orchestrator's.
		"a small idle box is bound by its cores": {
			machine:   Machine{Cores: 4, Load1: 0, MemoryFree: 13 * gib, DiskFree: 50 * gib},
			wantLanes: 1,
			wantBound: "cores",
		},
		// The same box mid-session, which is the state that produced this tool:
		// capacity says one lane, and the work already running says none.
		"a busy small box is bound by what it is already doing": {
			machine:   Machine{Cores: 4, Load1: 20, MemoryFree: 13 * gib, DiskFree: 50 * gib},
			wantLanes: 0,
			wantBound: "current load",
		},
		"a large idle box scales up": {
			machine:   Machine{Cores: 32, Load1: 0.5, MemoryFree: 128 * gib, DiskFree: 500 * gib},
			wantLanes: 15,
			wantBound: "cores",
		},
		// Cores are plentiful and memory is not, which is the shape of a
		// many-core container with a small limit — and the -race runs this
		// repository's gate performs are what make memory bind first.
		// 8 GiB free, 2 reserved, and a lane budgeted at four (LaneCores
		// processes at LaneProcessMemoryBytes, doubled for the race
		// detector's non-Go allocations) leaves room for one. The naive
		// per-process figure would have said three, which is the
		// over-recommendation that correction exists to stop.
		"a many-core box with little memory is bound by memory": {
			machine:   Machine{Cores: 32, Load1: 0.5, MemoryFree: 8 * gib, DiskFree: 500 * gib},
			wantLanes: 1,
			wantBound: "memory",
		},
		"a full disk stops dispatch before anything else is considered": {
			machine:   Machine{Cores: 32, Load1: 0.5, MemoryFree: 128 * gib, DiskFree: 2 * gib},
			wantLanes: 0,
			wantBound: "disk",
		},
		// A machine that reports no load average is not an idle machine, and
		// must not be read as one: the plan falls back to capacity alone.
		"an unknown load average does not read as idle": {
			machine:   Machine{Cores: 8, Load1: -1, MemoryFree: 64 * gib, DiskFree: 100 * gib},
			wantLanes: 3,
			wantBound: "cores",
		},
		"a single core still leaves the orchestrator a machine to run on": {
			machine:   Machine{Cores: 1, Load1: 0, MemoryFree: 64 * gib, DiskFree: 100 * gib},
			wantLanes: 0,
			wantBound: "cores",
		},
	} {
		t.Run(name, func(t *testing.T) {
			plan := PlanFor(test.machine)

			assert.Equal(t, test.wantLanes, plan.Lanes)
			assert.Equal(t, test.wantBound, plan.Bound, "the bound is what makes the number actionable")
		})
	}
}

// TestPlanForNeverRecommendsMoreThanTheMachineHas is the property the arithmetic
// exists to keep, asserted over the whole grid rather than at the points chosen
// above — a table of examples is exactly where an off-by-one in the reserve
// hides, because the examples were written from the same arithmetic.
func TestPlanForNeverRecommendsMoreThanTheMachineHas(t *testing.T) {
	for cores := 1; cores <= 128; cores++ {
		for _, memory := range []uint64{gib, 4 * gib, 64 * gib, 512 * gib} {
			plan := PlanFor(Machine{Cores: cores, Load1: 0, MemoryFree: memory, DiskFree: 1024 * gib})

			require.GreaterOrEqual(t, plan.Lanes, 0, "a negative lane count is not a refusal, it is a bug")
			require.LessOrEqual(t, plan.Lanes*LaneCores, cores-reservedCores,
				"the fleet may not spend cores the machine does not have, leaving the orchestrator one")
			require.LessOrEqual(t, uint64(plan.Lanes)*LaneMemoryBytes, memory,
				"the fleet may not promise more memory than is available")
		}
	}
}

// TestLaneEnvIsWhatThePlanWasComputedFrom pins the halves together. The fleet
// size is capacity divided by a lane's appetite, so a lane given a different
// appetite makes the division a statement about lanes that do not exist — and
// nothing else in the repository would notice.
func TestLaneEnvIsWhatThePlanWasComputedFrom(t *testing.T) {
	env := strings.Join(LaneEnv(), " ")

	assert.Contains(t, env, "GOMAXPROCS=2", "a lane must be told how many cores it may use")
	assert.Contains(t, env, "-p=2", "and how many packages it may build at once, which is the other half")
	assert.Contains(t, env, "GOMEMLIMIT=1024MiB", "per process, not the lane's whole budget")

	require.Equal(t, LaneCores, 2, "LaneEnv and the arithmetic above must not drift apart")
}

// TestLaneEnvIsExported is the whole tool in one assertion.
//
// `eval "$(fleet -env)"` over bare assignments sets shell variables, which no
// child process inherits — so a lane consuming it would run at exactly the
// unbounded defaults this exists to prevent, while the shell showed the value
// and `go test` never saw it. Demonstrated before the fix: `GOMAXPROCS=2` in
// the shell, and `GOMAXPROCS=[]` one `sh -c` down.
func TestLaneEnvIsExported(t *testing.T) {
	for _, assignment := range LaneEnv() {
		assert.True(t, strings.HasPrefix(assignment, "export "),
			"%q is shell-local: a lane given it runs unbounded and nothing says so", assignment)
	}
}

// TestALanesMemoryBudgetCountsEveryProcessItMayRun pins the correction that a
// per-process limit is not a per-lane one.
//
// `-p` is how many build commands or test binaries run at once, and GOMEMLIMIT
// applies to each of them separately — so budgeting a lane at the per-process
// figure recommends lanes a machine cannot hold. The limit is also soft and
// covers the Go heap only, which is why the budget carries headroom for the
// race detector's own allocations rather than matching the sum exactly.
func TestALanesMemoryBudgetCountsEveryProcessItMayRun(t *testing.T) {
	require.GreaterOrEqual(t, LaneMemoryBytes, LaneProcessMemoryBytes*LaneCores,
		"a lane may run LaneCores processes at LaneProcessMemoryBytes each; budgeting less over-recommends")
	assert.Greater(t, LaneMemoryBytes, LaneProcessMemoryBytes*LaneCores,
		"and GOMEMLIMIT excludes the race detector's non-Go allocations, so the budget needs headroom")
}

// TestAdviceIsGivenWhereItCanBeActedOn checks the sentences rather than only
// the number, because "0 lanes" with no reason is a dead end for the agent
// that reads it.
func TestAdviceIsGivenWhereItCanBeActedOn(t *testing.T) {
	t.Run("a full disk says what to prune", func(t *testing.T) {
		plan := PlanFor(Machine{Cores: 8, Load1: 0, MemoryFree: 64 * gib, DiskFree: gib})

		require.NotEmpty(t, plan.Advice)
		assert.Contains(t, strings.Join(plan.Advice, "\n"), "go clean -cache")
	})

	t.Run("a busy machine says to wait rather than to prune", func(t *testing.T) {
		plan := PlanFor(Machine{Cores: 4, Load1: 20, MemoryFree: 64 * gib, DiskFree: 100 * gib})

		require.NotEmpty(t, plan.Advice)
		assert.Contains(t, strings.Join(plan.Advice, "\n"), "Wait rather than adding to it")
	})

	t.Run("a healthy machine is told nothing it cannot use", func(t *testing.T) {
		plan := PlanFor(Machine{Cores: 16, Load1: 1, MemoryFree: 64 * gib, DiskFree: 100 * gib})

		assert.Empty(t, plan.Advice, "advice on a machine with no problem is noise that trains readers to skip it")
	})
}

// TestUnknownIsNotNone is the finding that made the tool useless off Linux.
//
// A macOS box has no /proc/meminfo, so the reader returns zero — and zero read
// as "no memory free" made the memory bound win on every machine the tool
// could not measure, answering zero lanes forever. Unknown has to fall back to
// the resources that *were* readable, and say so.
func TestUnknownIsNotNone(t *testing.T) {
	t.Run("unreadable memory falls back to the other bounds", func(t *testing.T) {
		plan := PlanFor(Machine{Cores: 16, Load1: 0.2, MemoryFree: 0, DiskFree: 500 * gib})

		assert.Positive(t, plan.Lanes, "a machine whose memory could not be read is not a machine with no memory")
		assert.Equal(t, "cores", plan.Bound)
		assert.Contains(t, strings.Join(plan.Advice, "\n"), "could not be read",
			"a plan computed from fewer facts must say so rather than pass as a whole answer")
	})

	t.Run("an unreadable load average still does not read as idle", func(t *testing.T) {
		busy := PlanFor(Machine{Cores: 16, Load1: -1, MemoryFree: 64 * gib, DiskFree: 500 * gib})

		assert.Equal(t, 7, busy.Lanes, "capacity alone, with no load discount and no refusal")
	})
}

// TestHostLoadIsNotComparedWithAContainerQuota keeps two scopes apart.
//
// /proc/loadavg has no cgroup scope: inside a container it is the host's. Where
// the core count came from a quota, comparing the two describes different
// machines — a two-core container on a busy sixty-four-core host would be
// refused every lane on the strength of work it is not competing for.
func TestHostLoadIsNotComparedWithAContainerQuota(t *testing.T) {
	container := Machine{Cores: 2, Load1: 40, MemoryFree: 32 * gib, DiskFree: 500 * gib, LoadIsHostWide: true}

	plan := PlanFor(container)

	assert.Equal(t, "cores", plan.Bound, "the host's load must not decide a container's fleet")
	assert.Contains(t, strings.Join(plan.Advice, "\n"), "load average is the host's",
		"and dropping a bound silently would be worse than not having it")

	// The same numbers on a machine whose cores are its own: the load bound
	// applies, and refuses.
	own := container
	own.LoadIsHostWide = false
	assert.Equal(t, 0, PlanFor(own).Lanes, "where the scopes match, a busy machine is still a busy machine")
}
