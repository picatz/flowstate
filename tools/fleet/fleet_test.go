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
		"a many-core box with little memory is bound by memory": {
			machine:   Machine{Cores: 32, Load1: 0.5, MemoryFree: 8 * gib, DiskFree: 500 * gib},
			wantLanes: 3,
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
	assert.Contains(t, env, "GOMEMLIMIT=2048MiB")

	require.Equal(t, LaneCores, 2, "LaneEnv and the arithmetic above must not drift apart")
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
