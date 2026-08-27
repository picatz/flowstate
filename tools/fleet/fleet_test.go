package main

import (
	"math"
	"os"
	"path/filepath"
	"strconv"
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
			machine:   Machine{Cores: 4, Load1: 0, MemoryFree: 13 * gib, MemoryKnown: true, DiskFree: 50 * gib},
			wantLanes: 1,
			wantBound: "cores",
		},
		// The same box mid-session, which is the state that produced this tool:
		// capacity says one lane, and the work already running says none.
		"a busy small box is bound by what it is already doing": {
			machine:   Machine{Cores: 4, Load1: 20, MemoryFree: 13 * gib, MemoryKnown: true, DiskFree: 50 * gib},
			wantLanes: 0,
			wantBound: "current load",
		},
		"a large idle box scales up": {
			machine:   Machine{Cores: 32, Load1: 0.5, MemoryFree: 128 * gib, MemoryKnown: true, DiskFree: 500 * gib},
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
			machine:   Machine{Cores: 32, Load1: 0.5, MemoryFree: 8 * gib, MemoryKnown: true, DiskFree: 500 * gib},
			wantLanes: 1,
			wantBound: "memory",
		},
		"a full disk stops dispatch before anything else is considered": {
			machine:   Machine{Cores: 32, Load1: 0.5, MemoryFree: 128 * gib, MemoryKnown: true, DiskFree: 2 * gib},
			wantLanes: 0,
			wantBound: "disk",
		},
		// A machine that reports no load average is not an idle machine, and
		// must not be read as one: the plan falls back to capacity alone.
		"an unknown load average does not read as idle": {
			machine:   Machine{Cores: 8, Load1: -1, MemoryFree: 64 * gib, MemoryKnown: true, DiskFree: 100 * gib},
			wantLanes: 3,
			wantBound: "cores",
		},
		"a single core still leaves the orchestrator a machine to run on": {
			machine:   Machine{Cores: 1, Load1: 0, MemoryFree: 64 * gib, MemoryKnown: true, DiskFree: 100 * gib},
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
			plan := PlanFor(Machine{Cores: cores, Load1: 0, MemoryFree: memory, MemoryKnown: true, DiskFree: 1024 * gib})

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
		plan := PlanFor(Machine{Cores: 8, Load1: 0, MemoryFree: 64 * gib, MemoryKnown: true, DiskFree: gib})

		require.NotEmpty(t, plan.Advice)

		// The remedy has to be one that leaves the machine able to work. This
		// used to name `go clean -cache`, which is correct and expensive: it
		// discards every entry and charges a cold rebuild to every lane, and
		// the rebuild is itself load enough to keep the fleet at zero for as
		// long as it runs. `-prune` gives back what a lane needs and keeps the
		// rest.
		assert.Contains(t, strings.Join(plan.Advice, "\n"), "tools/fleet -prune")
	})

	t.Run("a busy machine says to wait rather than to prune", func(t *testing.T) {
		plan := PlanFor(Machine{Cores: 4, Load1: 20, MemoryFree: 64 * gib, MemoryKnown: true, DiskFree: 100 * gib})

		require.NotEmpty(t, plan.Advice)
		assert.Contains(t, strings.Join(plan.Advice, "\n"), "Wait rather than adding to it")
	})

	t.Run("a healthy machine is told nothing it cannot use", func(t *testing.T) {
		plan := PlanFor(Machine{Cores: 16, Load1: 1, MemoryFree: 64 * gib, MemoryKnown: true, DiskFree: 100 * gib})

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
		plan := PlanFor(Machine{Cores: 16, Load1: 0.2, MemoryFree: 0, MemoryKnown: false, DiskFree: 500 * gib})

		assert.Positive(t, plan.Lanes, "a machine whose memory could not be read is not a machine with no memory")
		assert.Equal(t, "cores", plan.Bound)
		assert.Contains(t, strings.Join(plan.Advice, "\n"), "could not be read",
			"a plan computed from fewer facts must say so rather than pass as a whole answer")
	})

	// The other direction, which is what a single field could not express. A
	// Linux cgroup sitting at its memory.max reports zero free honestly, and
	// that machine must be refused rather than handed the fallback: lanes
	// dispatched against memory a parent will not grant do not run slowly,
	// they are OOM-killed mid-build.
	t.Run("a measured zero is a bound rather than a fallback", func(t *testing.T) {
		plan := PlanFor(Machine{Cores: 64, Load1: 0.2, MemoryFree: 0, MemoryKnown: true, DiskFree: 500 * gib})

		assert.Equal(t, 0, plan.Lanes, "a container at its memory limit can hold no lanes")
		assert.Equal(t, "memory", plan.Bound, "and must say which resource refused, so the reader can raise it")
		assert.NotContains(t, strings.Join(plan.Advice, "\n"), "could not be read",
			"a reading of zero was read; calling it unavailable sends the reader to look for a platform problem")
		assert.Contains(t, strings.Join(plan.Advice, "\n"), "measured reading")
	})

	// And the two must not be the same plan, which is the whole finding: with
	// one field they were.
	t.Run("the two answers differ", func(t *testing.T) {
		unknown := PlanFor(Machine{Cores: 64, Load1: 0.2, MemoryFree: 0, DiskFree: 500 * gib})
		exhausted := PlanFor(Machine{Cores: 64, Load1: 0.2, MemoryFree: 0, MemoryKnown: true, DiskFree: 500 * gib})

		assert.NotEqual(t, unknown.Lanes, exhausted.Lanes,
			"an unmeasured machine and a full one held the same value and got the same plan; only one of them was right")
	})

	t.Run("an unreadable load average still does not read as idle", func(t *testing.T) {
		busy := PlanFor(Machine{Cores: 16, Load1: -1, MemoryFree: 64 * gib, MemoryKnown: true, DiskFree: 500 * gib})

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
	container := Machine{Cores: 2, Load1: 40, MemoryFree: 32 * gib, MemoryKnown: true, DiskFree: 500 * gib, LoadIsHostWide: true}

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

// TestACgroupLimitIsTheTightestOverEveryLevel is the finding that a leaf read
// is not an effective limit.
//
// Under cgroup v2 the quota a process actually gets is the tightest over its
// own cgroup and every ancestor up to the mount root — the kernel enforces all
// of them. A session's cgroup whose own `cpu.max` says `max`, nested under a
// parent carrying a quota, therefore looks unlimited to a reader that stops at
// the leaf, which falls through to [runtime.NumCPU] and recommends a
// host-sized fleet on a container that can run one or two lanes. The symptom
// is not a refusal anyone investigates: it is thirty lanes sharing two cores,
// each slow enough that its owner reports a flake.
//
// Posed as directory layouts rather than as a real cgroup, for the same reason
// [PlanFor] takes a [Machine]: the interesting hierarchies are ones this
// process cannot create.
func TestACgroupLimitIsTheTightestOverEveryLevel(t *testing.T) {
	for name, test := range map[string]struct {
		// levels is root-first; each entry is one directory's cpu.max, or ""
		// for a level with no such file at all.
		levels    []string
		wantCores int
		wantFound bool
	}{
		"a leaf quota binds": {
			levels:    []string{"max 100000", "400000 100000"},
			wantCores: 4,
			wantFound: true,
		},
		// The finding. The leaf declines to bound anything and the parent does.
		"an unlimited leaf inherits its parent's quota": {
			levels:    []string{"200000 100000", "max 100000"},
			wantCores: 2,
			wantFound: true,
		},
		"the tightest wins wherever it sits": {
			levels:    []string{"1600000 100000", "200000 100000", "800000 100000"},
			wantCores: 2,
			wantFound: true,
		},
		// A missing file is not a limit of zero, and must not end the walk
		// before a level that does carry one.
		"a level with no cpu.max is skipped rather than ending the walk": {
			levels:    []string{"300000 100000", "", "max 100000"},
			wantCores: 3,
			wantFound: true,
		},
		"nothing anywhere reports nothing rather than zero cores": {
			levels:    []string{"max 100000", "max 100000"},
			wantFound: false,
		},
		"a period of zero is not divided by": {
			levels:    []string{"100000 0"},
			wantFound: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cores, found := tightestCPUQuota(cgroupLayout(t, "cpu.max", test.levels))

			require.Equal(t, test.wantFound, found)
			if test.wantFound {
				assert.Equal(t, test.wantCores, cores)
			}
		})
	}
}

// TestAMemoryLimitIsAlsoInheritedFromEveryAncestor is the same argument for
// the resource whose failure is loudest: exceeding any enforced limit is an
// OOM kill, not a slow build, so the headroom a lane may spend is the smallest
// (limit - usage) over the whole chain.
func TestAMemoryLimitIsAlsoInheritedFromEveryAncestor(t *testing.T) {
	// Root-first: each level is a "max current" pair, or "" for a level with
	// neither file.
	dirs := cgroupPairLayout(t, []string{"8589934592 4294967296", "", "max 1073741824"})

	free, found := tightestMemoryFree(dirs, "memory.max", "memory.current")

	require.True(t, found, "a leaf that declines to bound anything does not make its parent's limit disappear")
	assert.Equal(t, uint64(4*gib), free, "the parent has 4 GiB of its 8 GiB left, and the leaf may not spend more")

	t.Run("a cgroup at its limit reports a real zero", func(t *testing.T) {
		free, found := tightestMemoryFree(
			cgroupPairLayout(t, []string{"2147483648 2147483648"}), "memory.max", "memory.current")

		require.True(t, found, "at-the-limit is a reading; reporting it as unknown is what disabled the bound")
		assert.Zero(t, free)
	})

	t.Run("an unlimited sentinel is not arithmetic", func(t *testing.T) {
		// cgroup v1 spells "no limit" as a page counter near the top of int64.
		// Subtracting usage from it yields an enormous headroom figure that
		// would win any minimum it was folded into.
		_, found := tightestMemoryFree(
			cgroupPairLayout(t, []string{"9223372036854771712 1073741824"}), "memory.max", "memory.current")

		assert.False(t, found, "a sentinel is a way of writing 'unlimited', not a number to subtract from")
	})
}

// TestPageCacheIsNotMemoryALaneWaitsFor is [memoryFree]'s own distinction —
// MemAvailable rather than MemFree — asserted one level down, where it was
// missed until a machine demonstrated it.
//
// Both cgroup v1's memory.usage_in_bytes and v2's memory.current count file
// cache as used. So the reading is worst exactly after a build, which is what
// fills the cache and also what makes the next lane cheapest: measured here, a
// leaf limited to 13.3 GiB reported 9.7 GiB used while holding 6 MiB of RSS
// and 9.3 GiB of cache, and the fleet answered "dispatch nothing — memory is
// the bound" on an idle box, advising a wait for lanes that did not exist.
func TestPageCacheIsNotMemoryALaneWaitsFor(t *testing.T) {
	// 8 GiB limit, 6 GiB "used", of which 5 GiB is evictable file cache. A
	// lane's real headroom is 7 GiB, not 2.
	dirs := cgroupPairLayout(t, []string{"8589934592 6442450944 5368709120"})

	free, found := tightestMemoryFree(dirs, "memory.max", "memory.current")

	require.True(t, found)
	assert.Equal(t, uint64(7*gib), free,
		"page cache was counted as memory a lane has to wait for, which is what held the fleet "+
			"at zero on a box with a hot build cache and nothing running")

	t.Run("a cgroup with no stat file is read as holding all of it", func(t *testing.T) {
		// The conservative direction, and the one a missing file must take: an
		// unreadable breakdown is not evidence that the usage is reclaimable.
		free, found := tightestMemoryFree(
			cgroupPairLayout(t, []string{"8589934592 6442450944"}), "memory.max", "memory.current")

		require.True(t, found)
		assert.Equal(t, uint64(2*gib), free)
	})

	t.Run("more cache than usage cannot invent headroom", func(t *testing.T) {
		// The two files are read separately and a cgroup is a moving target,
		// so the subtraction can be handed a larger cache than usage. That must
		// clamp rather than wrap: unsigned arithmetic would turn 1 GiB used
		// into sixteen exabytes free, which wins every minimum it is folded
		// into and hands out lanes against memory that is not there.
		free, found := tightestMemoryFree(
			cgroupPairLayout(t, []string{"8589934592 1073741824 4294967296"}), "memory.max", "memory.current")

		require.True(t, found)
		assert.Equal(t, uint64(8*gib), free, "the whole limit is free, and not one byte more")
	})
}

// TestProtectedCacheIsNotHeadroom is the other half of the question
// [TestPageCacheIsNotMemoryALaneWaitsFor] answers.
//
// File cache is evictable *unless* something asked the kernel not to evict it.
// `memory.min` is that request, and a page under it is not headroom — counting
// it as headroom is how a budget tool dispatches a lane straight into an OOM
// kill, which is worse than the refusal it replaced (Codex, #1134).
func TestProtectedCacheIsNotHeadroom(t *testing.T) {
	// 8 GiB limit, 6 GiB used, 5 GiB of it cache — but 4 GiB of the level is
	// protected, so only 1 GiB of that cache may be counted back.
	dir := protectedLayout(t, protection{cache: 5 * gib, min: 4 * gib})

	free, found := tightestMemoryFree([]string{dir}, "memory.max", "memory.current")

	require.True(t, found)
	assert.Equal(t, uint64(3*gib), free,
		"protected cache was counted as headroom, so a lane would be dispatched against memory "+
			"the kernel has been told to keep")

	t.Run("protection in a descendant counts too", func(t *testing.T) {
		// The reason the walk descends at all: cgroup v2's memory.stat is
		// recursive, so this level's `inactive_file` already counts a
		// descendant's cache — including a descendant in a subtree the chain
		// walk climbing to the root never visits. Reading `memory.min` only at
		// this level would miss it entirely.
		dir := protectedLayout(t, protection{cache: 5 * gib},
			protection{cache: 0, min: 4 * gib})

		free, found := tightestMemoryFree([]string{dir}, "memory.max", "memory.current")

		require.True(t, found)
		assert.Equal(t, uint64(3*gib), free)
	})

	t.Run("unprotected cache is still headroom", func(t *testing.T) {
		// The direction that would make this whole change pointless: a
		// protection check that refused every subtraction would restore the
		// bug it was written to fix.
		dir := protectedLayout(t, protection{cache: 5 * gib})

		free, found := tightestMemoryFree([]string{dir}, "memory.max", "memory.current")

		require.True(t, found)
		assert.Equal(t, uint64(7*gib), free)
	})

	t.Run("an oversized protection saturates rather than wrapping", func(t *testing.T) {
		// `memory.min` has no ceiling this tool gets to impose, and the sum runs
		// over numbers a hierarchy chooses. Four descendants at 1<<62 wrap a
		// plain uint64 addition to exactly zero — and zero reads as "nothing is
		// protected", so the one hierarchy asking for maximal protection would
		// be the one lanes are dispatched against (Codex, #1134).
		huge := protection{min: 1 << 62}
		dir := protectedLayout(t, protection{cache: 5 * gib}, huge, huge, huge, huge)

		free, found := tightestMemoryFree([]string{dir}, "memory.max", "memory.current")

		require.True(t, found)
		assert.Equal(t, uint64(2*gib), free,
			"the protection sum wrapped, so maximal protection read as none at all")
	})

	// Read directly, because through `tightestMemoryFree` the three answers this
	// has to tell apart all come out as the same number. Complete protection
	// and an unreadable value both subtract nothing, so a test at that level
	// passes whichever one the code produces — which is what happened: the
	// first version of the `max` case here was green with `max` handling
	// deleted, asserting the right figure for the wrong reason.
	t.Run("a protection is a number, a saturation, or a refusal", func(t *testing.T) {
		for name, test := range map[string]struct {
			write       func(t *testing.T, dir string)
			floor       uint64
			present     bool
			established bool
		}{
			// cgroup v2 spells complete protection as the literal `max`, which
			// an integer parse refuses — so the one level asking the kernel to
			// reclaim nothing read as the one level protecting nothing, and its
			// cache came straight back as headroom (Codex, #1134).
			"max saturates":                {write: writesMin("max"), floor: math.MaxUint64, present: true, established: true},
			"a number is a number":         {write: writesMin("12345"), floor: 12345, present: true, established: true},
			"zero is a real answer":        {write: writesMin("0"), floor: 0, present: true, established: true},
			"a negative floor refuses":     {write: writesMin("-1"), floor: 0, present: true, established: false},
			"an unparseable value refuses": {write: writesMin("not a number"), floor: 0, present: true, established: false},
			// Not NotExist: a file this process cannot read is not a file
			// saying nothing is protected.
			"an unreadable file refuses": {
				write: func(t *testing.T, dir string) {
					t.Helper()
					require.NoError(t, os.MkdirAll(filepath.Join(dir, "memory.min"), 0o755))
				},
				floor: 0, present: false, established: false,
			},
			// A level with no memory controller cannot protect anything, and
			// nothing below it can either — so it contributes zero and does not
			// stop the walk.
			"an absent file contributes zero": {write: func(*testing.T, string) {}, floor: 0, present: false, established: true},
		} {
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				test.write(t, dir)

				floor, present, established := protectionAt(dir)

				assert.Equal(t, test.established, established)
				assert.Equal(t, test.present, present,
					"absence and a value are different answers, and only absence means there is "+
						"no controller here to walk for")
				assert.Equal(t, test.floor, floor)
			})
		}
	})

	t.Run("a level the walk cannot read refuses the whole hierarchy", func(t *testing.T) {
		// The reader's refusal has to be *consumed*. Testing that
		// `protectionAt` returns "unestablished" says nothing about whether the
		// walk acts on it, and dropping the check there left every table case
		// above perfectly green — the resolver-and-reader split this repository
		// keeps paying for, one more time.
		dir := protectedLayout(t, protection{cache: 5 * gib})
		nested := filepath.Join(dir, "opaque")
		require.NoError(t, os.MkdirAll(filepath.Join(nested, "memory.min"), 0o755))

		free, found := tightestMemoryFree([]string{dir}, "memory.max", "memory.current")

		require.True(t, found)
		assert.Equal(t, uint64(2*gib), free,
			"a descendant whose protection could not be read was counted as protecting nothing")
	})

	t.Run("an unreadable probe at the top refuses rather than establishing zero", func(t *testing.T) {
		// The preflight that skips the walk when there is no controller used a
		// bare `os.Stat`, which cannot tell absence from a permission denial or
		// an I/O error — so every one of those established a zero and counted
		// the whole cache back. That is the same fail-open this function had one
		// read further in, reintroduced at its own front door by the fix for
		// something else (Codex, #1134).
		//
		// A self-referential symlink, because it is the one error that is
		// neither absence nor success and needs no privileges to arrange.
		dir := protectedLayout(t, protection{cache: 5 * gib, noController: true})
		require.NoError(t, os.Symlink("memory.min", filepath.Join(dir, "memory.min")))

		free, found := tightestMemoryFree([]string{dir}, "memory.max", "memory.current")

		require.True(t, found)
		assert.Equal(t, uint64(2*gib), free,
			"a probe that failed for a reason other than absence was read as 'no controller here'")
	})

	t.Run("the scan budget is one budget, not one per ancestor", func(t *testing.T) {
		// Every level of the chain called the walk with a fresh allowance, so a
		// deep chain over a wide subtree multiplied the advertised bound by its
		// own depth — and the constant says one number (Codex, #1134).
		//
		// Two levels, each just over half the budget. Separately each fits;
		// together they do not, and the second is left unestablished, which is
		// the fail-closed direction.
		half := maxProtectionScan/2 + 100

		var chain []string
		for range 2 {
			dir := protectedLayout(t, protection{cache: 5 * gib})
			for i := range half {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "child"+strconv.Itoa(i)), 0o755))
			}
			chain = append(chain, dir)
		}

		free, found := tightestMemoryFree(chain, "memory.max", "memory.current")

		require.True(t, found)
		assert.Equal(t, uint64(2*gib), free,
			"each ancestor was given its own allowance, so the bound the constant advertises is "+
				"multiplied by the depth of the hierarchy")
	})

	t.Run("a host with no memory controller is not walked at all", func(t *testing.T) {
		// cgroup v1 has no `memory.min` anywhere, and a controller is enabled
		// top-down — so a level without the file has nothing protected beneath
		// it and nothing to walk for. Without this the walk descended the whole
		// hierarchy to read a file that was never going to be there, and a v1
		// host wider than the bound would refuse to establish reclaimability,
		// count none of its cache, and recreate the zero-lane reading this
		// change exists to fix (Codex, #1134).
		dir := protectedLayout(t, protection{cache: 5 * gib, noController: true})
		for i := range maxProtectionScan + 1 {
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "child"+strconv.Itoa(i)), 0o755))
		}

		free, found := tightestMemoryFree([]string{dir}, "memory.max", "memory.current")

		require.True(t, found)
		assert.Equal(t, uint64(7*gib), free,
			"a hierarchy that cannot protect anything defeated the fix through its own safeguard")
	})

	t.Run("a tree too large to read establishes nothing", func(t *testing.T) {
		// Fail closed. A partial sum of protections understates them, which
		// overstates headroom — the one direction a dispatch budget must not
		// be wrong in — so exceeding the bound subtracts nothing at all rather
		// than subtracting what was counted so far.
		dir := protectedLayout(t, protection{cache: 5 * gib})
		for i := range maxProtectionScan + 1 {
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "child"+strconv.Itoa(i)), 0o755))
		}

		free, found := tightestMemoryFree([]string{dir}, "memory.max", "memory.current")

		require.True(t, found)
		assert.Equal(t, uint64(2*gib), free,
			"the walk gave up and subtracted what it had counted, which overstates headroom")
	})
}

// writesMin writes a `memory.min` holding exactly this text.
func writesMin(value string) func(t *testing.T, dir string) {
	return func(t *testing.T, dir string) {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.min"), []byte(value+"\n"), 0o644))
	}
}

// protection describes one level of a fixture hierarchy: how much file cache it
// reports, and what it asks the kernel to protect.
type protection struct {
	cache uint64
	min   uint64

	// noController omits `memory.min` entirely, which is what a cgroup v1 level
	// looks like — and what a v2 level whose parent never enabled the memory
	// controller looks like. Distinct from a min of zero, which is a level that
	// *can* protect and has chosen not to.
	noController bool

	// raw writes `memory.min` verbatim, for the values that are not integers:
	// `max`, which v2 spells complete protection as, and whatever a corrupt or
	// future kernel might put there.
	raw string
}

// protectedLayout writes a cgroup at 8 GiB limit and 6 GiB used, with levels
// nested under it, and returns the top.
func protectedLayout(t *testing.T, levels ...protection) string {
	t.Helper()

	top := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(top, "memory.max"), []byte("8589934592\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(top, "memory.current"), []byte("6442450944\n"), 0o644))

	dir := top
	for i, level := range levels {
		if i > 0 {
			dir = filepath.Join(dir, "nested")
			require.NoError(t, os.MkdirAll(dir, 0o755))
		}
		if level.cache > 0 {
			require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.stat"),
				[]byte("inactive_file "+strconv.FormatUint(level.cache, 10)+"\n"), 0o644))
		}
		if !level.noController {
			value := strconv.FormatUint(level.min, 10)
			if level.raw != "" {
				value = level.raw
			}
			require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.min"),
				[]byte(value+"\n"), 0o644))
		}
	}

	return top
}

// TestTheCgroupChainStaysInsideItsMount pins the walk's two ends: it starts at
// the process's own cgroup and stops at the mount root, and a relative path
// carrying `..` cannot take it somewhere that is not a cgroup at all.
func TestTheCgroupChainStaysInsideItsMount(t *testing.T) {
	assert.Equal(t,
		[]string{"/sys/fs/cgroup/docker/abc", "/sys/fs/cgroup/docker", "/sys/fs/cgroup"},
		cgroupChain("/sys/fs/cgroup", "/docker/abc"),
		"leaf first, and every ancestor up to the mount, because every one of them is enforced")

	assert.Equal(t, []string{"/sys/fs/cgroup"}, cgroupChain("/sys/fs/cgroup", "/"),
		"a namespaced container sees its own cgroup as the root, and the chain is one directory")

	assert.Equal(t, []string{"/sys/fs/cgroup/etc", "/sys/fs/cgroup"}, cgroupChain("/sys/fs/cgroup", "/../../etc"),
		"the `..` is cancelled against the mount rather than climbing out of it, so the walk cannot "+
			"end up reading /etc for a cpu quota")
}

// cgroupLayout writes one controller file per level and returns the
// directories leaf-first, which is the order the readers walk.
func cgroupLayout(t *testing.T, file string, levels []string) []string {
	t.Helper()

	dir := t.TempDir()
	var dirs []string
	for _, contents := range levels {
		dir = filepath.Join(dir, "level")
		require.NoError(t, os.MkdirAll(dir, 0o755))
		if contents != "" {
			require.NoError(t, os.WriteFile(filepath.Join(dir, file), []byte(contents+"\n"), 0o644))
		}
		dirs = append([]string{dir}, dirs...)
	}

	return dirs
}

// cgroupPairLayout is [cgroupLayout] for the two files a memory limit takes,
// given as "max current" per level.
func cgroupPairLayout(t *testing.T, levels []string) []string {
	t.Helper()

	dir := t.TempDir()
	var dirs []string
	for _, pair := range levels {
		dir = filepath.Join(dir, "level")
		require.NoError(t, os.MkdirAll(dir, 0o755))
		// A third field is the level's reclaimable page cache, written as the
		// `memory.stat` a real cgroup carries beside the pair.
		if fields := strings.Fields(pair); len(fields) >= 2 {
			require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte(fields[0]+"\n"), 0o644))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.current"), []byte(fields[1]+"\n"), 0o644))
			if len(fields) == 3 {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.stat"),
					[]byte("rss 4096\ninactive_file "+fields[2]+"\ntotal_inactive_file "+fields[2]+"\n"), 0o644))
			}
		}
		dirs = append([]string{dir}, dirs...)
	}

	return dirs
}

// TestTheDiskBoundCoversWhereBuildIntermediatesLand is the third filesystem a
// lane writes to and the one nobody remembers.
//
// `go help environment` defines GOTMPDIR as "the directory where the go
// command will write temporary source files, packages, and binaries" — every
// test binary is linked there before it is copied anywhere — and its fallback
// is the platform's temporary directory, which is frequently tmpfs: RAM, on a
// mount orders of magnitude smaller than the checkout's. Measuring only the
// worktree and GOCACHE reports room those writes do not have, and the fleet it
// recommends fails at the link step with an ENOSPC naming neither disk nor
// memory.
func TestTheDiskBoundCoversWhereBuildIntermediatesLand(t *testing.T) {
	t.Run("a small tmp mount decides the bound", func(t *testing.T) {
		free := map[string]uint64{".": 500 * gib, "/cache": 300 * gib, "/tmp": 64 << 20}
		targets := laneWriteTargets(func(name string) string {
			return map[string]string{"GOCACHE": "/cache", "GOTMPDIR": "/tmp"}[name]
		})

		require.Contains(t, targets, "/tmp", "the go command's scratch directory is a filesystem a lane writes to")

		got, measured := tightestFree(targets, reading(free))
		assert.True(t, measured)
		assert.Equal(t, uint64(64<<20), got,
			"the tightest mount is the bound, whichever of the three it is")
	})

	t.Run("an unset GOTMPDIR still measures the directory the go command will use", func(t *testing.T) {
		targets := laneWriteTargets(func(string) string { return "" })

		assert.Contains(t, targets, os.TempDir(),
			"empty means the platform default, not that there is nothing to measure")
	})

	// An unmeasurable mount is a missing fact, not a full one: statfs failing
	// on a path reports nothing, and folding that into the minimum would refuse
	// the whole fleet on the strength of a read that did not happen.
	t.Run("an unreadable filesystem does not read as a full one", func(t *testing.T) {
		free := map[string]uint64{".": 100 * gib}

		got, measured := tightestFree([]string{".", "/gone", "/also-gone"}, reading(free))
		assert.True(t, measured)
		assert.Equal(t, uint64(100*gib), got)
	})

	// And the direction that cost a P1: a filesystem at ENOSPC has zero bytes
	// available and that is a *measurement*. While zero and unreadable shared
	// one spelling, a full GOCACHE dropped out of the minimum entirely, so a
	// machine whose cache mount was the only full one read as healthy and
	// `-prune` answered "nothing to prune" (Codex, #1112).
	t.Run("a full filesystem is measured, not skipped", func(t *testing.T) {
		free := map[string]uint64{".": 100 * gib, "/cache": 0}

		got, measured := tightestFree([]string{".", "/cache"}, reading(free))
		assert.True(t, measured)
		assert.Zero(t, got, "a measured zero is the tightest mount there is")
	})
}

// reading turns a table of free bytes into the two-value reader tightestFree
// takes: present means measured, absent means the path could not be read.
func reading(free map[string]uint64) func(string) (uint64, bool) {
	return func(path string) (uint64, bool) {
		got, ok := free[path]

		return got, ok
	}
}
