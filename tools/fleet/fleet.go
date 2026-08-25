// Command fleet answers one question: how many agent lanes may run here right
// now, and what each of them is allowed to spend.
//
// # Why this is computed rather than written down
//
// The number is a property of the machine, and the machine changes. A lane
// count in a prompt or a doc is a constant that was right on one box, and the
// factory runs on whatever it is given — four cores today, thirty-two next
// week, and a container whose disk is nearly full in between. Every agent that
// re-derives the number by hand derives it differently, which is the second
// spelling this repository spends its review budget on. So it is derived once,
// here, and whoever dispatches reads it.
//
// # What a lane actually costs, which is the part that surprises people
//
// A lane is not one process. `go test ./...` builds with `-p` workers, and
// each package's test binary then runs `-parallel` tests inside itself; both
// default to GOMAXPROCS, which defaults to the core count. So one unbounded
// lane on a four-core box asks for four compilers and up to four parallel
// tests apiece — it can saturate the machine by itself, and four of them
// produced a load average above 20 here, with lanes reporting link failures
// they reasonably mistook for defects in their own diffs.
//
// A lane is therefore given an explicit appetite ([LaneEnv]) rather than left
// to discover the machine, and the fleet size is the machine's capacity
// divided by that appetite. Bounding the per-lane cost is what makes the
// division mean anything.
//
// # The resources, and which one binds
//
// Three, and they bind in different situations, so all three are computed and
// the smallest wins:
//
//   - CPU. The one that binds on a small box, and the one whose symptom is
//     misleading: everything still works, only slowly, and slow builds get
//     misread as flakes.
//   - Memory. Binds when a lane runs the race detector, which is where the
//     repository's own GOMEMLIMIT advice comes from.
//   - Disk. Binds without warning and fails loudly: the shared Go build cache
//     grows unboundedly until something prunes it, and this box hit ENOSPC
//     twice in one session with a 13 GiB cache. A floor is reserved rather
//     than divided, because a build that runs out of disk halfway leaves
//     wreckage the next build trips over.
//
// Load is read as well as capacity. Cores say what the machine could do; load
// says what it is already doing, including work this process cannot see — a
// sibling session's test run, or a lane that has not reported yet.
package main

import (
	"fmt"
	"math"
)

// Per-lane appetite. These are the numbers [LaneEnv] hands a lane, so the
// division below is a statement about the same lanes that will actually run.
const (
	// LaneCores is what one lane is allowed to spend on compilers and parallel
	// tests. Two rather than one because a lane with GOMAXPROCS=1 spends most
	// of its wall clock waiting on a single compiler, and wall clock is what
	// the human is watching; two is the smallest number that keeps a lane
	// usefully fast while making the fleet arithmetic honest.
	LaneCores = 2

	// LaneProcessMemoryBytes is the GOMEMLIMIT handed to one Go process, and
	// matches the repository's own bounded-test advice.
	LaneProcessMemoryBytes = 1 << 30

	// LaneMemoryBytes is what a lane is budgeted in the arithmetic below, and
	// it is deliberately four times the per-process limit rather than equal to
	// it. Two corrections, both of which make the naive figure recommend lanes
	// a machine cannot hold:
	//
	// A lane is allowed LaneCores concurrent Go processes — `-p` is how many
	// build commands or test binaries run at once — and GOMEMLIMIT is per
	// process, not per lane, so the Go heap alone is LaneProcessMemoryBytes
	// times LaneCores.
	//
	// And GOMEMLIMIT is a soft limit over the Go heap only: it excludes the C
	// allocations the race detector makes for its shadow memory, which is
	// precisely what the -race runs this repository gates on spend. The
	// remaining factor is headroom for that, and it is a budget rather than a
	// measurement — a lane can still exceed it, and the reserve below is what
	// absorbs the difference.
	LaneMemoryBytes = LaneProcessMemoryBytes * LaneCores * 2

	// LaneDiskBytes is what one lane adds to the shared build cache and its own
	// worktree before anything prunes it. Measured rather than guessed: a
	// worktree of this repository is tens of megabytes, and the cache grew past
	// 13 GiB across a session's lanes.
	LaneDiskBytes = 2 << 30
)

// Reserved for everything that is not a lane: the orchestrating session's own
// builds, the editor, and the headroom a disk needs to not fail mid-write.
const (
	reservedCores  = 1
	reservedMemory = 2 << 30

	// DiskFloorBytes is subtracted, not divided. Below this the answer is zero
	// lanes and a sentence about pruning, because the failure is not slowness:
	// a `go build` that meets ENOSPC leaves a partial object the next build
	// reports as a corrupt cache entry, which reads like a compiler bug.
	DiskFloorBytes = 6 << 30
)

// Prune is what the shared build cache can do about a full disk.
type Prune struct {
	// Short is how much the binding filesystem lacks before a lane fits. Zero
	// means nothing is wrong and there is nothing to do.
	Short uint64

	// Bytes is what to reclaim from the cache. Zero alongside a non-zero Short
	// means the cache *cannot help* — it is empty, unreadable, or on a
	// filesystem that is not the one that is short — which is a different
	// answer from "nothing is wrong" and must not be reported as one (Codex,
	// #1112).
	Bytes uint64

	// Enough records that reclaiming Bytes clears Short. False means the disk
	// will still be below the floor afterwards, so whoever reads it needs to
	// look at the worktrees and the module cache too.
	Enough bool

	// Advice is what to say about all of that, in the same shape [Plan] uses
	// and for the same reason: "0 lanes" with no sentence is a dead end for
	// the agent that reads it, and so is a prune that quietly did nothing.
	// Carried on the answer rather than composed at the print site so the
	// wording is a pure function that a test can hold to account — the first
	// cut said "already past the floor" at a machine short by seven gigabytes
	// (Codex, #1112).
	Advice []string
}

// PruneFor answers how much of the build cache has to go.
//
// The target is the floor *plus one lane*, not the floor alone. Freeing to the
// floor exactly earns the answer "dispatch zero lanes", which is the state
// this exists to end: a prune that leaves no room to work has unblocked
// nothing.
//
// Bounded by what the cache holds, because that is all this can reclaim. The
// worktrees, the module cache and everything else sharing the volume belong to
// somebody else, and a target larger than the cache would remove every entry
// and still report the disk short.
func PruneFor(m Machine) Prune {
	want := uint64(DiskFloorBytes) + LaneDiskBytes
	if m.DiskFree >= want {
		return Prune{}
	}

	short := want - m.DiskFree

	// Where the cache actually lives. Unknown reads as "the same mount",
	// which is true of most machines and is the direction that keeps a
	// blocked one moving — see [Machine.CacheDiskFree].
	cacheFree := m.DiskFree
	if m.CacheDiskKnown {
		cacheFree = m.CacheDiskFree
	}

	elsewhere := fmt.Sprintf(
		"the disk is %s short of the %s floor plus a lane's %s, and the build cache cannot reach it",
		bytes(short), bytes(DiskFloorBytes), bytes(LaneDiskBytes))
	lookElsewhere := "check worktrees (`git worktree list`) and $GOMODCACHE"

	if cacheFree >= want {
		// The cache's own filesystem has room. Something else is short, and
		// no amount of pruning reaches it. Named specifically, because the
		// obvious reading of a full disk is that the cache did it.
		return Prune{Short: short, Advice: []string{
			fmt.Sprintf("%s — GOCACHE is on a filesystem with %s free, so what is short is somewhere else",
				elsewhere, bytes(cacheFree)),
			lookElsewhere,
		}}
	}

	cacheShort := want - cacheFree

	// Clearing the machine's bound takes two things, and only the first is
	// about the cache: it must hold what its own filesystem is short of, and
	// every other mount a lane writes to must already be clear of the target.
	// A prune that fixes the cache mount while the worktree stays short has
	// bought a cold rebuild and changed the plan by nothing.
	othersClear := !m.OtherDiskKnown || m.OtherDiskFree >= want

	prune := Prune{
		Short:  short,
		Bytes:  min(cacheShort, m.CacheSizeBytes),
		Enough: m.CacheSizeBytes >= cacheShort && othersClear,
	}

	switch {
	case prune.Bytes == 0:
		prune.Advice = []string{elsewhere + " — the cache is empty or unreadable", lookElsewhere}

	case !othersClear:
		// The cache can do its part and it still will not be enough, because
		// something else a lane writes to is short too. Named separately from
		// a cache that is merely too small, since the remedy is different.
		prune.Advice = []string{
			fmt.Sprintf("the build cache can give back %s, but another filesystem a lane writes to "+
				"has only %s free and needs %s, so this will not fit a lane on its own",
				bytes(prune.Bytes), bytes(m.OtherDiskFree), bytes(want)),
			lookElsewhere,
		}

	case !prune.Enough:
		// Said before the work rather than after: the cache is about to give
		// back everything it has and the disk will still be short.
		prune.Advice = []string{
			fmt.Sprintf("the build cache can give back %s and the disk is %s short, so this frees "+
				"what it can and the disk will still be short", bytes(prune.Bytes), bytes(short)),
			lookElsewhere,
		}
	}

	return prune
}

// Machine is what the plan is computed from. A struct rather than direct reads
// so that [Plan] is a pure function and its tests can describe machines this
// one will never be.
type Machine struct {
	Cores int

	// Load1 is -1 when the platform does not report a load average.
	//
	// Unknown must not read as "none": a macOS box has no /proc/loadavg, and
	// treating that as an idle machine would hand out lanes on the strength of
	// a figure nobody measured. Unknown is therefore unbounded — the plan falls
	// back to the resources it can see — and the printed line says which
	// figures were unavailable rather than quietly presenting a partial answer
	// as a whole one.
	Load1 float64

	// MemoryFree is what a lane may actually be given, and MemoryKnown says
	// whether anything measured it. They are two fields for the same reason
	// Load1 spends a sentinel on the distinction and LoadIsHostWide carries
	// scope separately: an unavailable reading and a real reading are
	// different facts, and a single number cannot hold both.
	//
	// The distinction is load-bearing in both directions, and this type used
	// to make it in neither. Zero previously meant "unknown", which is the
	// macOS case [TestUnknownIsNotNone] pins — no /proc/meminfo, so treating
	// the read as zero free memory made the memory bound win every time and
	// the tool answer zero lanes forever, on every machine it could not
	// measure. But zero is also a reading a Linux container gives honestly:
	// a cgroup sitting *at* its memory.max reports exactly no memory free,
	// and that is the one machine that must be told to dispatch nothing. With
	// one field the second case was indistinguishable from the first, so the
	// memory bound switched itself off precisely when it was right — handing
	// lanes to a container with no memory to run them in, which then OOMs
	// mid-build and looks to the lane like a defect in its own diff.
	//
	// So: MemoryKnown false is unbounded and says so in the advice;
	// MemoryKnown true with MemoryFree 0 is a hard zero-lane bound named
	// "memory".
	MemoryFree  uint64
	MemoryKnown bool

	// DiskFree is the tightest of the filesystems a lane writes to, which is
	// not necessarily one: a checkout and GOCACHE can be on different mounts,
	// and it is cache growth this budgets.
	DiskFree       uint64
	CacheSizeBytes uint64

	// CacheDiskFree is free space on the filesystem holding GOCACHE, which is
	// not necessarily the one DiskFree reports: DiskFree is the *tightest* of
	// the mounts a lane writes to, and the cache can only ever give bytes back
	// to its own. A machine whose GOTMPDIR is a 64 MiB tmpfs while its cache
	// sits on a 300 GiB volume is short on the first and roomy on the second,
	// and pruning would discard gigabytes of hot cache without moving the
	// bound an inch — a cold rebuild bought for nothing (Codex, #1112).
	//
	// CacheDiskKnown false means it could not be read, and is treated as "the
	// same mount as everything else". That direction is deliberate: the cost
	// of pruning where it does not help is a rebuild, and the cost of refusing
	// to prune where it would have helped is the machine staying blocked,
	// which is the outage this whole lever exists to end.
	CacheDiskFree  uint64
	CacheDiskKnown bool

	// OtherDiskFree is the tightest of the mounts a lane writes to *other*
	// than the cache's, and it is what decides whether a prune can unblock
	// anything at all. Pruning moves bytes on one filesystem; if a different
	// one is also short, the machine still fits no lanes afterwards and the
	// cold rebuild was spent for nothing. Claiming otherwise is the same
	// mistake as measuring the aggregate, one level further in (Codex, #1112).
	OtherDiskFree  uint64
	OtherDiskKnown bool

	// LoadIsHostWide records that Load1 was read from a host-scoped source
	// while Cores describes a container's quota. Comparing those two mixes
	// scopes — a two-core container on a busy sixty-four-core host would be
	// refused every lane on the strength of work it is not competing for.
	LoadIsHostWide bool
}

// Plan is the answer, with the reason attached. The reason is not decoration:
// an agent told "2" learns nothing, and an agent told "2, memory is the bound"
// knows what to change.
type Plan struct {
	Lanes  int
	Bound  string // which resource decided it
	Advice []string
}

// PlanFor computes how many lanes fit, and which resource says so.
//
// Every bound is computed even after one of them has already reached zero,
// because the advice a caller needs is not only "you cannot" but "and this is
// the second thing that would have stopped you".
func PlanFor(m Machine) Plan {
	if m.Cores < 1 {
		m.Cores = 1
	}

	byCPU := (m.Cores - reservedCores) / LaneCores

	// A measured zero is a bound, not a missing fact. Only an unmeasured one
	// falls back to the other resources; see [Machine.MemoryKnown].
	byMemory := math.MaxInt32
	if m.MemoryKnown {
		byMemory = int((m.MemoryFree - min(m.MemoryFree, reservedMemory)) / LaneMemoryBytes)
	}

	var byDisk int
	if m.DiskFree > DiskFloorBytes {
		byDisk = int((m.DiskFree - DiskFloorBytes) / LaneDiskBytes)
	}

	// What the machine is already doing, which capacity alone cannot see. Load
	// counts runnable *and* uninterruptible tasks, so a box thrashing on disk
	// reads high here even when its cores look idle — which is exactly the
	// state in which adding a lane hurts most.
	// Applied only where it is measuring the same machine the core count
	// describes. Where the cores are a cgroup quota and the load average is the
	// host's, the two are different scopes and the comparison is meaningless in
	// the direction that refuses work.
	byLoad := math.MaxInt32
	if m.Load1 >= 0 && !m.LoadIsHostWide {
		byLoad = int((float64(m.Cores) - m.Load1) / LaneCores)
	}

	plan := Plan{Lanes: byCPU, Bound: "cores"}
	for _, candidate := range []struct {
		lanes int
		bound string
	}{
		{byMemory, "memory"},
		{byDisk, "disk"},
		{byLoad, "current load"},
	} {
		if candidate.lanes < plan.Lanes {
			plan.Lanes, plan.Bound = candidate.lanes, candidate.bound
		}
	}
	plan.Lanes = max(plan.Lanes, 0)

	if m.DiskFree <= DiskFloorBytes {
		plan.Advice = append(plan.Advice, fmt.Sprintf(
			"disk is below the %s floor: `go run ./tools/fleet -prune` trims the shared build cache "+
				"oldest-entry-first until a lane fits, or remove finished worktrees, before dispatching anything",
			bytes(DiskFloorBytes)))
	}
	if m.CacheSizeBytes > uint64(DiskFloorBytes) {
		plan.Advice = append(plan.Advice, fmt.Sprintf(
			"the shared build cache is %s; `go run ./tools/fleet -prune` gives back only what a lane "+
				"needs and keeps the hot entries, where `go clean -cache` reclaims all of it at the "+
				"price of one cold rebuild per lane",
			bytes(m.CacheSizeBytes)))
	}
	if !m.MemoryKnown {
		plan.Advice = append(plan.Advice,
			"free memory could not be read on this platform, so the plan is bound by cores and disk alone — treat the count as an upper bound")
	} else if plan.Lanes == 0 && plan.Bound == "memory" {
		plan.Advice = append(plan.Advice, fmt.Sprintf(
			"only %s of memory is free and a lane is budgeted %s: this is a measured reading, not a missing one, so dispatching anyway buys an OOM kill mid-build rather than a slow build. Wait for the running lanes to finish, or raise the container's memory limit",
			bytes(m.MemoryFree), bytes(LaneMemoryBytes)))
	}
	if m.LoadIsHostWide {
		plan.Advice = append(plan.Advice,
			"the load average is the host's while the core count is this container's quota, so load was not used as a bound; watch it yourself if the host is shared")
	}
	if plan.Lanes == 0 && plan.Bound == "current load" {
		plan.Advice = append(plan.Advice,
			"the machine is already busy with work this process cannot see — a sibling session, or a lane that has not reported. Wait rather than adding to it")
	}

	return plan
}

// LaneEnv is the appetite a lane must be given, and the reason the fleet
// arithmetic holds.
//
// Emitted as `export` statements, because the obvious way to consume this —
// `eval "$(go run ./tools/fleet -env)"` — evaluates bare assignments into the
// *shell's* variables, which no child process inherits. A lane consuming it
// that way would run at the unbounded defaults this exists to prevent, and
// would do so silently: the shell would show the value and `go test` would
// never see it.
//
// The memory value is per process, not the lane's whole budget; see
// [LaneMemoryBytes] for why those differ by a factor of four.
func LaneEnv() []string {
	return []string{
		fmt.Sprintf("export GOMAXPROCS=%d", LaneCores),
		fmt.Sprintf("export GOFLAGS=-p=%d", LaneCores),
		fmt.Sprintf("export GOMEMLIMIT=%dMiB", LaneProcessMemoryBytes>>20),
	}
}

func bytes(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.0f MiB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
