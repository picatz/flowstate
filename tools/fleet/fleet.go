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

	// LaneMemoryBytes is one lane's GOMEMLIMIT. The repository's own test
	// advice is 1 GiB bounded and 2 GiB under -race; this is the -race figure,
	// because a lane that cannot run the race detector is not a lane that can
	// finish its own gate.
	LaneMemoryBytes = 2 << 30

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

// Machine is what the plan is computed from. A struct rather than direct reads
// so that [Plan] is a pure function and its tests can describe machines this
// one will never be.
type Machine struct {
	Cores          int
	Load1          float64 // -1 when the platform does not report it
	MemoryFree     uint64
	DiskFree       uint64
	CacheSizeBytes uint64 // the shared build cache, when it could be measured
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
	byMemory := int((m.MemoryFree - min(m.MemoryFree, reservedMemory)) / LaneMemoryBytes)

	var byDisk int
	if m.DiskFree > DiskFloorBytes {
		byDisk = int((m.DiskFree - DiskFloorBytes) / LaneDiskBytes)
	}

	// What the machine is already doing, which capacity alone cannot see. Load
	// counts runnable *and* uninterruptible tasks, so a box thrashing on disk
	// reads high here even when its cores look idle — which is exactly the
	// state in which adding a lane hurts most.
	byLoad := math.MaxInt32
	if m.Load1 >= 0 {
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
			"disk is below the %s floor: prune the shared build cache (`go clean -cache`) or remove finished worktrees before dispatching anything",
			bytes(DiskFloorBytes)))
	}
	if m.CacheSizeBytes > uint64(DiskFloorBytes) {
		plan.Advice = append(plan.Advice, fmt.Sprintf(
			"the shared build cache is %s; `go clean -cache` reclaims it at the price of one cold rebuild per lane",
			bytes(m.CacheSizeBytes)))
	}
	if plan.Lanes == 0 && plan.Bound == "current load" {
		plan.Advice = append(plan.Advice,
			"the machine is already busy with work this process cannot see — a sibling session, or a lane that has not reported. Wait rather than adding to it")
	}

	return plan
}

// LaneEnv is the appetite a lane must be given, and the reason the fleet
// arithmetic holds. Handed to a lane verbatim.
func LaneEnv() []string {
	return []string{
		fmt.Sprintf("GOMAXPROCS=%d", LaneCores),
		fmt.Sprintf("GOFLAGS=-p=%d", LaneCores),
		fmt.Sprintf("GOMEMLIMIT=%dMiB", LaneMemoryBytes>>20),
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
