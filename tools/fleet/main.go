package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	number := flag.Bool("n", false, "print only the lane count, for a script to read")
	env := flag.Bool("env", false, "print only the environment a lane must be given")
	prune := flag.Bool("prune", false, "trim the shared build cache until a lane fits, oldest entry first")
	flag.Parse()

	if *env {
		// One per line: joined by spaces, `eval` reads the second `export` as
		// a variable name rather than as a keyword.
		fmt.Println(strings.Join(LaneEnv(), "\n"))
		return
	}

	machine := readMachine()

	if *prune {
		runPrune(machine)

		return
	}

	plan := PlanFor(machine)

	if *number {
		fmt.Println(plan.Lanes)
		return
	}

	fmt.Printf("fleet: %d core(s)", machine.Cores)
	if machine.Load1 >= 0 {
		fmt.Printf(", load %.1f", machine.Load1)
	}
	if machine.MemoryKnown {
		fmt.Printf(", %s memory free", bytes(machine.MemoryFree))
	} else {
		// Printed rather than shown as "0 B", which is a different machine:
		// see [Machine.MemoryKnown].
		fmt.Print(", memory free unknown")
	}
	fmt.Printf(", %s disk free", bytes(machine.DiskFree))
	if machine.CacheSizeBytes > 0 {
		fmt.Printf(" (build cache %s)", bytes(machine.CacheSizeBytes))
	}
	fmt.Println()

	switch plan.Lanes {
	case 0:
		fmt.Printf("fleet: dispatch nothing right now — %s is the bound\n", plan.Bound)
	case 1:
		fmt.Printf("fleet: 1 lane — %s is the bound\n", plan.Bound)
	default:
		fmt.Printf("fleet: %d lanes — %s is the bound\n", plan.Lanes, plan.Bound)
	}

	if plan.Lanes > 0 {
		fmt.Printf("fleet: give each lane: %s\n", strings.Join(LaneEnv(), "; "))
	}
	for _, advice := range plan.Advice {
		fmt.Printf("fleet: %s\n", advice)
	}
}

// readMachine reads what the platform will tell us, and says -1 rather than
// guessing where it will not. A missing load average is not an idle machine.
func readMachine() Machine {
	visible := runtime.NumCPU()
	cores := containerCores(visible)
	free, known := containerMemoryFree(memoryFree())
	machine := Machine{
		Cores:       cores,
		Load1:       -1,
		MemoryFree:  free,
		MemoryKnown: known,
		DiskFree:    laneDiskFree(),

		// The load average has no cgroup scope: /proc/loadavg is the host's
		// even inside a container. Where the core count came from a quota, the
		// two describe different machines.
		LoadIsHostWide: cores != visible,
	}
	if raw, err := os.ReadFile("/proc/loadavg"); err == nil {
		if first, _, ok := strings.Cut(string(raw), " "); ok {
			if load, err := strconv.ParseFloat(first, 64); err == nil {
				machine.Load1 = load
			}
		}
	}
	machine.CacheSizeBytes = cacheSize(goEnv("GOCACHE"))

	// The cache's own mount and everything else a lane writes to, kept apart:
	// the cache can only give bytes back to its own filesystem, and a prune
	// only unblocks the machine when every *other* target is already clear of
	// the target too — see [Machine.CacheDiskFree] and [Machine.OtherDiskFree].
	cache := goEnv("GOCACHE")
	if cache != "" {
		machine.CacheDiskFree, machine.CacheDiskKnown = diskFree(cache)
	}

	others := otherFilesystems(laneWriteTargets(goEnv), cache, deviceOf)
	machine.OtherDiskFree, machine.OtherDiskKnown = tightestFree(others, diskFree)

	return machine
}

// memoryFree prefers MemAvailable, which is the kernel's own estimate of what
// a new workload can have without swapping — MemFree is not that, and using it
// would refuse lanes a box could easily run.
//
// The second return says whether anything was read, because a machine with no
// /proc/meminfo and a machine with no memory left are opposite answers and
// zero cannot mean both; see [Machine.MemoryKnown].
func memoryFree() (uint64, bool) {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		field, value, ok := strings.Cut(line, ":")
		if !ok || field != "MemAvailable" {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) == 0 {
			continue
		}
		if kb, err := strconv.ParseUint(fields[0], 10, 64); err == nil {
			return kb << 10, true
		}
	}

	return 0, false
}

// laneDiskFree is the tightest of the filesystems a lane actually writes to.
//
// The checkout, GOCACHE and the go command's temporary directory can be on
// three different mounts, and it is those writes this budgets — a roomy
// workspace beside a cache filesystem at ENOSPC is exactly the partial-object
// failure the floor exists to prevent, and measuring only the workspace would
// report room that the writes do not have.
//
// The temporary directory is the one that catches people out, and it is not a
// small effect. `go help environment` defines GOTMPDIR as "the directory where
// the go command will write temporary source files, packages, and binaries" —
// every link of a test binary lands there before it is copied anywhere — and
// its fallback, /tmp, is frequently tmpfs, which is to say RAM, on a mount
// orders of magnitude smaller than the checkout's. A box with 200 GiB free
// under the worktree and a 64 MiB /tmp can hold no lanes at all, and reading
// only the first two paths would confidently recommend a fleet that fails at
// the link step with an error naming neither disk nor memory.
func laneDiskFree() uint64 {
	free, _ := tightestFree(laneWriteTargets(goEnv), diskFree)

	return free
}

// laneWriteTargets names the directories a lane's writes land in, in the order
// they are worth thinking about: its worktree, the shared build cache, and the
// go command's scratch directory.
//
// The variable lookup is a parameter so a test can pose a machine whose
// GOCACHE and GOTMPDIR are somewhere this process cannot put them.
func laneWriteTargets(env func(string) string) []string {
	targets := []string{"."}
	if cache := env("GOCACHE"); cache != "" {
		targets = append(targets, cache)
	}

	// GOTMPDIR empty is not "no temporary directory": it means the go command
	// falls back to the platform's, which is what [os.TempDir] answers. Reading
	// the empty value as "nothing to measure" would skip the mount most likely
	// to be the small one.
	tmp := env("GOTMPDIR")
	if tmp == "" {
		tmp = os.TempDir()
	}
	if tmp != "" {
		targets = append(targets, tmp)
	}

	return targets
}

// otherFilesystems returns the write targets that are not on the cache's own
// filesystem — the ones a prune cannot give bytes back to.
//
// By filesystem identity rather than by path, and the difference is the common
// case rather than an edge one: on a single-volume machine the worktree, the
// cache and GOTMPDIR are three different paths on one device, so excluding by
// path alone left two of them in this list reporting the cache's own low
// reading. Every warning about "another filesystem" would then fire exactly
// when pruning was about to work, because deleting cache entries raises free
// space for all three at once (Codex, #1113).
//
// The device lookup is a parameter for the reason [laneWriteTargets]'s
// environment lookup is: so a test can pose a machine whose mounts this
// process cannot arrange. A target whose device cannot be read stays in the
// list, which over-reports rather than over-promises — the safe direction for
// a claim that a prune will be enough.
func otherFilesystems(targets []string, cache string, device func(string) (uint64, bool)) []string {
	cacheDevice, known := device(cache)

	var others []string

	for _, target := range targets {
		if target == cache {
			continue
		}
		if known {
			if got, ok := device(target); ok && got == cacheDevice {
				continue
			}
		}
		others = append(others, target)
	}

	return others
}

// deviceOf identifies the filesystem a path sits on.
func deviceOf(path string) (uint64, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, false
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}

	return uint64(stat.Dev), true
}

// tightestFree is the minimum over the readable answers, and it is the readable
// part that needs saying: a path whose filesystem cannot be stat'ed reports
// zero, and folding that in would make every unreadable target a full disk and
// refuse the fleet outright. An unmeasurable mount is a missing fact, not a
// full one — the same distinction [Machine.MemoryKnown] carries for memory.
func tightestFree(paths []string, free func(string) (uint64, bool)) (uint64, bool) {
	var (
		tightest uint64
		measured bool
	)

	for _, path := range paths {
		got, ok := free(path)
		if !ok {
			continue
		}
		if !measured || got < tightest {
			tightest, measured = got, true
		}
	}

	return tightest, measured
}

// goEnv asks the go command for one of its own variables, which is the only
// answer that accounts for the layered defaults (an env var, then GOENV, then
// the built-in) that decide where GOCACHE and GOTMPDIR actually are.
func goEnv(name string) string {
	out, err := exec.Command("go", "env", name).Output()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(out))
}

// diskFree reports free bytes on a path's filesystem, and whether it could be
// read at all.
//
// The two answers are separate because zero is a legitimate reading. A
// filesystem at ENOSPC has zero bytes available and that is a *fact*; folding
// it into the same value an unreadable path returns made a full GOCACHE
// indistinguishable from a missing one, so it dropped out of the tightest-mount
// calculation and `-prune` answered "nothing to prune" on precisely the
// condition it exists to fix (Codex, #1112). The same distinction
// [Machine.MemoryKnown] already draws, for the same reason.
func diskFree(path string) (uint64, bool) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, false
	}

	return stat.Bavail * uint64(stat.Bsize), true
}

// cacheSize is how much of a build cache a prune could actually reclaim.
//
// Best-effort: it decides advice, not a bound, so a slow or missing answer
// costs a line of output rather than a wrong plan.
//
// The directory is a parameter rather than read from the environment, and that
// is not decoration — while it read GOCACHE itself there was no way to pose a
// cache to it, so the one function whose number the prune promises had no test
// at all, and it went out counting a fuzz corpus it could never remove (Codex,
// #1112).
func cacheSize(dir string) uint64 {
	if dir == "" {
		return 0
	}

	// Only what [pruneCache] could actually remove, which is not the whole
	// tree: the fuzz corpus lives under GOCACHE and is deliberately preserved,
	// so a walk of everything promised a prune it could not deliver — eight
	// gigabytes of corpus beside one of entries would ask for eight and free
	// one, without ever reporting that it fell short (Codex, #1112). Both
	// numbers come from one enumeration now, so they cannot drift.
	var total uint64
	for _, entry := range cacheEntries(dir) {
		total += entry.size
	}

	return total
}

// runPrune trims the build cache to what the plan needs, and says what it did.
func runPrune(m Machine) {
	want := PruneFor(m)

	// Nothing wrong and nothing this can fix are different answers, and the
	// first cut of this reported the second as the first: an empty or
	// unreadable cache under a full disk came back as Bytes zero and printed
	// "already past the floor" at a machine that fits no lanes (Codex, #1112).
	if want.Short == 0 {
		fmt.Printf("fleet: nothing to prune — %s free is already past the %s floor and a lane's %s\n",
			bytes(m.DiskFree), bytes(DiskFloorBytes), bytes(LaneDiskBytes))

		return
	}

	for _, line := range want.Advice {
		fmt.Printf("fleet: %s\n", line)
	}

	if want.Bytes == 0 {
		return
	}

	freed, removed, err := pruneCache(goEnv("GOCACHE"), want.Bytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleet: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("fleet: freed %s across %d cache entries, oldest first\n", bytes(freed), removed)

	// Re-read rather than compute: the number that matters is what the
	// filesystem says now, and something else may have been writing to it
	// throughout.
	after := readMachine()
	plan := PlanFor(after)
	fmt.Printf("fleet: %s free now — %d lane(s), %s is the bound\n",
		bytes(after.DiskFree), plan.Lanes, plan.Bound)
}

// pruneCache trims the shared build cache, least recently used first, until it
// has given back target bytes.
//
// This is cmd/go's own trimSubdir with a *size* cutoff where it has a *time*
// one, and that difference is the whole reason it exists. Go trims entries
// unused for five days, at most once a day ([cache.DiskCache.Trim]), which
// cannot help a machine that filled twenty-three gigabytes between breakfast
// and lunch: nothing in that cache is five days old. And Go has no size budget
// at all — `go help cache` offers exactly two levers, that five-day sweep and
// `go clean -cache`, which discards everything and charges a cold rebuild to
// every lane. This is the missing middle, and it is the difference between a
// machine that unblocks itself and one that waits for somebody to notice.
//
// The rules are Go's, deliberately, because the cache is its format and not
// ours: only `-a` and `-d` names are entries, an entry may be a *directory*
// (an executable cache entry) and needs RemoveAll rather than Remove, and only
// the 256 hex subdirectories hold them. Everything else the directory contains
// is left alone — trim.txt, the lock, and the fuzz corpus especially. Those
// corpus entries are inputs that once expanded coverage; they cost
// machine-hours to rediscover, and `go help cache` says plainly that removing
// them makes fuzzing less effective.
//
// mtime is the "last used" signal because that is what Go maintains it as, and
// its own accuracy note applies here unchanged: an entry's mtime is refreshed
// on use only when it is already more than an hour stale, a deliberate trade
// against disk churn. An hour of imprecision is nothing against a cache whose
// entries span days, and it is the same signal Go's own trim sorts on.
//
// Safe while builds are in flight. The cache is content-addressed, so a removed
// entry is a miss and a miss is a rebuild. That is exactly what separates this
// from the failure [DiskFloorBytes] exists to prevent, where a write meets
// ENOSPC halfway and leaves a partial object that surfaces later as a corrupt
// cache entry and reads like a compiler bug.
func pruneCache(dir string, target uint64) (freed uint64, removed int, err error) {
	if dir == "" || target == 0 {
		return 0, 0, nil
	}

	entries := cacheEntries(dir)

	// Oldest first, which is the order the cache can most afford to lose.
	slices.SortFunc(entries, func(a, b cacheEntry) int { return a.used.Compare(b.used) })

	for _, e := range entries {
		if freed >= target {
			break
		}

		if e.isDir {
			err = os.RemoveAll(e.path)
		} else {
			err = os.Remove(e.path)
		}
		if err != nil {
			// A racing `go build` may have removed or replaced this entry
			// already, which is not a failure of the prune: the bytes are gone
			// either way and the next entry is still there to take.
			if !os.IsNotExist(err) {
				return freed, removed, fmt.Errorf("pruning %s: %w", e.path, err)
			}
			err = nil

			continue
		}

		freed += e.size
		removed++
	}

	return freed, removed, nil
}

// cacheEntry is one thing [pruneCache] may remove.
type cacheEntry struct {
	path  string
	size  uint64
	used  time.Time
	isDir bool
}

// cacheEntries enumerates the removable entries of a build cache.
//
// One enumeration serves both the prune and [cacheSize], deliberately: they
// were two walks with different rules, so the size promised and the size
// reclaimable disagreed by exactly the fuzz corpus (Codex, #1112). A number
// derived from the thing it describes cannot drift from it.
//
// The rules are cmd/go's, because the cache is its format and not ours: only
// `-a` and `-d` names are entries, they live only in the 256 hex
// subdirectories, and an entry may be a directory — an executable cache entry.
// Everything else the directory holds is left alone: trim.txt, the lock, and
// the fuzz corpus especially, whose values are inputs that once expanded
// coverage, cost machine-hours to rediscover, and which `go help cache` says
// plainly it is worse off without.
func cacheEntries(dir string) []cacheEntry {
	if dir == "" {
		return nil
	}

	var entries []cacheEntry

	for i := range 256 {
		subdir := filepath.Join(dir, fmt.Sprintf("%02x", i))

		// Read the whole directory before anything is removed, for the reason
		// cmd/go gives at the same point: removing files during a scan can
		// invalidate the offset the scan is walking.
		f, err := os.Open(subdir)
		if err != nil {
			continue
		}
		names, _ := f.Readdirnames(-1)
		_ = f.Close()

		for _, name := range names {
			if !strings.HasSuffix(name, "-a") && !strings.HasSuffix(name, "-d") {
				continue
			}

			path := filepath.Join(subdir, name)

			info, err := os.Stat(path)
			if err != nil {
				continue
			}

			size := uint64(info.Size())
			if info.IsDir() {
				size = treeSize(path)
			}

			entries = append(entries, cacheEntry{
				path:  path,
				size:  size,
				used:  info.ModTime(),
				isDir: info.IsDir(),
			})
		}
	}

	return entries
}

// treeSize totals a directory entry, which an executable cache entry is.
func treeSize(root string) uint64 {
	var total uint64

	_ = filepath.WalkDir(root, func(_ string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		if info, err := entry.Info(); err == nil {
			total += uint64(info.Size())
		}

		return nil
	})

	return total
}

// containerCores clamps the visible processor count to a cgroup CPU quota.
//
// [runtime.NumCPU] reports the CPUs this process may be scheduled on, which in
// a container is usually every CPU the host has: an affinity mask is not a
// quota. A container given two cores' worth of CPU time on a
// sixty-four-core host would otherwise be told to dispatch thirty-one lanes.
//
// Go's own default GOMAXPROCS does read the quota, which is worth knowing for
// a second reason: the lane environment sets GOMAXPROCS explicitly and thereby
// turns that automatic adjustment off. Setting it from an unclamped count
// would replace the runtime's correct answer with a wrong one, so this has to
// be right for the environment to be safe.
func containerCores(visible int) int {
	if quota, ok := tightestCPUQuota(cgroupChainV2()); ok {
		return clampCores(quota, visible)
	}
	if quota, ok := tightestCPUQuotaV1(cgroupChainV1("cpu")); ok {
		return clampCores(quota, visible)
	}

	return visible
}

// tightestCPUQuota reads cgroup v2's cpu.max at every level of the chain and
// returns the tightest, in whole cores.
//
// Every level, because a cgroup limit is inherited: the quota a process
// actually gets is the tightest over its own cgroup and every ancestor up to
// the mount root, and the kernel enforces all of them. Reading only the leaf
// is the common shape of this bug and it fails in the dangerous direction —
// a session's cgroup whose own cpu.max says `max`, nested under a parent
// carrying a two-core quota, falls all the way through to [runtime.NumCPU] and
// recommends a host-sized fleet on a container that can run two lanes. The
// symptom is not a refusal anyone investigates; it is thirty lanes sharing two
// cores, every one of them slow enough to look broken.
//
// A quota is rounded up to whole cores, and taking the minimum of the rounded
// figures is the same answer as rounding the minimum, because rounding up is
// monotonic.
func tightestCPUQuota(dirs []string) (int, bool) {
	tightest, found := 0, false
	for _, dir := range dirs {
		raw, err := os.ReadFile(filepath.Join(dir, "cpu.max"))
		if err != nil {
			continue
		}
		// "<quota> <period>", or "max <period>" when this level is unlimited —
		// which bounds nothing and must not end the walk, because a level above
		// it still can.
		fields := strings.Fields(string(raw))
		if len(fields) != 2 || fields[0] == "max" {
			continue
		}
		quota, qErr := strconv.ParseInt(fields[0], 10, 64)
		period, pErr := strconv.ParseInt(fields[1], 10, 64)
		if qErr != nil || pErr != nil || quota <= 0 || period <= 0 {
			continue
		}
		if cores := int((quota + period - 1) / period); !found || cores < tightest {
			tightest, found = cores, true
		}
	}

	return tightest, found
}

// tightestCPUQuotaV1 is [tightestCPUQuota] for the v1 hierarchy, where the
// quota and the period are separate files and a negative quota means
// unlimited. Limits are inherited there too, so it walks for the same reason.
func tightestCPUQuotaV1(dirs []string) (int, bool) {
	tightest, found := 0, false
	for _, dir := range dirs {
		quota, qErr := readInt(filepath.Join(dir, "cpu.cfs_quota_us"))
		period, pErr := readInt(filepath.Join(dir, "cpu.cfs_period_us"))
		if qErr != nil || pErr != nil || quota <= 0 || period <= 0 {
			continue
		}
		if cores := int((quota + period - 1) / period); !found || cores < tightest {
			tightest, found = cores, true
		}
	}

	return tightest, found
}

func clampCores(quota, visible int) int {
	if quota < 1 {
		quota = 1
	}

	return min(quota, visible)
}

// containerMemoryFree clamps host-wide availability to what a cgroup will
// actually let this container have. /proc/meminfo is not namespaced, so
// MemAvailable in a container is usually the host's — a number the container
// would be killed for trying to use.
//
// The bool is carried through rather than folded into the number: see
// [Machine.MemoryKnown] for why a cgroup that answers "nothing free" and a
// platform that answers nothing at all must not arrive here as the same value.
func containerMemoryFree(hostFree uint64, hostKnown bool) (uint64, bool) {
	for _, limits := range []struct {
		dirs         []string
		max, current string
	}{
		{cgroupChainV2(), "memory.max", "memory.current"},
		{cgroupChainV1("memory"), "memory.limit_in_bytes", "memory.usage_in_bytes"},
	} {
		free, ok := tightestMemoryFree(limits.dirs, limits.max, limits.current)
		if !ok {
			continue
		}
		if hostKnown {
			return min(free, hostFree), true
		}

		return free, true
	}

	return hostFree, hostKnown
}

// tightestMemoryFree is [tightestCPUQuota]'s argument applied to memory: the
// headroom a process has is the smallest of (limit - usage) over its own
// cgroup and every ancestor, because every one of those limits is enforced and
// exceeding any of them is an OOM kill. A leaf-only read of a nested cgroup
// whose own memory.max is `max` reports the host's MemAvailable and hands out
// lanes against memory a parent will never let this container touch.
func tightestMemoryFree(dirs []string, maxFile, currentFile string) (uint64, bool) {
	tightest, found := uint64(0), false
	for _, dir := range dirs {
		limit, err := readInt(filepath.Join(dir, maxFile))
		// A level with no limit — "max" in v2 fails to parse, and v1 spells
		// unlimited as a page-counter sentinel near the top of int64 — bounds
		// nothing, and must not stop the walk before a level that does.
		if err != nil || limit <= 0 || limit > unlimitedMemory {
			continue
		}
		used, err := readInt(filepath.Join(dir, currentFile))
		if err != nil || used < 0 {
			continue
		}

		// Page cache is not memory a lane has to wait for — except the part
		// something has asked the kernel not to reclaim.
		held := uint64(used) - min(uint64(used), evictableFile(dir))

		free := uint64(limit) - min(uint64(limit), held)
		if !found || free < tightest {
			tightest, found = free, true
		}
	}

	return tightest, found
}

// evictableFile is the part of a cgroup's recorded usage that is file-backed
// page cache the kernel will actually evict before it kills anything.
//
// Two questions, and the second one is easy to forget. How much file cache does
// this level hold, and how much of it is something the kernel has been told to
// keep — because a page protected by `memory.min` is *not* headroom, and
// counting it as headroom is how this tool would dispatch a lane into an OOM
// kill (Codex, #1134).
//
// The protection is not necessarily declared where the pages are counted. In
// cgroup v2 an ancestor's `memory.stat` is recursive, so it counts a protected
// descendant's cache — including a descendant in a sibling subtree this walk
// never visits on its way up. So the protection is summed over the level and
// everything beneath it, and the walk is bounded: a tree too large to read is
// a tree whose reclaimability was not established, and that subtracts nothing.
func evictableFile(dir string) uint64 {
	cache := reclaimableFile(dir)
	if cache == 0 {
		return 0
	}

	protected, established := protectedBelow(dir)
	if !established {
		// Fail closed. The old, conservative reading — every byte of usage
		// counted as held — is exactly what a level whose protection could not
		// be established deserves.
		return 0
	}

	return cache - min(cache, protected)
}

// maxProtectionScan bounds the descendant walk [protectedBelow] performs, and
// protectionReadChunk is how many directory entries it reads at a time.
//
// The shape of a cgroup tree is not this tool's to choose, and a budget tool
// that walks an unbounded directory tree on every dispatch decision has swapped
// one resource problem for another. Generous enough that a real hierarchy fits
// with room to spare, and exceeding it is a refusal rather than a truncation:
// a partial sum of protections is an *understatement*, which is the direction
// that hands out lanes that are not there.
//
// The chunk exists because counting visits does not bound reading them.
// [filepath.WalkDir] reads a whole directory and lexically sorts it before
// invoking the callback once, so a level with a million children allocates and
// sorts a million entries before any counter has been consulted — a bound on
// the wrong resource, which is the mistake this repository has a section about
// (Codex, #1134). Reading incrementally charges each entry as it arrives.
//
// One honest limit, stated rather than left for somebody to assume. The tests
// pin the *count*: a hierarchy past maxProtectionScan is refused, and it is
// refused because entries are charged as they are read. They do not observe the
// allocation, so raising this chunk until it swallows a whole directory in one
// call passes every test here while restoring exactly the behaviour the
// paragraph above rejects. What that would cost is memory rather than
// correctness, which is why it is a comment and not a check.
const (
	maxProtectionScan   = 4096
	protectionReadChunk = 64
)

// protectedBelow is the memory `memory.min` protects from reclaim at this level
// and every level under it, and whether the walk finished.
//
// A ceiling rather than an exact figure, and deliberately so: `memory.min` is a
// floor on a cgroup's *total* memory rather than on its file cache, so the
// protected cache is at most the floor and usually less. Overstating the
// protection understates the headroom, which is the direction a dispatch budget
// should be wrong in.
func protectedBelow(dir string) (uint64, bool) {
	// No `memory.min` here means no memory controller here, and a controller is
	// enabled top-down — so nothing below this level can protect anything and
	// there is nothing to walk. cgroup v1 is the whole-host case: the file does
	// not exist anywhere, and without this every limited level walked its entire
	// descendant hierarchy to read a file that was never going to be there.
	//
	// It is not only a cost. A v1 host with a hierarchy wider than the bound
	// would refuse to establish reclaimability, count none of its cache, and
	// recreate the zero-lane reading this whole change exists to fix — the fix
	// defeated by its own safeguard (Codex, #1134).
	if _, err := os.Stat(filepath.Join(dir, memoryMinFile)); err != nil {
		return 0, true
	}

	total, seen := uint64(0), 0

	// Explicit rather than recursive, so the pending set is the same bounded
	// thing the counter counts.
	pending := []string{dir}
	for len(pending) > 0 {
		level := pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		floor, established := protectionAt(level)
		if !established {
			return 0, false
		}
		if floor > 0 {
			// Saturating, because this sum runs over numbers a hierarchy
			// chooses and `memory.min` has no ceiling this tool gets to impose.
			// Four descendants declaring 1<<62 wrap a plain uint64 addition to
			// exactly zero, and zero here reads as "nothing is protected" — so
			// the one hierarchy asking for maximal protection would be the one
			// this hands out lanes against (Codex, #1134).
			//
			// Saturation is the fail-closed direction: an overstated protection
			// understates headroom, which costs a lane, while a wrapped one
			// overstates headroom, which costs an OOM kill. Nothing found later
			// can lower the sum, so the walk stops here.
			if total > math.MaxUint64-uint64(floor) {
				return math.MaxUint64, true
			}
			total += uint64(floor)
		}

		children, err := childDirectories(level, &seen)
		if err != nil {
			// A directory that vanished mid-walk, one this process may not
			// read, or a tree past the bound. None is evidence that nothing
			// below is protected.
			return 0, false
		}
		pending = append(pending, children...)
	}

	return total, true
}

// protectionAt is what one level declares with `memory.min`, and whether that
// could be read at all.
//
// Three outcomes rather than two, because "unreadable" and "unprotected" are
// opposite answers and a parse failure was quietly giving the second. cgroup v2
// spells complete protection as the literal `max`, which [readInt] refuses —
// so the one level asking the kernel to reclaim nothing read as the one level
// protecting nothing, and its cache was counted straight back as headroom
// (Codex, #1134).
//
// That is the same fail-open shape as the wrap before it, and this closes the
// class rather than the instance: `max` saturates, a number is a number, an
// absent file is a level with no memory controller and so nothing to protect,
// and *everything else* refuses. There is no longer a way for an unreadable
// value to arrive as a zero.
func protectionAt(dir string) (uint64, bool) {
	raw, err := os.ReadFile(filepath.Join(dir, memoryMinFile))
	if errors.Is(err, fs.ErrNotExist) {
		// No memory controller at this level, so nothing here is protected —
		// and nothing below it can be either, since a controller is enabled
		// top-down.
		return 0, true
	}
	if err != nil {
		return 0, false
	}

	value := strings.TrimSpace(string(raw))
	if value == "max" {
		return math.MaxUint64, true
	}

	floor, err := strconv.ParseInt(value, 10, 64)
	if err != nil || floor < 0 {
		return 0, false
	}

	return uint64(floor), true
}

// memoryMinFile is where a cgroup declares what it will not have reclaimed.
const memoryMinFile = "memory.min"

// childDirectories are a level's immediate subdirectories, read a chunk at a
// time and charged against the budget as they arrive.
//
// The charging is the point: a counter consulted after a whole directory has
// been read and sorted bounds the walk and not the work.
func childDirectories(dir string, seen *int) ([]string, error) {
	open, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer open.Close()

	var children []string
	for {
		entries, err := open.ReadDir(protectionReadChunk)
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			if *seen++; *seen > maxProtectionScan {
				return nil, errProtectionScanTooLarge
			}
			children = append(children, filepath.Join(dir, entry.Name()))
		}
		if errors.Is(err, io.EOF) {
			return children, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// errProtectionScanTooLarge stops [protectedBelow] at its bound.
var errProtectionScanTooLarge = errors.New("fleet: cgroup tree too large to establish reclaimability")

// reclaimableFile is the file-backed page cache a cgroup's usage includes,
// before asking whether any of it is protected.
//
// This is [memoryFree]'s own distinction — MemAvailable rather than MemFree —
// one level down, where it was missed. Both cgroup v1's memory.usage_in_bytes
// and v2's memory.current count page cache as used, so a container whose Go
// build cache is hot reports almost no headroom while holding almost no
// anonymous memory at all. Measured on the machine that found this: a leaf
// cgroup limited to 13.3 GiB reported 9.7 GiB used and held 6 MiB of RSS
// against 9.3 GiB of cache, so the fleet was told memory was the bound and
// dispatched nothing — on a box with a hot cache and nothing running.
//
// That is the worst direction for this tool to be wrong in. A build is what
// fills the cache, so the reading is most wrong exactly after the work that
// makes the next lane cheapest, and the advice it prints ("wait for the
// running lanes to finish") names a cause that is not there.
//
// Only *inactive* file pages count. Active ones are reclaimable too, but
// reclaiming them costs somebody's working set, and a budget for deciding
// whether to add load should err toward the tighter number. Anything
// unreadable counts as zero, which is the same direction.
func reclaimableFile(dir string) uint64 {
	raw, err := os.ReadFile(filepath.Join(dir, "memory.stat"))
	if err != nil {
		return 0
	}

	// v1 reports the hierarchy under `total_inactive_file` and this cgroup
	// alone under `inactive_file`; v2 has only the latter and is already
	// recursive. The totals are what pair with a hierarchical usage, so they
	// are preferred where they exist.
	byKey := map[string]uint64{}
	for line := range strings.SplitSeq(string(raw), "\n") {
		key, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64); err == nil {
			byKey[key] = n
		}
	}

	if total, ok := byKey["total_inactive_file"]; ok {
		return total
	}

	return byKey["inactive_file"]
}

// unlimitedMemory is the threshold above which a memory limit is a way of
// spelling "no limit" rather than a number. cgroup v1 writes PAGE_COUNTER_MAX
// (2^63-1 rounded down to a page) into memory.limit_in_bytes for an unlimited
// cgroup, and folding that into the minimum would be arithmetic on a sentinel.
const unlimitedMemory = 1 << 53

func readInt(path string) (int64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
}

// cgroupChainV2 lists the cgroup v2 directories whose controller files bound
// this process, leaf first: its own cgroup, then every ancestor up to the
// mount root.
//
// The chain rather than one directory, because a cgroup limit is the tightest
// of every level, not the innermost one that happens to name a number — see
// [tightestCPUQuota].
//
// Resolving the leaf at all is the other half. With a cgroup namespace — the
// common container case — the process sees its own cgroup as the root and the
// chain has one element. Without one, the process can sit in a nested
// hierarchy such as /docker/<id> while the root files describe the host or
// report no limit, which would put the tool back on affinity-visible host CPUs
// and over-dispatch. /proc/self/cgroup names the path to join.
func cgroupChainV2() []string {
	const root = "/sys/fs/cgroup"

	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return []string{root}
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		// v2 lines are "0::<path>"; anything else is a v1 controller line.
		if rest, ok := strings.CutPrefix(line, "0::"); ok {
			return cgroupChain(root, strings.TrimSpace(rest))
		}
	}

	return []string{root}
}

// cgroupChainV1 is the same resolution for a v1 controller, whose mount point
// carries the controller's own name and whose per-process path is the third
// field of its /proc/self/cgroup line.
func cgroupChainV1(controller string) []string {
	root := filepath.Join("/sys/fs/cgroup", controller)

	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return []string{root}
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		fields := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(fields) != 3 || !slices.Contains(strings.Split(fields[1], ","), controller) {
			continue
		}

		return cgroupChain(root, fields[2])
	}

	return []string{root}
}

// cgroupChain expands a cgroup-relative path into the directories to read,
// leaf first and root last.
//
// rel is cleaned as an absolute path before it is joined, so a hostile or
// merely odd /proc/self/cgroup carrying `..` cannot walk the chain out of the
// mount and start reading files that are not cgroup controllers at all. The
// walk stops at root for the same reason it starts at the leaf: above the
// mount there are no limits to inherit.
func cgroupChain(root, rel string) []string {
	root = filepath.Clean(root)

	var dirs []string
	for dir := filepath.Join(root, filepath.Clean("/"+strings.TrimSpace(rel))); ; dir = filepath.Dir(dir) {
		dirs = append(dirs, dir)
		if dir == root || !strings.HasPrefix(dir, root+string(filepath.Separator)) {
			break
		}
	}

	return dirs
}
