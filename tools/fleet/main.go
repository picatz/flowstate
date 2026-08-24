package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

func main() {
	number := flag.Bool("n", false, "print only the lane count, for a script to read")
	env := flag.Bool("env", false, "print only the environment a lane must be given")
	flag.Parse()

	if *env {
		// One per line: joined by spaces, `eval` reads the second `export` as
		// a variable name rather than as a keyword.
		fmt.Println(strings.Join(LaneEnv(), "\n"))
		return
	}

	machine := readMachine()
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
	machine.CacheSizeBytes = cacheSize()

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
	return tightestFree(laneWriteTargets(goEnv), diskFree)
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

// tightestFree is the minimum over the readable answers, and it is the readable
// part that needs saying: a path whose filesystem cannot be stat'ed reports
// zero, and folding that in would make every unreadable target a full disk and
// refuse the fleet outright. An unmeasurable mount is a missing fact, not a
// full one — the same distinction [Machine.MemoryKnown] carries for memory.
func tightestFree(paths []string, free func(string) uint64) uint64 {
	var tightest uint64
	for _, path := range paths {
		if got := free(path); got > 0 && (tightest == 0 || got < tightest) {
			tightest = got
		}
	}

	return tightest
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

func diskFree(path string) uint64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0
	}

	return stat.Bavail * uint64(stat.Bsize)
}

// cacheSize is best-effort: it is advice, not a bound, so a slow or missing
// answer costs a line of output rather than a wrong plan.
func cacheSize() uint64 {
	dir := goEnv("GOCACHE")
	if dir == "" {
		return 0
	}

	var total uint64
	_ = filepath.WalkDir(dir, func(_ string, entry os.DirEntry, err error) error {
		// An unreadable subtree makes the number smaller, which is the right
		// direction for a figure that only ever prints advice.
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
		free := uint64(limit) - min(uint64(limit), uint64(used))
		if !found || free < tightest {
			tightest, found = free, true
		}
	}

	return tightest, found
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
