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
	fmt.Printf(", %s memory free, %s disk free", bytes(machine.MemoryFree), bytes(machine.DiskFree))
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
	machine := Machine{
		Cores:      cores,
		Load1:      -1,
		MemoryFree: containerMemoryFree(memoryFree()),
		DiskFree:   laneDiskFree(),

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
func memoryFree() uint64 {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		field, value, ok := strings.Cut(line, ":")
		if !ok || field != "MemAvailable" {
			continue
		}
		if kb, err := strconv.ParseUint(strings.Fields(value)[0], 10, 64); err == nil {
			return kb << 10
		}
	}

	return 0
}

// laneDiskFree is the tightest of the filesystems a lane actually writes to.
//
// The checkout and GOCACHE can be on different mounts, and it is the cache's
// growth this budgets — a roomy workspace beside a cache filesystem at ENOSPC
// is exactly the partial-object failure the floor exists to prevent, and
// measuring only the workspace would report room that the writes do not have.
func laneDiskFree() uint64 {
	free := diskFree(".")
	if out, err := exec.Command("go", "env", "GOCACHE").Output(); err == nil {
		if cache := strings.TrimSpace(string(out)); cache != "" {
			if cacheFree := diskFree(cache); cacheFree > 0 && (free == 0 || cacheFree < free) {
				return cacheFree
			}
		}
	}

	return free
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
	out, err := exec.Command("go", "env", "GOCACHE").Output()
	if err != nil {
		return 0
	}
	dir := strings.TrimSpace(string(out))
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
	// cgroup v2: "<quota> <period>", or "max <period>" when unlimited.
	if raw, err := os.ReadFile(cgroupPath("cpu.max")); err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) == 2 && fields[0] != "max" {
			quota, qErr := strconv.ParseInt(fields[0], 10, 64)
			period, pErr := strconv.ParseInt(fields[1], 10, 64)
			if qErr == nil && pErr == nil && period > 0 && quota > 0 {
				return clampCores(int((quota+period-1)/period), visible)
			}
		}
	}

	// cgroup v1: a negative quota means unlimited.
	quota, qErr := readInt(cgroupPathV1("cpu", "cpu.cfs_quota_us"))
	period, pErr := readInt(cgroupPathV1("cpu", "cpu.cfs_period_us"))
	if qErr == nil && pErr == nil && quota > 0 && period > 0 {
		return clampCores(int((quota+period-1)/period), visible)
	}

	return visible
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
func containerMemoryFree(hostFree uint64) uint64 {
	for _, limits := range []struct{ max, current string }{
		{cgroupPath("memory.max"), cgroupPath("memory.current")},
		{cgroupPathV1("memory", "memory.limit_in_bytes"), cgroupPathV1("memory", "memory.usage_in_bytes")},
	} {
		limit, err := readInt(limits.max)
		if err != nil || limit <= 0 {
			continue
		}
		used, err := readInt(limits.current)
		if err != nil || used < 0 {
			continue
		}
		if free := uint64(limit) - min(uint64(limit), uint64(used)); free < hostFree || hostFree == 0 {
			return free
		}

		return hostFree
	}

	return hostFree
}

func readInt(path string) (int64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
}

// cgroupPath resolves a cgroup v2 controller file for *this process's* cgroup
// rather than for the root.
//
// With a cgroup namespace — the common container case — the process sees its
// own cgroup as the root and the two are the same file. Without one, the
// process can sit in a nested hierarchy such as /docker/<id> while the root
// files describe the host or report no limit at all, which would put the tool
// back on affinity-visible host CPUs and over-dispatch. /proc/self/cgroup names
// the path to join; when it names nothing, the root is the right answer.
func cgroupPath(file string) string {
	const root = "/sys/fs/cgroup"

	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return filepath.Join(root, file)
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		// v2 lines are "0::<path>"; anything else is a v1 controller line.
		if rest, ok := strings.CutPrefix(line, "0::"); ok {
			if nested := filepath.Join(root, strings.TrimSpace(rest), file); nested != "" {
				if _, err := os.Stat(nested); err == nil {
					return nested
				}
			}

			break
		}
	}

	return filepath.Join(root, file)
}

// cgroupPathV1 is the same resolution for a v1 controller, whose mount point
// carries the controller's own name and whose per-process path is the third
// field of its /proc/self/cgroup line.
func cgroupPathV1(controller, file string) string {
	root := filepath.Join("/sys/fs/cgroup", controller)

	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return filepath.Join(root, file)
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		fields := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(fields) != 3 || !slices.Contains(strings.Split(fields[1], ","), controller) {
			continue
		}
		if nested := filepath.Join(root, fields[2], file); nested != "" {
			if _, err := os.Stat(nested); err == nil {
				return nested
			}
		}
	}

	return filepath.Join(root, file)
}
