package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

func main() {
	number := flag.Bool("n", false, "print only the lane count, for a script to read")
	env := flag.Bool("env", false, "print only the environment a lane must be given")
	flag.Parse()

	if *env {
		fmt.Println(strings.Join(LaneEnv(), " "))
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
		fmt.Printf("fleet: give each lane: %s\n", strings.Join(LaneEnv(), " "))
	}
	for _, advice := range plan.Advice {
		fmt.Printf("fleet: %s\n", advice)
	}
}

// readMachine reads what the platform will tell us, and says -1 rather than
// guessing where it will not. A missing load average is not an idle machine.
func readMachine() Machine {
	machine := Machine{
		Cores:      runtime.NumCPU(),
		Load1:      -1,
		MemoryFree: memoryFree(),
		DiskFree:   diskFree("."),
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
