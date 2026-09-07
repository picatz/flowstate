// Command wallclock reports the sleeps in this repository's tests that spend
// real time.
//
//	go run ./tools/wallclock           # a count per file
//	go run ./tools/wallclock -sites    # every site
//	go run ./tools/wallclock ./pkg/... # one subtree
//
// A `time.Sleep` in a test is a guess written as a number: the author decided
// how long a listener takes to close or a goroutine takes to reach a call, and
// the test is as slow as that guess on every run and flaky whenever a runner is
// slower than it. `testing/synctest` removes both at once: inside a bubble the
// same sleep returns the instant every goroutine is blocked, and a wait for a
// condition becomes [synctest.Wait] rather than a poll. Only a test with a
// goroutine outside the bubble — a subprocess, a network peer, a dev server —
// has to keep a real sleep, and one of those should say what it is waiting for.
//
// This command changes nothing. Its count is held by
// [TestTheRepositoryWallClockSleepsOnlyGoDown], a ratchet: a sleep added to a
// test fails it until the sleep moves into a bubble or is recorded there with
// its reason, and a sleep removed fails it until the table shrinks to match,
// so the table cannot keep stale entries (#1706).
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	sites := flag.Bool("sites", false, "list every site rather than a count per file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: go run ./tools/wallclock [-sites] [path]\n\n")
		fmt.Fprintf(os.Stderr, "Reports the sleeps in tests that spend real time. Changes nothing.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	root := "."
	if flag.NArg() > 0 {
		root = strings.TrimSuffix(strings.TrimSuffix(flag.Arg(0), "..."), "/")
		if root == "" {
			root = "."
		}
	}

	sleeps, files, err := Analyze(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wallclock: %v\n", err)
		os.Exit(2)
	}

	writeReport(os.Stdout, sleeps, files, root, *sites)
}

// writeReport prints the sleeps, per file or per site.
func writeReport(out io.Writer, sleeps []Sleep, files int, root string, sites bool) {
	fmt.Fprintf(out, "wallclock: %d sleep(s) spending real time across %d test file(s) under %s\n\n", len(sleeps), files, root)

	if sites {
		for _, sleep := range sleeps {
			fmt.Fprintf(out, "    %s:%d\n", relative(root, sleep.File), sleep.Line)
		}
	} else {
		for _, file := range CountByFile(sleeps) {
			fmt.Fprintf(out, "    %4d  %s\n", file.Count, relative(root, file.File))
		}
	}

	fmt.Fprintln(out)
	fmt.Fprintf(out, "A sleep inside synctest.Test is not counted: it returns the instant the bubble is\n")
	fmt.Fprintf(out, "idle. One that waits on a process outside the bubble should say so beside it.\n")
}

// FileCount is the number of real-time sleeps in one file.
type FileCount struct {
	File  string
	Count int
}

// CountByFile folds the sites into a count per file, sorted by path.
func CountByFile(sleeps []Sleep) []FileCount {
	counts := map[string]int{}
	for _, sleep := range sleeps {
		counts[sleep.File]++
	}

	out := make([]FileCount, 0, len(counts))
	for file, count := range counts {
		out = append(out, FileCount{File: file, Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].File < out[j].File })

	return out
}

// relative shortens a path against the root it was walked from.
func relative(root, path string) string {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return path
	}
	if shorter, err := filepath.Rel(absolute, path); err == nil && !strings.HasPrefix(shorter, "..") {
		// Slashes whatever the host writes, since the ratchet's keys are
		// spelled with them.
		return filepath.ToSlash(shorter)
	}

	return path
}
