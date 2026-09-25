// Command wallclock reports the waits in this repository's tests that spend
// real time: `time.Sleep`, and testify's Eventually family.
//
//	go run ./tools/wallclock           # a count per file
//	go run ./tools/wallclock -sites    # every site
//	go run ./tools/wallclock ./pkg/... # one subtree
//
// A `time.Sleep` in a test is a guess written as a number: the author decided
// how long a listener takes to close or a goroutine takes to reach a call, and
// the test is as slow as that guess on every run and flaky whenever a runner is
// slower than it. A `require.Eventually` is the same guess written as a
// ceiling: cheaper on an idle machine, because the first tick usually answers,
// and red rather than slow on a loaded one. It costs something else too — a
// polled predicate can only ever report "condition never satisfied", so the
// site says nothing about which half of what it checked was wrong.
//
// `testing/synctest` removes all of that at once: inside a bubble the same
// sleep returns the instant every goroutine is blocked, and a wait for a
// condition becomes [synctest.Wait] followed by the assertions themselves.
// Only a test with a goroutine outside the bubble — a subprocess, a network
// peer, a dev server — has to keep a real wait, and one of those should say
// what it is waiting for.
//
// This command changes nothing. Its two counts are held by
// [TestTheRepositoryWallClockSleepsOnlyGoDown] and
// [TestTheRepositoryPollsOnlyGoDown], ratchets: a wait added to a test fails
// one of them until it moves into a bubble or is recorded there with its
// reason, and a wait removed fails it until the table shrinks to match, so the
// tables cannot keep stale entries (#1706).
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

func main() {
	sites := flag.Bool("sites", false, "list every site rather than a count per file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: go run ./tools/wallclock [-sites] [path]\n\n")
		fmt.Fprintf(os.Stderr, "Reports the waits in tests that spend real time. Changes nothing.\n\n")
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

	waits, files, err := Analyze(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "wallclock: %v\n", err)
		os.Exit(2)
	}

	writeReport(os.Stdout, waits, files, root, *sites)
}

// writeReport prints each kind of wait, per file or per site.
func writeReport(out io.Writer, waits []Wait, files int, root string, sites bool) {
	sleeps, polls := OfKind(waits, KindSleep), OfKind(waits, KindPoll)

	fmt.Fprintf(out, "wallclock: %d sleep(s) and %d poll(s) spending real time across %d test file(s) under %s\n",
		len(sleeps), len(polls), files, root)

	writeSection(out, "sleeps", sleeps, root, sites)
	writeSection(out, "polls", polls, root, sites)

	fmt.Fprintln(out)
	fmt.Fprintf(out, "A wait inside synctest.Test is not counted: a sleep there returns the instant the\n")
	fmt.Fprintf(out, "bubble is idle, and a condition is waited for with synctest.Wait rather than asked\n")
	fmt.Fprintf(out, "about repeatedly. One that waits on a process outside the bubble should say so\n")
	fmt.Fprintf(out, "beside it.\n")
}

// writeSection prints one kind's waits, per file or per site.
func writeSection(out io.Writer, title string, waits []Wait, root string, sites bool) {
	fmt.Fprintf(out, "\n  %s\n\n", title)

	if len(waits) == 0 {
		fmt.Fprintf(out, "    none\n")
		return
	}

	if sites {
		for _, wait := range waits {
			fmt.Fprintf(out, "    %s:%d\n", relative(root, wait.File), wait.Line)
		}
		return
	}

	for _, file := range CountByFile(waits) {
		fmt.Fprintf(out, "    %4d  %s\n", file.Count, relative(root, file.File))
	}
}

// OfKind returns the waits of one kind, keeping their order.
func OfKind(waits []Wait, kind Kind) []Wait {
	out := make([]Wait, 0, len(waits))
	for _, wait := range waits {
		if wait.Kind == kind {
			out = append(out, wait)
		}
	}

	return out
}

// FileCount is the number of real-time waits in one file.
type FileCount struct {
	File  string
	Count int
}

// CountByFile folds the sites into a count per file, sorted by path.
func CountByFile(waits []Wait) []FileCount {
	counts := map[string]int{}
	for _, wait := range waits {
		counts[wait.File]++
	}

	out := make([]FileCount, 0, len(counts))
	for file, count := range counts {
		out = append(out, FileCount{File: file, Count: count})
	}
	slices.SortFunc(out, func(a, b FileCount) int { return strings.Compare(a.File, b.File) })

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
