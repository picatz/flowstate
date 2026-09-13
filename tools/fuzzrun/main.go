// Command fuzzrun fuzzes a list of targets, several at a time.
//
// It reads `<target> <package directory>` lines on standard input — the shape
// tools/fuzztargets/list.sh prints — and runs one `go test -fuzz` per line.
// Selection stays list.sh's job: this command never reads targets.txt and never
// decides which tier a target belongs to, so the one written source of the
// target list keeps its single reader and the test that pins list.sh against
// tools/fuzztargets still pins everything that chooses what to fuzz. This
// command only decides how many chosen targets run at once.
//
// # Why the tier was slower than the fuzzing it did
//
// A tier gives each target `-parallel 1`: one fuzzing worker, which is what
// keeps a crash and the memory behind it attributable to a single input. That
// bound is about one target. Running the *targets* one after another is a
// different bound, and the loop this replaces conflated the two — so on a
// four-core runner one worker fuzzed, three cores idled, and thirteen targets
// cost thirteen consecutive thirty-second budgets.
//
// Spending those idle cores on other targets is not the same as giving a target
// less. `-fuzztime` bounds wall clock rather than executions, so the question
// this design turns on is whether a target sharing the machine still gets
// through as many inputs. Measured against this tree on four cores, with the
// fuzz corpus cache cleared before each run so corpus growth could not explain
// the difference: the smoke tier took 448s serially and 139s four-at-a-time,
// and total executions were 456,000 and 467,519 — the same fuzzing in a third
// of the wall clock.
//
// Per-target execution counts are not the metric and should not be read as one.
// They swing by orders of magnitude between two identical runs, in both
// directions, because what a fuzzer reaches depends on what its corpus happened
// to grow: one target here went from 18 executions to 107,201 between the two
// runs above. Only the total is stable enough to compare, which is why the
// claim this command makes is about the total.
//
// # Output
//
// Concurrency costs the serial loop's one readable property — that the target
// named last is the target that hung — so this command keeps that property
// rather than the interleaved stream that would replace it. A target announces
// itself when it starts, its output is buffered and printed in one piece when
// it finishes, and the summary lists failures in input order. A run killed by
// an outer timeout therefore ends with the targets that started and never
// reported still named on standard output, which is the question an operator
// reading a cancelled job has.
package main

import (
	"bufio"
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

// A target is one line of input: the fuzz function and the package holding it.
type target struct {
	name string
	dir  string
}

// result is one target's outcome, held so the summary can report in input order.
type result struct {
	target target
	err    error
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "fuzzrun: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	flags := flag.NewFlagSet("fuzzrun", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var (
		fuzztime = flags.Duration("fuzztime", 30*time.Second, "how long to fuzz each target")
		timeout  = flags.Duration("timeout", 120*time.Second, "`go test -timeout` for each target")
		memlimit = flags.String("memlimit", "512MiB", "GOMEMLIMIT for each target's process")
		jobs     = flags.Int("jobs", 0, "targets to fuzz at once (0: one per CPU, never more than there are targets)")
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] < <target> <dir> lines\n\n", flags.Name())
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("targets are read from standard input, not from arguments: %s", strings.Join(flags.Args(), " "))
	}
	if *jobs < 0 {
		return fmt.Errorf("-jobs must not be negative, got %d", *jobs)
	}

	targets, err := parse(stdin)
	if err != nil {
		return err
	}
	// Fail closed on an empty list for list.sh's reason: a tier that fuzzed
	// nothing at all must stop the run rather than report the success of a
	// check that never ran.
	if len(targets) == 0 {
		return errors.New("no fuzz targets on standard input")
	}

	// Each target's process is capped at -memlimit, so the run's ceiling is
	// that times the worker count: one per CPU keeps a runner's ceiling in
	// proportion to the machine rather than to the length of the target list.
	workers := min(cmp.Or(*jobs, runtime.NumCPU()), len(targets))

	fmt.Fprintf(stdout, "fuzzing %d target(s), %d at a time, %s each\n", len(targets), workers, *fuzztime)

	var failed []string
	for _, r := range fuzzAll(context.Background(), stdout, targets, workers, options{
		fuzztime: *fuzztime,
		timeout:  *timeout,
		memlimit: *memlimit,
	}) {
		if r.err != nil {
			failed = append(failed, r.target.name)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d of %d target(s) failed: %s", len(failed), len(targets), strings.Join(failed, " "))
	}
	fmt.Fprintf(stdout, "all %d target(s) passed\n", len(targets))
	return nil
}

// options are the per-target bounds, identical for every target in a tier.
type options struct {
	fuzztime time.Duration
	timeout  time.Duration
	memlimit string
}

// fuzzAll fuzzes every target, at most workers at a time, and returns the
// results in the order the targets were read. Writes to out are serialized so
// one target's buffered output never lands inside another's.
func fuzzAll(ctx context.Context, out io.Writer, targets []target, workers int, opts options) []result {
	var (
		mu      sync.Mutex
		results = make([]result, len(targets))
		sem     = make(chan struct{}, workers)
		wg      sync.WaitGroup
	)
	for i, t := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			mu.Lock()
			fmt.Fprintf(out, "==> %s (%s) started\n", t.name, t.dir)
			mu.Unlock()

			output, err := fuzzOne(ctx, t, opts)
			results[i] = result{target: t, err: err}

			verdict := "ok"
			if err != nil {
				verdict = "FAILED: " + err.Error()
			}
			mu.Lock()
			defer mu.Unlock()
			fmt.Fprintf(out, "==> %s (%s) %s\n", t.name, t.dir, verdict)
			out.Write(output)
			if n := len(output); n > 0 && output[n-1] != '\n' {
				fmt.Fprintln(out)
			}
		}()
	}
	wg.Wait()
	return results
}

// fuzzOne runs a single target and returns its combined output. The bounds are
// the tier's: one fuzzing worker, a time budget, and a memory limit, because
// -fuzztime bounds time and GOMEMLIMIT is what bounds memory. -run=XXX keeps
// the package's ordinary tests out of the budget by matching none of them.
func fuzzOne(ctx context.Context, t target, opts options) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "go", "test",
		"-timeout", opts.timeout.String(),
		"-parallel", "1",
		"-run=XXX",
		"-fuzz", t.name,
		"-fuzztime", opts.fuzztime.String(),
		"./"+t.dir+"/",
	)
	cmd.Env = append(os.Environ(), "GOMEMLIMIT="+opts.memlimit)
	return cmd.CombinedOutput()
}

// parse reads `<target> <dir>` lines, which is what list.sh prints. Anything
// else is an error naming the line rather than a target quietly skipped.
func parse(r io.Reader) ([]target, error) {
	var out []target
	scanner := bufio.NewScanner(r)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		fields := strings.Fields(text)
		if len(fields) != 2 {
			return nil, fmt.Errorf("stdin:%d: want `<target> <dir>`, got %d field(s): %q", line, len(fields), text)
		}
		out = append(out, target{name: fields[0], dir: strings.TrimSuffix(fields[1], "/")})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading targets: %w", err)
	}
	return out, nil
}
