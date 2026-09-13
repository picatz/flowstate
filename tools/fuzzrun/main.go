// Command fuzzrun fuzzes a list of targets, several at a time.
//
// It is the smoke tier's loop. deep.yml still runs its own, deliberately: ten
// minutes a target is a different budget with different crasher handling, and
// nobody waits on it.
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
// less — but it is not free either, and how much it costs is measured rather
// than assumed here. `-fuzztime` bounds wall clock rather than executions, so a
// target sharing the machine stops after thirty seconds having done whatever it
// managed in a contended thirty seconds. [defaultWorkers] carries the numbers
// and is why the default is half the CPUs rather than all of them.
//
// Whether the targets fuzzed as hard is not settled, and the first version of
// this comment said it was. Total executions went 456,000 to 467,519, which
// reads as flat until the per-target numbers are opened: one target supplied
// +107,183 of that, leaving the other twelve at −95,664, or −21%. One sample
// per arm cannot separate "same fuzzing" from "one outlier hiding a broad
// loss", and execution counts cannot settle it at any sample size — they
// depend on the corpus a run happened to grow, so they move by large factors
// between runs of one configuration, and a sum inherits whichever target swung
// hardest. What concurrency owes a target is its share of the machine; CPU
// seconds per target is what measures that, and [defaultWorkers] carries the
// measurement and the default it decided.
//
// # Output
//
// Concurrency costs the serial loop's one readable property — that the target
// named last is the target that hung. With targets in flight together that is
// simply false: a later one can start and finish while an earlier one is still
// blocked. So this command answers the same question a different way rather
// than leaving the old reading in place to mislead. A target announces itself
// when it starts, its output is printed in one piece when it finishes, and the
// summary lists failures in input order — so a hung target is one named as
// started that never reported, which a run killed by an outer timeout still
// shows. .github/workflows/ci.yml says the same thing where it tells an
// operator how to read the job's log.
//
// The capture is bounded: see [boundedOutput].
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
	"slices"
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
	return runWith(args, stdin, stdout, nil)
}

// runWith is run with the per-target runner named, so a test can drive a mix
// of passing and failing targets without spending a fuzz budget on either.
// fuzz is nil everywhere but tests, and nil means [fuzzOne].
func runWith(args []string, stdin io.Reader, stdout io.Writer, fuzz func(context.Context, target, options) ([]byte, error)) error {
	flags := flag.NewFlagSet("fuzzrun", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var (
		fuzztime = flags.Duration("fuzztime", 30*time.Second, "how long to fuzz each target")
		timeout  = flags.Duration("timeout", 120*time.Second, "`go test -timeout` for each target")
		memlimit = flags.String("memlimit", "512MiB", "GOMEMLIMIT for each target's process")
		jobs     = flags.Int("jobs", 0, "targets to fuzz at once (0: one per two CPUs, never more than there are targets)")
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

	workers := min(cmp.Or(*jobs, defaultWorkers()), len(targets))

	fmt.Fprintf(stdout, "fuzzing %d target(s), %d at a time, %s each\n", len(targets), workers, *fuzztime)

	var failed []string
	for _, r := range fuzzAll(context.Background(), stdout, targets, workers, options{
		fuzztime: *fuzztime,
		timeout:  *timeout,
		memlimit: *memlimit,
		run:      fuzz,
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

// defaultWorkers is half the CPUs this process may actually spend, not half the
// CPUs the host has, and the halving is the measured part rather than caution.
//
// A fuzzing target is two processes, not one: `go test -fuzz` runs a
// coordinator that mutates and dispatches inputs and a worker that executes
// them, and `-parallel 1` bounds the workers. So N targets at once is 2N
// processes, and on a four-core runner four targets is eight — which the cores
// do not absorb, whatever the process count suggests about how idle they look.
//
// Measured on four cores over the smoke tier, with the fuzz corpus cache
// cleared before each arm and the test binaries compiled first so build
// contention was not counted as fuzzing. Per-target CPU seconds against the
// same target run alone, which is what asks whether a target still gets its
// share of the machine — unlike execution counts, which depend on the corpus a
// run happened to grow and move by large factors between runs of one
// configuration:
//
//	workers   wall   per-target CPU (median, range)
//	1         423s   1.00
//	NumCPU/2  224s   0.92 (0.88-0.95)
//	NumCPU    151s   0.66 (0.49-0.89)
//
// At one worker per CPU a target keeps about two thirds of the CPU it would
// have had to itself, and the worst keeps half: that is a third of the tier's
// fuzzing traded for the last 73 seconds, which is not a trade a smoke tier
// should make silently. Half the CPUs costs a median 8% and a worst 12%, and
// that shortfall is systematic rather than noise — every one of the thirteen
// targets lost some, and no arm was repeated, so there is no measured noise
// floor to call it small against. It buys nearly half the wall clock, which is
// the trade this default makes and states.
//
// GOMAXPROCS(0) rather than NumCPU() because an affinity mask is not a quota,
// which tools/fleet already says in this repository's own words: a container
// given two cores' worth of CPU time on a sixty-four-core host reads NumCPU as
// sixty-four. Go's default GOMAXPROCS reads the cgroup limit, and a fleet lane
// exports an explicit GOMAXPROCS besides, so this one call honours both. Under
// NumCPU a two-core lane on a large host would have dispatched sixteen targets
// — thirty-two processes — and every one of them would still have "completed"
// its thirty seconds having fuzzed almost nothing, which is the failure this
// whole function exists to prevent. On a GitHub runner the two agree at 4.
//
// -jobs overrides it, including upwards, for a machine that is not a runner.
func defaultWorkers() int {
	return max(runtime.GOMAXPROCS(0)/2, 1)
}

// options are the per-target bounds, identical for every target in a tier.
type options struct {
	fuzztime time.Duration
	timeout  time.Duration
	memlimit string

	// run fuzzes one target. Always nil outside tests, where [fuzzOne] is
	// what runs. It exists because every case that reaches a real `go test`
	// has to name a package, and a package that exists would spend a fuzz
	// budget while one that does not fails identically for every target —
	// so without a seam here, "a target that passed did not clear an
	// earlier one's failure" is a claim no test in this package can make.
	run func(context.Context, target, options) ([]byte, error)
}

// fuzz runs one target through whatever this options value says runs targets.
func (o options) fuzz(ctx context.Context, t target) ([]byte, error) {
	if o.run != nil {
		return o.run(ctx, t, o)
	}
	return fuzzOne(ctx, t, o)
}

// fuzzAll fuzzes every target, at most workers at a time, and returns the
// results in the order the targets were read. Writes to out are serialized so
// one target's buffered output never lands inside another's.
func fuzzAll(ctx context.Context, out io.Writer, targets []target, workers int, opts options) []result {
	// max rather than trusting the caller: run guards this, but a helper that
	// deadlocks on a zero-capacity channel is a bad thing to leave lying about.
	var (
		mu      sync.Mutex
		results = make([]result, len(targets))
		sem     = make(chan struct{}, max(workers, 1))
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

			output, err := opts.fuzz(ctx, t)
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

// fuzzCommand builds the command for one target. It is a function of its own,
// rather than three lines inside [fuzzOne], because these arguments are the
// tier's safety bounds and a test has to be able to assert them: one fuzzing
// worker, a time budget, and a memory limit, because -fuzztime bounds time and
// GOMEMLIMIT is what bounds memory. -run=XXX keeps the package's ordinary tests
// out of the budget by matching none of them. Dropping any of them would
// otherwise leave every test in this package green.
func fuzzCommand(ctx context.Context, t target, opts options) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "go", "test",
		"-timeout", opts.timeout.String(),
		"-parallel", "1",
		"-run=XXX",
		"-fuzz", t.name,
		"-fuzztime", opts.fuzztime.String(),
		"./"+t.dir+"/",
	)
	cmd.Env = append(os.Environ(), "GOMEMLIMIT="+opts.memlimit)
	return cmd
}

// The per-target output ceiling. What a target prints is not this command's to
// trust: it is decided by the inputs the fuzzer manufactured, and a failing
// target prints the one it found. Reading all of it into this process would put
// an unbounded buffer *outside* the GOMEMLIMIT that bounds each child — the one
// memory bound this design claims — so the capture is bounded here and the
// ceiling is stated: at most headBytes+tailBytes of content per target in
// flight, and headBytes+2*tailBytes of memory, because the append that
// overshoots leaves the tail's capacity at twice tailBytes after the trim
// shrinks its length.
//
// Head and tail rather than either alone, because a fuzz run's two useful ends
// are both ends: the head names the seed corpus and the worker count, and the
// tail carries the failure and the input that caused it.
const (
	headBytes = 8 << 10
	tailBytes = 56 << 10
)

// boundedOutput captures a child's output with an explicit ceiling, keeping the
// first headBytes and the last tailBytes and counting what it dropped between
// them. One goroutine — the one running that target — writes to it, and it is
// read after that target's process has exited.
type boundedOutput struct {
	head  []byte
	tail  []byte
	total int
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	written := len(p)
	b.total += written

	if room := headBytes - len(b.head); room > 0 {
		take := min(room, len(p))
		b.head = append(b.head, p[:take]...)
		p = p[take:]
	}
	if len(p) == 0 {
		return written, nil
	}

	// Trim the incoming write before appending it, so that one enormous write
	// cannot make this buffer enormous on its way to being trimmed.
	if len(p) > tailBytes {
		p = p[len(p)-tailBytes:]
	}
	b.tail = append(b.tail, p...)
	if extra := len(b.tail) - tailBytes; extra > 0 {
		// copy rather than append: source and destination overlap.
		b.tail = b.tail[:copy(b.tail, b.tail[extra:])]
	}
	return written, nil
}

// Bytes renders what was kept, saying so when anything was dropped rather than
// presenting a truncated log as if it were a whole one.
func (b *boundedOutput) Bytes() []byte {
	elided := b.total - len(b.head) - len(b.tail)
	if elided <= 0 {
		return slices.Concat(b.head, b.tail)
	}
	marker := fmt.Sprintf("\n... %d byte(s) elided: this target printed %d, and fuzzrun keeps the first %d and the last %d ...\n",
		elided, b.total, len(b.head), len(b.tail))
	return slices.Concat(b.head, []byte(marker), b.tail)
}

// fuzzOne runs a single target and returns the bounded capture of its output.
func fuzzOne(ctx context.Context, t target, opts options) ([]byte, error) {
	cmd := fuzzCommand(ctx, t, opts)
	// One writer for both streams: os/exec gives the child a single pipe when
	// Stdout and Stderr are the same value, which is what CombinedOutput does
	// and is why the two cannot interleave a partial line here.
	out := &boundedOutput{}
	cmd.Stdout = out
	cmd.Stderr = out
	err := cmd.Run()
	return out.Bytes(), err
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
