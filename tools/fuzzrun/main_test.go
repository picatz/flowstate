package main

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

// absentPackage is a directory no package lives in, so `go test` fails fast
// and predictably. The tests below assert on how a failure is reported, which
// needs a reliable failure and not a real fuzz budget.
const absentPackage = "tools/fuzzrun/testdata-no-such-package"

func TestParseReadsTargetAndDirectory(t *testing.T) {
	got, err := parse(strings.NewReader("FuzzA pkg/one\n\n  FuzzB pkg/two/  \n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []target{{name: "FuzzA", dir: "pkg/one"}, {name: "FuzzB", dir: "pkg/two"}}
	if !slices.Equal(got, want) {
		t.Errorf("parse returned %v, want %v", got, want)
	}
}

// A line that is not a target is an error naming the line. The failure this
// guards is the one the whole fuzz-target design is about: a malformed list
// quietly fuzzing fewer targets than it claims.
func TestParseRefusesAMalformedLine(t *testing.T) {
	for _, line := range []string{"FuzzA", "FuzzA pkg/one extra"} {
		if _, err := parse(strings.NewReader(line)); err == nil {
			t.Errorf("parse(%q) succeeded, want an error naming the line", line)
		} else if !strings.Contains(err.Error(), "stdin:1") {
			t.Errorf("parse(%q) error %q does not name the line", line, err)
		}
	}
}

// Empty input is a refusal, not a successful run of nothing: list.sh fails
// closed on an empty tier and this end must not turn that into a pass.
func TestRunRefusesAnEmptyTargetList(t *testing.T) {
	err := run(nil, strings.NewReader("   \n\n"), &bytes.Buffer{})
	if err == nil {
		t.Fatal("run with no targets succeeded, want a refusal")
	}
	if !strings.Contains(err.Error(), "no fuzz targets") {
		t.Errorf("error %q does not say the list was empty", err)
	}
}

func TestRunRefusesTargetsAsArguments(t *testing.T) {
	err := run([]string{"FuzzA"}, strings.NewReader("FuzzA pkg/one\n"), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "standard input") {
		t.Fatalf("run with a positional argument: %v, want a refusal naming standard input", err)
	}
}

func TestRunRefusesNegativeJobs(t *testing.T) {
	err := run([]string{"-jobs", "-1"}, strings.NewReader("FuzzA pkg/one\n"), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "-jobs") {
		t.Fatalf("run with -jobs -1: %v, want a refusal naming the flag", err)
	}
}

// A failing target must fail the run and be named. Concurrency makes this
// worth pinning: a result collected on the wrong goroutine, or an error
// dropped because a later target passed, would turn a crasher into a green
// tier — the one outcome a fuzz tier must never produce.
func TestRunFailsAndNamesEveryFailingTarget(t *testing.T) {
	var out bytes.Buffer
	err := run([]string{"-fuzztime", "1ms"},
		strings.NewReader("FuzzA "+absentPackage+"\nFuzzB "+absentPackage+"\n"), &out)
	if err == nil {
		t.Fatal("fuzzing packages that do not exist succeeded, want a failure")
	}
	for _, name := range []string{"FuzzA", "FuzzB"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name failing target %s", err, name)
		}
	}
	if strings.Contains(out.String(), "all 2 target(s) passed") {
		t.Errorf("a failing run still reported a pass:\n%s", out.String())
	}
}

// The contract concurrency costs and this command keeps: results stay in input
// order however the goroutines finish, and both the start and the verdict name
// the target, so a run killed by an outer timeout still names what was in
// flight.
func TestFuzzAllReportsInInputOrderAndNamesEveryTarget(t *testing.T) {
	targets := []target{
		{name: "FuzzA", dir: absentPackage},
		{name: "FuzzB", dir: absentPackage},
		{name: "FuzzC", dir: absentPackage},
	}
	var out bytes.Buffer
	results := fuzzAll(context.Background(), &out, targets, 3, options{
		fuzztime: time.Millisecond, timeout: 30 * time.Second, memlimit: "512MiB",
	})

	if len(results) != len(targets) {
		t.Fatalf("got %d result(s), want %d", len(results), len(targets))
	}
	for i, r := range results {
		if r.target != targets[i] {
			t.Errorf("result %d is %v, want %v — results must stay in input order", i, r.target, targets[i])
		}
		if r.err == nil {
			t.Errorf("%s: fuzzing a package that does not exist succeeded", r.target.name)
		}
	}

	text := out.String()
	for _, tg := range targets {
		if !strings.Contains(text, "==> "+tg.name+" ("+tg.dir+") started") {
			t.Errorf("output never announces %s starting:\n%s", tg.name, text)
		}
		if !strings.Contains(text, "==> "+tg.name+" ("+tg.dir+") FAILED") {
			t.Errorf("output never reports %s failing:\n%s", tg.name, text)
		}
	}
}

// Concurrency is bounded. With one worker no two targets may be in flight at
// once, which is what a machine that wants its cores back asks for and what
// proves the semaphore is real rather than decorative.
//
// The assertion is "nothing else started while this one ran" and not "they ran
// in input order": which goroutine wins a free worker is the scheduler's
// choice, so an order assertion here passes or fails by luck.
func TestFuzzAllHonoursTheWorkerBound(t *testing.T) {
	var out bytes.Buffer
	targets := []target{
		{name: "FuzzA", dir: absentPackage},
		{name: "FuzzB", dir: absentPackage},
		{name: "FuzzC", dir: absentPackage},
	}
	fuzzAll(context.Background(), &out, targets, 1, options{
		fuzztime: time.Millisecond, timeout: 30 * time.Second, memlimit: "512MiB",
	})

	// Each target's verdict must be the next "==>" line after its own start.
	// Anything else means a second target was running while the first had not
	// finished, which one worker must prevent.
	var inFlight string
	for line := range strings.SplitSeq(out.String(), "\n") {
		name, ok := strings.CutPrefix(line, "==> ")
		if !ok {
			continue
		}
		name, _, _ = strings.Cut(name, " ")
		switch {
		case strings.HasSuffix(line, "started"):
			if inFlight != "" {
				t.Fatalf("%s started while %s was still running, so -jobs 1 did not serialize:\n%s",
					name, inFlight, out.String())
			}
			inFlight = name
		default:
			if inFlight != name {
				t.Fatalf("%s reported a verdict but %q was the target in flight:\n%s",
					name, inFlight, out.String())
			}
			inFlight = ""
		}
	}
	if inFlight != "" {
		t.Errorf("%s started and never reported:\n%s", inFlight, out.String())
	}
}

// The tier's safety bounds are arguments, so nothing but an assertion on the
// arguments can keep them. Every test above runs `go test` against a package
// that does not exist, which fails before any of these matter — so without
// this, dropping `-parallel 1` or `-run=XXX` would leave the package green.
func TestFuzzCommandCarriesTheTiersBounds(t *testing.T) {
	cmd := fuzzCommand(context.Background(),
		target{name: "FuzzThing", dir: "pkg/some/where"},
		options{fuzztime: 30 * time.Second, timeout: 2 * time.Minute, memlimit: "512MiB"})

	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{
		"-parallel 1",       // one fuzzing worker: a crash stays attributable
		"-run=XXX",          // the package's ordinary tests are not the budget
		"-fuzz FuzzThing",   // this target and no other
		"-fuzztime 30s",     // the time bound
		"-timeout 2m0s",     // the deadline that outlives it
		"./pkg/some/where/", // the package the target lives in
	} {
		if !strings.Contains(args, want) {
			t.Errorf("command is %q, which is missing %q", args, want)
		}
	}
	if !slices.Contains(cmd.Env, "GOMEMLIMIT=512MiB") {
		t.Errorf("GOMEMLIMIT=512MiB is not in the child's environment; -fuzztime bounds time and this is what bounds memory")
	}
}

// A target's output is decided by inputs the fuzzer manufactured, so the
// capture of it must be bounded — and it must say when it dropped something
// rather than present a truncated log as a whole one.
func TestBoundedOutputKeepsBothEndsAndSaysWhatItDropped(t *testing.T) {
	var b boundedOutput
	// Written in many small pieces, the way a child's pipe actually arrives.
	const total = headBytes + tailBytes + 100_000
	for written := 0; written < total; {
		chunk := min(4096, total-written)
		if _, err := b.Write(bytes.Repeat([]byte("x"), chunk)); err != nil {
			t.Fatalf("Write: %v", err)
		}
		written += chunk
	}

	if len(b.head) != headBytes || len(b.tail) != tailBytes {
		t.Fatalf("kept %d head and %d tail bytes, want %d and %d", len(b.head), len(b.tail), headBytes, tailBytes)
	}
	got := b.Bytes()
	if len(got) > headBytes+tailBytes+512 {
		t.Errorf("rendered %d bytes for a %d-byte stream, which is not a bound", len(got), total)
	}
	if !strings.Contains(string(got), "elided") {
		t.Errorf("output dropped %d bytes without saying so", total-headBytes-tailBytes)
	}
}

// One write larger than the whole ceiling must not first be buffered whole.
func TestBoundedOutputBoundsASingleEnormousWrite(t *testing.T) {
	var b boundedOutput
	if _, err := b.Write(bytes.Repeat([]byte("y"), 4<<20)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(b.tail) > tailBytes {
		t.Errorf("one write left %d tail bytes, want at most %d", len(b.tail), tailBytes)
	}
	if cap(b.tail) > 2*tailBytes {
		t.Errorf("one write grew the tail's capacity to %d, want at most %d", cap(b.tail), 2*tailBytes)
	}
}

// The case the ceiling is actually easy to breach in, and which the enormous
// single write above does not reach: a full tail plus a nearly-full write.
// Appending before trimming let the length pass tailBytes for the duration of
// the append, and the capacity kept what the length reached — 2.57x tailBytes,
// above the ceiling the type states.
func TestBoundedOutputBoundsAFullTailPlusALargeWrite(t *testing.T) {
	var b boundedOutput
	for written := 0; written < headBytes+tailBytes+8192; written += 4096 {
		if _, err := b.Write(bytes.Repeat([]byte("x"), 4096)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if _, err := b.Write(bytes.Repeat([]byte("y"), tailBytes-1)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(b.tail) != tailBytes {
		t.Errorf("tail holds %d byte(s), want exactly %d", len(b.tail), tailBytes)
	}
	if cap(b.tail) > 2*tailBytes {
		t.Errorf("the tail's capacity reached %d, want at most %d — the length passed the bound", cap(b.tail), 2*tailBytes)
	}
	// The newest bytes are the ones kept: a tail that dropped the wrong end
	// would lose the failure a fuzz run ends with.
	if got := b.tail[len(b.tail)-1]; got != 'y' {
		t.Errorf("the tail ends with %q, want the most recent byte %q", got, byte('y'))
	}
}

// Short output is passed through whole and unannotated: the bound must not
// cost the ordinary case its exact log.
func TestBoundedOutputPassesShortOutputThroughUnchanged(t *testing.T) {
	var b boundedOutput
	const want = "fuzz: elapsed: 30s, execs: 76108\nPASS\nok\n"
	if _, err := b.Write([]byte(want)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := string(b.Bytes()); got != want {
		t.Errorf("short output rendered as %q, want %q", got, want)
	}
}

// A target that passes must not clear an earlier target's failure. Every other
// case here fails every target, so this is the half of the failure contract
// that nothing could observe before the runner became injectable: a mutation
// that reset the failure list whenever a later target passed would survive.
func TestAPassingTargetDoesNotClearAnEarlierFailure(t *testing.T) {
	var out bytes.Buffer
	results := fuzzAll(context.Background(), &out,
		[]target{{name: "FuzzBroken", dir: "pkg/one"}, {name: "FuzzFine", dir: "pkg/two"}}, 1,
		options{
			fuzztime: time.Millisecond, timeout: time.Minute, memlimit: "512MiB",
			run: func(_ context.Context, tg target, _ options) ([]byte, error) {
				if tg.name == "FuzzBroken" {
					return []byte("boom\n"), errors.New("exit status 1")
				}
				return []byte("ok\n"), nil
			},
		})

	if results[0].err == nil {
		t.Error("the failing target reported no error")
	}
	if results[1].err != nil {
		t.Errorf("the passing target reported %v", results[1].err)
	}
}

// The same, end to end through run, so the exit status and the summary are
// what a red job would actually show.
func TestRunFailsWhenOnlyOneOfSeveralTargetsFails(t *testing.T) {
	var out bytes.Buffer
	err := runWith([]string{"-fuzztime", "1ms"},
		strings.NewReader("FuzzFine pkg/one\nFuzzBroken pkg/two\nFuzzAlsoFine pkg/three\n"), &out,
		func(_ context.Context, tg target, _ options) ([]byte, error) {
			if tg.name == "FuzzBroken" {
				return nil, errors.New("exit status 1")
			}
			return nil, nil
		})
	if err == nil {
		t.Fatal("a run with one failing target succeeded")
	}
	if !strings.Contains(err.Error(), "FuzzBroken") {
		t.Errorf("error %q does not name the failing target", err)
	}
	if strings.Contains(err.Error(), "FuzzFine") || strings.Contains(err.Error(), "FuzzAlsoFine") {
		t.Errorf("error %q names a target that passed", err)
	}
	if strings.Contains(out.String(), "target(s) passed") {
		t.Errorf("a run with a failing target still reported a pass:\n%s", out.String())
	}
}

// Half the CPUs, not all of them. A fuzzing target is a coordinator process
// and a worker process, so N at once is 2N processes; measured on four cores,
// one worker per CPU left each target about two thirds of the CPU it had to
// itself and the worst half, while one per two CPUs held at 0.92. The default
// is a measured number and a test keeps it from drifting back.
func TestDefaultWorkersIsHalfTheCPUsThisProcessMaySpend(t *testing.T) {
	if defaultWorkers() < 1 {
		t.Error("defaultWorkers() must never be zero: fuzzAll would have nothing to run with")
	}
	if got := defaultWorkers(); got > runtime.GOMAXPROCS(0) {
		t.Errorf("defaultWorkers() = %d oversubscribes the %d CPU(s) this process may spend", got, runtime.GOMAXPROCS(0))
	}

	// The bound follows GOMAXPROCS and not NumCPU, because an affinity mask is
	// not a quota: a lane told it may use two cores of a sixty-four-core host
	// reads NumCPU as sixty-four and would dispatch half of that, capped only
	// by the target count. Driving GOMAXPROCS across several values is what
	// distinguishes the two on any machine, since procs/2 varies where
	// NumCPU/2 is constant.
	restore := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(restore) })
	for _, procs := range []int{1, 2, 4, 8} {
		runtime.GOMAXPROCS(procs)
		if got, want := defaultWorkers(), max(procs/2, 1); got != want {
			t.Errorf("at GOMAXPROCS=%d defaultWorkers() = %d, want %d", procs, got, want)
		}
	}
}
