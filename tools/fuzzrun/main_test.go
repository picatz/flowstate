package main

import (
	"bytes"
	"context"
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
