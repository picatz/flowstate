// Package dst is the deterministic simulation tier: one workflow run under many
// schedules, held to the claim that the schedule is not something an author can
// see (issue #477, slice 0).
//
// # The property
//
// `async:` is the marker that lets execution depart from written order, and
// #418's promise about it is that completion order is never observable. That is a
// claim about every schedule, and a driver that only ever takes one of them
// cannot check it. So the local driver's scheduling decisions became a value
// ([v1.Scheduler]), and this package is what does something with that: run the
// same workflow, with the same inputs and the same doubles, once per seed, and
// assert every observable is identical to the written-order baseline's.
//
// What counts as an observable is the narrow thing the execution model actually
// promises, and the narrowness is load-bearing in both directions. The
// transcript, the run's declared outputs, the failure text (which carries the
// compensation account, so undo order is checked here too), and — where a case
// records them — the effects the model orders. Not the order two independent
// tasks were invoked in: that is precisely the freedom `async:` and `parallel:`
// exist to grant, and a property that forbade it would be a property no correct
// engine could pass. [Result.UnorderedPrefix] is where a case says which of its
// effects are a set rather than a sequence, the same distinction
// [conformance.UndoCase] already draws for the same reason.
//
// # Every failure is a seed
//
// A divergence names the seed that produced it and prints the command that
// replays it, because a simulation whose failures cannot be replayed is a flake
// generator rather than a debugging aid. The seed is the whole of the state: a
// [v1.SeededScheduler] draws from its PRNG in the order the run reaches its
// decision points, and the local driver reaches them in the same order every
// time.
//
// # Bounds
//
// Two, because two resources run away, and they are not the same resource. The
// number of schedules explored bounds this package's own wall-clock time and is
// [Budget.Schedules], reported on every run so a search that explored nothing is
// visible rather than green. The number of decisions inside one schedule bounds
// what a single pathological workflow can spend, and is
// [v1.MaxScheduleDecisions], reported here as [Observation.Truncated].
//
// # Which driver
//
// The local one, alone, and that is the cut issue #477 draws rather than a
// shortcut. This tier tests *our* engine's semantics under orderings we are free
// to choose; the durable driver's orderings are Temporal's, and what it owes is
// replay determinism against its own history, which its own replay corpus already
// exercises. Holding the durable driver to this property would mean driving
// Temporal's test environment's coroutine scheduler from a seed — slice 3's work,
// and a different mechanism. What keeps the two honest in the meantime is
// unchanged: both drivers run every case in pkg/flowstate/v1/internal/conformance, and the cases
// this package explores are those same cases.
//
// [conformance.UndoCase]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance#UndoCase
package dst

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// DefaultSchedules is how many schedules one property run explores when nothing
// says otherwise.
//
// Sized for the PR lane: the whole shared corpus at this width is seconds of pure
// Go with no server, which is what lets this be an ordinary test in an existing
// job rather than a job of its own. The weekly deep tier raises it through
// [ScheduleBudgetEnv], which is the venue where signal accumulates with clock
// time rather than with a diff.
const DefaultSchedules = 24

// MaxSchedules caps what [ScheduleBudgetEnv] may ask for.
//
// The resource is wall-clock time, and the party choosing is a caller's
// environment: every schedule is a whole workflow run, so the cost is linear in
// this number and a typo with an extra digit is the difference between a job that
// finishes and a job that is killed at its timeout with nothing to report.
const MaxSchedules = 10_000

// DefaultSeed0 is the first seed of the search when nothing says otherwise.
//
// Fixed rather than drawn from the clock, deliberately: a tier that picks fresh
// seeds every run turns a real defect into an intermittent one and makes the
// question "did this commit break it" unanswerable. Exploring a *different* part
// of the space is what the deep tier's own seed is for.
const DefaultSeed0 uint64 = 1

const (
	// ScheduleBudgetEnv raises or lowers how many schedules each property run
	// explores, bounded by [MaxSchedules].
	ScheduleBudgetEnv = "FLOWSTATE_DST_SCHEDULES"

	// Seed0Env moves the whole search to a different part of the seed space; the
	// search walks seeds upward from it.
	Seed0Env = "FLOWSTATE_DST_SEED0"

	// SeedEnv pins the search to exactly one seed — the replay switch a failure
	// tells you to set, and the reason a divergence is a thing you can hold
	// still and look at.
	SeedEnv = "FLOWSTATE_DST_SEED"
)

// ReproducePackage is the package path printed in the command a failure tells
// you to run. A var so a caller driving this harness from somewhere else can
// print a command that actually reproduces its own failure.
var ReproducePackage = "./pkg/flowstate/v1/dst/"

// Budget is what one property run is allowed to spend.
type Budget struct {
	// Schedules is how many seeded schedules to explore, beyond the
	// written-order baseline. Clamped to [MaxSchedules].
	Schedules int

	// Seed0 is the first seed; the search walks upward from it.
	Seed0 uint64

	// Pinned, when set, replaces the whole search with the single seed named —
	// what [SeedEnv] sets, and what a replay is.
	Pinned *uint64
}

// DefaultBudget is [DefaultSchedules] schedules from [DefaultSeed0], with the
// three environment variables applied over it.
//
// A malformed value is a failure rather than a silent fallback: someone setting
// FLOWSTATE_DST_SCHEDULES=1O (with a letter O) meant to explore more and would
// otherwise be told nothing while exploring the default, which is the shape of
// green a search job must never report.
func DefaultBudget() (Budget, error) {
	budget := Budget{Schedules: DefaultSchedules, Seed0: DefaultSeed0}

	if raw, set := os.LookupEnv(ScheduleBudgetEnv); set {
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || n < 0 {
			return budget, fmt.Errorf("%s=%q is not a count of schedules; write a non-negative integer", ScheduleBudgetEnv, raw)
		}
		if n > MaxSchedules {
			return budget, fmt.Errorf("%s=%d is above the %d this harness will explore in one run; "+
				"a schedule is a whole workflow run, so the cost is linear in this number", ScheduleBudgetEnv, n, MaxSchedules)
		}
		budget.Schedules = n
	}

	if raw, set := os.LookupEnv(Seed0Env); set {
		seed, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return budget, fmt.Errorf("%s=%q is not a seed; write an unsigned integer", Seed0Env, raw)
		}
		budget.Seed0 = seed
	}

	if raw, set := os.LookupEnv(SeedEnv); set {
		seed, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return budget, fmt.Errorf("%s=%q is not a seed; write the number a failing run printed", SeedEnv, raw)
		}
		budget.Pinned = &seed
	}

	return budget, nil
}

// seeds is the search this budget describes.
func (b Budget) seeds() []uint64 {
	if b.Pinned != nil {
		return []uint64{*b.Pinned}
	}

	seeds := make([]uint64, 0, b.Schedules)
	for i := range b.Schedules {
		seeds = append(seeds, b.Seed0+uint64(i))
	}

	return seeds
}

// Result is one run's observable outcome — everything the execution model
// promises about it, and nothing it does not.
type Result struct {
	// Transcript is what the run recorded, including its declared outputs.
	Transcript *v1.Workflow_StepOutputs

	// Err is the run's failure, if it had one. Compared by its text, which is
	// where the compensation account ("undid %q", in unwind order) lives.
	Err error

	// Effects are side effects the model orders — the tokens a recording server
	// received, for a case that has one. Empty for a case whose claims are all
	// in the transcript.
	Effects []string

	// UnorderedPrefix is how many leading entries of Effects are compared as a
	// set rather than a sequence: work whose order is the schedule's to choose,
	// which is exactly what this harness varies. The same distinction
	// [conformance.UndoCase] draws, for the same reason — beyond the prefix, order is
	// a claim.
	//
	// [conformance.UndoCase]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance#UndoCase
	UnorderedPrefix int
}

// RunFunc runs one schedule. ctx already carries the scheduler; a caller passes
// it to [v1.RunWithInputs] (or to whatever it drives) unchanged.
type RunFunc func(ctx context.Context) Result

// Observation is one schedule's rendering, and the number that reproduces it.
type Observation struct {
	// Seed is the scheduler's seed, or absent for the written-order baseline.
	Seed uint64

	// Baseline marks the written-order run, the one every other is compared
	// against.
	Baseline bool

	// Digest is what equality is decided on: a hash over the deterministic
	// encoding of the transcript, the failure text, and the ordered effects.
	Digest string

	// Rendering is the same content in a form a person can read in a failure.
	Rendering string

	// Decisions is how many scheduling choices this run asked for. Zero on every
	// schedule of a workflow with no junctions, which is what makes a property
	// run that explored nothing detectable.
	Decisions int

	// Truncated reports that the run spent [v1.MaxScheduleDecisions] and took
	// written order for the rest of itself.
	Truncated bool
}

// name is how an observation is referred to in a failure.
func (o Observation) name() string {
	if o.Baseline {
		return "the written-order baseline"
	}

	return fmt.Sprintf("seed %d", o.Seed)
}

// Divergence is two schedules of one workflow that produced different
// observables — which, if the engine is right, cannot happen.
type Divergence struct {
	Baseline Observation
	Diverged Observation
}

// Report is what exploring a workflow's schedule space produced.
type Report struct {
	// Observations is the baseline followed by every schedule explored.
	Observations []Observation

	// Divergence is the first schedule that disagreed with the baseline, or nil
	// when every one of them agreed.
	Divergence *Divergence
}

// Schedules is how many seeded schedules this report covers, not counting the
// baseline.
func (r *Report) Schedules() int {
	if len(r.Observations) == 0 {
		return 0
	}

	return len(r.Observations) - 1
}

// Decisions is the largest number of scheduling choices any one schedule made.
//
// The number that says whether a search explored anything at all: a corpus of
// workflows with no `parallel:` and no `async:` has no junctions, so every
// schedule of it is the written-order one and a green result means nothing. A
// caller asserts on this rather than trusting it.
func (r *Report) Decisions() int {
	most := 0
	for _, observation := range r.Observations {
		if observation.Decisions > most {
			most = observation.Decisions
		}
	}

	return most
}

// Truncated reports whether any schedule spent its whole decision budget.
func (r *Report) Truncated() bool {
	for _, observation := range r.Observations {
		if observation.Truncated {
			return true
		}
	}

	return false
}

// Explore runs run once under written order and once per seed in budget,
// returning the first schedule that disagreed with the baseline.
//
// It returns rather than fails, so that a test can assert a divergence *is*
// found — which is the only way to know the property is capable of failing at
// all. [CheckScheduleEquivalence] is the ordinary caller.
func Explore(ctx context.Context, budget Budget, run RunFunc) *Report {
	report := &Report{}

	baseline := observe(ctx, nil, run)
	report.Observations = append(report.Observations, baseline)

	for _, seed := range budget.seeds() {
		scheduler := v1.NewSeededScheduler(seed)
		observation := observe(ctx, scheduler, run)
		report.Observations = append(report.Observations, observation)

		if observation.Digest != baseline.Digest && report.Divergence == nil {
			report.Divergence = &Divergence{Baseline: baseline, Diverged: observation}
		}
	}

	return report
}

// observe runs one schedule and renders what it produced.
func observe(ctx context.Context, scheduler *v1.SeededScheduler, run RunFunc) Observation {
	observation := Observation{Baseline: scheduler == nil}
	if scheduler != nil {
		observation.Seed = scheduler.Seed()
		ctx = v1.NewContextWithScheduler(ctx, scheduler)
	}

	result := run(ctx)
	observation.Rendering = render(result)
	sum := sha256.Sum256([]byte(observation.Rendering))
	observation.Digest = hex.EncodeToString(sum[:])

	if scheduler != nil {
		observation.Decisions = scheduler.Decisions()
		observation.Truncated = scheduler.Truncated()
	}

	return observation
}

// render turns a result into the exact text two schedules are compared by.
//
// Deterministic proto encoding rather than protojson: protojson deliberately
// varies its whitespace between calls, so two renderings of one message are not
// reliably the same string — which would make this harness report divergences
// that are the encoder's and miss none of its own. The bytes are hex so the
// rendering a failure prints is the same thing the comparison used, rather than
// a second rendering that could disagree with it.
func render(result Result) string {
	var b strings.Builder

	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(result.Transcript)
	if err != nil {
		// A transcript that will not encode is not a divergence to report; it is
		// the same failure on every schedule, so it renders identically and the
		// property stays quiet about it.
		fmt.Fprintf(&b, "transcript: unencodable: %v\n", err)
	} else {
		fmt.Fprintf(&b, "transcript: %s\n", hex.EncodeToString(encoded))
	}

	failure := ""
	if result.Err != nil {
		failure = result.Err.Error()
	}
	fmt.Fprintf(&b, "error: %q\n", failure)

	prefix := min(max(result.UnorderedPrefix, 0), len(result.Effects))
	unordered := append([]string(nil), result.Effects[:prefix]...)
	sort.Strings(unordered)
	fmt.Fprintf(&b, "effects (set): %q\n", unordered)
	fmt.Fprintf(&b, "effects (ordered): %q\n", result.Effects[prefix:])

	return b.String()
}

// CheckScheduleEquivalence explores one workflow's schedule space and fails tb
// on the first schedule whose observables differ from written order's.
//
// The failure names the seed and prints the command that replays it, because a
// seed nobody can act on is a random number.
func CheckScheduleEquivalence(tb testing.TB, run RunFunc) *Report {
	tb.Helper()

	budget, err := DefaultBudget()
	if err != nil {
		tb.Fatalf("the schedule budget is not usable: %v", err)
	}

	report := Explore(tb.Context(), budget, run)

	// Printed on every run, pass or fail: a search that explored no junctions is
	// a search that proved nothing, and a bound that was reached changes what a
	// green means. Both are facts about the *check*, so neither is allowed to be
	// silent.
	tb.Logf("schedule equivalence: %d schedules explored, up to %d scheduling decisions each, truncated=%t",
		report.Schedules(), report.Decisions(), report.Truncated())

	if report.Divergence == nil {
		return report
	}

	tb.Fatalf("%s", FailureText(tb.Name(), report.Divergence))

	return report
}

// FailureText is what a divergence says for itself: which seed, what differed,
// and the exact command that runs that one schedule again.
func FailureText(testName string, divergence *Divergence) string {
	var b strings.Builder

	fmt.Fprintf(&b, "schedule equivalence failed: %s produced observables %s did not.\n",
		divergence.Diverged.name(), divergence.Baseline.name())
	fmt.Fprintf(&b, "\nThe execution model promises the schedule is not something an author can see, "+
		"so this is a defect in the engine or in what this case counts as an observable.\n")

	fmt.Fprintf(&b, "\nREPRODUCE THIS EXACT SCHEDULE:\n\n    %s=%d go test -count=1 -run %q %s\n\n",
		SeedEnv, divergence.Diverged.Seed, "^"+testName+"$", ReproducePackage)

	if divergence.Diverged.Truncated {
		fmt.Fprintf(&b, "This schedule spent its whole %d-decision budget and took written order for the rest "+
			"of the run, so the interleaving it reports is only the part before the bound.\n\n", v1.MaxScheduleDecisions)
	}

	fmt.Fprintf(&b, "%s (%d scheduling decisions):\n%s\n", divergence.Baseline.name(),
		divergence.Baseline.Decisions, indent(divergence.Baseline.Rendering))
	fmt.Fprintf(&b, "%s (%d scheduling decisions):\n%s", divergence.Diverged.name(),
		divergence.Diverged.Decisions, indent(divergence.Diverged.Rendering))

	return b.String()
}

// indent shifts a rendering right so a failure reads as two blocks rather than
// as one wall.
func indent(text string) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	for i, line := range lines {
		lines[i] = "    " + line
	}

	return strings.Join(lines, "\n") + "\n"
}
