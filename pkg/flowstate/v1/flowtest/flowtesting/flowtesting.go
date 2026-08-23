// Package flowtesting is the [testing]-shaped front door to `flow test`'s
// harness (issue #930): it runs a `*.test.yaml` suite's cases as real Go
// subtests, one [testing.T.Run] per case, so the tooling an author already has
// — `go test -run`, `-v`, an IDE's per-test rerun, CI's per-test timing —
// addresses a Flowfile case exactly as it addresses a Go one. A module that
// embeds workflows (pkg/flowstate/embed) pins its suites into its own `go
// test` with one call:
//
//	func TestWorkflows(t *testing.T) {
//		flowtesting.RunFile(t, "workflows/deploy.test.yaml")
//	}
//
// A separate package, on the model of net/http/httptest and this repository's
// own dsttest and secretstest: flowtest is imported by the `flow` binary — the
// CLI and the MCP surface both run suites through it — and a package a binary
// imports should not carry a [testing.TB]-shaped entry point. flowtest runs a
// suite and reports what happened; flowtesting turns the report into verdicts
// on a [testing.T].
//
// # One case, one subtest
//
// [Run] and [RunFile] start one subtest per case, named by the case's own
// `name:`, and each subtest runs exactly that case through [flowtest.Run],
// selected by name. `go test -run 'TestWorkflows/retries_then_recovers'`
// therefore reruns one case the way it reruns one Go test (Go's -run matching
// spells a space as an underscore). Because the name is the address, a file
// whose cases share one is refused before anything runs: two cases answering
// to one subtest name cannot be told apart by the mechanism this package
// exists to provide. (`flow test` itself accepts duplicates — it runs by
// written order and never addresses a case by name.)
//
// Running a subset under -run needs no further honesty line here, unlike the
// CLI's `--run` (issue #929): the subtest structure is the account. `go test`
// itself reports which subtests ran, so a green over a subset never reads as
// the file's green.
//
// Subtests run sequentially, and none calls [testing.T.Parallel]: cases swap
// the process-wide task registry for their duration and serialize through
// [v1.LockDefaultRegistry], so parallelism would buy contention, not speed.
// Two packages' tests calling into this package concurrently are safe for the
// same reason.
//
// # Coverage is a property of the file, not of a case
//
// [WithCoverageRequired] holds the suite to `flow test --coverage-required`'s
// bar: every step and every switch arm of every targeted workflow reached by
// some case, every deliberate residual recorded under
// `coverage.allow_unreached`, and no stale record. A per-case run cannot
// measure that — the unit is the file — so the option adds one whole-suite
// pass after the per-case subtests and fails the parent test on any gap. That
// pass runs every case once more, deliberately: cases are hermetic and fast by
// construction (no network, no Temporal, a virtual clock), and the alternative
// is a second spelling of the coverage accumulator for this package to
// disagree with the CLI about. It also ignores -run, which is what "a property
// of the file" means: filtering subtests changes what go test verdicts, never
// what the suite's coverage is.
package flowtesting

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// Option adjusts how [Run] and [RunFile] run a suite. The zero configuration
// is `flow test`'s own default: every case once, written order, no coverage
// bar.
type Option func(*config)

type config struct {
	dir              string
	dirSet           bool
	budget           dst.Budget
	coverageRequired bool
}

// WithDir names the directory a case's `workflow:` and a trigger case's
// `payload:` resolve against, for a [flowtest.File] built or loaded in Go —
// the fact a file on disk carries in its own path and an in-memory one has
// nowhere to record.
//
// It is for [Run] only. [RunFile] refuses it: a `*.test.yaml` resolves
// against its own directory, the same rule the file's `workflow:` field
// documents, and an override would make the suite mean something different
// here than under `flow test`.
func WithDir(dir string) Option {
	return func(c *config) {
		c.dir = dir
		c.dirSet = true
	}
}

// WithSchedules explores each case under the seeded schedules the budget
// describes, beyond its written-order run (issue #800), and fails a case's
// subtest when a schedule changed what that case observed. The written-order
// run still decides the case's own verdict, exactly as `flow test --seeds`
// keeps the two findings apart.
func WithSchedules(budget dst.Budget) Option {
	return func(c *config) { c.budget = budget }
}

// WithCoverageRequired holds the suite to `flow test --coverage-required`'s
// bar, with one whole-suite pass after the per-case subtests — see the
// package comment for why the unit is the file and what the pass costs.
func WithCoverageRequired() Option {
	return func(c *config) { c.coverageRequired = true }
}

// RunFile loads a `*.test.yaml` and runs it as [Run] does, resolving each
// case's paths against the file's own directory. A file that does not load —
// a parse error, a case with no name, a stub declaring both `returns:` and
// `response:` — stops the test immediately: the refusal is about the file,
// not about any one case.
func RunFile(t *testing.T, path string, opts ...Option) {
	t.Helper()

	cfg := configure(opts)
	if cfg.dirSet {
		t.Fatalf("flowtesting: WithDir is for a [flowtest.File] built in Go; %q resolves against its own directory, the same rule its cases' workflow: paths document", path)
	}

	file, err := flowtest.Load(path)
	if err != nil {
		t.Fatal(err.Error())
	}

	cfg.dir = filepath.Dir(path)
	run(t, file, path, cfg)
}

// Run runs a loaded or Go-built [flowtest.File]'s cases as subtests of t. Use
// [WithDir] when any case's `workflow:` or `payload:` is a relative path;
// without it those resolve against the empty directory and are refused, the
// same answer [flowtest.Run] gives.
func Run(t *testing.T, file *flowtest.File, opts ...Option) {
	t.Helper()

	cfg := configure(opts)
	run(t, file, "", cfg)
}

func configure(opts []Option) config {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// run is the shared body: refuse what cannot be addressed, one subtest per
// case, then the whole-suite coverage pass when the caller asked for the bar.
// path is where the suite came from when it came from disk, and "" for a
// built [flowtest.File]; it is only ever shown to a reader (a replay command,
// the report label), never resolved against.
func run(t *testing.T, file *flowtest.File, path string, cfg config) {
	t.Helper()

	if reason := refusal(file); reason != "" {
		t.Fatal(reason)
	}

	for _, test := range file.Tests {
		want := test.Name
		t.Run(want, func(t *testing.T) {
			runCase(t, file, path, cfg, want)
		})
	}

	if cfg.coverageRequired {
		coveragePass(t, file, path, cfg.dir)
	}
}

// runCase is one subtest's body: run exactly the named case, then turn what
// it produced into the subtest's verdict.
func runCase(t testing.TB, file *flowtest.File, path string, cfg config, want string) {
	t.Helper()

	result := flowtest.Run(t.Context(), file, cfg.dir, flowtest.RunOptions{
		Label:  path,
		Budget: cfg.budget,
		Select: func(name string) bool { return name == want },
	})

	// One name selected exactly one case: [refusal] guaranteed the name is
	// unique and the caller got it from the file, so anything else is this
	// package's own selection wiring gone wrong — which must fail the subtest
	// loudly, never run the wrong case quietly.
	if got := len(result.Report.GetCases()); got != 1 {
		t.Fatalf("flowtesting: selecting case %q by name ran %d cases; this is a bug in flowtesting, not in the suite", want, got)
		return
	}

	reportCase(t, result.Report.GetCases()[0])
	reportSchedules(t, path, result.Schedules)
}

// coveragePass is the whole-suite run behind [WithCoverageRequired]: every
// case once more, no selection, and the coverage account turned into the
// parent test's verdict.
func coveragePass(t testing.TB, file *flowtest.File, path, dir string) {
	t.Helper()

	result := flowtest.Run(t.Context(), file, dir, flowtest.RunOptions{Label: path})
	reportCoverage(t, result.Coverage)
}

// refusal is what stops a file from running as subtests at all, or "" when
// nothing does. Both refusals are about addressability rather than validity —
// `flow test` runs these files fine — so the sentences say what this package
// needs and why.
//
// Collisions are judged on the name [testing.T.Run] will expose, not on the
// written spelling: go test rewrites a subtest's name before it becomes an
// address (whitespace to underscores, unprintables to their escapes), so
// cases named "a b" and "a_b" are two spellings of one address — the second
// would run as "a_b#01", a name derived from declaration order that no
// documentation of this package teaches, and `-run 'TestX/a_b'` would match
// both. Reported by Codex on picatz/flowstate#1015.
func refusal(file *flowtest.File) string {
	if len(file.Tests) == 0 {
		return "flowtesting: the file declares no cases, so this run would pass by testing nothing"
	}

	seen := make(map[string]string, len(file.Tests))
	for _, test := range file.Tests {
		address := subtestName(test.Name)
		first, collides := seen[address]
		if !collides {
			seen[address] = test.Name
			continue
		}
		if first == test.Name {
			return fmt.Sprintf("flowtesting: two cases share the name %q; a subtest's name is how `go test -run` addresses a case, so every case needs its own", test.Name)
		}
		return fmt.Sprintf("flowtesting: cases %q and %q both become subtest %q under go test's own name rewriting, so `-run` cannot tell them apart; rename one", first, test.Name, address)
	}

	return ""
}

// subtestName is the name go test will expose for a case: the testing
// package's own rewrite (testing/match.go) — every whitespace rune becomes
// "_", every unprintable rune its escaped spelling — applied here so
// [refusal] judges collisions on the address `-run` really matches against.
func subtestName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case unicode.IsSpace(r):
			b.WriteByte('_')
		case !strconv.IsPrint(r):
			quoted := strconv.QuoteRune(r)
			b.WriteString(quoted[1 : len(quoted)-1])
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// reportCase renders one case's verdict onto its subtest: the harness error
// or the unmet expectations as failures, in the CLI's own wording, and the
// warnings as log lines — visible under -v and on failure, never a verdict,
// matching `flow test` without `--fail-on-warning`.
func reportCase(t testing.TB, c *v1.TestCase) {
	t.Helper()

	if err := c.GetError(); err != "" {
		t.Errorf("%s", err)
		return
	}
	for _, f := range c.GetFailures() {
		if f.GetStep() != "" {
			t.Errorf("%s (step %q): %s", f.GetField(), f.GetStep(), f.GetMessage())
			continue
		}
		t.Errorf("%s: %s", f.GetField(), f.GetMessage())
	}
	for _, w := range c.GetWarnings() {
		t.Logf("warning: %s: %s", w.GetField(), w.GetMessage())
	}
}

// reportSchedules renders what one case's seeded-schedule exploration found.
// The summary is logged on every explored run, pass or fail, for dsttest's
// reason: a search that reached no junction proved nothing, and that
// distinction is invisible from the outcome alone.
func reportSchedules(t testing.TB, path string, schedules *flowtest.ScheduleReport) {
	if schedules == nil {
		return
	}
	t.Helper()

	t.Logf("schedule exploration: %d schedules, up to %d scheduling decisions, truncated=%t",
		schedules.Schedules, schedules.Decisions, schedules.Truncated)
	if schedules.Decisions == 0 {
		t.Logf("nothing was explored: this case reached no `parallel:` or `async:` junction, so every schedule was written order")
	}

	d := schedules.Divergence
	if d == nil {
		return
	}

	replay := fmt.Sprintf("replay this exact schedule:\n\n    flow test --seed %d -- %q\n", d.Seed, path)
	if path == "" {
		replay = fmt.Sprintf("replay this exact schedule by pinning it:\n\n    seed := uint64(%d)\n    flowtesting.Run(t, file, flowtesting.WithSchedules(dst.Budget{Pinned: &seed}))\n", d.Seed)
	}
	truncated := ""
	if d.Truncated {
		truncated = fmt.Sprintf("\nThis schedule spent its whole %d-decision budget and took written order for the\nrest of its run, so what it explored stops before the bound.\n", v1.MaxScheduleDecisions)
	}

	t.Errorf("the schedule changed what this case observed (seed %d)\n\n"+
		"This explores only the orderings the LOCAL driver is free to choose — a\n"+
		"`parallel:` block's branch order, and whether an `async:` step's work happens\n"+
		"where it is written or at its join. So this says your file's observables depend\n"+
		"on one of those, or that the local engine does. It is not a claim about\n"+
		"Temporal's orderings.\n%s\n%s\nwritten order:\n%s\nseed %d (%d scheduling decisions):\n%s",
		d.Seed, truncated, replay,
		indent(d.WrittenOrder), d.Seed, d.Decisions, indent(d.Seeded))
}

// reportCoverage renders the whole-suite pass's account and fails on what
// `flow test --coverage-required` fails on: an unrecorded step gap, an
// unrecorded switch-arm gap, or a stale `coverage.allow_unreached` record —
// each in the CLI's own wording, so an author moving between the two surfaces
// reads one sentence per fact.
func reportCoverage(t testing.TB, coverage []*flowtest.Coverage) {
	t.Helper()

	for _, cov := range coverage {
		summary := fmt.Sprintf("coverage: %s: %d/%d steps reached", cov.Workflow, len(cov.Reached), cov.Total())
		if len(cov.Arms) > 0 {
			summary += fmt.Sprintf(", %d/%d switch arms taken", cov.ArmsReached(), len(cov.Arms))
		}
		t.Logf("%s", summary)

		if gaps := cov.Gaps(); len(gaps) > 0 {
			t.Errorf("coverage: %s: never ran: %s", cov.Workflow, strings.Join(gaps, ", "))
		}
		for _, arm := range cov.ArmGaps() {
			where := cov.Workflow
			if arm.Where.IsValid() {
				where += ":" + arm.Where.Start.String()
			}
			t.Errorf("coverage: %s: %s of switch %q was taken by no test case; add a case whose inputs reach it, or record why under coverage.allow_unreached: %s",
				where, arm.Label, arm.Step, arm.Key)
		}
		for _, stale := range cov.Stale {
			t.Errorf("coverage: %s", stale)
		}
	}
}

// indent shifts a schedule rendering right so a divergence reads as two
// blocks, the shape [dst.FailureText] and `flow test`'s own printer both give
// it.
func indent(text string) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	for i, line := range lines {
		lines[i] = "    " + line
	}
	return strings.Join(lines, "\n") + "\n"
}
