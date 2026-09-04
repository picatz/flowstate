package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// `flow test` is the third verb on the local driver's substrate (#155):
// `validate` asks whether a Flowfile is well-formed, `run local` asks what it
// does, and this asks whether it does what it promised. It is what makes a
// reusable workflow — a `call:`-able one especially — something an author can
// trust rather than something they hope works, without ever leaving their
// machine: no network, no Temporal, every task the workflow would otherwise
// call replaced by a stub, and time made virtual so a workflow that sleeps for
// a day resolves in well under a second. See pkg/flowstate/v1/flowtest's
// package doc for the design and pkg/flowstate/v1/clock.go for the piece that
// makes the virtual time possible.
//
// This runs the LOCAL driver only, by design (#155's "why this runs the local
// driver only"): sub-second, no-infrastructure feedback is the entire point.
// Rehearsing what only the durable driver does — Continue-As-New, versioning,
// a real Temporal timer — is `flow run` against a dev server, a different
// question with a different answer this command does not attempt to also be.

// newTestCommand builds the `flow test` command.
func newTestCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test [path...]",
		Short: "Run a workflow's own *.test.yaml files",
		Long: "Discover and run *.test.yaml files, each declaring a workflow, arguments to run it " +
			"with, task responses to stub in place of the real registry, scripted signals, and what " +
			"the run must produce. Every case runs through the local driver, in process: no network, " +
			"no Temporal server, and a virtual clock so a workflow that sleeps for a day resolves in " +
			"under a second.\n\n" +
			"A named file is taken as given. A directory is walked for *.test.yaml files.\n\n" +
			"Per file, `flow test` reports branch coverage: the set of the workflow's steps at least " +
			"one case ran, and the complement no case ever reached. Coverage is reported, not failed, " +
			"unless `--coverage-required` is set, which makes an unreached step a failure for any file " +
			"whose `coverage.allow_unreached` does not record a reason for it.\n\n" +
			"A `switch:` is measured a second way, per arm rather than per step, because an arm's " +
			"body may hold no steps at all: `steps: []` is how a switch writes down deliberately " +
			"ignoring a value, and `case: [closed, merged]` is one body two literals share. Which " +
			"arm a case took is read from the step's own `case` record, so an arm no case reached is " +
			"reported by the position it was written at — the only name an arm has. Record one under " +
			"`coverage.allow_unreached` by the key the diagnostic prints.\n\n" +
			"A case's `ran:`, `skipped:`, and `compensated:` name steps of the workflow, and a name " +
			"the workflow does not have refuses the case before it runs, with a suggestion — a claim " +
			"about a step that does not exist would otherwise pass vacuously forever. Three conditions " +
			"are reported as warnings: a stub the case declared and the run never answered through, " +
			"a task invoked with no stub declared for it, and an invocation that no declared stub " +
			"answered. Each is a fact about the case's own scaffolding, not a verdict, unless " +
			"`--fail-on-warning` promotes it. Stubs inherited from `defaults:` are exempt from the " +
			"idle-stub warning — a file-level catch-all is expected to sit idle in cases that " +
			"never invoke its task.\n\n" +
			"A failing case prints its transcript beneath the unmet expectation: what each step " +
			"produced and when virtual time moved, which stub answered it, each scripted signal " +
			"with its sender, and the `switch:` arm taken — the account the expectation was judged " +
			"against, with every value passing through the same redaction the stub diagnostics " +
			"apply. `-v` prints every case's transcript, passing or not.\n\n" +
			"`--run <pattern>` runs only the cases whose name matches the regular expression, the " +
			"way `go test -run` does, and says how many cases it filtered out — beside the file " +
			"and on the summary line — so a green over a subset never reads as the file's green. " +
			"`--coverage-required` is refused alongside it: coverage is a property of the whole " +
			"suite, and a filtered subset's gaps are not the suite's.\n\n" +
			"`--output json` or `--output jsonl` reports what ran as a schema message instead of " +
			"text, and carries the coverage sets under a `coverage` key so CI annotates rather than " +
			"parses prose.\n\n" +
			"`--seeds N` additionally runs every case under N seeded schedules and fails when a " +
			"case's observables change with the schedule. It explores only the orderings the local " +
			"driver is free to choose — the order a `parallel:` block advances its branches in, and " +
			"whether an `async:` step's work happens where it is written or at its join — so a green " +
			"says your file does not depend on those. It is not a claim about Temporal's orderings. " +
			"The number of scheduling decisions is reported alongside, because a workflow with no " +
			"`parallel:` and no `async:` reaches no junction and every schedule of it is written " +
			"order.",
		Args:          cobra.MinimumNArgs(1),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTest(cmd, args)
		},
		Example: `# Run every test beside the workflows in a directory:
flow test examples/

# Run one test file:
flow test deploy.test.yaml

# Rerun just the failing case, by name:
flow test --run 'rolls back on a 500' deploy.test.yaml

# As a report CI can parse instead of scraping stderr:
flow test -o jsonl examples/`,
	}

	addOutputFlag(cmd)

	// Opt-in, fail-closed once opted in: coverage is a result every run
	// reports, and this is the flag that promotes an unreached branch from a
	// line worth reading to a reason the command exits non-zero. A file exempts
	// a branch it cannot reach by recording it under `coverage.allow_unreached`
	// with a reason (see flowtest.CoverageStanza).
	cmd.Flags().Bool("coverage-required", false,
		"fail when a workflow has a step, or a `switch:` arm, no test case reached and no "+
			"coverage.allow_unreached entry records why")

	// The same opt-in shape for the warning tier (#926, #1356): a warning is a
	// fact worth reading that is not a verdict, and this is the flag that
	// promotes it to a reason the command exits non-zero. Three classes: an
	// idle stub (declared and never answered through), an unstubbed task
	// (invoked with no stub declared for it), and an unmatched invocation
	// (stubs declared but none matching). Stubs inherited from `defaults:`
	// are exempt from the idle-stub warning (see flowtest's
	// unusedStubWarnings), so a file-level catch-all never trips a suite
	// that opted in.
	cmd.Flags().Bool("fail-on-warning", false,
		"fail when a case reports a warning — a stub declared and never answered through, "+
			"a task invoked with no stub declared, or an invocation that no declared stub answered — "+
			"instead of only printing it")

	// The `go test -run` precedent (#929 slice 1). The honesty line is the
	// non-negotiable half of the feature: a skip must be a decision you can
	// read (the gate's printed-line rule, the schedules "nothing was explored"
	// line), so every filtered run says how many cases it left out, in the
	// output and on the summary line, and a green over a subset can never be
	// mistaken for the file's green.
	cmd.Flags().String("run", "",
		"run only the cases whose name matches this regular expression; the output says how "+
			"many cases were filtered out, and --coverage-required is refused alongside it, "+
			"because a subset's coverage gaps are not the suite's")

	// Off by default, and the default is the whole of the compatibility promise:
	// at zero seeds every case runs exactly once, under v1.WrittenOrder, which is
	// what `flow test` has always done. See flowtest's schedules.go.
	cmd.Flags().Int("seeds", 0,
		"also run every case under N seeded schedules of the local driver's own choices "+
			"(`parallel:` branch order, where an `async:` step's work happens), and fail when a "+
			"case's observables depend on which one ran; 0, the default, runs written order only")
	cmd.Flags().Uint64("seed0", dst.DefaultSeed0,
		"the first seed --seeds walks upward from, to move the search to a different part of "+
			"the seed space")
	cmd.Flags().Uint64("seed", 0,
		"replay exactly one schedule, the seed a reported divergence names, instead of searching")

	// The step debugger (#928 slice 1). Interactive by nature, so it is
	// refused wherever "interactive" is not true of the run: a machine-format
	// run whose document a prompt would corrupt, a seeded exploration that
	// runs a case many times, and a selection that is not exactly one case.
	cmd.Flags().Bool("debug", false,
		"stop before each step of one case and read commands from the terminal — step, "+
			"continue, until, break, inspect, scope, quit; requires --run to name exactly "+
			"one case, and is refused with --output json and with seeded exploration")

	return cmd
}

// scheduleBudget reads the three seed flags into the budget
// [flowtest.RunFileUnderSchedules] spends, or refuses the combination.
//
// Refuses rather than resolves, in all four shapes, because every one of them is
// a person asking for exploration and not getting what they asked for. A flag
// that silently does nothing is the same failure as a check that silently does
// not run: `--seed0 7` with no `--seeds` explores nothing while reading like it
// explores from 7; `--seeds 24 --seed 7` explores one schedule while reading like
// it explores 24; and `--seed 7 --seed0 3` reads like it does something with 3,
// while [dst.Budget] ignores Seed0 outright whenever Pinned is set. Reported by
// Codex on picatz/flowstate#814.
//
// The bound on `--seeds` is [dst.MaxSchedules] and is the same number, for the
// same reason, as the one the Go tier enforces on FLOWSTATE_DST_SCHEDULES: a
// schedule is a whole run of every case in the file, so the cost is linear in
// it.
func scheduleBudget(cmd *cobra.Command) (dst.Budget, error) {
	seeds, _ := cmd.Flags().GetInt("seeds")
	seed0, _ := cmd.Flags().GetUint64("seed0")
	seed, _ := cmd.Flags().GetUint64("seed")

	pinned := cmd.Flags().Changed("seed")

	switch {
	case seeds < 0:
		return dst.Budget{}, fmt.Errorf("--seeds %d is not a count of schedules; write a non-negative integer", seeds)
	case seeds > dst.MaxSchedules:
		return dst.Budget{}, fmt.Errorf(
			"--seeds %d is above the %d this command will explore in one run; a schedule runs every "+
				"case in the file again, so the cost is linear in this number", seeds, dst.MaxSchedules)
	case pinned && seeds > 0:
		return dst.Budget{}, errors.New(
			"--seed replays one schedule and --seeds searches many; pass one or the other")
	case pinned && cmd.Flags().Changed("seed0"):
		return dst.Budget{}, errors.New(
			"--seed replays the one schedule it names, so there is no search for --seed0 to start; " +
				"drop --seed0, or replace --seed with --seeds N")
	case cmd.Flags().Changed("seed0") && seeds == 0:
		return dst.Budget{}, errors.New(
			"--seed0 names where --seeds starts walking, and no --seeds was given, so nothing " +
				"would be explored; pass --seeds N")
	}

	budget := dst.Budget{Schedules: seeds, Seed0: seed0}
	if pinned {
		budget.Pinned = &seed
	}

	return budget, nil
}

// errTestsFailed reports that at least one case did not pass. It carries no
// message of its own because the diagnostics have already been printed or
// are in the machine report.
var errTestsFailed = errors.New("tests failed")

// runTest discovers and runs every *.test.yaml under the paths given.
func runTest(cmd *cobra.Command, paths []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}
	coverageRequired, _ := cmd.Flags().GetBool("coverage-required")
	failOnWarning, _ := cmd.Flags().GetBool("fail-on-warning")
	// The root's own persistent -v/--verbose, reused rather than shadowed
	// (#929 slice 2): under `flow test` it additionally means "show every
	// case's transcript", the `go test -v` reading, while a failing case
	// prints its transcript regardless — the account is the whole point of
	// having one when something failed.
	verbose, _ := cmd.Flags().GetBool("verbose")

	budget, err := scheduleBudget(cmd)
	if err != nil {
		return err
	}

	// The filter (#929 slice 1). Refusals before anything runs, both fail
	// closed: a pattern that does not compile selects nothing knowable, and
	// `--coverage-required` over a subset would enforce the suite's bar
	// against a run that deliberately is not the suite.
	runPattern, _ := cmd.Flags().GetString("run")
	var selectCase func(string) bool
	if runPattern != "" {
		if coverageRequired {
			return errors.New("--coverage-required cannot be combined with --run: coverage is a property " +
				"of the whole suite, and a filtered subset's gaps are not the suite's; drop one of the two")
		}
		re, err := regexp.Compile(runPattern)
		if err != nil {
			return fmt.Errorf("--run %q is not a valid regular expression: %w", runPattern, err)
		}
		selectCase = re.MatchString
	}

	files, err := collectTestFiles(paths)
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	machine := format.Machine()

	// The debugger's own refusals, all of them stating what is incompatible
	// rather than quietly doing something else. Each is a shape where the
	// word "interactive" stops being true of the run, and a session that
	// attached anyway would be a prompt nobody is answering.
	debugging, _ := cmd.Flags().GetBool("debug")
	var session *flowdebug.Session
	restoreTerminal := func() {}
	if debugging {
		if session, restoreTerminal, err = debugSession(cmd, surface, machine, budget, files, selectCase); err != nil {
			return err
		}
		// A terminal the session put into raw mode, put back — before this
		// command returns and whatever it returns, because a shell handed back
		// a raw terminal is one where nothing a person types appears. Called
		// again below the moment the run is over, and idempotent for that
		// reason: this one is the safety net.
		defer restoreTerminal()
		// The session's reader is this command's to release — see
		// [flowdebug.Session.Close]. Free here, since the process exits after;
		// closed anyway, because the surface where it is not free
		// (`flow mcp serve`) is served by the same habit rather than a
		// different one.
		defer func() { _ = session.Close() }()
	}

	started := time.Now()

	var (
		anyFailed bool
		results   []testFileResult
	)
	for _, path := range files {
		// cmd.Context() rather than a background one: `flow test` is where a
		// legal Flowfile can park forever (a `wait_for_signal:` with no timeout
		// and no scripted signal has no deadline the virtual clock can advance
		// to), and `--seeds N` multiplies whatever that costs by N. The signal
		// context main installs is what makes ^C end it. See
		// [flowtest.RunSourceContext] for the same bound on the serving side.
		// Coverage arrives already attached to the report — flowtest's runSuite
		// owns that now, for every door at once, so the MCP tool and this
		// command cannot disagree about what the document carries (#931).
		run := flowtest.RunPath(cmd.Context(), path, flowtest.RunOptions{
			Budget: budget,
			Select: selectCase,
			// nil unless --debug, and a nil interface value in this field is
			// what every other run in the world passes: the engine's boundary
			// does one context lookup and finds nothing.
			Debugger: debuggerOrNil(session),
		})
		// The console owned the line for as long as the run did — the autopsy
		// is inside that call — and no longer: the report below prints onto
		// what should be an ordinary terminal again, and a transcript printed
		// in raw mode walks down the screen a column at a time. Idempotent, so
		// the loop's other iterations (there are none under `--debug`, which
		// refuses more than one file) cost nothing.
		restoreTerminal()

		report, coverage, schedules := run.Report, run.Coverage, run.Schedules
		result := testFileResult{report: report, coverage: coverage, schedules: schedules, filtered: run.Filtered}
		results = append(results, result)

		if !machine {
			printTestReport(surface.Out, surface.Theme, report, run.Transcripts, failOnWarning, verbose)
			printCoverage(surface.Out, surface.Theme, report, coverage, coverageRequired)
			printSchedules(surface.Out, surface.Theme, report, schedules)
			printFiltered(surface.Out, surface.Theme, report, runPattern, run.Filtered)
		} else {
			// The account of the exploration goes to stderr in machine mode, so
			// stdout stays exactly the JSON document a consumer parses while the
			// honesty line — how many schedules ran, and how many scheduling
			// decisions they actually made — is still somewhere a person or a CI
			// log can see it. A `--seeds` run whose exploration was silent would
			// be a green nobody could check. The document carries the same
			// account in its `schedules` schema field ([v1.ScheduleExploration],
			// issue #931), attached by flowtest's runSuite; this prose is the
			// person-facing half, not the only copy.
			printSchedules(surface.Err, surface.ErrTheme, report, schedules)
			// The filter's honesty line goes to stderr for the same reason:
			// the cases the document carries are only the selected ones, and
			// somebody reading the log must be able to see that the file held
			// more (#929 slice 1; slice 1 adds no schema field).
			printFiltered(surface.Err, surface.ErrTheme, report, runPattern, run.Filtered)
		}

		if result.failed(coverageRequired, failOnWarning) {
			anyFailed = true
		}
	}

	if machine {
		if err := writeTestResults(surface, format, results); err != nil {
			return err
		}
	} else {
		printSummary(surface.Out, surface.Theme, results, coverageRequired, failOnWarning, runPattern != "", time.Since(started))
	}

	if anyFailed {
		return errTestsFailed
	}
	return nil
}

// printSummary ends a text-mode run with the whole-run account (#936): how
// many files and cases ran, how many passed, and the wall time — so a CI log
// ends with a total rather than mid-list, and a person reads one line instead
// of scanning backward for red. What decides the exit code leads the line and
// renders in the danger style — every reason [testFileResult.failed] can
// answer true, because a summary reading green above a non-zero exit would be
// the one lie this line exists to prevent: failed cases, refused files,
// warnings where `--fail-on-warning` promoted them, schedule divergences, and
// — only when `--coverage-required` opted them in — coverage gaps and stale
// records. The two flag-gated counts appear only under their flag, on the
// package doc's rule: the color, and the count, are the claim.
//
// Text mode only: the machine formats carry every one of these counts
// structurally in `cases[]`, `coverage[]` and `schedules`, so stdout's JSON
// is untouched. Wall time is wall-clock, the "is the suite still fast"
// number, never a reading of virtual time — [v1.TestCase]'s own duration
// field states that distinction.
func printSummary(out io.Writer, theme ui.Theme, results []testFileResult, coverageRequired, failOnWarning, filterActive bool, elapsed time.Duration) {
	files, cases, passed, failed, refused := len(results), 0, 0, 0, 0
	gaps, stale, filtered, warned, diverged := 0, 0, 0, 0, 0
	for _, r := range results {
		if r.report.GetRefused() != "" {
			refused++
		}
		for _, c := range r.report.GetCases() {
			cases++
			if c.GetPassed() {
				passed++
			} else {
				failed++
			}
			if failOnWarning && len(c.GetWarnings()) > 0 {
				warned++
			}
		}
		filtered += r.filtered
		if r.schedules != nil && r.schedules.Divergence != nil {
			diverged++
		}
		if coverageRequired {
			for _, cov := range r.coverage {
				gaps += len(cov.Gaps()) + len(cov.ArmGaps())
				stale += len(cov.Stale)
			}
		}
	}

	var parts []string
	if failed > 0 {
		parts = append(parts, theme.Danger.Render(fmt.Sprintf("%d failed", failed)))
	}
	if refused > 0 {
		parts = append(parts, theme.Danger.Render(count(refused, "file refused", "files refused")))
	}
	// Counted only under `--fail-on-warning`: without it a warning does not
	// decide the exit code, and a danger-styled count of it here would be the
	// inverse of the lie this line prevents. The cases themselves still say
	// `passed` — a promoted warning fails the run, not the case's
	// expectations, which is exactly what this wording draws apart.
	if warned > 0 {
		parts = append(parts, theme.Danger.Render(count(warned, "case failed on warnings", "cases failed on warnings")))
	}
	if diverged > 0 {
		parts = append(parts, theme.Danger.Render(count(diverged, "schedule divergence", "schedule divergences")))
	}
	if gaps > 0 {
		parts = append(parts, theme.Danger.Render(count(gaps, "coverage gap", "coverage gaps")))
	}
	if stale > 0 {
		parts = append(parts, theme.Danger.Render(count(stale, "stale coverage record", "stale coverage records")))
	}
	parts = append(parts,
		count(files, "file", "files"),
		count(cases, "case", "cases"),
		fmt.Sprintf("%d passed", passed),
	)
	// Whenever the filter was active, even at zero: the count is what keeps a
	// green over a subset from reading as the file's green, and "0 filtered"
	// says the pattern happened to exclude nothing, which is itself worth
	// knowing. Warning tier — a fact worth reading, never a verdict.
	if filterActive {
		parts = append(parts, theme.Warning.Render(count(filtered, "case filtered out", "cases filtered out")))
	}
	parts = append(parts, fmt.Sprintf("%.1fs", elapsed.Seconds()))

	fmt.Fprintf(out, "\n%s\n", strings.Join(parts, " · "))
}

// printFiltered says, beside one file, what `--run` left out of it (#929
// slice 1): the skip-is-a-decision-you-can-read rule, applied per file, so a
// reader matching the report back to the file knows the file holds more than
// the report shows. Nothing prints without the flag; a refused file prints
// nothing either, since no case of it was selected or filtered.
func printFiltered(out io.Writer, theme ui.Theme, report *v1.TestReport, pattern string, filtered int) {
	if pattern == "" || report.GetRefused() != "" {
		return
	}

	ran := len(report.GetCases())
	fmt.Fprintf(out, "%s  %s\n", theme.Muted.Render(report.GetFile()), theme.Warning.Render(
		fmt.Sprintf("--run %q: ran %d of %s, %d filtered out",
			pattern, ran, count(ran+filtered, "case", "cases"), filtered)))
}

// testFileResult pairs one file's report with the branch coverage its cases
// achieved, one entry per workflow the file targeted, so the two travel
// together into rendering. coverage is nil for a file with no workflow to
// account for (a refused file); see [flowtest.RunFileWithCoverage]. schedules is
// nil unless `--seeds`/`--seed` asked for schedule exploration.
type testFileResult struct {
	report    *v1.TestReport
	coverage  []*flowtest.Coverage
	schedules *flowtest.ScheduleReport

	// filtered is how many of this file's cases `--run` excluded — the number
	// the honesty line and the summary surface, because a green over a subset
	// must never read as the file's green (#929).
	filtered int
}

// failed reports whether this file's result makes the command exit non-zero.
//
// One function rather than a run of conditions inside the loop, because the
// four reasons a file fails are four different kinds of claim and each needs
// saying: a case that did not pass, a coverage gap the run opted in to treating
// as one, a warning the run opted in to treating as one, and a schedule that
// changed what a case observed.
func (r testFileResult) failed(coverageRequired, failOnWarning bool) bool {
	if r.report.GetRefused() != "" {
		return true
	}

	for _, c := range r.report.GetCases() {
		if !c.GetPassed() {
			return true
		}
	}

	// A failure only when the run opted in, exactly as coverage below: a
	// warning is a fact about the case's own scaffolding, not a verdict, until
	// `--fail-on-warning` says a suite wants it enforced (#926).
	if failOnWarning {
		for _, c := range r.report.GetCases() {
			if len(c.GetWarnings()) > 0 {
				return true
			}
		}
	}

	// A failure only when the run opted in: coverage is otherwise a result,
	// not a verdict. Both an unrecorded gap and a stale record fail, because
	// a record that no longer describes a real residual is a false statement
	// about the suite, not a smaller one than a gap. Checked per workflow the
	// file targets, so a gap in one workflow is not masked by another
	// (Finding 3).
	if coverageRequired {
		for _, c := range r.coverage {
			if len(c.Gaps()) > 0 || len(c.ArmGaps()) > 0 || len(c.Stale) > 0 {
				return true
			}
		}
	}

	// A divergence needs no opt-in beyond the `--seeds` that found it: asking
	// for the schedule space to be explored is already the opt-in, and a case
	// whose observables move with the schedule is a finding, not a statistic.
	return r.schedules != nil && r.schedules.Divergence != nil
}

// printSchedules renders what seeded schedule exploration found for one file:
// nothing at all when nobody asked for it, one summary line when every schedule
// agreed, and the divergence with the command that replays it when one did not.
//
// The summary line carries the decision count even on the happy path, and that
// is the point of it rather than decoration. A workflow with no `parallel:` and
// no `async:` reaches no junction, so a scheduler is never asked anything and
// every schedule of it *is* written order — a green from exploring it is a green
// from exploring nothing. Printing "0 scheduling decisions" and saying what that
// means is what stops `--seeds 500` on such a file from reading as evidence.
// [flowtest.ScheduleReport.Decisions] is the number; this is where an author
// meets it.
func printSchedules(out io.Writer, theme ui.Theme, report *v1.TestReport, schedules *flowtest.ScheduleReport) {
	if schedules == nil {
		return
	}

	file := theme.Muted.Render(report.GetFile())

	summary := fmt.Sprintf("%s explored per case over %s, up to %s",
		count(schedules.Schedules, "schedule", "schedules"),
		count(schedules.Cases, "case", "cases"),
		count(schedules.Decisions, "scheduling decision", "scheduling decisions"))
	if schedules.Decisions == 0 {
		summary += "; " + theme.Warning.Render(
			"nothing was explored: no case reached a `parallel:` or `async:` junction, "+
				"so every schedule was written order")
	}
	if schedules.Truncated {
		summary += "; " + theme.Warning.Render(fmt.Sprintf(
			"a schedule spent its whole %d-decision budget and took written order for the rest of its run",
			v1.MaxScheduleDecisions))
	}
	fmt.Fprintf(out, "%s  %s\n", file, summary)

	divergence := schedules.Divergence
	if divergence == nil {
		return
	}

	fmt.Fprintf(out, "%s  %s: %s\n", file, divergence.Case, theme.Danger.Render(
		fmt.Sprintf("the schedule changed what this case observed (seed %d)", divergence.Seed)))

	explanation := []string{
		fmt.Sprintf("Seed %d produced observables the written-order run did not.", divergence.Seed),
		"`flow test` explores only the orderings the LOCAL driver is free to choose —",
		"a `parallel:` block's branch order, and whether an `async:` step's work happens",
		"where it is written or at its join. So this says your file's observables depend",
		"on one of those, or that the local engine does. It is not a claim about",
		"Temporal's orderings.",
	}
	if divergence.Truncated {
		explanation = append(explanation, fmt.Sprintf(
			"This schedule spent its whole %d-decision budget and took written order for the",
			v1.MaxScheduleDecisions), "rest of its run, so what it explored stops before the bound.")
	}
	for _, line := range explanation {
		fmt.Fprintf(out, "       %s\n", line)
	}

	fmt.Fprintf(out, "\n       REPLAY THIS EXACT SCHEDULE:\n\n           flow test --seed %d -- %s\n\n",
		divergence.Seed, shellArg(report.GetFile()))
	fmt.Fprintf(out, "       written order:\n%s", indentRendering(divergence.WrittenOrder))
	fmt.Fprintf(out, "       seed %d (%d scheduling decisions):\n%s",
		divergence.Seed, divergence.Decisions, indentRendering(divergence.Seeded))
}

// shellArg renders one path as a shell-safe argument for a command a human is
// told to copy and run. A path with a space or a shell metacharacter would
// otherwise split or be interpreted, and the "exact" replay command would not
// replay the failing file — the one job that line has. Single quotes disable
// every shell expansion, and an embedded single quote uses the standard
// '"'"'-style splice; a path needing neither is left bare so the common case
// stays readable. The `--` its caller prints before it handles the remaining
// hazard, a path beginning with `-` parsing as a flag.
func shellArg(path string) string {
	if path != "" && !strings.ContainsAny(path, " \t'\"\\$&|;<>()*?[]#~%{}!\n") {
		return path
	}
	return "'" + strings.ReplaceAll(path, "'", `'"'"'`) + "'"
}

// count renders a number with the noun that agrees with it.
//
// A line an author reads on every `--seeds` run, including the one that says
// nothing was explored, so "1 cases" and "0 scheduling decision" are not
// acceptable: the whole job of that line is to be believed.
func count(n int, singular, plural string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, singular)
	}

	return fmt.Sprintf("%d %s", n, plural)
}

// indentRendering shifts a rendering right so a divergence reads as two blocks
// rather than as one wall, the same shape [dst.FailureText] gives the Go tier's
// version of this failure.
func indentRendering(text string) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	for i, line := range lines {
		lines[i] = "           " + line
	}

	return strings.Join(lines, "\n") + "\n"
}

// printTestReport renders one file's report in the CLI's ordinary text style:
// one line per case, a diagnostic per unmet expectation for a case that
// failed, and a line per warning — coloured as a fault only where the run
// opted in to treat one as such (`--fail-on-warning`), on the exact rule
// [printCoverage] applies to an unrecorded gap.
//
// A failing case's transcript prints beneath its diagnostics (#929 slice 2):
// the unmet expectation arrives with the account it was judged against —
// which stub answered each step, when virtual time moved, what each step
// produced — instead of alone. verbose (`-v`) prints every case's, the
// `go test -v` reading of the flag.
func printTestReport(out io.Writer, theme ui.Theme, report *v1.TestReport, transcripts [][]flowtest.TranscriptLine, failOnWarning, verbose bool) {
	if refused := report.GetRefused(); refused != "" {
		fmt.Fprintf(out, "%s: %s\n", theme.Muted.Render(report.GetFile()), theme.Danger.Render(refused))
		return
	}

	for i, c := range report.GetCases() {
		status := theme.Success.Render("PASS")
		if !c.GetPassed() {
			status = theme.Danger.Render("FAIL")
		}
		fmt.Fprintf(out, "%s  %s: %s\n", status, theme.Muted.Render(report.GetFile()), c.GetName())

		if c.GetError() != "" {
			fmt.Fprintf(out, "       %s\n", c.GetError())
			continue
		}
		for _, f := range c.GetFailures() {
			// `file:line:column:` first, the way `flow validate` writes a
			// diagnostic, so an editor's problem matcher opens the test file at
			// the claim rather than at its first line (#1558). Omitted when the
			// claim has no position — a key the author never wrote — since a
			// prefix pointing at line 0 would be worse than none.
			where := ""
			if f.GetLine() != 0 {
				where = theme.Muted.Render(fmt.Sprintf("%s:%d:%d: ",
					report.GetFile(), f.GetLine(), f.GetColumn()))
			}

			if f.GetStep() != "" {
				fmt.Fprintf(out, "       %s%s (step %q): %s\n", where, f.GetField(), f.GetStep(), f.GetMessage())
				continue
			}
			fmt.Fprintf(out, "       %s%s: %s\n", where, f.GetField(), f.GetMessage())
		}
		for _, w := range c.GetWarnings() {
			line := fmt.Sprintf("%s: %s", w.GetField(), w.GetMessage())
			if failOnWarning {
				line = theme.Danger.Render(line)
			} else {
				line = theme.Warning.Render(line)
			}
			fmt.Fprintf(out, "       %s\n", line)
		}

		if (verbose || !c.GetPassed()) && i < len(transcripts) {
			printTranscript(out, theme, transcripts[i])
		}
	}
}

// printTranscript renders one case's account, each line in the tone flowtest
// assigned it — the package doc's color rule, decided where the fact was
// recorded rather than re-derived here.
func printTranscript(out io.Writer, theme ui.Theme, lines []flowtest.TranscriptLine) {
	for _, line := range lines {
		text := line.Text
		switch line.Tone {
		case flowtest.ToneDanger:
			text = theme.Danger.Render(text)
		case flowtest.ToneWarning:
			text = theme.Warning.Render(text)
		}
		fmt.Fprintf(out, "     %s\n", text)
	}
}

// printCoverage renders one file's branch coverage, one line per workflow the
// file's cases targeted: how many of that workflow's steps at least one case
// reached, and the complement it never did. A file testing one workflow, the
// ordinary case, prints exactly one line.
//
// A result, not a diagnostic (#420): `flow validate` has no warning tier, but
// `flow test` owns its own output and a coverage line is an account of what the
// suite exercised. An unreached step with no recorded reason is a gap, coloured
// to be found; one the file recorded under `coverage.allow_unreached` is an
// accepted residual, named plainly with the reason. A stale record, a reason
// kept past the branch it explained, is called out on its own line so it is not
// mistaken for either.
func printCoverage(out io.Writer, theme ui.Theme, report *v1.TestReport, coverage []*flowtest.Coverage, required bool) {
	// Labelled by the test file, not the workflow, so a file testing one
	// workflow prints the exact line it always did. A file targeting several
	// prints one line each, sharing the label and distinguished by content and
	// by the per-workflow identity the machine report carries.
	file := theme.Muted.Render(report.GetFile())
	for _, cov := range coverage {
		summary := fmt.Sprintf("%d/%d steps reached", len(cov.Reached), cov.Total())
		// Only where the workflow has a switch, so a file without one prints the
		// exact line it always did rather than a second number reading "0/0".
		if len(cov.Arms) > 0 {
			summary += fmt.Sprintf(", %d/%d switch arms taken", cov.ArmsReached(), len(cov.Arms))
		}

		gaps := cov.Gaps()
		tail := ""
		if len(gaps) > 0 {
			phrase := "never ran: " + strings.Join(gaps, ", ")
			// Coloured as a fault only where the run opted in to treat it as
			// one; otherwise it is a fact worth reading, not a failure.
			if required {
				phrase = theme.Danger.Render(phrase)
			} else {
				phrase = theme.Warning.Render(phrase)
			}
			tail += "; " + phrase
		}
		if len(cov.Accepted) > 0 {
			accepted := make([]string, 0, len(cov.Accepted))
			for step := range cov.Accepted {
				accepted = append(accepted, step)
			}
			sort.Strings(accepted)
			tail += "; " + theme.Muted.Render("accepted-unreached: "+strings.Join(accepted, ", "))
		}

		fmt.Fprintf(out, "%s  %s%s\n", file, summary, tail)

		printArmGaps(out, theme, cov, required)

		for _, stale := range cov.Stale {
			fmt.Fprintf(out, "       %s\n", theme.Danger.Render(stale))
		}
	}
}

// printArmGaps names each switch arm no case took, one positioned line each.
//
// A line of its own rather than a name on the summary line, because an arm has
// no name to put there. A step has an id an author can search for; `case
// "synchronize"` is one of possibly several in one step and is findable only by
// where it is written — which is the whole reason issue #801 threads positions
// through at all. So these follow the `flowfile/validate.go` diagnostic
// standard: the file, the line and column, what is wrong, and what to do.
//
// An arm the file recorded a reason for is not printed here. It is an accepted
// residual, already named on the line below as a decision rather than a hole,
// exactly as an accepted step is.
func printArmGaps(out io.Writer, theme ui.Theme, cov *flowtest.Coverage, required bool) {
	for _, arm := range cov.ArmGaps() {
		where := cov.Workflow
		if arm.Where.IsValid() {
			where += ":" + arm.Where.Start.String()
		}

		message := fmt.Sprintf("%s: %s of switch %q was taken by no test case; add a case whose "+
			"inputs reach it, or record why under coverage.allow_unreached: %s",
			where, arm.Label, arm.Step, arm.Key)
		if required {
			message = theme.Danger.Render(message)
		} else {
			message = theme.Warning.Render(message)
		}

		fmt.Fprintf(out, "       %s\n", message)
	}

	accepted := make([]string, 0, len(cov.Arms))
	for _, arm := range cov.Arms {
		if !arm.Reached && arm.Reason != "" {
			accepted = append(accepted, arm.Key)
		}
	}
	if len(accepted) > 0 {
		fmt.Fprintf(out, "       %s\n",
			theme.Muted.Render("accepted-unreached arms: "+strings.Join(accepted, ", ")))
	}
}

// writeTestResults emits the machine report.
//
// Coverage is a schema field on each [v1.TestReport] now
// ([v1.TestReport.Coverage]), attached before this is called, so the whole
// document renders through protojson (via [v1.MarshalSchemaJSON]): one encoder, the
// schema's field names and enum spellings, and no second rendering of the
// report to disagree with the first, which is the mixing the house rule against
// "one thing spelled twice" warns about. JSONL emits one report per line; JSON
// wraps them in a [v1.TestReports] envelope, the same `{"files": [...]}` shape
// as before.
func writeTestResults(surface *ui.UI, format OutputFormat, results []testFileResult) error {
	if format == FormatJSONL {
		for _, r := range results {
			encoded, err := v1.MarshalSchemaJSON(r.report, false)
			if err != nil {
				return fmt.Errorf("rendering a test report: %w", err)
			}
			if _, err := fmt.Fprintf(surface.Out, "%s\n", encoded); err != nil {
				return err
			}
		}
		return nil
	}

	reports := &v1.TestReports{Files: make([]*v1.TestReport, 0, len(results))}
	for _, r := range results {
		reports.Files = append(reports.Files, r.report)
	}
	encoded, err := v1.MarshalSchemaJSON(reports, true)
	if err != nil {
		return fmt.Errorf("rendering the test report as %s: %w", format, err)
	}
	_, err = fmt.Fprintf(surface.Out, "%s\n", encoded)
	return err
}

// collectTestFiles expands the paths given into the *.test.yaml files to run,
// on the same rule [collectFlowfiles] uses for a Flowfile: a named file is
// taken as given, a directory is walked.
func collectTestFiles(paths []string) ([]string, error) {
	var out []string
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("error reading %s: %w", path, err)
		}
		if !info.IsDir() {
			out = append(out, path)
			continue
		}
		err = filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if strings.HasSuffix(p, ".test.yaml") || strings.HasSuffix(p, ".test.yml") {
				out = append(out, p)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("error walking %s: %w", path, err)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no *.test.yaml files found in the paths given")
	}
	return out, nil
}
