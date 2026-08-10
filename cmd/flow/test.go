package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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
			"`--output json` or `--output jsonl` reports what ran as a schema message instead of " +
			"text, and carries the coverage sets under a `coverage` key so CI annotates rather than " +
			"parses prose.",
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
		"fail when a workflow has a step no test case reached and no coverage.allow_unreached "+
			"entry records why")

	return cmd
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

	files, err := collectTestFiles(paths)
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	machine := format.Machine()

	var (
		anyFailed bool
		results   []testFileResult
	)
	for _, path := range files {
		report, coverage := flowtest.RunFileWithCoverage(path)
		// Attach each workflow's coverage to the report so the whole document
		// renders through protojson: there is one rendering of the report and no
		// second, hand-shaped encoder beside it to disagree with the first.
		for _, c := range coverage {
			report.Coverage = append(report.Coverage, c.Report())
		}
		results = append(results, testFileResult{report: report, coverage: coverage})

		if !machine {
			printTestReport(surface.Out, surface.Theme, report)
			printCoverage(surface.Out, surface.Theme, report, coverage, coverageRequired)
		}

		if report.GetRefused() != "" {
			anyFailed = true
			continue
		}
		for _, c := range report.GetCases() {
			if !c.GetPassed() {
				anyFailed = true
			}
		}
		// A failure only when the run opted in: coverage is otherwise a result,
		// not a verdict. Both an unrecorded gap and a stale record fail, because
		// a record that no longer describes a real residual is a false statement
		// about the suite, not a smaller one than a gap. Checked per workflow the
		// file targets, so a gap in one workflow is not masked by another
		// (Finding 3).
		if coverageRequired {
			for _, c := range coverage {
				if len(c.Gaps()) > 0 || len(c.Stale) > 0 {
					anyFailed = true
				}
			}
		}
	}

	if machine {
		if err := writeTestResults(surface, format, results); err != nil {
			return err
		}
	}

	if anyFailed {
		return errTestsFailed
	}
	return nil
}

// testFileResult pairs one file's report with the branch coverage its cases
// achieved, one entry per workflow the file targeted, so the two travel
// together into rendering. coverage is nil for a file with no workflow to
// account for (a refused file); see [flowtest.RunFileWithCoverage].
type testFileResult struct {
	report   *v1.TestReport
	coverage []*flowtest.Coverage
}

// printTestReport renders one file's report in the CLI's ordinary text style:
// one line per case, a diagnostic per unmet expectation for a case that
// failed.
func printTestReport(out io.Writer, theme ui.Theme, report *v1.TestReport) {
	if refused := report.GetRefused(); refused != "" {
		fmt.Fprintf(out, "%s: %s\n", theme.Muted.Render(report.GetFile()), theme.Danger.Render(refused))
		return
	}

	for _, c := range report.GetCases() {
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
			if f.GetStep() != "" {
				fmt.Fprintf(out, "       %s (step %q): %s\n", f.GetField(), f.GetStep(), f.GetMessage())
				continue
			}
			fmt.Fprintf(out, "       %s: %s\n", f.GetField(), f.GetMessage())
		}
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

		for _, stale := range cov.Stale {
			fmt.Fprintf(out, "       %s\n", theme.Danger.Render(stale))
		}
	}
}

// writeTestResults emits the machine report.
//
// Coverage is a schema field on each [v1.TestReport] now
// ([v1.TestReport.Coverage]), attached before this is called, so the whole
// document renders through protojson (via [marshalJSON]): one encoder, the
// schema's field names and enum spellings, and no second rendering of the
// report to disagree with the first, which is the mixing the house rule against
// "one thing spelled twice" warns about. JSONL emits one report per line; JSON
// wraps them in a [v1.TestReports] envelope, the same `{"files": [...]}` shape
// as before.
func writeTestResults(surface *ui.UI, format OutputFormat, results []testFileResult) error {
	if format == FormatJSONL {
		for _, r := range results {
			encoded, err := marshalJSON(r.report, false)
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
	encoded, err := marshalJSON(reports, true)
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
