package main

import (
	"encoding/json"
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
		// about the suite, not a smaller one than a gap.
		if coverageRequired && coverage != nil && (len(coverage.Gaps()) > 0 || len(coverage.Stale) > 0) {
			anyFailed = true
		}
	}

	if machine {
		if err := writeTestResults(surface, format, results, coverageRequired); err != nil {
			return err
		}
	}

	if anyFailed {
		return errTestsFailed
	}
	return nil
}

// testFileResult pairs one file's report with the branch coverage its cases
// achieved, so the two travel together into rendering. coverage is nil for a
// file with no workflow to account for (a refused file); see
// [flowtest.RunFileWithCoverage].
type testFileResult struct {
	report   *v1.TestReport
	coverage *flowtest.Coverage
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

// printCoverage renders one file's branch coverage: how many of the workflow's
// steps at least one case reached, and the complement it never did.
//
// A result, not a diagnostic (#420): `flow validate` has no warning tier, but
// `flow test` owns its own output and a coverage line is an account of what the
// suite exercised. An unreached step with no recorded reason is a gap, coloured
// to be found; one the file recorded under `coverage.allow_unreached` is an
// accepted residual, named plainly with the reason. A stale record, a reason
// kept past the branch it explained, is called out on its own line so it is not
// mistaken for either.
func printCoverage(out io.Writer, theme ui.Theme, report *v1.TestReport, coverage *flowtest.Coverage, required bool) {
	if coverage == nil {
		return
	}

	file := theme.Muted.Render(report.GetFile())
	summary := fmt.Sprintf("%d/%d steps reached", len(coverage.Reached), coverage.Total())

	gaps := coverage.Gaps()
	tail := ""
	if len(gaps) > 0 {
		phrase := "never ran: " + strings.Join(gaps, ", ")
		// Coloured as a fault only where the run opted in to treat it as one;
		// otherwise it is a fact worth reading, not a failure.
		if required {
			phrase = theme.Danger.Render(phrase)
		} else {
			phrase = theme.Warning.Render(phrase)
		}
		tail += "; " + phrase
	}
	if len(coverage.Accepted) > 0 {
		accepted := make([]string, 0, len(coverage.Accepted))
		for step := range coverage.Accepted {
			accepted = append(accepted, step)
		}
		sort.Strings(accepted)
		tail += "; " + theme.Muted.Render("accepted-unreached: "+strings.Join(accepted, ", "))
	}

	fmt.Fprintf(out, "%s  %s%s\n", file, summary, tail)

	for _, stale := range coverage.Stale {
		fmt.Fprintf(out, "       %s\n", theme.Danger.Render(stale))
	}
}

// coverageDoc is coverage as it rides the machine output: a `coverage` key
// merged into each file's report object, so a consumer already reading
// `.files[].cases` finds `.files[].coverage` beside it.
//
// Field names follow protojson's camelCase so the whole document reads with one
// convention, even though this part is hand-shaped rather than schema-derived
// (issue #420 keeps coverage out of the schema).
type coverageDoc struct {
	StepsTotal   int               `json:"stepsTotal"`
	StepsReached int               `json:"stepsReached"`
	Reached      []string          `json:"reached"`
	Unreached    []string          `json:"unreached"`
	Gaps         []string          `json:"gaps"`
	Accepted     map[string]string `json:"accepted"`
	Stale        []string          `json:"stale"`
	Required     bool              `json:"required"`
}

func newCoverageDoc(coverage *flowtest.Coverage, required bool) *coverageDoc {
	doc := &coverageDoc{
		StepsTotal:   coverage.Total(),
		StepsReached: len(coverage.Reached),
		Reached:      coverage.Reached,
		Unreached:    coverage.Unreached,
		Gaps:         coverage.Gaps(),
		Accepted:     coverage.Accepted,
		Stale:        coverage.Stale,
		Required:     required,
	}
	// Empty slices rather than null, matching protojson's EmitUnpopulated
	// posture on the schema fields beside them: a consumer indexing the array
	// finds a list to range over rather than a null to guard.
	if doc.Reached == nil {
		doc.Reached = []string{}
	}
	if doc.Unreached == nil {
		doc.Unreached = []string{}
	}
	if doc.Gaps == nil {
		doc.Gaps = []string{}
	}
	if doc.Accepted == nil {
		doc.Accepted = map[string]string{}
	}
	if doc.Stale == nil {
		doc.Stale = []string{}
	}
	return doc
}

// writeTestResults emits the machine report, merging each file's coverage into
// its report object.
//
// The report fields are rendered by protojson (through [marshalJSON]) and pass
// through untouched as raw JSON; only the `coverage` object and the wrapper are
// hand-shaped. So the schema half keeps protojson's field names and enum
// spellings, and there is no second rendering of the report to disagree with
// the first, which is the mixing the house rule against "one thing spelled
// twice" warns about.
func writeTestResults(surface *ui.UI, format OutputFormat, results []testFileResult, required bool) error {
	objects := make([]json.RawMessage, 0, len(results))
	for _, r := range results {
		merged, err := mergeCoverageIntoReport(r.report, r.coverage, required)
		if err != nil {
			return err
		}
		objects = append(objects, merged)
	}

	if format == FormatJSONL {
		for _, obj := range objects {
			if _, err := fmt.Fprintf(surface.Out, "%s\n", obj); err != nil {
				return err
			}
		}
		return nil
	}

	document, err := json.MarshalIndent(map[string]any{"files": objects}, "", "  ")
	if err != nil {
		return fmt.Errorf("rendering the test report as %s: %w", format, err)
	}
	_, err = fmt.Fprintf(surface.Out, "%s\n", document)
	return err
}

// mergeCoverageIntoReport renders one TestReport through protojson and adds a
// `coverage` key beside its fields. A file with no coverage to account for gets
// no key rather than a null one, the same way it gets no coverage line.
func mergeCoverageIntoReport(report *v1.TestReport, coverage *flowtest.Coverage, required bool) (json.RawMessage, error) {
	encoded, err := marshalJSON(report, false)
	if err != nil {
		return nil, fmt.Errorf("rendering a test report: %w", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, fmt.Errorf("reading back a rendered test report: %w", err)
	}

	if coverage != nil {
		covBytes, err := json.Marshal(newCoverageDoc(coverage, required))
		if err != nil {
			return nil, fmt.Errorf("rendering coverage: %w", err)
		}
		fields["coverage"] = covBytes
	}

	merged, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("assembling a test report: %w", err)
	}
	return merged, nil
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
