package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
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
			"`--output json` or `--output jsonl` reports what ran as a schema message instead of " +
			"text, for CI that wants structured data rather than stderr text.",
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

	files, err := collectTestFiles(paths)
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	machine := format.Machine()

	var (
		anyFailed bool
		reports   []*v1.TestReport
	)
	for _, path := range files {
		report := flowtest.RunFile(path)
		reports = append(reports, report)

		if !machine {
			printTestReport(surface.Out, surface.Theme, report)
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
	}

	if machine {
		if format == FormatJSONL {
			for _, report := range reports {
				if err := writeJSON(surface, format, report); err != nil {
					return err
				}
			}
		} else if err := writeJSON(surface, format, &v1.TestReports{Files: reports}); err != nil {
			return err
		}
	}

	if anyFailed {
		return errTestsFailed
	}
	return nil
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
