package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow lint` is tier 4 of the style charter (docs/STYLE.md, Part II), which
// until now was a row in that table reading "does not exist yet".
//
// # Why a verb rather than a flag on `validate`
//
// The charter sorts checks by whose problem a violation is, and gives each tier
// its own tool: `flow audit` measures, `flow validate` refuses, `flow fmt`
// normalizes, `flow fix` migrates. A style finding is none of those things — it
// is advice about a file that is correct — so it lands as the fifth verb in that
// row rather than as a mode of one of them.
//
// `flow validate --style` was the smaller surface and was rejected on what it
// would have cost, which is worth writing down because "smaller" is usually the
// right answer:
//
//   - `validate`'s exit status means the file is refused, and the machine form
//     of that answer is [v1.DiagnosticReport] — a schema message whose
//     `Diagnostic` has no severity field. Mixing advice into that stream needs
//     one, which is a change to a public contract every plugin compiles
//     against, made so that consumers can filter back out something they never
//     asked for.
//   - Every existing consumer of `flow validate -o json` would start receiving
//     entries that are not refusals. A reader that treated them as refusals
//     would be failing a build on taste, which is the exact promotion of a
//     tier-4 rule to a tier-1 refusal the charter forbids.
//   - The language server publishes what `validate` finds. Style advice on a
//     keystroke is a plausible feature and a real decision, and it should be
//     made deliberately rather than arrive as a side effect of a flag.
//
// The cost of the verb, stated rather than glossed: a fifth file-reading command
// for a reader to tell apart from `validate`, and a surface that has to stay
// distinguishable from it in help text forever. And style findings do not reach
// an editor today, because nothing but this command calls [flowfile.Lint] — a
// follow-up, and a deliberate one.
//
// # Advisory, and the one flag that changes it
//
// Zero exit on every finding, exactly as `flow audit` does, because tier 4
// warns and never blocks. `--strict` is the opt-in: it exits nonzero when there
// is anything to report, and it exists for the CI leg over `examples/`, where
// R8 holds the shown corpus to a standard the language as a whole is not held
// to. `shown ⊆ canonical ⊂ legal` is that sentence, and a flag is how one
// implementation serves both ends of it.

// newLintCommand builds the `flow lint` command.
func newLintCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "lint [path...]",
		Short: "Suggest the canonical spelling where a Flowfile is legal but not idiomatic",
		Long: "Walk Flowfiles and report where one is written in a way the style charter " +
			"(docs/STYLE.md) has an opinion about: a conditional nested inside a conditional, " +
			"one expression stated three or more times, and a chain of sibling `if:` steps " +
			"testing one value for equality where a `switch:` would let the validator check " +
			"the branches.\n\n" +
			"Every file this reports on is legal, validates, and runs. These are suggestions, " +
			"which is what tier 4 of the charter means: it warns and never blocks, and this " +
			"command exits 0 on every finding it has. `--strict` opts into a nonzero exit, " +
			"which is what the CI leg over `examples/` uses — the files this repository " +
			"teaches from are held to a narrower standard than the language is, because they " +
			"are what an author copies.\n\n" +
			"Each finding names the rule it descends from, so `R5/nested-conditional` is a " +
			"heading to read in docs/STYLE.md rather than a number to look up in a table. " +
			"What it reports is a property of the file and nothing else: no deployment is " +
			"consulted, no policy is read, and nothing resolves over a network.\n\n" +
			"A named file is taken as given; a directory is walked for Flowfiles, the same " +
			"walk `validate` and `audit` use. A file that does not compile is skipped rather " +
			"than linted, since `validate` is the verb with something to say about it.",
		Args:          cobra.MinimumNArgs(1),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLint(cmd, args)
		},
		Example: `# Read the whole corpus:
flow lint examples/

# One workflow:
flow lint examples/expense-approval/workflow.yaml

# The way CI reads it, where a finding is a failure:
flow lint --strict examples/

# Every finding as data:
flow lint -o json examples/ | jq '.files[].findings[].rule'`,
	}

	cmd.Flags().Bool("strict", false,
		"exit nonzero when there is anything to report (tier 4 is advisory by default)")

	addOutputFlag(cmd)

	return cmd
}

// A lintReport is one run's answer, and the document `-o json` writes.
//
// A plain Go struct rather than a schema message, the same call [auditReport]
// makes and for the same reason: nothing in it travels between components, and
// a style finding is this process's advice about a file rather than any part of
// the contract the schema exists to be. Putting it in the proto would also put
// `buf breaking` in front of a check list the charter expects to grow.
type lintReport struct {
	// Files are the files that were read and had something to report, in the
	// order they were walked.
	Files []lintFile `json:"files"`

	// Skipped names the files that did not compile, which this command has
	// nothing to say about.
	Skipped []string `json:"skipped"`

	Totals lintTotals `json:"totals"`
}

// A lintFile is one file's findings.
type lintFile struct {
	Path     string        `json:"path"`
	Findings []lintFinding `json:"findings"`
}

// A lintFinding is one suggestion, in the shape `-o json` writes.
type lintFinding struct {
	// Rule is the charter rule, as `R<n>/<check>`.
	Rule string `json:"rule"`

	Line   int    `json:"line"`
	Column int    `json:"column"`
	Step   string `json:"step,omitempty"`
	Field  string `json:"field,omitempty"`

	// Message says what is written and what to write instead.
	Message string `json:"message"`

	// style is what the analysis returned, kept so the text rendering is
	// [flowfile.StyleFinding.String] rather than a second assembly of the same
	// line from the fields above. Two renderings of one position is how the
	// three spellings #384 had to unify came about in the first place.
	style flowfile.StyleFinding
}

// lintTotals is what a reader checks first and what a trend line plots.
type lintTotals struct {
	Files             int `json:"files"`
	FilesWithFindings int `json:"filesWithFindings"`
	Findings          int `json:"findings"`

	// ByRule counts each rule separately, because "twelve findings" and "twelve
	// findings all of one rule" are different facts about a corpus: the second
	// is one habit and the first is a spread.
	ByRule map[string]int `json:"byRule"`
}

// errLintFindings reports that `--strict` was given and there was something to
// report. It carries no message of its own because the findings have already
// been printed.
var errLintFindings = errors.New("style findings")

// runLint reads every Flowfile under the paths given and renders the
// suggestions.
func runLint(cmd *cobra.Command, paths []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	strict, err := cmd.Flags().GetBool("strict")
	if err != nil {
		return err
	}

	files, err := collectFlowfiles(paths)
	if err != nil {
		return err
	}

	report := lintReport{
		Files:   []lintFile{},
		Skipped: []string{},
		Totals:  lintTotals{ByRule: map[string]int{}},
	}

	for _, path := range files {
		wf, positions, err := flowfile.ParseFile(path)
		if err != nil {
			report.Skipped = append(report.Skipped, path)
			continue
		}

		report.Totals.Files++

		found := flowfile.Lint(wf, positions)
		if len(found) == 0 {
			continue
		}

		report.Totals.FilesWithFindings++
		report.Files = append(report.Files, lintFile{
			Path:     path,
			Findings: lintFindings(found, &report.Totals),
		})
	}

	surface := newSurface(cmd)

	if format.Machine() {
		err = writeLintJSON(surface, format, report)
	} else {
		err = writeLintText(surface, report, strict)
	}
	if err != nil {
		return err
	}

	// After the rendering, never instead of it: a caller running with --strict
	// is a caller that wants to read what failed.
	if strict && report.Totals.Findings > 0 {
		return errLintFindings
	}

	return nil
}

// lintFindings converts the analysis's answer into the rendered form, counting
// the totals on the way past.
func lintFindings(found []flowfile.StyleFinding, totals *lintTotals) []lintFinding {
	out := make([]lintFinding, 0, len(found))

	for _, finding := range found {
		totals.Findings++
		totals.ByRule[string(finding.Rule)]++

		out = append(out, lintFinding{
			Rule:    string(finding.Rule),
			Line:    finding.Line,
			Column:  finding.Column,
			Step:    finding.Step,
			Field:   finding.Field,
			Message: finding.Message,
			style:   finding,
		})
	}

	return out
}

// writeLintJSON writes the report the way a job reads it, indented for the
// format a person also opens and compact for the line-per-record one.
func writeLintJSON(surface *ui.UI, format OutputFormat, report lintReport) error {
	var (
		encoded []byte
		err     error
	)
	if format == FormatJSON {
		encoded, err = json.MarshalIndent(report, "", "  ")
	} else {
		encoded, err = json.Marshal(report)
	}
	if err != nil {
		return fmt.Errorf("rendering the findings as %s: %w", format, err)
	}

	_, err = fmt.Fprintf(surface.Out, "%s\n", encoded)

	return err
}

// writeLintText writes one finding per line, each naming its own file.
//
// `file:line:column: message` and no other spelling, because that is the form a
// terminal, an editor and a CI annotation all make clickable, and the form #384
// had to unify across three surfaces that each rendered a position themselves.
// A finding travelling on its own out of a CI log has to say which file it is
// about.
func writeLintText(surface *ui.UI, report lintReport, strict bool) error {
	out, theme := surface.Out, surface.Theme

	for _, file := range report.Files {
		path := theme.Muted.Render(file.Path)
		for _, finding := range file.Findings {
			fmt.Fprintln(out, positionLine(path, finding.style.String()))
		}
	}

	if report.Totals.Findings == 0 {
		fmt.Fprintf(out, "%s\n", theme.Success.Render(fmt.Sprintf(
			"%d files read, nothing to suggest", report.Totals.Files)))
		return nil
	}

	fmt.Fprintf(out, "\n%s\n", theme.Strong.Render(fmt.Sprintf(
		"%d suggestion(s) across %d of %d files",
		report.Totals.Findings, report.Totals.FilesWithFindings, report.Totals.Files)))

	// The sentence that keeps a suggestion from reading as a refusal. Skipped
	// under --strict, where the caller has said that for this corpus it is one.
	if !strict {
		fmt.Fprintf(out, "%s\n", theme.Muted.Render(
			"Every file above is legal and runs; these are style suggestions "+
				"(docs/STYLE.md), and this command exits 0."))
	}

	return nil
}
