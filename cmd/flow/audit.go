package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow audit` is the only verb in this CLI whose audience is not the author of
// the file it reads.
//
// Issue #411 proposes an entry a workflow can name and read back, so a question
// answered in four places is written once. The disposition table behind it waits
// on evidence: how much a real corpus repeats itself, where, and in which of the
// shapes that a held entry would collapse. That evidence has been gathered by
// hand twice, and by hand it is a number nobody can check and nobody can watch
// move. This is the same reading, mechanized, so it can ride CI as a trend.
//
// It reports and does not judge. Nothing it prints is a diagnostic, a defect, or
// advice to change a file, and it exits zero on every finding it has: a workflow
// stating a predicate three times is a workflow written in the language as it
// exists, and the friction belongs to the language. `validate`, `fix` and
// `breaking` are the verbs that tell an author something is wrong; this one tells
// a language designer what the language costs. Confusing the two would turn a
// measurement into a nag, and it would put pressure on authors to rewrite files
// around a feature that does not exist yet.
//
// The counting itself is [flowfile.Audit]; this file is the walk, the flags and
// the two renderings.

// newAuditCommand builds the `flow audit` command.
func newAuditCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "audit [path...]",
		Short: "Measure how often Flowfiles repeat an expression they cannot name",
		Long: "Walk Flowfiles and count the expressions each one states more than once, with every " +
			"occurrence placed at a line. A repetition where one occurrence is the hand-written " +
			"negation of the others is marked separately, because that pair is the one a De Morgan " +
			"slip corrupts silently.\n\n" +
			"The audience is whoever decides what the language grows, not the author of the file. " +
			"This is the evidence the held-entry proposal waits on (`value:`, issue #411): what a " +
			"corpus would collapse if a workflow could name a value and read it back. It is not a " +
			"linter, it has no warning tier, and it exits 0 on every finding it reports. A nonzero " +
			"exit means something went wrong reading a file, never that a file repeats itself.\n\n" +
			"What it reports is a property of the file and nothing else. No deployment is consulted, " +
			"no policy is read, and nothing resolves over a network.\n\n" +
			"Repetition is counted within one file, over expressions compared structurally: same " +
			"shape, same names, same literals, whatever the spacing. Two expressions that mean the " +
			"same thing while spelling a bound name differently are counted apart. Bare literals and " +
			"bare names are never reported, because a corpus repeating `true` or `item` is a language " +
			"working rather than a language charging for something; only computations are counted, " +
			"and a sub-expression that occurs exactly as often as an expression containing it is " +
			"dropped in favour of the larger one.\n\n" +
			"A named file is taken as given; a directory is walked for Flowfiles, the same walk " +
			"`validate` and `test` use. A file that does not compile is counted out rather than " +
			"measured, and named in the machine format, since `validate` is the verb that has " +
			"something to say about it.",
		Args:          cobra.MinimumNArgs(1),
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAudit(cmd, args)
		},
		Example: `# Read the whole corpus:
flow audit examples/

# One workflow:
flow audit examples/enterprise-fund-transfer/workflow.yaml

# The counts a CI job would track over time, without gating on them:
flow audit -o json examples/ | jq '.totals'`,
	}

	addOutputFlag(cmd)

	return cmd
}

// An auditReport is one run's answer, and the document `-o json` writes.
//
// A plain Go struct rather than a schema message, the same call
// [versionInfo] makes and for the same reason: nothing in it travels between
// components. It is this process's account of what it read, addressed to a
// person deciding about the language or to a job plotting a number, and it
// describes a measurement rather than any part of the system the schema is the
// contract for. Putting it in the proto would make `buf breaking` the guardian
// of a research instrument that is expected to change shape as the question it
// answers gets sharper.
type auditReport struct {
	// Files are the files that were read and had something to report, in the
	// order they were walked.
	Files []auditFile `json:"files"`

	// Skipped names the files that did not compile, which this command has
	// nothing to say about.
	Skipped []string `json:"skipped"`

	Totals auditTotals `json:"totals"`
}

// An auditFile is one file's findings.
type auditFile struct {
	Path     string        `json:"path"`
	Findings []auditRepeat `json:"findings"`
}

// An auditRepeat is one expression a file states more than once.
type auditRepeat struct {
	Expr  string `json:"expr"`
	Count int    `json:"count"`

	// Negated reports the hand-negated pair: the same expression stated plainly
	// in one place and under a `!` in another.
	Negated bool        `json:"negated"`
	Sites   []auditSite `json:"sites"`
}

// An auditSite is one place an expression is stated.
type auditSite struct {
	Step    string `json:"step"`
	Field   string `json:"field"`
	Line    int    `json:"line"`
	Negated bool   `json:"negated"`
}

// auditTotals is what a trend line plots.
//
// Occurrences rather than findings is the number the proposal is about: a
// predicate written four times is four things to keep in step, and collapsing it
// to one held entry removes three of them.
type auditTotals struct {
	Files            int `json:"files"`
	FilesWithRepeats int `json:"filesWithRepeats"`
	Repeated         int `json:"repeated"`
	Occurrences      int `json:"occurrences"`
	NegatedPairs     int `json:"negatedPairs"`
}

// runAudit reads every Flowfile under the paths given and renders the counts.
func runAudit(cmd *cobra.Command, paths []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	files, err := collectFlowfiles(paths)
	if err != nil {
		return err
	}

	report := auditReport{
		Files:   []auditFile{},
		Skipped: []string{},
	}

	for _, path := range files {
		wf, positions, err := flowfile.ParseFile(path)
		if err != nil {
			report.Skipped = append(report.Skipped, path)
			continue
		}

		report.Totals.Files++

		repeats := flowfile.Audit(wf, positions)
		if len(repeats) == 0 {
			continue
		}

		report.Totals.FilesWithRepeats++
		report.Files = append(report.Files, auditFile{
			Path:     path,
			Findings: auditFindings(repeats, &report.Totals),
		})
	}

	surface := newSurface(cmd)

	if format.Machine() {
		return writeAuditJSON(surface, format, report)
	}

	return writeAuditText(surface, report)
}

// auditFindings converts the analysis's answer into the rendered form, counting
// the totals on the way past.
func auditFindings(repeats []flowfile.RepeatedExpr, totals *auditTotals) []auditRepeat {
	out := make([]auditRepeat, 0, len(repeats))

	for _, repeat := range repeats {
		totals.Repeated++
		totals.Occurrences += repeat.Count()
		if repeat.Negated {
			totals.NegatedPairs++
		}

		sites := make([]auditSite, 0, len(repeat.Sites))
		for _, site := range repeat.Sites {
			sites = append(sites, auditSite{
				Step:    site.Step,
				Field:   site.Field,
				Line:    site.Line,
				Negated: site.Negated,
			})
		}

		out = append(out, auditRepeat{
			Expr:    repeat.Expr,
			Count:   repeat.Count(),
			Negated: repeat.Negated,
			Sites:   sites,
		})
	}

	return out
}

// writeAuditJSON writes the report the way a job reads it, indented for the
// format a person also opens and compact for the line-per-record one.
func writeAuditJSON(surface *ui.UI, format OutputFormat, report auditReport) error {
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
		return fmt.Errorf("rendering the audit as %s: %w", format, err)
	}

	_, err = fmt.Fprintf(surface.Out, "%s\n", encoded)

	return err
}

// writeAuditText writes the report the way a person reads it: a block per file,
// most repeated first, every occurrence on its own line.
func writeAuditText(surface *ui.UI, report auditReport) error {
	out, theme := surface.Out, surface.Theme

	for _, file := range report.Files {
		fmt.Fprintln(out, theme.Accent.Render(file.Path))

		for _, finding := range file.Findings {
			label := fmt.Sprintf("%d occurrences", finding.Count)
			if finding.Negated {
				label += ", one of them hand-negated"
			}
			fmt.Fprintf(out, "  %s  %s\n", theme.Strong.Render(label), finding.Expr)

			for _, site := range collapseAuditSites(finding.Sites) {
				fmt.Fprintf(out, "    %s  %s\n",
					theme.Muted.Render(auditWhere(file.Path, site.at)), auditWhat(site))
			}
		}

		fmt.Fprintln(out)
	}

	// Counted rather than listed, and listed only in the machine format. Most of
	// what lands here is a `*.test.yaml` beside a workflow, which the walk picks
	// up because it is shaped like a Flowfile and which is not one; naming each
	// of the fifty in this corpus would bury the measurement under a list that is
	// not about the language at all. `validate` is the verb that reports a
	// workflow which genuinely will not compile.
	if len(report.Skipped) > 0 {
		fmt.Fprintf(out, "%s\n", theme.Muted.Render(fmt.Sprintf(
			"%d files read did not compile and were not counted; `flow validate` is the verb with something to say about that",
			len(report.Skipped))))
	}

	return writeAuditSummary(out, theme, report.Totals)
}

// auditWhere renders a site's position the way an editor and a terminal both
// expect, so a reader can jump straight to it.
func auditWhere(path string, site auditSite) string {
	if site.Line <= 0 {
		return path
	}

	return fmt.Sprintf("%s:%d", path, site.Line)
}

// A collapsedSite is one place an expression is stated, with how many times it
// is stated there.
//
// An expression can repeat inside a single written expression: `${timestamp(x)
// - timestamp(y) > timestamp(x)}` states one of them twice, and printing that
// as two identical lines reads like a defect in this command rather than the
// repetition it is. Collapsed only for the text tier; the machine format keeps
// one entry per occurrence, since a program counting them should not have to
// undo a presentation decision.
type collapsedSite struct {
	at    auditSite
	times int
}

// collapseAuditSites groups the occurrences that share a position.
func collapseAuditSites(sites []auditSite) []collapsedSite {
	out := make([]collapsedSite, 0, len(sites))

	for _, site := range sites {
		if last := len(out) - 1; last >= 0 && out[last].at == site {
			out[last].times++
			continue
		}
		out = append(out, collapsedSite{at: site, times: 1})
	}

	return out
}

// auditWhat names the field a site was written as, how many times it is stated
// there, and whether it is the negated half.
func auditWhat(site collapsedSite) string {
	where := site.at.Field
	if site.at.Step != "" {
		where = site.at.Step + "." + site.at.Field
	}
	if site.times > 1 {
		where += fmt.Sprintf(" (%d times in the one expression)", site.times)
	}
	if site.at.Negated {
		where += " (negated)"
	}

	return where
}

// writeAuditSummary writes the one paragraph a reader who scrolled past the
// findings needs, including the sentence that keeps this from being read as a
// list of defects.
func writeAuditSummary(out io.Writer, theme ui.Theme, totals auditTotals) error {
	fmt.Fprintf(out, "%s\n", theme.Strong.Render(fmt.Sprintf(
		"%d expressions repeated across %d of %d files, %d occurrences in all, %d of them hand-negated pairs.",
		totals.Repeated, totals.FilesWithRepeats, totals.Files, totals.Occurrences, totals.NegatedPairs)))

	_, err := fmt.Fprintln(out, theme.Muted.Render(
		"None of this is a defect. Where the repeated value spans steps or reads an input, "+
			"a `value:` step (#411) names it once and every reader compares that name."))

	return err
}
