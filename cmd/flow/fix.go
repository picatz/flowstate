package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow fix` is what makes the language's no-deprecation rule affordable.
//
// Surface syntax here gets no deprecation window: a replaced spelling is gone
// rather than warned about, because carrying two spellings costs the parser, the
// validator, the language server, the marshaller, and every test that crosses
// them, for as long as the window lasts. The trade only works if migrating is a
// command someone runs in a second, which is this one.
//
// Two properties it has to have, and both are about trust rather than
// correctness. It has to be safe to run over a directory — so a file with
// nothing to change comes back byte for byte, and `--check` reports without
// writing. And it has to refuse rather than guess: a shape it cannot rewrite is
// reported with its position and left alone, because a file that looks fixed and
// is not is worse than one that was never touched.

// fixOptions are the flags `flow fix` takes.
type fixOptions struct {
	// check reports what would change without writing anything, and exits
	// non-zero if anything would. This is the form CI runs.
	check bool

	// stdout writes the result to standard output instead of back to the file,
	// which is how a single file is piped somewhere else.
	stdout bool
}

// newFixCommand builds the `flow fix` command.
func newFixCommand() *cobra.Command {
	var opts fixOptions

	cmd := &cobra.Command{
		Use:   "fix [path...]",
		Short: "Rewrite Flowfiles into the current edition",
		Long: "Rewrite Flowfiles written in an older edition of the language into the current one, " +
			"preserving comments, formatting, and everything the change does not touch. " +
			"A directory is walked for .yaml and .yml files. A file with nothing to change is left " +
			"byte for byte as it was.\n\n" +
			"Shapes that cannot be rewritten without guessing — a task written in flow style, or one " +
			"standing behind a YAML alias — are reported with their position and left alone, so the " +
			"file is never silently mangled.\n\n" +
			"`--output json` or `--output jsonl` turns `--check` into a report a program reads " +
			"instead of scrapes: what changed or would change, and what was refused, per file. CI " +
			"that wants structured data rather than stderr text asks for one of those.",
		Args:          cobra.MinimumNArgs(1),
		SilenceErrors: true,
		// A file that needs fixing is not a command someone invoked wrongly, and
		// printing the usage block after the diagnostics reads as though it were.
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFix(cmd, args, opts)
		},
		Example: `# Rewrite one file in place:
flow fix workflow.yaml

# Rewrite a whole directory:
flow fix examples/

# Report what would change without writing, for CI:
flow fix --check examples/

# The same, as a report CI can parse instead of scraping stderr:
flow fix --check -o jsonl examples/*/workflow.yaml

# Write the result somewhere else:
flow fix --stdout old.yaml > new.yaml`,
	}

	cmd.Flags().BoolVar(&opts.check, "check", false,
		"report what would change and exit non-zero if anything would, without writing")
	cmd.Flags().BoolVar(&opts.stdout, "stdout", false,
		"write the result to standard output instead of back to the file")

	// Diagnostics are a schema message, so `-o json`/`-o jsonl` mean here what they
	// mean on `validate`: the fields are the schema's and addressable by name.
	addOutputFlag(cmd)

	return cmd
}

// errFixIncomplete reports that some file could not be fully rewritten, or that
// --check found work to do. It carries no message because the detail has already
// been printed.
var errFixIncomplete = errors.New("fix did not finish")

// runFix rewrites each path given.
func runFix(cmd *cobra.Command, paths []string, opts fixOptions) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	// Every refusal from here down is about the flags asking for two different
	// things, decided before a single file is touched — an invocation mistake
	// rather than a finding about any file named, so each is marked with
	// newUsageError the same way resolveOutputFormat marks its own.
	if opts.stdout && opts.check {
		return newUsageError(errors.New("--stdout and --check ask for different things: one writes the result, the other only reports"))
	}
	if opts.stdout && format.Machine() {
		// Both want stdout for something different — the rewritten document, or the
		// report — and only one document belongs on a stream a pipe reads.
		return newUsageError(fmt.Errorf("--stdout and --output %s both want stdout: one is the rewritten document, the other the report", format))
	}

	files, err := collectFlowfiles(paths)
	if err != nil {
		return err
	}
	if opts.stdout && len(files) != 1 {
		return newUsageError(fmt.Errorf("--stdout writes one document, but %d files were named", len(files)))
	}

	// Reports go to stderr and the rewritten document to stdout, so that
	// `flow fix --stdout old.yaml > new.yaml` cannot produce a new.yaml whose first
	// line is a diagnostic about old.yaml. A tool that writes its own complaints
	// into its output is a tool that cannot be piped.
	// Through the surface rather than the raw writers, and carrying the theme that
	// belongs to whichever stream the reports land on — they go to stderr only when
	// stdout is carrying a document, so the two cases have different palettes for
	// the same reason `flow get x | jq` does.
	surface := newSurface(cmd)

	out := surface.Out
	reports, reportTheme := surface.Err, surface.ErrTheme
	if !opts.stdout {
		reports, reportTheme = surface.Out, surface.Theme
	}

	var (
		refused    bool
		pending    bool
		machine    = format.Machine()
		fixReports []*v1.FixReport
	)
	for _, path := range files {
		result, err := fixOne(out, reports, reportTheme, path, opts, machine)
		if err != nil {
			return err
		}
		refused = refused || result.refused
		pending = pending || (result.changed && opts.check)
		if machine {
			fixReports = append(fixReports, result.report)
		}
	}

	if machine {
		// Projected from the same outcome the text form prints, never recomputed —
		// two readings of one rewrite that could otherwise drift.
		if format == FormatJSONL {
			// One line per file, so a consumer reads the first report without
			// waiting for the last.
			for _, report := range fixReports {
				if err := writeJSON(surface, format, report); err != nil {
					return err
				}
			}
		} else if err := writeJSON(surface, format, &v1.FixReports{Files: fixReports}); err != nil {
			// One document per invocation, the same as everywhere else `json` means
			// that in this CLI: fixing three files is still one answer.
			return err
		}
	}

	// Non-zero for either. `--check` finding work is the CI case, and a refusal is
	// the case that matters more: `flow fix . && git commit` must not succeed while
	// steps are still in a spelling that does not compile.
	if refused || pending {
		return errFixIncomplete
	}
	return nil
}

// A fixOutcome is what one file's rewrite amounted to.
type fixOutcome struct {
	// changed reports that the file was rewritten, or would be under --check.
	changed bool

	// refused reports that some part of the file could not be rewritten safely, so
	// the file is not finished whatever else happened to it.
	refused bool

	// report is the same outcome as a schema message, built whether or not a
	// machine format asked for it — the cost of building the struct is nothing next
	// to building a rewriter twice, once for a person and once for a program.
	report *v1.FixReport
}

// fixOne rewrites a single file.
//
// machine suppresses the human-readable lines this would otherwise write to
// reports: a machine format renders [fixOutcome.report] itself, and printing both
// would be the same fact said twice on the same stream.
func fixOne(out, reports io.Writer, theme ui.Theme, path string, opts fixOptions, machine bool) (fixOutcome, error) {
	report := &v1.FixReport{File: path}

	data, err := os.ReadFile(path)
	if err != nil {
		return fixOutcome{}, fmt.Errorf("error reading %s: %w", path, err)
	}

	result, err := flowfile.Fix(data)
	if err != nil {
		// Not YAML at all. Reported rather than returned, so one unparseable file
		// does not stop the rest of a directory — but counted as a refusal, because
		// the file is certainly not in the current edition.
		if !machine {
			fmt.Fprintf(reports, "%s: %v\n", theme.Muted.Render(path), err)
		}
		// Unpositioned: this is a fact about the whole document rather than a line
		// within it, the same distinction [Diagnostic] draws with Line and Column
		// both zero.
		report.Refusals = []*v1.Diagnostic{{Message: err.Error()}}
		return fixOutcome{refused: true, report: report}, nil
	}

	for _, refusal := range result.Refusals {
		if !machine {
			fmt.Fprintf(reports, "%s:%s\n", theme.Muted.Render(path), refusal.Error())
		}
		report.Refusals = append(report.Refusals, refusal.Proto())
	}
	// Notes do not affect the outcome. They are places worth a reader's eye, not
	// work left undone, and failing on one would let a comment nobody has to change
	// stop `flow fix . && git commit`.
	for _, note := range result.Notes {
		if !machine {
			fmt.Fprintf(reports, "%s:%s\n", theme.Muted.Render(path), note.Error())
		}
		report.Notes = append(report.Notes, note.Proto())
	}
	outcome := fixOutcome{changed: result.Changed(), refused: len(result.Refusals) > 0, report: report}
	report.Changed = outcome.changed

	if opts.stdout {
		_, err := out.Write(result.Source)
		return outcome, err
	}

	if !result.Changed() {
		if !outcome.refused && !machine {
			fmt.Fprintf(reports, "%s: %s\n",
				theme.Muted.Render(path), theme.Muted.Render("already current"))
		}
		return outcome, nil
	}

	for _, change := range result.Changes {
		if !machine {
			fmt.Fprintf(reports, "%s:%d: %s\n",
				theme.Muted.Render(path), change.Line, change.Message)
		}
		report.Changes = append(report.Changes, &v1.FixChange{
			Line:    uint32(max(change.Line, 0)),
			Message: change.Message,
		})
	}

	if opts.check {
		return outcome, nil
	}

	// Written through the file's own mode, so fixing a file does not change who
	// can read it. A rewriter that widens permissions is a rewriter nobody should
	// run over a repository.
	info, err := os.Stat(path)
	if err != nil {
		return outcome, fmt.Errorf("error reading mode of %s: %w", path, err)
	}
	if err := os.WriteFile(path, result.Source, info.Mode().Perm()); err != nil {
		return outcome, fmt.Errorf("error writing %s: %w", path, err)
	}
	return outcome, nil
}

// collectFlowfiles expands the paths given into the files to rewrite.
//
// A named file is taken as given, whatever it is called: someone naming a file
// explicitly has said what they mean, and it reaches [flowfile.Fix] directly —
// which is what refuses it, with a diagnostic, if it turns out not to be a
// Flowfile or a Flowfile test. That is the one place this refusal belongs: a
// path typed on the command line is a claim, and the claim gets checked.
//
// A directory is walked for more than its file extensions. Filtering by
// `.yaml`/`.yml` alone was once enough, but this tree keeps an egress policy, two
// auth/trust policies, and unrelated YAML (docker-compose.yaml, Grafana
// provisioning) beside its Flowfiles, all with the same extensions and none of
// them a Flowfile — so a sweep also reads each file's shape and selects only
// what [flowfile.LooksLikeFlowfile] recognizes. This is the same allowlist
// [flowfile.Fix] enforces from the inside, read here so a walk never hands a
// policy file to the rewriter to begin with, and a sweep over a mixed directory
// does not surface a refusal for every file in it that was never a Flowfile.
func collectFlowfiles(paths []string) ([]string, error) {
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
			switch filepath.Ext(p) {
			case ".yaml", ".yml":
				data, err := os.ReadFile(p)
				if err != nil {
					return fmt.Errorf("error reading %s: %w", p, err)
				}
				if flowfile.LooksLikeFlowfile(data) {
					out = append(out, p)
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("error walking %s: %w", path, err)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no Flowfiles found in the paths given")
	}
	return out, nil
}
