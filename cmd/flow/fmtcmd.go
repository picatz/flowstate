package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow fmt` is [flowfile.Format] exposed as a formatter, the way `gofmt` is a
// command wrapped around go/printer.
//
// It is not a text rewriter. `flow fix` edits a file's source lines in place;
// this instead reads a file into a [v1.Workflow] and renders that back out, and
// then carries the source's comments across onto what it rendered. Marshal's own
// doc comment says why one function supplies both directions' contract: `flow
// fmt` and the language server both rely on Marshal(Unmarshal(x)) meaning the
// same thing as x, and neither relies on it meaning the same *bytes*.
//
// What is lost running a real file through it: every blank line that is not part
// of a comment's own grouping, the order YAML mapping keys were written in (task
// inputs and `vars:` entries come back sorted), which quote style a string
// literal used (a CEL string literal is written back with double quotes
// regardless of how it was typed), and whether the file ended in a trailing
// newline (it always will afterward). What survives: the shape of every step,
// every value, every comment, and, because Marshal is the same function `flow
// fix` writes its output through, a file this produces compiles under the same
// edition it started in.
//
// Comments are not a formatting opinion, which is why they are the one thing here
// that is kept rather than normalized: they are the author's own content, and a
// production file's "loop bounded at 50 because the API pages at 50" is the
// expensive one to lose (#381). Where a comment cannot be carried across at all,
// because what it was written against is not written back in the same shape, the
// whole file is refused and left alone rather than rewritten without it.
//
// It refuses to touch a file that will not parse. Reporting a syntax error and
// leaving the file exactly as it was is the same rule `flow fix` follows: a file
// that looks handled and is not is worse than one nothing happened to.

// fmtOptions are the flags `flow fmt` takes.
type fmtOptions struct {
	// check reports which files would change without writing anything, and
	// exits non-zero if any would. This is the form CI runs.
	check bool

	// stdout writes the result to standard output instead of back to the file,
	// which is how a single file is piped somewhere else.
	stdout bool
}

// newFmtCommand builds the `flow fmt` command.
func newFmtCommand() *cobra.Command {
	var opts fmtOptions

	cmd := &cobra.Command{
		Use:   "fmt [path...]",
		Short: "Rewrite Flowfiles into the form flowfile.Format writes, keeping comments",
		Long: "Rewrite a Flowfile from its parsed form rather than editing its source text, the way " +
			"`flow fix` does. A directory is walked for .yaml and .yml files.\n\n" +
			"Comments are kept, carried onto the document this writes at the key, value or list " +
			"entry they were written against. Whitespace is not: blank lines, the order a mapping's " +
			"keys were written in, and a string literal's quote style are all normalized away, " +
			"because they are not part of the parsed workflow this reads a file into.\n\n" +
			"A file that does not parse is reported with its position and left untouched, and so is " +
			"a file carrying a comment this cannot keep, which happens when what the comment was " +
			"written against is not written back in the same shape.\n\n" +
			"A Flowfile test (`*.test.yaml`, declaring `tests:` rather than `steps:`) is a different " +
			"document kind this command does not yet format; it is passed over with a note rather " +
			"than parsed as a workflow and refused, so a directory `flow init` writes, tests " +
			"included, is something this command can walk.\n\n" +
			"`--output json` or `--output jsonl` turns `--check` into a report a program reads " +
			"instead of scrapes: which files would change, and which were refused, per file. CI " +
			"that wants structured data rather than stderr text asks for one of those.",
		Args:          cobra.MinimumNArgs(1),
		SilenceErrors: true,
		// A file that needs reformatting is not a command someone invoked
		// wrongly, and printing the usage block after the diagnostics reads as
		// though it were.
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFmt(cmd, args, opts)
		},
		Example: `# Rewrite one file in place:
flow fmt workflow.yaml

# Rewrite a whole directory:
flow fmt examples/

# Report which files would change without writing, for CI:
flow fmt --check examples/

# The same, as a report CI can parse instead of scraping stderr:
flow fmt --check -o jsonl examples/*/workflow.yaml

# Write the result somewhere else:
flow fmt --stdout old.yaml > new.yaml`,
	}

	cmd.Flags().BoolVar(&opts.check, "check", false,
		"report which files would change and exit non-zero if any would, without writing")
	cmd.Flags().BoolVar(&opts.stdout, "stdout", false,
		"write the result to standard output instead of back to the file")

	// Diagnostics are a schema message, so `-o json`/`-o jsonl` mean here what they
	// mean on `validate` and `fix`: the fields are the schema's and addressable by
	// name.
	addOutputFlag(cmd)

	return cmd
}

// errFmtIncomplete reports that some file could not be formatted, or that
// --check found work to do. It carries no message because the detail has
// already been printed.
var errFmtIncomplete = errors.New("fmt did not finish")

// runFmt formats each path given.
func runFmt(cmd *cobra.Command, paths []string, opts fmtOptions) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	// Every refusal from here down is an invocation mistake — flags asking for
	// two different things, decided before a single file is touched — rather
	// than a finding about any file named, so each is marked the same way
	// resolveOutputFormat marks its own.
	if opts.stdout && opts.check {
		return newUsageError(errors.New("--stdout and --check ask for different things: one writes the result, the other only reports"))
	}
	if opts.stdout && format.Machine() {
		// Both want stdout for something different — the rewritten document, or
		// the report — and only one document belongs on a stream a pipe reads.
		return newUsageError(fmt.Errorf("--stdout and --output %s both want stdout: one is the rewritten document, the other the report", format))
	}

	// collectFlowfiles is `flow fix`'s: a directory is walked for .yaml and
	// .yml files, and a file named explicitly is taken as given whatever it is
	// called.
	files, err := collectFlowfiles(paths)
	if err != nil {
		return err
	}
	if opts.stdout && len(files) != 1 {
		return newUsageError(fmt.Errorf("--stdout writes one document, but %d files were named", len(files)))
	}

	// Reports go to stderr and the rewritten document to stdout under
	// --stdout, for the same reason `flow fix --stdout` splits the two: a
	// pipeline reading the document must never see a diagnostic as its first
	// line.
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
		fmtReports []*v1.FmtReport
	)
	for _, path := range files {
		result, err := fmtOne(out, reports, reportTheme, path, opts, machine)
		if err != nil {
			return err
		}
		refused = refused || result.refused
		pending = pending || (result.changed && opts.check)
		if machine {
			fmtReports = append(fmtReports, result.report)
		}
	}

	if machine {
		// Projected from the same outcome the text form prints, never recomputed.
		if format == FormatJSONL {
			for _, report := range fmtReports {
				if err := writeJSON(surface, format, report); err != nil {
					return err
				}
			}
		} else if err := writeJSON(surface, format, &v1.FmtReports{Files: fmtReports}); err != nil {
			return err
		}
	}

	if refused || pending {
		return errFmtIncomplete
	}
	return nil
}

// A fmtOutcome is what one file's formatting amounted to.
type fmtOutcome struct {
	// changed reports that the file was rewritten, or would be under --check.
	changed bool

	// refused reports that the file could not be parsed, so nothing was
	// written.
	refused bool

	// report is the same outcome as a schema message, built whether or not a
	// machine format asked for it.
	report *v1.FmtReport
}

// fmtOne formats a single file.
//
// machine suppresses the human-readable lines this would otherwise write to
// reports, the same rule [fixOne] follows and for the same reason.
func fmtOne(out, reports io.Writer, theme ui.Theme, path string, opts fmtOptions, machine bool) (fmtOutcome, error) {
	report := &v1.FmtReport{File: path}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmtOutcome{}, fmt.Errorf("error reading %s: %w", path, err)
	}

	// A Flowfile test (`*.test.yaml`, or any document whose top level is
	// `tests:` rather than `steps:`) has no [v1.Workflow] shape for
	// flowfile.ParseFile to parse it into: it is a different document kind,
	// not a workflow with a bad key, so handing it to that parser produces a
	// diagnostic that misdiagnoses a valid file ("unknown key \"tests\"",
	// issue #392). `flow fix` already tells the two shapes apart before it
	// rewrites, at the token level (see flowfile.Fix); this is the same
	// routing decision, made before the workflow parser is ever reached, so a
	// directory `flow init` just wrote is something both commands can walk.
	// This command has no marshaller for a test file's own shape yet, so
	// rather than guess at one, it passes over the file exactly the way it
	// would report an unchanged file: left alone, not refused, not an error.
	if flowfile.LooksLikeFlowfileTest(data) {
		// In --stdout mode the document stream is the product, and a pipeline
		// like `flow fmt --stdout f > g` replaces g with whatever is written
		// here. Passing over the file must therefore still write its bytes,
		// unchanged: an empty stream with exit 0 would be the pipeline
		// silently truncating a valid file, the exact class of loss this
		// command exists to never cause.
		if opts.stdout {
			if _, err := out.Write(data); err != nil {
				return fmtOutcome{}, fmt.Errorf("error writing %s to stdout: %w", path, err)
			}
		}
		if !machine {
			fmt.Fprintf(reports, "%s: %s\n", theme.Muted.Render(path),
				theme.Muted.Render("test file; flow fmt does not format test files"))
		}
		return fmtOutcome{report: report}, nil
	}

	// File-aware, so a `call:` step resolves relative to this file's own
	// directory rather than being refused for having no location to resolve
	// against.
	workflow, _, err := flowfile.ParseFile(path)
	if err != nil {
		// A file that does not compile has no workflow for Marshal to render,
		// so there is nothing safe to write. Reported and left alone, the same
		// as `flow fix` leaves a shape it refuses.
		if !machine {
			writeErrDiagnostics(reports, theme, path, err)
		}
		report.Refusals = errDiagnosticsProto(err)
		return fmtOutcome{refused: true, report: report}, nil
	}

	formatted, err := flowfile.Format(data, workflow)
	if err != nil {
		// A workflow this build compiled but cannot write back out — a literal
		// string containing ${, or an expression written with a macro cel-go
		// cannot render as source, is not safe to guess at either. Nor is a file
		// carrying a comment the rewrite cannot keep: dropping an author's prose
		// to win a reformat is the trade this command does not make.
		if !machine {
			writeErrDiagnostics(reports, theme, path, err)
		}
		report.Refusals = errDiagnosticsProto(err)
		return fmtOutcome{refused: true, report: report}, nil
	}

	changed := !bytes.Equal(data, formatted)
	outcome := fmtOutcome{changed: changed, report: report}
	report.Changed = changed

	if opts.stdout {
		_, err := out.Write(formatted)
		return outcome, err
	}

	if !changed {
		if !machine {
			fmt.Fprintf(reports, "%s: %s\n", theme.Muted.Render(path), theme.Success.Render("already formatted"))
		}
		return outcome, nil
	}

	if !machine {
		fmt.Fprintf(reports, "%s: %s\n", theme.Muted.Render(path), theme.Warning.Render("reformatted"))
	}

	if opts.check {
		return outcome, nil
	}

	// Written through the file's own mode, so formatting a file does not
	// change who can read it.
	info, err := os.Stat(path)
	if err != nil {
		return outcome, fmt.Errorf("error reading mode of %s: %w", path, err)
	}
	if err := os.WriteFile(path, formatted, info.Mode().Perm()); err != nil {
		return outcome, fmt.Errorf("error writing %s: %w", path, err)
	}
	return outcome, nil
}
