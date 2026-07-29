package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

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
			"file is never silently mangled.",
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

# Write the result somewhere else:
flow fix --stdout old.yaml > new.yaml`,
	}

	cmd.Flags().BoolVar(&opts.check, "check", false,
		"report what would change and exit non-zero if anything would, without writing")
	cmd.Flags().BoolVar(&opts.stdout, "stdout", false,
		"write the result to standard output instead of back to the file")

	return cmd
}

// errFixIncomplete reports that some file could not be fully rewritten, or that
// --check found work to do. It carries no message because the detail has already
// been printed.
var errFixIncomplete = errors.New("fix did not finish")

// runFix rewrites each path given.
func runFix(cmd *cobra.Command, paths []string, opts fixOptions) error {
	if opts.stdout && opts.check {
		return errors.New("--stdout and --check ask for different things: one writes the result, the other only reports")
	}

	files, err := collectFlowfiles(paths)
	if err != nil {
		return err
	}
	if opts.stdout && len(files) != 1 {
		return fmt.Errorf("--stdout writes one document, but %d files were named", len(files))
	}

	// Reports go to stderr and the rewritten document to stdout, so that
	// `flow fix --stdout old.yaml > new.yaml` cannot produce a new.yaml whose first
	// line is a diagnostic about old.yaml. A tool that writes its own complaints
	// into its output is a tool that cannot be piped.
	out, reports := cmd.OutOrStdout(), cmd.ErrOrStderr()
	if !opts.stdout {
		reports = out
	}

	var (
		refused bool
		pending bool
	)
	for _, path := range files {
		result, err := fixOne(out, reports, path, opts)
		if err != nil {
			return err
		}
		refused = refused || result.refused
		pending = pending || (result.changed && opts.check)
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
}

// fixOne rewrites a single file.
func fixOne(out, reports io.Writer, path string, opts fixOptions) (fixOutcome, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return fixOutcome{}, fmt.Errorf("error reading %s: %w", path, err)
	}

	result, err := flowfile.Fix(data)
	if err != nil {
		// Not YAML at all. Reported rather than returned, so one unparseable file
		// does not stop the rest of a directory — but counted as a refusal, because
		// the file is certainly not in the current edition.
		fmt.Fprintf(reports, "%s: %v\n", path, err)
		return fixOutcome{refused: true}, nil
	}

	for _, refusal := range result.Refusals {
		fmt.Fprintf(reports, "%s:%s\n", path, refusal.Error())
	}
	outcome := fixOutcome{changed: result.Changed(), refused: len(result.Refusals) > 0}

	if opts.stdout {
		_, err := out.Write(result.Source)
		return outcome, err
	}

	if !result.Changed() {
		if !outcome.refused {
			fmt.Fprintf(reports, "%s: already current\n", path)
		}
		return outcome, nil
	}

	for _, change := range result.Changes {
		fmt.Fprintf(reports, "%s:%d: %s\n", path, change.Line, change.Message)
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
// explicitly has said what they mean. A directory is walked for the extensions a
// Flowfile is written with, because walking one and rewriting a .json someone
// left there is not what anyone asked for.
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
				out = append(out, p)
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
