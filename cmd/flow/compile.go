package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow compile` is the answer to a question `flow validate` deliberately does not
// answer: not "is this file correct" but "what does this file become".
//
// A Flowfile is source, and the thing that runs is a [v1.Workflow] — the same
// message `flow run` submits and the same one a plugin, a review tool or an agent
// reads. Until this command existed the only ways to see it were to run the
// workload, which has side effects, or to drive the Compile RPC over MCP, which
// needs an agent in the loop to look at a file already sitting on disk.
//
// It calls [flowfile.ParseFile] and [flowfile.ValidateSourceFile] directly rather
// than dialing the Compile RPC the way it used to: this command already has the one
// thing the RPC deliberately does not — a real path on the machine it is running
// on — and a `call:` step needs that path to resolve. The RPC and `flowstate_compile`
// stay bytes-only on purpose (see their own docs), because a browser or an agent
// driving them may have no filesystem at all; this command always does, so paying
// for that restriction here would refuse a file this command is the one place able
// to compile in full. All three still call the same [flowfile.Parse] underneath —
// what differs is only whether a path travels with the bytes.

// newCompileCommand builds the `flow compile` command.
func newCompileCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compile [workflow-file]",
		Short: "Print the workflow specification a Flowfile compiles to",
		Long: "Compile a Flowfile and write the resulting workflow specification to standard " +
			"output, executing nothing and contacting no server.\n\n" +
			"This is the sibling of `flow validate` and the two answer different questions. " +
			"`flow validate` answers whether a file is correct, and its answer is the list of " +
			"problems. This answers what a correct file becomes, and its answer is the " +
			"specification: the same `Workflow` message `flow run` submits, so a reviewer, a " +
			"diff, or a tool reading a step's compiled expressions is reading exactly what " +
			"would have executed.\n\n" +
			"A file with problems is refused: the diagnostics go to standard error in the " +
			"same `file:line:column: message` form `flow validate` writes, standard output " +
			"stays empty, and the exit status is non-zero. A specification handed out beside " +
			"a list of its problems would be an invitation to run it anyway.\n\n" +
			"There is no --input or --input-file here, and the omission is the design rather " +
			"than a gap. Compilation takes no arguments: a workflow's `inputs:` are bound to " +
			"values when a run is submitted, so the specification is the same document " +
			"whatever it will later be run with. Give arguments to `flow run` or " +
			"`flow run local`.\n\n" +
			"The specification is a protobuf message, and protojson is the only faithful way " +
			"to write one down, so `--output text` writes the same document `--output json` " +
			"does rather than inventing a second rendering nobody could read back. " +
			"`--output jsonl` writes that document on a single line.",
		Args:          cobra.ExactArgs(1),
		RunE:          runCompile,
		SilenceErrors: true,
		// A file with a problem in it is not a command someone invoked wrongly, and
		// a usage block after the diagnostics sends the reader to check their flags
		// instead of the line they were just told about — the same reason `validate`
		// and `fix` silence it.
		SilenceUsage: true,
		Example: `# See what a Flowfile compiles to:
flow compile examples/hello-world/workflow.yaml

# Keep the specification for a review or a diff:
flow compile examples/hello-world/workflow.yaml > hello-world.json

# Ask what one step became:
flow compile examples/hello-world/workflow.yaml | jq '.steps[0]'`,
	}

	// The specification is a schema message, so `--output json` means here what it
	// means everywhere else in this CLI: the fields are the schema's, addressable
	// by name, with no encoder of this command's own between them and a reader.
	addOutputFlag(cmd)

	return cmd
}

// errCompileRefused reports that the file did not compile.
//
// It carries a short message rather than none, because unlike `flow validate` — where
// the diagnostics *are* the answer and saying "validation failed" after them adds a
// word — this command promised a document on stdout and is not writing one. The line
// says why stdout is empty.
var errCompileRefused = errors.New("the file has problems, so there is no specification to write")

// runCompile compiles one Flowfile and writes the specification.
func runCompile(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	path := args[0]

	// Path-aware, so a `call:` step resolves relative to this file's own
	// directory exactly as `flow validate` and `flow run` resolve it.
	workflow, _, err := flowfile.ParseFile(path)
	surface := newSurface(cmd)
	if err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			// Not a diagnostic: the path itself cannot be read, which is a fact
			// about the invocation rather than about a workflow, and listing it
			// beside "this step references a step that does not exist" would put
			// a mistake fixed in the shell in among mistakes fixed in the file.
			return fmt.Errorf("error reading %s: %w", path, err)
		}

		// Diagnostics from a file that failed to compile, or — the one shape that
		// is not a [flowfile.Diagnostics] — a document that is not YAML at all.
		// Both are the file's problem rather than the invocation's, so both are
		// reported the same way.
		var diagnostics flowfile.Diagnostics
		if !errors.As(err, &diagnostics) {
			diagnostics = flowfile.Diagnostics{{Message: err.Error()}}
		}
		writeDiagnostics(surface.Err, surface.ErrTheme.Muted.Render(path), diagnostics)
		return errCompileRefused
	}

	// The compiler accepts more than the validator does — a parse can succeed on
	// a file validation would still object to — so the full check runs too, and a
	// file with diagnostics answers with them and no specification. A
	// specification handed out beside a list of its problems would be an
	// invitation to run it anyway.
	diagnostics, err := flowfile.ValidateSourceFile(path)
	if err != nil {
		return fmt.Errorf("validating %s: %w", path, err)
	}
	if len(diagnostics) > 0 {
		// stderr, which is the split this command turns on. `flow validate` writes
		// diagnostics to stdout because they are its answer; here the answer is
		// the specification, so a diagnostic is the account of why there is not
		// one — and a reader piping this into `jq` must never receive one.
		writeDiagnostics(surface.Err, surface.ErrTheme.Muted.Render(path), diagnostics)
		return errCompileRefused
	}

	// Indented unless the line-per-record form was asked for. There is one document
	// either way, because there is one specification — `jsonl` here is the compact
	// spelling rather than a different answer.
	document := format
	if document == FormatText {
		document = FormatJSON
	}

	return writeJSON(surface, document, workflow)
}
