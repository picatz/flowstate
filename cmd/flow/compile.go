package main

import (
	"errors"
	"fmt"
	"os"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
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
// It is a projection of the Compile RPC and nothing more, which is why it dials
// nothing: [server.FlowstateServer.Compile] parses and validates in this process
// over a nil Temporal client, so the specification printed here is byte-for-byte
// what a server would answer with for the same bytes. One compiler, three callers —
// this command, the RPC, and `flowstate_compile`.

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
			"specification — the same `Workflow` message `flow run` submits, so a reviewer, a " +
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

	data, err := os.ReadFile(path)
	if err != nil {
		// Not a diagnostic: a path that cannot be read is a fact about the
		// invocation rather than about a workflow, and listing it beside "this step
		// references a step that does not exist" would put a mistake fixed in the
		// shell in among mistakes fixed in the file.
		return fmt.Errorf("error reading workflow file: %w", err)
	}

	// The handler over no Temporal client, exactly as `flow mcp` builds it: Compile
	// touches no run and no tenant, so there is nothing for a client to serve.
	local := server.New(nil)

	response, err := local.Compile(cmd.Context(), connect.NewRequest(&v1.CompileRequest{
		File: &v1.SourceFile{Name: path, Source: data},
	}))
	if err != nil {
		// The request itself was refused — a file past the megabyte the schema
		// bounds a source file at is the one way to reach this offline.
		return fmt.Errorf("compiling %s: %w", path, err)
	}

	surface := newSurface(cmd)

	if diagnostics := response.Msg.GetReport().GetDiagnostics(); len(diagnostics) > 0 {
		// stderr, which is the split this command turns on. `flow validate` writes
		// diagnostics to stdout because they are its answer; here the answer is the
		// specification, so a diagnostic is the account of why there is not one —
		// and a reader piping this into `jq` must never receive one.
		for _, diagnostic := range diagnostics {
			fmt.Fprintf(surface.Err, "%s:%s\n",
				surface.ErrTheme.Muted.Render(path), diagnosticFromProto(diagnostic).Error())
		}

		return errCompileRefused
	}

	// Indented unless the line-per-record form was asked for. There is one document
	// either way, because there is one specification — `jsonl` here is the compact
	// spelling rather than a different answer.
	document := format
	if document == FormatText {
		document = FormatJSON
	}

	// The workflow rather than the whole CompileResponse, because the specification
	// is what was asked for and the envelope's other field is the report — which is
	// empty on every path that reaches here, since a file with diagnostics has
	// already left above. Wrapping the answer in a level of nesting that can only
	// ever hold nothing would cost every consumer a `.workflow` and buy them no
	// fact.
	return writeJSON(surface, document, response.Msg.GetWorkflow())
}

// diagnosticFromProto returns the working type the diagnostic renderer takes.
//
// The rendering itself is [flowfile.Diagnostic.Error] — the same function `flow
// validate`, `flow fix` and the language server print through — so this is an
// adapter and not a second renderer. It exists because the compiler is reached here
// through its RPC, which answers in the schema message [flowfile.Diagnostic.Proto]
// produces, and the round trip has to come back before a line can be written.
//
// Positions narrow back to int unchanged, zero and all: a diagnostic with no
// position is a real answer, and Error already knows how to say so.
func diagnosticFromProto(d *v1.Diagnostic) flowfile.Diagnostic {
	return flowfile.Diagnostic{
		Line:    int(d.GetLine()),
		Column:  int(d.GetColumn()),
		Step:    d.GetStep(),
		Field:   d.GetField(),
		Kind:    d.GetKind(),
		Value:   d.GetValue(),
		Message: d.GetMessage(),
	}
}
