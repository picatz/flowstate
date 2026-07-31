package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Two audiences read this CLI and they want opposite things.
//
// A person wants a table they can scan, with the status findable before they have
// read the row. A program — a script, a CI job, an agent driving the CLI as a tool
// — wants a document with named fields and a schema behind it, because it has to
// address a value rather than recognize one, and column positions are not a
// contract.
//
// Serving one by guessing from a terminal check serves the other badly: a script
// that happens to run attached to a TTY would get a table, and the same script in
// CI would get something else. So the choice is a flag, defaulted to the human
// reading and asked for explicitly by everything else.
//
// The machine shape is protojson over the same messages the RPC returns, which is
// the whole reason it is worth having. The schema already exists, is versioned, is
// wire-stable, and is what the server actually sent — so a consumer reading
// `.runs[].workflowId` is reading a documented field rather than a shape invented
// here for the occasion, and a field added to the schema appears without this file
// changing.

// OutputFormat is how an answer is rendered.
type OutputFormat string

const (
	// FormatText is a person reading a terminal: aligned columns, a status that
	// can be found at a glance, prose on stderr.
	FormatText OutputFormat = "text"

	// FormatJSON is one document per invocation. The right shape for `| jq`,
	// which wants something it can index into.
	FormatJSON OutputFormat = "json"

	// FormatJSONL is one document per line. The right shape for a long listing
	// consumed as it arrives, and for `| jq -c` in a loop — a reader gets the
	// first run without waiting for the last.
	FormatJSONL OutputFormat = "jsonl"
)

// outputFormats are the accepted values, in the order help lists them.
var outputFormats = []OutputFormat{FormatText, FormatJSON, FormatJSONL}

// resolveOutputFormat reads and validates the flag on the command being run.
//
// Read off the command rather than out of a package variable, which is what
// `--output` used to be. pflag stores a flag's value in the FlagSet that declared
// it, and every command gets its own from [newRootCommand] — so asking the command
// is asking per-invocation state, while a package variable is one word shared by
// six commands and written by *building* any of them.
//
// That write is not hypothetical: pflag stores a flag's default into its bound
// pointer at declaration, so constructing the CLI mutated shared state. It has
// already produced one data race under -race and hidden one silent bug, where a
// declared default overwrote the value an environment variable had supplied.
//
// Refused rather than defaulted, because a caller who wrote --output yaml wants
// YAML, and quietly handing them a table is a worse answer than saying no. The
// message lists what is accepted, since that is the question they are about to ask.
func resolveOutputFormat(cmd *cobra.Command) (OutputFormat, error) {
	// Empty where the command does not declare one, which resolves to text below
	// rather than erroring: a verb with no `--output` has one rendering, and asking
	// it for its format should answer that rather than fail.
	requested, _ := cmd.Flags().GetString("output")
	if strings.TrimSpace(requested) == "" {
		requested = string(FormatText)
	}

	format := OutputFormat(strings.ToLower(strings.TrimSpace(requested)))
	for _, accepted := range outputFormats {
		if format == accepted {
			return format, nil
		}
	}

	names := make([]string, 0, len(outputFormats))
	for _, accepted := range outputFormats {
		names = append(names, string(accepted))
	}

	return "", fmt.Errorf("--output %q is not a format this understands; use one of %s",
		requested, strings.Join(names, ", "))
}

// Machine reports whether the format is for a program rather than a person.
func (f OutputFormat) Machine() bool { return f == FormatJSON || f == FormatJSONL }

// addOutputFlag declares --output on a command that has an answer to render.
//
// Only on the verbs that produce one. `flow cancel` reports that it asked a run to
// stop, which is an account and not an answer, so offering it a format would be
// offering something it cannot honour.
func addOutputFlag(cmd *cobra.Command) {
	names := make([]string, 0, len(outputFormats))
	for _, accepted := range outputFormats {
		names = append(names, string(accepted))
	}

	cmd.Flags().StringP("output", "o", string(FormatText),
		"how to render the answer: "+strings.Join(names, ", ")+". "+
			"json and jsonl carry the server's own schema, so a field is addressable by name")

	// Shell completion for the values, because a flag with a closed set of
	// answers should not need the help text opened to remember them.
	_ = cmd.RegisterFlagCompletionFunc("output",
		func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
			return names, cobra.ShellCompDirectiveNoFileComp
		})
}

// marshalJSON renders a message the way the schema describes it.
//
// protojson rather than encoding/json, so the field names are the schema's and an
// enum is its name rather than the integer behind it — `"STATUS_COMPLETED"` reads,
// and survives a renumbering that `4` would not.
//
// EmitUnpopulated is deliberate: a consumer indexing `.closeTime` on a run that has
// not finished should find null rather than a missing key, because the two are the
// same question and only one of them is answerable without knowing the schema.
func marshalJSON(message proto.Message, indent bool) ([]byte, error) {
	options := protojson.MarshalOptions{EmitUnpopulated: true}
	if indent {
		options.Indent = "  "
	}

	return options.Marshal(message)
}

// writeJSON writes one document, indented for a person who is about to read it and
// compact for the line-per-record form.
func writeJSON(surface *ui.UI, format OutputFormat, message proto.Message) error {
	encoded, err := marshalJSON(message, format == FormatJSON)
	if err != nil {
		return fmt.Errorf("rendering the answer as %s: %w", format, err)
	}

	_, err = fmt.Fprintf(surface.Out, "%s\n", encoded)

	return err
}

// writeRun writes a finished run, in the shape the format asks for.
//
// One function for both drivers. `flow run` and `flow run local` execute a workload
// two different ways and are otherwise the same command from a caller's position —
// so a caller reading `.outputs.stepValues` has to be reading one document, and the
// surest way to guarantee that is for one function to write it.
//
// The split between the shapes is the CLI's rule about its two streams rather than
// two renderings of one thing. A person is handed the *answer*, which is the
// outputs, because the account of how the run went has already been narrated to
// them on stderr as it happened. A program is handed the whole state document,
// because it was not watching and the status is part of what it asked for.
func writeRun(surface *ui.UI, format OutputFormat, response *v1.GetResponse) error {
	if format.Machine() {
		return writeJSON(surface, format, response)
	}

	return writeStepOutputs(surface, response)
}

// writeStepOutputs writes a finished run's outputs, and nothing at all when it has
// none.
//
// Nothing rather than an empty document, because a failed run produced no outputs
// and `{}` would claim it produced none *successfully* — a distinction a shell
// reader has only the exit code to recover.
func writeStepOutputs(surface *ui.UI, response *v1.GetResponse) error {
	outputs := response.GetOutputs()
	if outputs == nil {
		return nil
	}

	encoded, err := marshalJSON(outputs, false)
	if err != nil {
		return fmt.Errorf("formatting the outputs of the run: %w", err)
	}

	_, err = fmt.Fprintf(surface.Out, "%s\n", encoded)

	return err
}

// statusTone maps a run's status onto the palette's outcome roles.
//
// One mapping, used by every surface that shows a status, so a listing and a
// report cannot disagree about whether a terminated run reads as a failure.
//
// TERMINATED is Danger and CANCELED is Neutral, which is the distinction the whole
// lifecycle turns on: cancelling asks a workload to stop and lets it clean up, so
// a cancelled run did what it was told; terminating takes it away mid-flight and
// leaves whatever it held still held.
func statusTone(status v1.RunResponse_Status) ui.Tone {
	switch status {
	case v1.RunResponse_STATUS_COMPLETED:
		return ui.ToneSuccess
	case v1.RunResponse_STATUS_RUNNING:
		return ui.ToneInfo
	case v1.RunResponse_STATUS_FAILED, v1.RunResponse_STATUS_TERMINATED:
		return ui.ToneDanger
	case v1.RunResponse_STATUS_TIMED_OUT:
		return ui.ToneWarning
	default:
		// CANCELED and UNSPECIFIED. A run somebody stopped on purpose is not a
		// fault and must not be coloured as one.
		return ui.ToneNeutral
	}
}

// newSurface builds the rendering surface for a command's streams.
//
// A command's writers are usually the process's own, and are sometimes a buffer a
// test supplied. When they are not files there is no terminal to detect and no
// question to ask, so the surface is the plain one — which is deliberately the
// same code path a pipe takes, rather than a second implementation of "unstyled"
// that could drift from the real one.
func newSurface(cmd *cobra.Command) *ui.UI {
	out, outIsFile := cmd.OutOrStdout().(*os.File)
	errOut, errIsFile := cmd.ErrOrStderr().(*os.File)

	if !outIsFile || !errIsFile {
		return ui.Plain(cmd.OutOrStdout(), cmd.ErrOrStderr())
	}

	return ui.New(os.Stdin, out, errOut, os.Environ())
}
