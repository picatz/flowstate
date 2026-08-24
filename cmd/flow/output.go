package main

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
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

	// A flag that parsed but names something this command does not accept is an
	// invocation mistake, not a finding about whatever the command was pointed
	// at — nothing has run yet — so it is marked the same way a usage error
	// cobra itself would refuse is, per [usageError].
	return "", newUsageError(fmt.Errorf("--output %q is not a format this understands; use one of %s",
		requested, strings.Join(names, ", ")))
}

// Machine reports whether the format is for a program rather than a person.
func (f OutputFormat) Machine() bool { return f == FormatJSON || f == FormatJSONL }

// addOutputFlag declares --output on a command that has an answer to render.
//
// Only on the verbs that produce one, which now includes the verbs that change
// something: `flow cancel` was once the example of a command with only an account
// to give, but which run it acted on and whether the act is done are answers, and a
// script that cannot read them has to ask the server a second time. See
// [v1.MutationResult] for what those verbs render.
//
// Still excluded are the verbs whose whole output is the account: `flow server` and
// `flow worker` narrate a process that is running, and there is no moment at which
// they have a document to write.
func addOutputFlag(cmd *cobra.Command) {
	names := make([]string, 0, len(outputFormats))
	for _, accepted := range outputFormats {
		names = append(names, string(accepted))
	}

	// The description says "named fields" rather than "the server's own schema",
	// which is what it used to say, because that promise stopped being true for
	// every verb the moment the mutations gained the flag: they answer with
	// [v1.MutationResult] rather than with a response, since the RPCs behind them
	// answer with nothing. The schema still describes it, so a field is as
	// addressable and as guarded there as anywhere else, but it is this process's
	// account of its own request rather than something the server sent. A flag help
	// that overstates its contract is worse than one that is vague about it, and
	// each verb's own help names the document it writes.
	cmd.Flags().StringP("output", "o", string(FormatText),
		"how to render the answer: "+strings.Join(names, ", ")+". "+
			"json and jsonl are named fields rather than columns, so a value is addressable "+
			"by name: the server's own schema where a verb reads something, and the result "+
			"document this verb's help describes where it changes something")

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

// A mutation's result is an answer, so the verbs that perform one carry `--output`
// too.
//
// `flow cancel` used to be the example of a verb with nothing to render, on the
// grounds that it reports an account rather than an answer. That reading was wrong
// for the audience the flag exists for. A script that cancels a run wants to know
// which workflow it acted on, which attempt, and whether the act is done or merely
// requested, and today it has to re-`get` to learn what it just did: a round trip
// for a fact the CLI already held.
//
// The shape is one envelope shared by every mutation verb rather than seven, so a
// caller writes one `jq` expression and a verb added later is already covered. It
// is [v1.MutationResult], a schema message like every other document this CLI
// writes, and for the reason the schema exists at all: it travels. It was briefly a
// hand-written struct here on the argument that these RPCs answer with an empty
// message today (`CancelResponse{}`, `SignalResponse{}`, `TriggerScheduleResponse{}`,
// and so on) and so have no field to render, but that argues about where the values
// come from rather than about where the *shape* is described, and the shape is the
// part a script depends on. In the schema it is versioned, rendered by the one
// encoder, and guarded by `buf breaking` along with everything else a caller reads.
//
// The emptiness of those responses is still the real defect, filed as
// picatz/flowstate#374: a server that says nothing about a mutation cannot tell any
// surface whether the act changed anything. When those messages gain fields, this
// envelope stops being the whole answer and starts being the part of it this
// process knows, beside protojson of the response, exactly as `get` and `list`
// already render one.
//
// So the rule for what may appear here: only what this process knows for certain,
// which is what it asked and that the server accepted the asking. Nothing is
// inferred about the resulting state, because inventing "the run is now cancelled"
// out of an empty response is precisely the claim the prose on stderr has always
// refused to make.

// The vocabulary of `result`, which is the field a caller branches on.
const (
	// resultApplied is an act that is true once the server has answered:
	// terminating a run, deleting, pausing or resuming a schedule.
	resultApplied = "applied"

	// resultRequested is an act the server has accepted and not yet performed.
	//
	// Cancellation is cooperative, so the run is still finishing its response; a
	// triggered schedule fires after the answer, which is why there is no run id
	// to report. Saying "applied" for either would hand a script the claim the
	// prose deliberately does not make.
	resultRequested = "requested"

	// resultDelivered is the signal verb, and it is neither of the other two.
	//
	// The server has taken the signal, into the waiting gate or into the bounded
	// pending set for a gate the run has not reached yet. That is a claim about
	// the server and not about the workflow: a signal still pending when the run
	// continues as new is dropped once the carry limit is full (see
	// `drainSignals` in pkg/flowstate/v1/engine/wait.go), so a workflow that
	// never observes the signal is a possible ending of a delivery that
	// succeeded. "applied" would promise the workflow acted on it, and this
	// process cannot know that.
	resultDelivered = "delivered"
)

// writeMutationResult writes the document a mutation verb answers with.
//
// Rendered by [writeJSON] like every other answer, which is what makes the field
// names the schema's rather than this file's, and which carries EmitUnpopulated
// with it: a consumer indexing `.runId` on a schedule verb finds `""` rather than a
// missing key, because the two are the same question and only one of them is
// answerable without knowing the shape in advance.
//
// json and jsonl differ only in indentation here. A mutation is a single act, so
// the line-per-record form has exactly one record, and refusing `-o jsonl` would
// make a script that formats every flow invocation the same way special-case these.
func writeMutationResult(surface *ui.UI, format OutputFormat, result *v1.MutationResult) error {
	return writeJSON(surface, format, result)
}

// mutationFlagHelp is the paragraph every mutation verb's Long text ends with.
//
// One string rather than seven copies, because the document's shape is one
// decision and a help text restating it per verb is a thing that drifts. The fields
// are named here because a caller reading `--help` is deciding what to index, and
// sending them to the source to find out would defeat the flag.
const mutationFlagHelp = "\n\nWith `-o json` (or `-o jsonl` for one line), stdout carries a single result " +
	"document and nothing else, while the prose above is not written: " +
	"`{\"verb\", \"workflowId\", \"runId\", \"scheduleName\", \"signalName\", \"result\"}`, the " +
	"schema's `flowstate.v1.MutationResult`. `result` is \"applied\" for an act that is done when " +
	"the server answers, \"requested\" for one it has accepted and not yet performed, and " +
	"\"delivered\" for a signal the server has taken, which says nothing about whether the workflow " +
	"went on to observe it. Fields that do not apply to a verb are present and empty, so one " +
	"expression reads every one of them."

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
// # Who is on the other end of stdout
//
// The text format's stdout is machine-shaped by design, and that is load-bearing:
// `flow run local … | jq .stepValues.hello.namedValues` is documented in this
// command's own help, with no `-o json` in it, so the transcript cannot move
// behind a flag without breaking every one of those.
//
// It is also, on a terminal, the least useful line this command prints. A
// workflow that declares no outputs — which is most of them — ends a clean run
// with `{"stepValues":{"hello":{"namedValues":{}}},"runOutputs":null}`, two empty
// containers and a null under three lines of well-judged narration. Every
// decision that produces it is right on its own: stdout carries the answer,
// unpopulated fields are emitted so one jq expression works against both
// drivers, and an unset `run_outputs` is honestly "nothing to report" rather
// than an empty result. The composite is what reads badly.
//
// So the destination decides, which is the same answer `git`, `ls` and `gh`
// reach. Piped or redirected, the bytes are what they always were. On a terminal
// there is provably no parser, the narration on stderr is the whole answer, and
// [writeRunOutputs] has already said what the run produced (#551).
func writeRun(surface *ui.UI, format OutputFormat, response *v1.GetResponse) error {
	if format.Machine() {
		return writeJSON(surface, format, response)
	}

	writeRunOutputs(surface, response)

	if surface.Caps.TTY && everyDeclaredOutputRendersFaithfully(response) {
		return nil
	}

	return writeStepOutputs(surface, response)
}

// writeRunOutputs names what the run answered with, for a person.
//
// The declared outputs are already in the document on stdout — nested under the
// transcript, where a `jq` expression finds them — so this is not the answer being
// delivered a second time. It is the same service [runPosition] and the pending
// activities perform: the one or two values worth finding before the JSON is read,
// on the stream where this CLI puts its account of a run.
//
// stderr, therefore, and only for a person. A machine format carries `runOutputs` as
// a field of the message it already writes, and saying it again on another stream
// would be a second spelling of one fact.
//
// Silent when the workflow declared none, which is most workflows: an "outputs"
// heading over nothing would read as a run that failed to produce what it promised.
func writeRunOutputs(surface *ui.UI, response *v1.GetResponse) {
	values := response.GetRunOutputs().GetValues()
	if len(values) == 0 {
		return
	}

	fmt.Fprintf(surface.Err, "%s\n", surface.ErrTheme.Header.Render("outputs"))

	// Sorted, because these arrive in a protobuf map and an unsorted list would
	// reshuffle itself between two readings of the same finished run.
	for _, name := range slices.Sorted(maps.Keys(values)) {
		fmt.Fprintf(surface.Err, "  %s %s\n",
			surface.ErrTheme.Strong.Render(name),
			surface.ErrTheme.Muted.Render(renderOutputValue(values[name])))
	}
}

// renderOutputValue writes one declared output the way somebody reads it.
//
// A string is written as itself rather than quoted, because a URL somebody is about
// to copy should be copyable. Everything else is written the way it would be written
// in JSON, which is the notation the rest of this line's reader already has: a list
// is a list and a struct is an object.
//
// This is a *summary*, and it is allowed to be one. The value itself is on stdout in
// the run's own document, addressable by name and typed by the schema — so a shape
// this cannot render is a line that says less, never a value that was lost.
func renderOutputValue(value *v1.Value) string {
	return renderLiteral(value.GetLiteral())
}

// renderLiteral renders a CEL literal in JSON notation.
//
// Written here rather than borrowed because the engine's own conversion to native Go
// values is unexported, and this is a rendering for a terminal rather than a
// conversion anything computes with. A kind it does not know is named rather than
// guessed at — a line reading `(bytes)` sends a reader to the document on stdout,
// which is where the value actually is.
func renderLiteral(literal *expr.Value) string {
	switch kind := literal.GetKind().(type) {
	case *expr.Value_StringValue:
		return kind.StringValue
	case *expr.Value_BoolValue:
		return strconv.FormatBool(kind.BoolValue)
	case *expr.Value_Int64Value:
		return strconv.FormatInt(kind.Int64Value, 10)
	case *expr.Value_Uint64Value:
		return strconv.FormatUint(kind.Uint64Value, 10)
	case *expr.Value_DoubleValue:
		return strconv.FormatFloat(kind.DoubleValue, 'g', -1, 64)
	case *expr.Value_NullValue:
		return "null"

	case *expr.Value_ListValue:
		items := make([]string, 0, len(kind.ListValue.GetValues()))
		for _, item := range kind.ListValue.GetValues() {
			items = append(items, quotedLiteral(item))
		}

		return "[" + strings.Join(items, ", ") + "]"

	case *expr.Value_MapValue:
		entries := make([]string, 0, len(kind.MapValue.GetEntries()))
		for _, entry := range kind.MapValue.GetEntries() {
			entries = append(entries, quotedLiteral(entry.GetKey())+": "+quotedLiteral(entry.GetValue()))
		}
		// Sorted, because a protobuf map's entries arrive in no order and this line
		// should read the same twice.
		slices.Sort(entries)

		return "{" + strings.Join(entries, ", ") + "}"

	case *expr.Value_BytesValue:
		// Named rather than written out: bytes on a terminal are either unreadable
		// or a very long line. The value itself is on stdout —
		// [everyDeclaredOutputRendersFaithfully] is what keeps that true, by
		// holding the document back whenever this branch is reached.
		return fmt.Sprintf("(%d bytes)", len(kind.BytesValue))

	case *expr.Value_TypeValue:
		return kind.TypeValue

	case nil:
		return ""

	default:
		// A kind CEL has and this does not render — an enum, a message. Named, so
		// the line says a value is there and where to read it, rather than nothing.
		// Like the bytes branch, this one is lossy, and
		// [everyDeclaredOutputRendersFaithfully] keeps the document available
		// wherever it fires.
		return "(" + strings.TrimPrefix(fmt.Sprintf("%T", kind), "*v1alpha1.Value_") + ")"
	}
}

// everyDeclaredOutputRendersFaithfully reports whether the summary on stderr
// carries the whole of what the run answered with.
//
// [renderLiteral] is allowed to be a summary, and that permission rests entirely
// on the value itself being on stdout in the run's own document: bytes are named
// as a length, and a kind CEL has that this does not render is named as its type.
// Suppressing the document on a terminal removes the thing that made those two
// branches acceptable, so it is suppressed only when neither is reached — which
// is every workflow whose outputs are strings, numbers, booleans, lists and maps
// of the same, and so nearly all of them.
//
// The asymmetry with the step transcript is deliberate. The transcript is not the
// answer and has always been reachable through `-o json`; the declared outputs
// *are* the answer, and a person must not have to know they were shortchanged to
// go looking for it.
func everyDeclaredOutputRendersFaithfully(response *v1.GetResponse) bool {
	for _, value := range response.GetRunOutputs().GetValues() {
		literal, ok := value.GetKind().(*v1.Value_Literal)
		if !ok || !literalRendersFaithfully(literal.Literal) {
			return false
		}
	}

	return true
}

// literalRendersFaithfully is the recursive half, because a lossy value hides
// just as well inside a list or a map as it does at the top level.
func literalRendersFaithfully(literal *expr.Value) bool {
	switch kind := literal.GetKind().(type) {
	case *expr.Value_StringValue, *expr.Value_BoolValue, *expr.Value_Int64Value,
		*expr.Value_Uint64Value, *expr.Value_DoubleValue, *expr.Value_NullValue,
		*expr.Value_TypeValue, nil:
		return true

	case *expr.Value_ListValue:
		for _, item := range kind.ListValue.GetValues() {
			if !literalRendersFaithfully(item) {
				return false
			}
		}

		return true

	case *expr.Value_MapValue:
		for _, entry := range kind.MapValue.GetEntries() {
			if !literalRendersFaithfully(entry.GetKey()) || !literalRendersFaithfully(entry.GetValue()) {
				return false
			}
		}

		return true

	default:
		// Bytes, and anything renderLiteral names by type rather than by value.
		return false
	}
}

// quotedLiteral renders a literal *inside* a structure, where a string needs its
// quotes back: `[alpha, beta]` and `["alpha", "beta"]` are different values, and the
// top-level unquoted form is a convenience for the one-value case only.
func quotedLiteral(literal *expr.Value) string {
	if text, ok := literal.GetKind().(*expr.Value_StringValue); ok {
		return strconv.Quote(text.StringValue)
	}

	return renderLiteral(literal)
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

	return ui.New(os.Stdin, out, errOut, environForSurface(cmd))
}

// environForSurface is the process environment [ui.Detect] resolves colour,
// background and symbols from, with `--no-color` folded in.
//
// Folded in rather than given a second mechanism: [ui.Capabilities.Profile] is
// already NO_COLOR's own plumbing, `colorprofile.Detect` already treats it as the
// one setting nothing else overrides, and a flag that instead flipped a field on
// [ui.Capabilities] after the fact would be a second way to reach the same
// decision — the kind of duplication CLAUDE.md's "one vocabulary" section warns a
// concept spelled twice always drifts. Appended last, so it wins over whatever the
// environment already carries: `--no-color` is the most explicit ask there is, and
// a flag typed on this invocation must not lose to a variable exported for every
// invocation.
func environForSurface(cmd *cobra.Command) []string {
	environ := os.Environ()

	if noColor, _ := cmd.Flags().GetBool("no-color"); noColor {
		environ = append(environ, "NO_COLOR=1")
	}

	return environ
}
