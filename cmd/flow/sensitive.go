package main

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// PR #205 landed `sensitive:` on InputDeclaration and OutputDeclaration, parsed and
// marshaled by flowfile, and read by nothing: `flow get`, `flow watch`, the TUI and
// the MCP tools all printed a value declared sensitive in the clear. This file is
// what makes the declaration do something, everywhere this binary renders a run's
// declared outputs for a person or an agent to read.
//
// # What this is, restated where a reader of this file will actually see it
//
// [v1.InputDeclaration.Sensitive] and [v1.OutputDeclaration.Sensitive] are display
// etiquette, not containment. The value is an ordinary part of the run's history
// exactly like any other input or output, and anyone with access to that history —
// Temporal's UI, an operator with cluster access — reads it in the clear, the same
// way they read anything else. Nothing here changes that, and nothing in this file's
// output, help text or comments may be read as though it did. The mechanism that
// keeps a value out of history entirely is `${secret(...)}`, resolved only inside
// the activity that needs it — see that field's own doc comment for the boundary.
// What this file does is keep a *declared* value off a terminal and out of an
// agent's transcript by default, which is a real property worth having and a much
// smaller one than containment.
//
// # The fail-closed case this schema forces
//
// Whether a named value is sensitive is a fact about the *workflow specification*
// that produced it — [v1.Workflow.DeclaredOutputs] — and it is the specification
// that *executed*, which is not always the one a caller submitted. `flow run
// local` and the `flowstate_run_local` MCP tool run the parsed [*v1.Workflow] in
// this same process, so there is no gap between the two and redaction is precise:
// only what the file itself marked `sensitive: true` is withheld. `flow run` sends
// its copy to a deployment that may hold one of its own under that name and run
// that instead, so it redacts precisely only against a server attestation that the
// two are the same specification, and falls back to the fail-closed case below
// otherwise — see [executedSpecification].
//
// `flow get <id>`, `flow watch <id>`, and the generic per-RPC MCP tools (a
// `flowstate_get` call, or `flow watch` polling on a later, separate invocation)
// have no such thing. [v1.GetResponse] carries the run's declared outputs by name
// and value and nothing that says which declaration produced them — by design,
// per CLAUDE.md's proto-first section, adding that would be a wire change this
// package does not own. So for every one of those call sites, whether a name is
// sensitive genuinely cannot be determined, which is exactly the case CLAUDE.md's
// fail-closed rule names: "the safe answer is to redact, not to reveal." Every
// declared output is withheld there, not only the ones a specification would have
// marked, because there is no specification to ask. The same rule covers an older
// run whose spec predates this field even when one is nominally in hand: a
// [*v1.Workflow] with no [v1.OutputDeclaration] naming a value at all answers
// [sensitiveOutputNames] with an empty set, which redacts nothing for that name —
// deliberately not fail-closed in that one case, because a value the file never
// declared sensitive is not this file's business to guess about; see
// [sensitiveOutputNames]'s own comment.
//
// # The transcript, which is not [v1.RunOutputs]
//
// Everything above redacted [v1.RunOutputs] — the run's answer, by declared name.
// It did nothing to [v1.Workflow_StepOutputs.StepValues], the transcript of every
// step's own outputs that the same call sites render beside the answer. Codex
// found the gap on PR #212: a sensitive output computed from a step —
// `outputs.token.value: ${steps.fetch.token}` with `sensitive: true` — was
// withheld at the name it surfaced under and left in the clear, unredacted, in
// the step transcript one line down, which is probably the *common* case rather
// than a corner one. [redactStepValues] is the fix, and its own comment argues
// for redacting the whole transcript rather than tracing which step fed which
// sensitive output — read it before assuming a narrower fix would do.
//
// # The carried state, which is not the transcript either
//
// A third projection of the workload's own values travels on the same response
// and was reached by neither of the above: [v1.GetResponse.EntityState], the
// bounded snapshot of a RUNNING run's top-level `vars:` and of the value each
// active `loop:` is carrying between iterations (#975). It is the same data by
// another route — a loop's `state:` binding is what a step reads back as
// `${...}` on the next iteration, and `vars:` is routinely `${inputs.<name>}` —
// so a value withheld from the transcript of a finished run was legible in the
// clear the whole time the run was still going. [redactEntityState] closes it,
// on [decideCarriedValues], the single decision it now shares with the
// transcript.
//
// # The failure sentence, which is none of the three
//
// A fourth surface, and the one a `for_each` puts a sensitive value on without
// anything naming it (#974). Everything above redacts a value the workload
// *produced*, keyed or unkeyed. A run's failure text is a sentence Flowstate
// composed *around* a value the workload was given: `for_each: ${inputs.customers}`
// over a `sensitive:` list binds each element into the body, and an http step
// that fails there says which URL it dialed. That text is neither an output nor
// a transcript entry nor carried state, so none of the three decisions above
// touched it — the same invocation printed a withheld transcript on stdout and
// the bound item, in the clear, in the prose two lines up.
//
// It is redacted rather than withheld, which is the opposite of what the three
// above do, and the difference is what a reader loses. Withholding a transcript
// costs values the specification's own author can look up in their file;
// withholding the reason a run failed leaves nothing else on the response that
// answers it, which is the argument [redactGetResponse] makes for letting the
// message through at all and this does not overturn. So the sentence stays and
// the material leaves it: [v1.SensitiveValues] is the mechanism, and it needs
// the run's *arguments*, not only its specification.
//
// That is what scopes this to the two verbs that hold both. `flow run local`
// bound the inputs in this process; `flow run` submitted them from it. `flow
// get <id>` and `flow watch <id>` are later invocations holding neither the
// file nor the arguments, and they are unchanged: a set that cannot be built is
// [v1.WithheldSensitiveValues], and withholding every failed run's reason on
// every `flow get` is precisely the cost [redactGetResponse] refuses to pay.
// `flow test` already did all of this, in flowtest, against the same set —
// which is why the set moved to [v1] rather than being written a second time.
//
// What deliberately keeps travelling in the clear, and why, is written down in
// [redactGetResponse]'s own comment rather than left to the absence of a line.

// The redaction itself is pkg/flowstate/v1's (sensitiveresponse.go), where the
// server applies it too; these names are this package's view of it, kept so
// the renderers read as they always have.

func redactedMarker(name string) string { return v1.SensitiveRedactedMarker(name) }

func sensitiveOutputNames(workflow *v1.Workflow) map[string]bool {
	return v1.SensitiveOutputNames(workflow)
}

type carriedValues = v1.CarriedValues

const (
	carriedValuesShown      = v1.CarriedValuesShown
	carriedValuesDeclared   = v1.CarriedValuesDeclared
	carriedValuesUnverified = v1.CarriedValuesUnverified

	stepTranscriptMarkerDeclared   = v1.StepTranscriptWithheldDeclared
	stepTranscriptMarkerUnverified = v1.StepTranscriptWithheldUnverified
	entityStateMarkerDeclared      = v1.EntityStateWithheldDeclared
	entityStateMarkerUnverified    = v1.EntityStateWithheldUnverified
	redactedEntityStateAllowance   = v1.RedactedEntityStateAllowance
	failureWithheldMarker          = v1.FailureWithheldMarker
)

func decideCarriedValues(workflow *v1.Workflow, reveal bool) carriedValues {
	return v1.DecideCarriedValues(workflow, reveal)
}

// redactGetResponse is where a server's decision meets this process's own.
//
// A current server decides before a response leaves it (sensitive_disclosure):
// it withholds against the specification the run executed, which this process
// usually does not hold, so its answer is both more precise and safer than one
// made here, and is rendered as given. Two answers are still redacted here: an
// older server's, which decided nothing (UNSPECIFIED), and a REVEALED answer
// that this process's own posture did not ask for. The second is how a
// request field set by something other than the operator, an agent's MCP tool
// call among them, gains nothing: the server honours the caller's authority,
// and this process honours its operator's intent.
func redactGetResponse(response *v1.GetResponse, workflow *v1.Workflow, reveal bool) *v1.GetResponse {
	switch response.GetSensitiveDisclosure() {
	case v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_NONE_DECLARED, v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD:
		return response
	case v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_REVEALED:
		if reveal {
			return response
		}
	}
	return v1.RedactGetResponse(response, workflow, reveal)
}

// noteWithheldDespiteReveal says, once, that --reveal-sensitive was typed and
// the server withheld anyway, so the operator is told what to change rather
// than left wondering why the flag did nothing.
func noteWithheldDespiteReveal(surface *ui.UI, disclosure v1.SensitiveDisclosure) {
	if disclosure != v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD {
		return
	}
	fmt.Fprintf(surface.Err, "%s the server withheld this run's sensitive values: revealing them needs an "+
		"authenticated caller whose trust policy entry lists the workload.reveal_sensitive action explicitly "+
		"(a server without authentication never reveals them)\n",
		surface.ErrTheme.Pill(ui.ToneWarning, "withheld"))
}

// noteWithheldOnce is [noteWithheldDespiteReveal] for a follow, which polls
// many times and should say so once.
func noteWithheldOnce(surface *ui.UI) func() {
	return sync.OnceFunc(func() {
		noteWithheldDespiteReveal(surface, v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD)
	})
}

func redactFailureText(response *v1.GetResponse, sensitive v1.SensitiveValues) *v1.GetResponse {
	return v1.RedactGetResponseFailures(response, sensitive)
}

// executedSpecification is the specification a follow may redact against: the one
// this process submitted, when the server attested that it is also the one that
// ran, and nil — the fail-closed case every function above already handles —
// otherwise.
//
// # The gap this closes (#734)
//
// A deployment may register its own copy of a workflow under a name a caller
// submits, and the server then executes the *registered* copy: that substitution
// is what makes `manual: denied` authorization policy rather than caller input
// (see the server's trustedWorkflow). `flow run` parsed a file, submitted it, and
// then redacted the run's outputs against that file — the copy it *sent*, which in
// exactly the case the substitution exists for is not the copy that ran. An output
// the deployment's copy marks `sensitive: true` and the submitted copy does not was
// printed in the clear, with no `--reveal-sensitive` typed, because the local file
// said it was ordinary.
//
// Holding a specification is therefore not sufficient grounds to redact against it.
// The grounds are the server saying that the specification held is the one that
// ran, which is what [v1.RunResponse.RanSubmittedSpecification] answers — and
// answers false both for a substitution and for a server too old to have an
// opinion, since a client cannot tell a deliberate silence from an absent one and
// must not treat either as assent.
//
// The cost is stated rather than hidden: a run whose specification *was*
// substituted loses the precise view, and every declared output is withheld
// instead of only the sensitive ones. That is the same answer `flow watch <id>`
// has always given for a run it did not start, for the same reason — nothing
// present can say which names are sensitive — and the alternative is printing a
// value the deployment declared secret.
func executedSpecification(submitted *v1.Workflow, started *v1.RunResponse) *v1.Workflow {
	if !started.RanSubmittedSpecification() {
		return nil
	}

	return submitted
}

// noteUnattestedSpecification says why a follow is about to withhold outputs it
// would ordinarily have shown, once, before the view starts.
//
// Without it the degraded view is indistinguishable from a bug: an author who
// wrote a file declaring one sensitive output among five sees all five withheld
// and has nothing on screen connecting that to a specification they did not
// write.
//
// It names both readings, because the client genuinely cannot tell them apart —
// a deployment-owned copy ran instead of this file, or the server is older than
// the attestation — and picking one would be this command asserting something it
// does not know. Either way the consequence is the same and is the part an author
// has to act on.
//
// stderr, and once per invocation, for the reasons [noteRevealedSensitiveValues]
// gives.
func noteUnattestedSpecification(surface *ui.UI) {
	fmt.Fprintf(surface.Err, "%s the server did not confirm this run executes the file submitted — a "+
		"deployment-owned copy may have replaced it, or the server predates the attestation — so every "+
		"declared output is withheld rather than guessed at\n",
		surface.ErrTheme.Pill(ui.ToneWarning, "unattested"))
}

// revealSensitiveFlagName is `--reveal-sensitive`, the one deliberate escape hatch
// this file provides.
const revealSensitiveFlagName = "reveal-sensitive"

// addRevealSensitiveFlag declares `--reveal-sensitive` on a command that can render
// a run's declared outputs.
//
// Defaults to false with no environment-variable fallback anywhere in this binary —
// unlike `--deployment-name` or `--auth-policy` a few flags over, which deliberately
// default from FLOWSTATE_* variables. A value that must be typed on purpose, every
// time, cannot also be satisfied by something exported once for a whole shell
// session or baked into a CI job's environment: that would be exactly the
// "allowed by default, allowed on error" shape CLAUDE.md's "fail closed" section
// refuses for a policy surface, applied here to the one flag whose entire job is to
// require deliberate, per-invocation intent.
func addRevealSensitiveFlag(cmd *cobra.Command) {
	cmd.Flags().Bool(revealSensitiveFlagName, false,
		"show values declared `sensitive: true` in the clear, instead of `[redacted: <name>]`. "+
			"Display etiquette only: the value already sits in the run's history exactly like "+
			"any other input or output, and this flag does not add or remove that; see "+
			"${secret(...)} for keeping a value out of history in the first place. "+
			"Typed on purpose, every invocation: there is no configuration default.")
}

// revealSensitiveRequested reads whether this invocation asked for the escape
// hatch. False on a command that never declared the flag, which is the same
// fail-closed answer [addRevealSensitiveFlag] documents for every other case.
func revealSensitiveRequested(cmd *cobra.Command) bool {
	reveal, _ := cmd.Flags().GetBool(revealSensitiveFlagName)
	return reveal
}

// noteRevealedSensitiveValues tells stderr that this invocation is showing
// declared-sensitive values in the clear, so a terminal session or a piped
// transcript log carries the deliberate choice next to its effect rather than
// only the effect.
//
// stderr, for the same reason every other account of a run's handling goes there
// in this CLI: stdout is the answer a pipe reads, stderr is the narration of how it
// was produced — see output.go's own header comment. Printed once per invocation
// rather than once per redacted value, because the fact worth recording is that
// the escape hatch was used at all, not how many values it happened to apply to.
func noteRevealedSensitiveValues(surface *ui.UI) {
	fmt.Fprintf(surface.Err, "%s revealing values declared sensitive, in the clear (--reveal-sensitive)\n",
		surface.ErrTheme.Pill(ui.ToneWarning, "reveal"))
}

// redactedFailure is a run's failure text with the run's own sensitive values
// removed, carrying forward the one thing the renderer asks an error for
// besides its words.
//
// # No Unwrap, deliberately
//
// It would be one line, and it would undo the whole thing. CLAUDE.md's
// "unwrapping into persisted failures" names this exactly: a scrubbed error
// that wraps the original hands the unredacted text back to anything that
// walks the chain — `%+v`, a failure converter, a future caller reaching for
// the cause — and secrets.ScrubError makes the identical choice one layer
// down. So the chain stops here, and what the chain was still needed for is
// read out at construction instead: [nextCommandsFor] is resolved once against
// the original error, so a loopback denial keeps its NEXT block.
//
// [isUsageError]'s prefix match still works, because it reads the text, and a
// run failure is not a usage error in the first place.
type redactedFailure struct {
	// text is already redacted, so holding it in a field is safe: it is what
	// this value exists to print.
	text string

	// next is [nextCommandsFor] of the error this replaced, resolved before
	// the chain was dropped. Commands are this repository's own prose, never
	// a value the workload chose.
	next []commandBlock
}

func (e *redactedFailure) Error() string { return e.text }

func (e *redactedFailure) nextCommands() []commandBlock { return e.next }

// redactFailureError returns err with the run's own sensitive values removed
// from its text — the sentence a person reads on stderr and the one `main`
// exits on.
//
// An error the set does not change is returned as itself, chain intact. That
// is the overwhelmingly common case (a workflow declaring nothing sensitive,
// or a failure that never quoted a value), and returning the original there
// means this cannot cost any other behaviour that depends on the chain in the
// runs where it has nothing to do.
func redactFailureError(err error, sensitive v1.SensitiveValues) error {
	if err == nil || sensitive.Empty() {
		return err
	}

	text := sensitive.RedactText(err.Error(), failureWithheldMarker)
	if text == err.Error() {
		return err
	}

	return &redactedFailure{text: text, next: nextCommandsFor(err)}
}

// refusedRunSensitiveValues is the redaction set for `flow run local`, `flow
// run`, `flow schedule create` and the MCP `run_local` tool when the run's
// arguments are refused before the run starts: a word the shell handed over
// that cannot be the declared type, a JSON number too large for it, or
// arguments the binder refuses. All four surfaces read the same `inputs:`
// declarations the same way and refuse the same two calls — [runInputs] (or
// the tool's own [runLocalToolInputs]) and [checkRunInputs] (or
// [checkToolRunInputs]) — so one set serves every refusal rather than one per
// caller (#2076).
//
// cmd carries [sensitiveInputWords]'s one CLI-specific source, `--input
// name=value` flags, which the MCP tool never has: its arguments arrive as
// JSON, not flags, so [sensitiveInputWords] reads an empty flag set for it
// and contributes nothing — correctly, since there is no shell word to have
// quoted in the first place.
//
// [runSensitiveValues] cannot answer here, and its fail-closed answer is the
// reason. It binds, and on this path the bind is the thing that failed, so it
// would return [v1.WithheldSensitiveValues] and the refusal would print as the
// withheld marker and nothing else — on every input mistake, on every workflow
// declaring anything sensitive. That is the wrong direction for this surface
// specifically: the schema's fourth redacted surface is "the sentence a failed
// run reports", and it says the value is removed from the text rather than the
// text being withheld, "because nothing else on a response answers 'why did
// this fail'" (workflow.proto's `sensitive:`). Withholding it would answer a
// mistyped argument with silence.
//
// So the set is built from what this process holds *without* binding: for each
// `sensitive:` declaration, the value submitted for it, and otherwise the
// default the file declares. Those are the two values a bind would have
// produced, and so the two a refusal about the arguments can quote — a `must:`
// failure reports `got <value>` for whichever of them it checked.
//
// A submitted name the workflow does not declare is deliberately not in the
// set. `sensitive:` is a property of a declaration, this one has none, and the
// refusal it earns is precisely that nothing declares it.
//
// The words themselves are in the set too, and they have to be. A refusal that
// the coercion made — `--input pin=hunter2` against `type: int` — quotes a
// word that never became a [v1.Value] at all, so no set built out of values
// can hold it, and it is the first refusal a mistyped sensitive argument
// earns. refusal is that error, read rather than reconstructed, for the one
// other refusal a set built from values and flags cannot reach either — a
// numeric-overflow refusal's own quoted number; see [sensitiveOverflowWords].
func refusedRunSensitiveValues(cmd *cobra.Command, workflow *v1.Workflow, submitted map[string]*v1.Value, refusal error, reveal bool) v1.SensitiveValues {
	names := v1.SensitiveInputNames(workflow)
	if reveal || len(names) == 0 {
		return v1.SensitiveValues{}
	}

	values := make(map[string]*v1.Value, len(names))
	for _, declaration := range workflow.GetDeclaredInputs() {
		if names[declaration.GetName()] && declaration.GetDefault() != nil {
			values[declaration.GetName()] = declaration.GetDefault()
		}
	}

	// Second, so a submitted value wins over the default it replaces, which is
	// the order [v1.BindRunInputs] applies them in.
	for name, value := range submitted {
		if names[name] {
			values[name] = value
		}
	}

	words := sensitiveInputWords(cmd, names)
	words = append(words, sensitiveOverflowWords(refusal, names)...)

	return v1.SensitiveInputValues(values, names).WithValues(words...)
}

// sensitiveInputWords is the text of every `--input <name>=<value>` this
// invocation carries whose name is declared `sensitive:`, as the shell handed
// it over.
//
// This exists for the one refusal a value set cannot otherwise reach: the
// coercion's. `--input pin=hunter2` against `type: int` never produces a
// [v1.Value] to put in a set, so the word had nowhere else to join it —
// which is what [v1.SensitiveValues.WithValues] is for.
//
// [inputCoercionError] itself no longer needs this backstop for a
// `sensitive:` declaration: it now refuses to quote the word at all in that
// case, at construction (#2073), rather than print it and trust WithValues's
// substring floor to catch it afterward — a floor that a word shorter than
// [minSensitiveSubstringRunes] survives regardless of what reads this
// function's answer. This function is kept anyway, reading every declared
// `--input` word whether or not its own coercion failed: a plaintext added
// here costs nothing when nothing else quotes it, and stands ready for
// whatever this refusal's chain still carries that does.
//
// It reads only where the name ends, never what the value means. inputs.go's
// header is emphatic that a second reader of --input is how one grammar
// becomes two, and this is deliberately not one: coercion, precedence,
// --input-file and every question about what a word *is* stay in
// [parseInputFlag], which this function does not duplicate and must follow if
// the `name=value` shape ever changes.
//
// A value given through --input-file rather than a flag is not read back
// here, and neither is a structured --input flag's own JSON. Both reach the
// set as a bound value by the ordinary path whenever they decode; the one
// refusal that quotes one of them before then — a JSON number too large for
// the declared type to carry — is [sensitiveOverflowWords]'s instead, read
// off the refusal itself rather than by opening --input-file a second time.
// A second open cannot be made to work for every source this flag and
// --input-file both accept: a FIFO is drained by the first read and blocks
// forever on a second open, and a pipe or /dev/stdin has already reached end
// of file by the time a refusal is being redacted (#2044).
func sensitiveInputWords(cmd *cobra.Command, names map[string]bool) []string {
	flags, _ := cmd.Flags().GetStringArray("input")

	words := make([]string, 0, len(flags))
	for _, flag := range flags {
		name, value, found := strings.Cut(flag, "=")
		if found && names[strings.TrimSpace(name)] {
			words = append(words, value)
		}
	}

	return words
}

// sensitiveOverflowWords returns the exact decoded text a numeric-overflow
// refusal quotes, when the input it names is declared `sensitive:` —
// otherwise nil.
//
// This exists for the one refusal a value set and [sensitiveInputWords] both
// miss: a JSON number past what an int64 or a float64 can hold —
// `{"pin":1e999}` against `sensitive: true, type: int`, from --input-file or
// from a structured --input flag's own JSON alike — fails inside
// [normalizeJSON] or [valueFromJSON] before a [*v1.Value] exists at all, so
// no set built out of values can hold it, and it never reaches the shell-word
// text [sensitiveInputWords] reads either (a structured flag's value is JSON,
// not a word). Both call sites construct a [*numericOverflowError] for
// exactly this failure, which is what this reads back rather than opening
// --input-file a second time: a second open cannot be made to work for every
// source that flag and --input-file both accept — a FIFO already drained by
// the first read blocks forever on a second open, and a pipe or /dev/stdin
// has already reached end of file — so the fix has to be reading the one
// refusal already in hand, not reading the input again (#2044).
func sensitiveOverflowWords(refusal error, names map[string]bool) []string {
	var overflow *numericOverflowError
	if !errors.As(refusal, &overflow) || !names[overflow.Input] {
		return nil
	}

	return []string{overflow.Text}
}

// runSensitiveValues is the redaction set for a run this process is starting:
// [v1.RunFailureSensitiveValues], the set `flow server` uses for the same run,
// so that both drivers withhold the same failure text. That is every value
// the workflow's `sensitive:` inputs carry, bound the way the engine will bind
// them so that a declared default is in the set exactly like a submitted
// argument, or the withhold-all set where a callee's sensitive input or a
// sensitive output cannot be enumerated from those arguments.
//
// reveal is `--reveal-sensitive`, which empties the set here rather than being
// checked at each use, so the one deliberate escape hatch stays one decision.
//
// # The specification this reads, and why it is the submitted one
//
// [executedSpecification] exists because holding a file is not holding the one
// that *ran*, and [redactGetResponse] redacts declared output names against
// the attested copy for that reason. This asks a different question, and the
// answer is the author's own file: "which of the arguments I am sending did I
// mark sensitive". Those values are in this process's hands because this
// process bound them, and a deployment that substituted its own specification
// cannot make an author's `sensitive: true` untrue about the value they typed.
// Reading the attested copy instead would mean a substitution silently
// unredacted the caller's own argument, which is the fail-open direction.
//
// A bind that fails yields the fail-closed set: this runs before the engine's
// own bind, so a refusal here means the arguments could not be enumerated, and
// [v1.SensitiveValues]'s answer for that is to withhold rather than to allow.
func runSensitiveValues(workflow *v1.Workflow, submitted map[string]*v1.Value, reveal bool) v1.SensitiveValues {
	if reveal {
		return v1.SensitiveValues{}
	}

	return v1.RunFailureSensitiveValues(workflow, submitted)
}
