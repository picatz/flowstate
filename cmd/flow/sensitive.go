package main

import (
	"fmt"

	"github.com/spf13/cobra"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"

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
// What deliberately keeps travelling in the clear, and why, is written down in
// [redactGetResponse]'s own comment rather than left to the absence of a line.
const redactedMarkerFormat = "[redacted: %s]"

// redactedMarker is the text a redacted value renders as — [v1.InputDeclaration]'s
// own doc comment promises exactly this shape. It has to be unmistakably
// Flowstate's own annotation and not a value the workload could have produced
// itself: no workload output is spelled with a leading `[redacted:` by convention
// anywhere else in this schema, and the name inside is the declared name, not the
// value, so the marker cannot be confused with the four-character string "name"
// coming back literally.
func redactedMarker(name string) string {
	return fmt.Sprintf(redactedMarkerFormat, name)
}

// redactedValue is the placeholder [*v1.Value] a redacted entry renders as, in both
// the human and the machine surface — see this file's package comment for why the
// same value has to serve both.
func redactedValue(name string) *v1.Value {
	return &v1.Value{
		Kind: &v1.Value_Literal{
			Literal: &expr.Value{
				Kind: &expr.Value_StringValue{StringValue: redactedMarker(name)},
			},
		},
	}
}

// sensitiveOutputNames is the set of declared output names a workflow specification
// marked `sensitive: true`, or nil when no specification is available to consult at
// all.
//
// nil and "empty set" are different answers and callers below rely on the
// difference: an empty, non-nil set from a real specification means "this file
// declared no sensitive outputs," which redacts nothing; nil means "there is no
// file to ask," which is the fail-closed case that redacts everything. Collapsing
// the two would either reveal a declared-sensitive value when the wrong renderer
// forgot to pass its spec, or redact every unsensitive value the moment any
// workflow anywhere declares one sensitive output — neither is the answer this
// function's callers want.
func sensitiveOutputNames(workflow *v1.Workflow) map[string]bool {
	if workflow == nil {
		return nil
	}

	names := make(map[string]bool)
	for _, declared := range workflow.GetDeclaredOutputs() {
		if declared.GetSensitive() {
			names[declared.GetName()] = true
		}
	}

	return names
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

// redactRunOutputsValues returns values with every entry this call site cannot
// vouch for replaced by [redactedValue].
//
// sensitive nil means no specification was available at all, which is the
// fail-closed case CLAUDE.md's "fail closed" section requires: every name is
// withheld rather than guessed at, because nothing here can determine which ones
// the workflow actually marked. A non-nil sensitive redacts precisely the names it
// names and nothing else — see [sensitiveOutputNames].
//
// reveal is `--reveal-sensitive`, typed on purpose for this one invocation. It is
// the only thing that defeats either path, and it defeats both the same way: shown
// in the clear, same as an ordinary value, because an operator who asked for this by
// name gets what they asked for.
func redactRunOutputsValues(values map[string]*v1.Value, sensitive map[string]bool, reveal bool) map[string]*v1.Value {
	if reveal || len(values) == 0 {
		return values
	}

	failClosed := sensitive == nil

	redacted := make(map[string]*v1.Value, len(values))
	for name, value := range values {
		if failClosed || sensitive[name] {
			redacted[name] = redactedValue(name)
			continue
		}

		redacted[name] = value
	}

	return redacted
}

// redactRunOutputs applies [redactRunOutputsValues] to one [*v1.RunOutputs],
// returning nil unchanged the way every other reader of this message does.
func redactRunOutputs(outputs *v1.RunOutputs, sensitive map[string]bool, reveal bool) *v1.RunOutputs {
	if outputs == nil {
		return nil
	}

	return &v1.RunOutputs{Values: redactRunOutputsValues(outputs.GetValues(), sensitive, reveal)}
}

// The two reasons a step transcript is withheld, which are not the same reason
// and must not read as though they were — see [redactStepValues] for when each
// applies.
//
// A reader who is told the workflow declared something sensitive goes and looks
// at the file. On the fail-closed path there is no file to look at: `flow get`
// deliberately holds no specification, and a follow whose specification the
// server did not attest has one it is not entitled to redact against. Telling
// that reader about a declaration is telling them about something this process
// never saw.
const (
	stepTranscriptMarkerDeclared   = "step transcript withheld: this run's workflow declares sensitive data"
	stepTranscriptMarkerUnverified = "step transcript withheld: this view holds no specification to check against"
)

// The same two reasons, said about [v1.EntityState] instead of about the
// transcript. Two vocabularies for one decision, deliberately: the sentence a
// reader needs names the thing that went missing, and "step transcript
// withheld" in a `vars:` map would send them looking at the wrong part of their
// file. The *decision* is not duplicated — see [decideCarriedValues].
const (
	entityStateMarkerDeclared   = "carried state withheld: this run's workflow declares sensitive data"
	entityStateMarkerUnverified = "carried state withheld: this view holds no specification to check against"
)

// carriedValues is what one call site may do with an unnamed projection of a
// workload's own values — the step transcript, and the carried state of a
// running run. Neither can be redacted by name (see [redactStepValues] for why
// a per-name trace is not attempted), so the answer is the whole of it or none
// of it, and the two withholding answers differ only in what they may honestly
// tell the reader.
type carriedValues int

const (
	// carriedValuesShown: a real specification that declared nothing
	// sensitive, or --reveal-sensitive typed on purpose.
	carriedValuesShown carriedValues = iota

	// carriedValuesDeclared: a specification is in hand and it declares
	// sensitive data.
	carriedValuesDeclared

	// carriedValuesUnverified: there is no specification to consult, which is
	// CLAUDE.md's fail-closed case.
	carriedValuesUnverified
)

// decideCarriedValues is the one decision the step transcript and the carried
// state share. It exists so that they cannot come to disagree about what a
// specification says, which is CLAUDE.md's "a value with one meaning, written
// down twice" applied to a policy answer rather than to a constant.
//
// # Why this reads declared inputs as well as declared outputs
//
// [sensitiveOutputNames] is the right question for [v1.RunOutputs], which is
// keyed by declared output name and can therefore be redacted precisely. It is
// the wrong question here. `vars:` is very often just `${inputs.<name>}`, and a
// loop's `state:` carries whatever the body computed from it, so a workflow
// that declares one sensitive *input* and no sensitive outputs at all puts that
// input's value into [v1.EntityState.Vars] — and, by the same route, into a
// step's own outputs. Deciding on outputs alone answered "nothing sensitive
// here" for exactly that file.
//
// So the question both blunt surfaces ask is the specification-level one: does
// this file declare *anything* sensitive. The cost is stated rather than
// hidden, and it is a real one: a run whose workflow marks a single input
// sensitive now has its whole transcript withheld too, where before only a
// sensitive output did that. That is the same trade [redactStepValues] already
// argues for at length — blunt and honest beats precise-looking and leaky — and
// `--reveal-sensitive` is the escape hatch for the author who wants it back.
func decideCarriedValues(workflow *v1.Workflow, reveal bool) carriedValues {
	if reveal {
		return carriedValuesShown
	}

	if workflow == nil {
		return carriedValuesUnverified
	}

	for _, declared := range workflow.GetDeclaredInputs() {
		if declared.GetSensitive() {
			return carriedValuesDeclared
		}
	}

	for _, declared := range workflow.GetDeclaredOutputs() {
		if declared.GetSensitive() {
			return carriedValuesDeclared
		}
	}

	return carriedValuesShown
}

// redactStepValues implements this file's answer to the gap Codex found on PR
// #212: a declared output computed from a step's output — `outputs.token.value:
// ${steps.fetch.token}` with `sensitive: true` — was withheld at the *name* it
// surfaced under in [v1.RunOutputs], while the same raw value still shipped, in
// the clear, in [v1.Workflow_StepOutputs.StepValues] — the transcript `flow get`,
// `flow watch`, `flow run local` and the MCP result all render beside it. Every
// one of those readers is an untrusted-consumer surface exactly like a terminal,
// so the bypass was not a corner case; a declared output computed from a step is
// the ordinary shape a Flowfile takes.
//
// # Two designs, and why this one
//
// The precise alternative is to parse each sensitive output's `value` expression,
// collect its `steps.<id>.<name>` references (the machinery already exists —
// `flowfile`'s reference checking, `collectFreeIdentifiers` in
// pkg/flowstate/v1/constraints.go) and redact exactly those entries. It reads
// better: a transcript with one sensitive output would still show every other
// step untouched.
//
// It also has a trap that makes it the wrong choice here. Tracing catches only a
// *direct* reference. A value that reaches a sensitive output indirectly — routed
// through another step's output, or assigned to a step's own `vars:` and read
// back from there — has no `steps.<id>.<name>` selector in the sensitive output's
// own expression at all, so the trace finds nothing to redact and the raw value
// renders anyway. Worse than a blunt rule: the UI would imply coverage ("this
// file traces sensitive data") over a case it silently does not catch, which is
// exactly the shape CLAUDE.md's "fail closed" section warns against — a mechanism
// that looks precise and is not is more dangerous than one that is honestly
// blunt, because a reader trusts the one that looks precise.
//
// Making the precise version fail closed on anything it cannot trace — an
// unparseable expression, an indirect reference, anything unexpected — collapses
// it to this rule's behavior for that response anyway, on every path an author is
// actually likely to hit (a step feeding another step, or a `vars:` assignment,
// are ordinary Flowfile shapes, not edge cases). So the fallback would be doing
// most of the real work, while the traced path bought only the cases where a
// sensitive output happens to read a step directly — the minority, per the
// Codex finding itself ("most outputs are computed from steps").
//
// So: this redacts the *whole* step transcript — every named value on every
// step — the moment the specification declares anything `sensitive: true` (or
// the fail-closed case: no specification to consult at all, same as
// [redactRunOutputsValues]). "Anything", input or output: that widening is
// #975's, and [decideCarriedValues] — which now makes this call and the carried
// state's — carries the argument for it. It does not attempt to say which step actually fed
// the sensitive output, because that is exactly the claim the traced version
// could not keep honestly. The cost is real and stated here rather than
// papered over: a caller reading `.outputs.stepValues` loses the transcript of
// a run that produced one sensitive output among many unrelated ones, not only
// the one value that mattered. What survives is the *shape* — which step ids
// ran, and which named outputs each produced — because that information is
// already implied by the workflow specification itself (an author who wrote the
// file already knows its step ids and output names); only the values change,
// to one of the two markers above, so `flow watch`'s step-progress display still shows
// a run advancing rather than going dark the moment a workflow declares anything
// sensitive.
func redactStepValues(values map[string]*v1.Node_Outputs, decision carriedValues) map[string]*v1.Node_Outputs {
	if len(values) == 0 || decision == carriedValuesShown {
		return values
	}

	marker := stepTranscriptMarkerDeclared
	if decision == carriedValuesUnverified {
		marker = stepTranscriptMarkerUnverified
	}

	redacted := make(map[string]*v1.Node_Outputs, len(values))
	for stepID, outputs := range values {
		named := outputs.GetNamedValues()
		redactedNamed := make(map[string]*v1.Value, len(named))
		for name := range named {
			redactedNamed[name] = redactedValue(marker)
		}
		redacted[stepID] = &v1.Node_Outputs{NamedValues: redactedNamed}
	}

	return redacted
}

// redactStepOutputs applies [redactStepValues] to one [*v1.Workflow_StepOutputs],
// leaving [v1.Workflow_StepOutputs.RunOutputs] to its own caller — see
// [redactGetResponse], which redacts that field separately so both places
// [v1.RunOutputs] travels stay in agreement.
func redactStepOutputs(outputs *v1.Workflow_StepOutputs, decision carriedValues) *v1.Workflow_StepOutputs {
	if outputs == nil {
		return nil
	}

	outputs.StepValues = redactStepValues(outputs.GetStepValues(), decision)

	return outputs
}

// redactedEntityStateAllowance is how much larger than the answer it arrived in
// a withheld [v1.EntityState] may be — see [redactEntityState] for the rule this
// is half of.
//
// Sixteen kibibytes: about a hundred and eighty marker-replaced entries, which
// is far past what any workflow's `vars:` and concurrently-active `loop:` state
// plausibly runs to, and small enough that a surface handed the maximum is
// handed something nobody needs to bound further. It is this file's own number
// rather than a reading of the engine's, because it answers a different
// question — not "how big may a projection be" but "how much may censoring one
// cost" — and the two are free to move independently.
const redactedEntityStateAllowance = 16 << 10

// redactEntityState withholds the carried state of a RUNNING run — its
// top-level `vars:` and the value each active `loop:` is carrying into the next
// iteration — on [decideCarriedValues], the decision it shares with the step
// transcript.
//
// # Why the whole of it, and not by name
//
// For [v1.EntityState.LoopState] there is no name to redact by that means
// anything to an author: the keys are loop step ids, and the value under one is
// whatever that loop's `state:` expression last evaluated to, which the schema
// does not describe and no declaration names. For [v1.EntityState.Vars] there
// is a name — the `vars:` key — and still no declaration attached to it:
// `sensitive:` exists on [v1.InputDeclaration] and [v1.OutputDeclaration] and
// nowhere else, so a var is not something a file can mark. Redacting the subset
// of vars whose names happen to match a sensitive input would be precise-looking
// and wrong in both directions: it would miss `vars: {auth: "Bearer ${inputs.token}"}`,
// and it would blank an unrelated var that shares a name.
//
// What survives is the shape, for [redactStepValues]'s reason: the keys stay, so
// a reader still sees which vars exist and which loops are carrying state, and
// only the values become the marker.
//
// # Redaction may not inflate a message that was deliberately bounded
//
// The marker is longer than the values it replaces — around eighty bytes against
// a `vars:` entry that may be two — and this projection is bounded on purpose:
// `entityStateMaxBytes` refuses to let one serialize past 256 KiB, precisely
// because a query answer is its own resource read by a caller who did not ask
// how big it is, and how many short keys a run carries is the *workload's*
// choice. Replacing every value with a sentence therefore turns a message that
// passed that bound into one several times its size, which is CLAUDE.md's
// "bounding one resource does not bound another the peer controls the ratio to"
// with this function supplying the ratio. Codex found it on PR #1067.
//
// `entityStateMaxBytes` is deliberately not re-derived here. It lives in the
// engine, a copy of it in this file would be the same number written down twice,
// and a client enforcing its own idea of the server's bound would be wrong the
// moment the server's moved. What is enforced instead is a rule this function
// can check entirely by itself:
//
//	the withheld answer is never larger than the arrived one, or than
//	[redactedEntityStateAllowance], whichever of those two is larger.
//
// Which preserves whatever bound the answer already satisfied — a message the
// server capped at 256 KiB stays under 256 KiB — without this file having an
// opinion about what that cap is, and caps the amplification at a few kilobytes
// in absolute terms besides.
//
// It is not the simpler "never larger than what arrived", which was tried first
// and is wrong: a marker is longer than most real values, so two ordinary vars
// carrying a token each already exceed their own arrived size, and the rule
// would truncate every run it was meant to protect. The allowance is what
// separates "this projection grew a little because censoring costs words" from
// "this projection was multiplied by the number of keys a workload chose".
//
// Over that, the answer falls back to [v1.EntityState.Truncated], which is the
// schema's own existing spelling for this exact situation and not a new one:
// "cut down to stay inside this message's own bound ... omits vars and loop_state
// entirely rather than reporting a partial, silently-incomplete map". A reader
// gets a flag saying the keys are not all there rather than a projection that
// grew in the act of being censored. It costs the shape only for a run carrying
// hundreds of very short vars — and a reader who wants it back can type
// `--reveal-sensitive`, which never reaches here at all.
//
// [v1.EntityState.Truncated] is otherwise left alone: it is a fact about this
// projection's own byte bound, not a value the workload produced.
func redactEntityState(state *v1.EntityState, decision carriedValues) *v1.EntityState {
	if state == nil || decision == carriedValuesShown {
		return state
	}

	marker := entityStateMarkerDeclared
	if decision == carriedValuesUnverified {
		marker = entityStateMarkerUnverified
	}

	withheld := func(values map[string]*v1.Value) map[string]*v1.Value {
		if len(values) == 0 {
			return values
		}

		redacted := make(map[string]*v1.Value, len(values))
		for name := range values {
			redacted[name] = redactedValue(marker)
		}

		return redacted
	}

	arrived := proto.Size(state)

	state.Vars = withheld(state.GetVars())
	state.LoopState = withheld(state.GetLoopState())

	if size := proto.Size(state); size > arrived && size > redactedEntityStateAllowance {
		return &v1.EntityState{Truncated: true}
	}

	return state
}

// redactGetResponse returns a [*v1.GetResponse] with every declared run output this
// call site cannot vouch for replaced by its marker, in both places the answer
// travels, and with the step transcript and the carried state of a running run
// withheld entirely when [decideCarriedValues] says so — see [redactStepValues]
// for why those two get the blunt treatment rather than a precise one, and
// [redactEntityState] for why the carried state cannot be redacted by name at
// all.
//
// Every field of this message that can carry a workload's own values goes
// through one of those decisions. The fields that deliberately pass through
// untouched are named in the body below, with the reason, rather than left to
// the absence of a line.
//
// Both places the answer travels, because server.go sets [v1.GetResponse.RunOutputs]
// and the nested [v1.Workflow_StepOutputs.RunOutputs] inside the completed-run oneof
// to the same run's answer — "one finished run reads the same document," which
// CLAUDE.md's "both execution drivers must agree" section states for the two
// drivers and this schema states for the two fields carrying one value. Redacting
// only one would leave a caller who reads `.outputs.runOutputs` instead of the
// top-level field seeing the real value.
//
// workflow is the specification whose declarations should be trusted; nil is the
// fail-closed case this file's package comment explains: an older run whose spec
// predates this field, or a renderer with no specification in hand at all — `flow
// get`, `flow watch`, a generic MCP tool call addressed by run id alone.
//
// A clone, never the input pointer: a caller may render the same message twice
// (writeRun's text form calls writeRunOutputs and then writeStepOutputs on one
// message), and both must see the redacted answer rather than one of them racing
// ahead of a mutation to the original.
func redactGetResponse(response *v1.GetResponse, workflow *v1.Workflow, reveal bool) *v1.GetResponse {
	// No field-presence guard, on purpose, and this is the second time that
	// lesson has been paid for. The guard here used to read "return early
	// unless there are run outputs", which made the fail-closed path in
	// [redactStepValues] unreachable for a run that declared no outputs; it was
	// widened to "run outputs or a transcript", and then #975 found the third
	// field — a RUNNING entity has neither of those and a full
	// [v1.EntityState], so the guard returned the response untouched and the
	// carried state rendered in the clear. A guard that lists the fields
	// carrying values is the same list of facts written down twice, in the one
	// place nothing checks it, and it fails open every time somebody adds a
	// field and does not extend it. So the only early returns left are the two
	// that are about this call rather than about the message.
	if response == nil || reveal {
		return response
	}

	sensitive := sensitiveOutputNames(workflow)
	carried := decideCarriedValues(workflow, reveal)

	clone, ok := proto.Clone(response).(*v1.GetResponse)
	if !ok {
		// Unreachable: proto.Clone of a *v1.GetResponse always yields a
		// *v1.GetResponse. Fail closed anyway rather than assume the impossible
		// away — see CLAUDE.md's "fail closed" section — by refusing to render
		// the unredacted original.
		return &v1.GetResponse{
			WorkflowId: response.GetWorkflowId(),
			RunId:      response.GetRunId(),
			Status:     response.GetStatus(),
			StartTime:  response.GetStartTime(),
			CloseTime:  response.GetCloseTime(),
		}
	}

	clone.RunOutputs = redactRunOutputs(clone.RunOutputs, sensitive, false)

	// The carried state of a running run: the third place a workload's own
	// values travel on this message, and the one #975 found. Redacted on the
	// same decision as the transcript below, because it is the same data
	// reached by another route.
	clone.EntityState = redactEntityState(clone.GetEntityState(), carried)

	// [v1.GetResponse.Starter] passes through untouched, deliberately, and it is
	// worth saying so rather than leaving it to the absence of a line.
	//
	// What this file redacts is *the workload's data* - values a run computed or
	// was given, whose sensitivity is a property of a specification this call
	// site may not hold. A starter is not that. It is metadata the service itself
	// recorded about the run at submit, from the authenticated caller, in exactly
	// the form the run's own [v1.WorkloadIdentity] already carries and the form a
	// `signals:` rule already names - the same class as the workflow id, the run
	// id and the timestamps beside it, none of which are redacted either. A
	// caller authorized to read this response is, by construction, authorized
	// within the tenant that submitted the run.
	//
	// It is also the one field here whose whole purpose is to be *compared*: the
	// reason it carries the raw `issuer#subject` rather than a display form is so
	// a surface can check it against a policy rule. Redacting it would leave a
	// field that exists to be compared and cannot be.
	//
	// # The two failure texts pass through as well, and that is a decision (#975)
	//
	// [v1.RunResponse.Error.Message] — the arm of the oneof a failed run
	// carries — and [v1.PendingActivity.LastFailure] are both workload-chosen
	// text that can quote what a task was given: an http task's error names the
	// URL it called, which may carry a query parameter, and a plugin's error is
	// whatever that plugin decided to say. `taskspan.go` refuses to export
	// either to a collector for exactly that reason, and the question of whether
	// this file should follow it was asked here rather than left implicit.
	//
	// The answer is no, and the audiences are why. A collector is a third party
	// outside the tenancy boundary this service enforces, receiving telemetry
	// nobody asked it for; a reader of this response is inside that boundary,
	// authorized for the run, and asking one question — why did this fail. There
	// is no other field that answers it. Withholding the message would silence
	// that answer on *every* `flow get`, since `flow get` holds no specification
	// and so takes the fail-closed path unconditionally: a run reported FAILED,
	// with a marker where the reason goes, and nothing left in the response to
	// look at. CLAUDE.md's "diagnostics are a feature" is the standard the rest
	// of this binary is held to, and this would be the one place a value was
	// removed that no other field replaces.
	//
	// The rest of what travels here carries no workload values to decide about,
	// and is listed so a reader can check that rather than infer it: the two
	// ids, the status, the two timestamps, [v1.RunProgress] (step ids, signal
	// names and deadlines — the shape of the file its author already has, not
	// values it computed), and the metadata beside [v1.PendingActivity.LastFailure]
	// on the same message (an attempt count, a schedule, a phase word the engine
	// chose).

	if outs, ok := clone.Kind.(*v1.GetResponse_Outputs); ok && outs.Outputs != nil {
		outs.Outputs.RunOutputs = redactRunOutputs(outs.Outputs.RunOutputs, sensitive, false)
		outs.Outputs = redactStepOutputs(outs.Outputs, carried)
	}

	return clone
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
