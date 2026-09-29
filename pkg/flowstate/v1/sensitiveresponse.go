package flowstatev1

import (
	"fmt"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// The redaction a [GetResponse] gets for values its run's workflow declared
// `sensitive: true`.
//
// It lives here, rather than in the CLI that wrote it first, because the
// decision belongs where the executed specification and the caller's authority
// both are: the server. `flow server` applies it before a response leaves the
// process unless the caller holds workload.reveal_sensitive and asked
// (GetRequest.reveal_sensitive); the CLI and the MCP server apply the same
// functions to answers from servers too old to have decided. One mechanism,
// two callers, and the argument for each choice below travels with it.
//
// It is display control, not containment, and the rationale comments below
// say so where it matters: the values are in history like any others,
// protected there only by payload encryption (docs/ENCRYPTION.md).

// sensitiveRedactedMarkerFormat is the shape [SensitiveRedactedMarker] renders.
const sensitiveRedactedMarkerFormat = "[redacted: %s]"

// redactedMarker is the text a redacted value renders as — [InputDeclaration]'s
// own doc comment promises exactly this shape. It has to be unmistakably
// Flowstate's own annotation and not a value the workload could have produced
// itself: no workload output is spelled with a leading `[redacted:` by convention
// anywhere else in this schema, and the name inside is the declared name, not the
// value, so the marker cannot be confused with the four-character string "name"
// coming back literally.
func SensitiveRedactedMarker(name string) string {
	return fmt.Sprintf(sensitiveRedactedMarkerFormat, name)
}

// redactedValue is the placeholder [*Value] a redacted entry renders as, in both
// the human and the machine surface — see this file's package comment for why the
// same value has to serve both.
func sensitiveRedactedValue(name string) *Value {
	return &Value{
		Kind: &Value_Literal{
			Literal: &expr.Value{
				Kind: &expr.Value_StringValue{StringValue: SensitiveRedactedMarker(name)},
			},
		},
	}
}

// redactRunOutputsValues returns values with every entry this call site cannot
// vouch for replaced by [sensitiveRedactedValue].
//
// sensitive nil means no specification was available at all, which is the
// fail-closed case CLAUDE.md's "fail closed" section requires: every name is
// withheld rather than guessed at, because nothing here can determine which ones
// the workflow actually marked. A non-nil sensitive redacts precisely the names it
// names and nothing else — see [SensitiveOutputNames].
//
// reveal is `--reveal-sensitive`, typed on purpose for this one invocation. It is
// the only thing that defeats either path, and it defeats both the same way: shown
// in the clear, same as an ordinary value, because an operator who asked for this by
// name gets what they asked for.
func RedactRunOutputsValues(values map[string]*Value, sensitive map[string]bool, reveal bool) map[string]*Value {
	if reveal || len(values) == 0 {
		return values
	}

	failClosed := sensitive == nil

	redacted := make(map[string]*Value, len(values))
	for name, value := range values {
		if failClosed || sensitive[name] {
			redacted[name] = sensitiveRedactedValue(name)
			continue
		}

		redacted[name] = value
	}

	return redacted
}

// redactRunOutputs applies [RedactRunOutputsValues] to one [*RunOutputs],
// returning nil unchanged the way every other reader of this message does.
func redactRunOutputs(outputs *RunOutputs, sensitive map[string]bool, reveal bool) *RunOutputs {
	if outputs == nil {
		return nil
	}

	return &RunOutputs{Values: RedactRunOutputsValues(outputs.GetValues(), sensitive, reveal)}
}

// The two reasons a step transcript is withheld, which are not the same reason
// and must not read as though they were — see [RedactStepValues] for when each
// applies.
//
// A reader who is told the workflow declared something sensitive goes and looks
// at the file. On the fail-closed path there is no file to look at: `flow get`
// deliberately holds no specification, and a follow whose specification the
// server did not attest has one it is not entitled to redact against. Telling
// that reader about a declaration is telling them about something this process
// never saw.
const (
	StepTranscriptWithheldDeclared   = "step transcript withheld: this run's workflow declares sensitive data"
	StepTranscriptWithheldUnverified = "step transcript withheld: this view holds no specification to check against"
)

// The same two reasons, said about [EntityState] instead of about the
// transcript. Two vocabularies for one decision, deliberately: the sentence a
// reader needs names the thing that went missing, and "step transcript
// withheld" in a `vars:` map would send them looking at the wrong part of their
// file. The *decision* is not duplicated — see [DecideCarriedValues].
const (
	EntityStateWithheldDeclared   = "carried state withheld: this run's workflow declares sensitive data"
	EntityStateWithheldUnverified = "carried state withheld: this view holds no specification to check against"
)

// CarriedValues is what one call site may do with an unnamed projection of a
// workload's own values — the step transcript, and the carried state of a
// running run. Neither can be redacted by name (see [RedactStepValues] for why
// a per-name trace is not attempted), so the answer is the whole of it or none
// of it, and the two withholding answers differ only in what they may honestly
// tell the reader.
type CarriedValues int

const (
	// CarriedValuesShown: a real specification that declared nothing
	// sensitive, or --reveal-sensitive typed on purpose.
	CarriedValuesShown CarriedValues = iota

	// CarriedValuesDeclared: a specification is in hand and it declares
	// sensitive data.
	CarriedValuesDeclared

	// CarriedValuesUnverified: there is no specification to consult, which is
	// CLAUDE.md's fail-closed case.
	CarriedValuesUnverified
)

// decideCarriedValues is the one decision the step transcript and the carried
// state share. It exists so that they cannot come to disagree about what a
// specification says, which is CLAUDE.md's "a value with one meaning, written
// down twice" applied to a policy answer rather than to a constant.
//
// # Why this reads declared inputs as well as declared outputs
//
// [SensitiveOutputNames] is the right question for [RunOutputs], which is
// keyed by declared output name and can therefore be redacted precisely. It is
// the wrong question here. `vars:` is very often just `${inputs.<name>}`, and a
// loop's `state:` carries whatever the body computed from it, so a workflow
// that declares one sensitive *input* and no sensitive outputs at all puts that
// input's value into [EntityState.Vars] — and, by the same route, into a
// step's own outputs. Deciding on outputs alone answered "nothing sensitive
// here" for exactly that file.
//
// So the question both blunt surfaces ask is the specification-level one: does
// this file declare *anything* sensitive. The cost is stated rather than
// hidden, and it is a real one: a run whose workflow marks a single input
// sensitive now has its whole transcript withheld too, where before only a
// sensitive output did that. That is the same trade [RedactStepValues] already
// argues for at length — blunt and honest beats precise-looking and leaky — and
// `--reveal-sensitive` is the escape hatch for the author who wants it back.
func DecideCarriedValues(workflow *Workflow, reveal bool) CarriedValues {
	if reveal {
		return CarriedValuesShown
	}

	if workflow == nil {
		return CarriedValuesUnverified
	}

	declared, err := DeclaresSensitiveValues(workflow)
	if err != nil {
		return CarriedValuesUnverified
	}
	if declared {
		return CarriedValuesDeclared
	}

	return CarriedValuesShown
}

// redactStepValues implements this file's answer to the gap Codex found on PR
// #212: a declared output computed from a step's output — `outputs.token.value:
// ${steps.fetch.token}` with `sensitive: true` — was withheld at the *name* it
// surfaced under in [RunOutputs], while the same raw value still shipped, in
// the clear, in [Workflow_StepOutputs.StepValues] — the transcript `flow get`,
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
// [RedactRunOutputsValues]). "Anything", input or output: that widening is
// #975's, and [DecideCarriedValues] — which now makes this call and the carried
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
func RedactStepValues(values map[string]*Node_Outputs, decision CarriedValues) map[string]*Node_Outputs {
	if len(values) == 0 || decision == CarriedValuesShown {
		return values
	}

	marker := StepTranscriptWithheldDeclared
	if decision == CarriedValuesUnverified {
		marker = StepTranscriptWithheldUnverified
	}

	arrived, withheld := 0, 0
	redacted := make(map[string]*Node_Outputs, len(values))
	for stepID, outputs := range values {
		named := outputs.GetNamedValues()
		redactedNamed := make(map[string]*Value, len(named))
		for name := range named {
			redactedNamed[name] = sensitiveRedactedValue(marker)
		}
		redacted[stepID] = &Node_Outputs{NamedValues: redactedNamed}
		arrived += proto.Size(outputs)
		withheld += proto.Size(redacted[stepID])
	}

	// The marker is longer than a short value, so a transcript of many short
	// outputs would grow in the act of being censored, past the bound it was
	// admitted under: [redactEntityState]'s rule, and its allowance, apply.
	// Over it, each step keeps its id and loses its output names, which is
	// never larger than what arrived.
	if withheld > max(arrived, RedactedEntityStateAllowance) {
		for stepID := range redacted {
			redacted[stepID] = &Node_Outputs{}
		}
	}

	return redacted
}

// redactStepOutputs applies [RedactStepValues] to one [*Workflow_StepOutputs],
// leaving [Workflow_StepOutputs.RunOutputs] to its own caller — see
// [RedactGetResponse], which redacts that field separately so both places
// [RunOutputs] travels stay in agreement.
func redactStepOutputs(outputs *Workflow_StepOutputs, decision CarriedValues) *Workflow_StepOutputs {
	if outputs == nil {
		return nil
	}

	outputs.StepValues = RedactStepValues(outputs.GetStepValues(), decision)

	return outputs
}

// RedactedEntityStateAllowance is how much larger than the answer it arrived in
// a withheld [EntityState] may be — see [RedactEntityState] for the rule this
// is half of.
//
// Sixteen kibibytes: about a hundred and eighty marker-replaced entries, which
// is far past what any workflow's `vars:` and concurrently-active `loop:` state
// plausibly runs to, and small enough that a surface handed the maximum is
// handed something nobody needs to bound further. It is this file's own number
// rather than a reading of the engine's, because it answers a different
// question — not "how big may a projection be" but "how much may censoring one
// cost" — and the two are free to move independently.
const RedactedEntityStateAllowance = 16 << 10

// redactEntityState withholds the carried state of a RUNNING run — its
// top-level `vars:` and the value each active `loop:` is carrying into the next
// iteration — on [DecideCarriedValues], the decision it shares with the step
// transcript.
//
// # Why the whole of it, and not by name
//
// For [EntityState.LoopState] there is no name to redact by that means
// anything to an author: the keys are loop step ids, and the value under one is
// whatever that loop's `state:` expression last evaluated to, which the schema
// does not describe and no declaration names. For [EntityState.Vars] there
// is a name — the `vars:` key — and still no declaration attached to it:
// `sensitive:` exists on [InputDeclaration] and [OutputDeclaration] and
// nowhere else, so a var is not something a file can mark. Redacting the subset
// of vars whose names happen to match a sensitive input would be precise-looking
// and wrong in both directions: it would miss `vars: {auth: "Bearer ${inputs.token}"}`,
// and it would blank an unrelated var that shares a name.
//
// What survives is the shape, for [RedactStepValues]'s reason: the keys stay, so
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
//	[RedactedEntityStateAllowance], whichever of those two is larger.
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
// Over that, the answer falls back to [EntityState.Truncated], which is the
// schema's own existing spelling for this exact situation and not a new one:
// "cut down to stay inside this message's own bound ... omits vars and loop_state
// entirely rather than reporting a partial, silently-incomplete map". A reader
// gets a flag saying the keys are not all there rather than a projection that
// grew in the act of being censored. It costs the shape only for a run carrying
// hundreds of very short vars — and a reader who wants it back can type
// `--reveal-sensitive`, which never reaches here at all.
//
// [EntityState.Truncated] is otherwise left alone: it is a fact about this
// projection's own byte bound, not a value the workload produced.
func RedactEntityState(state *EntityState, decision CarriedValues) *EntityState {
	if state == nil || decision == CarriedValuesShown {
		return state
	}

	marker := EntityStateWithheldDeclared
	if decision == CarriedValuesUnverified {
		marker = EntityStateWithheldUnverified
	}

	withheld := func(values map[string]*Value) map[string]*Value {
		if len(values) == 0 {
			return values
		}

		redacted := make(map[string]*Value, len(values))
		for name := range values {
			redacted[name] = sensitiveRedactedValue(marker)
		}

		return redacted
	}

	arrived := proto.Size(state)

	state.Vars = withheld(state.GetVars())
	state.LoopState = withheld(state.GetLoopState())

	if size := proto.Size(state); size > arrived && size > RedactedEntityStateAllowance {
		return &EntityState{Truncated: true}
	}

	return state
}

// redactGetResponse returns a [*GetResponse] with every declared run output this
// call site cannot vouch for replaced by its marker, in both places the answer
// travels, and with the step transcript and the carried state of a running run
// withheld entirely when [DecideCarriedValues] says so — see [RedactStepValues]
// for why those two get the blunt treatment rather than a precise one, and
// [RedactEntityState] for why the carried state cannot be redacted by name at
// all.
//
// Every field of this message that can carry a workload's own values goes
// through one of those decisions. The fields that deliberately pass through
// untouched are named in the body below, with the reason, rather than left to
// the absence of a line.
//
// Both places the answer travels, because server.go sets [GetResponse.RunOutputs]
// and the nested [Workflow_StepOutputs.RunOutputs] inside the completed-run oneof
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
func RedactGetResponse(response *GetResponse, workflow *Workflow, reveal bool) *GetResponse {
	// No field-presence guard, on purpose, and this is the second time that
	// lesson has been paid for. The guard here used to read "return early
	// unless there are run outputs", which made the fail-closed path in
	// [RedactStepValues] unreachable for a run that declared no outputs; it was
	// widened to "run outputs or a transcript", and then #975 found the third
	// field — a RUNNING entity has neither of those and a full
	// [EntityState], so the guard returned the response untouched and the
	// carried state rendered in the clear. A guard that lists the fields
	// carrying values is the same list of facts written down twice, in the one
	// place nothing checks it, and it fails open every time somebody adds a
	// field and does not extend it. So the only early returns left are the two
	// that are about this call rather than about the message.
	if response == nil || reveal {
		return response
	}

	// A callee's declarations reach the caller's values through expressions
	// nothing here traces, so a run embedding one is withheld whole, prompts
	// included: the answer `flow server` gives the same run.
	if workflow != nil {
		if callee, err := CalleeDeclaresSensitiveValues(workflow); err != nil || callee {
			withheld := RedactGetResponseDecided(response, nil, CarriedValuesUnverified)
			WithholdPendingWaitPrompts(withheld)
			return withheld
		}
	}

	return RedactGetResponseDecided(response, SensitiveOutputNames(workflow), DecideCarriedValues(workflow, reveal))
}

// RedactGetResponseDecided is [RedactGetResponse] from the two answers it
// derives from a specification rather than from the specification itself:
// which output names are sensitive (nil for none known, the fail-closed
// case) and what may be done with carried values. A caller that answers
// the same question for many responses, as `flow server` does for every Get
// of one run, keeps the two small answers rather than the specification they
// came from.
func RedactGetResponseDecided(response *GetResponse, sensitive map[string]bool, carried CarriedValues) *GetResponse {
	if response == nil {
		return nil
	}

	clone, ok := proto.Clone(response).(*GetResponse)
	if !ok {
		// Unreachable: proto.Clone of a *GetResponse always yields a
		// *GetResponse. Fail closed anyway rather than assume the impossible
		// away — see CLAUDE.md's "fail closed" section — by refusing to render
		// the unredacted original.
		return &GetResponse{
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
	clone.EntityState = RedactEntityState(clone.GetEntityState(), carried)

	// A wait's prompt is a value the specification computed, and the compile-
	// and submit-time checks in waitprompt.go keep it from reaching a sensitive
	// *input*. Nothing traces a sensitive *output* back to its sources, so a
	// prompt reading `steps.fetch.token` can show exactly what a sensitive output
	// of the same step withholds. For the reason [redactStepValues] withholds
	// the whole transcript rather than trace, a known specification declaring a
	// sensitive output has its prompts withheld while carried values are not
	// shown. With no specification at all, prompts pass through here, as the
	// failure texts do below; a server that could not read the specification
	// withholds them itself.
	if carried != CarriedValuesShown && len(sensitive) > 0 {
		WithholdPendingWaitPrompts(clone)
	}

	// [GetResponse.Starter] passes through untouched, deliberately, and it is
	// worth saying so rather than leaving it to the absence of a line.
	//
	// What this file redacts is *the workload's data* - values a run computed or
	// was given, whose sensitivity is a property of a specification this call
	// site may not hold. A starter is not that. It is metadata the service itself
	// recorded about the run at submit, from the authenticated caller, in exactly
	// the form the run's own [WorkloadIdentity] already carries and the form a
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
	// [RunResponse.Error.Message] — the arm of the oneof a failed run
	// carries — and [PendingActivity.LastFailure] are both workload-chosen
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
	// ids, the status, the two timestamps, [RunProgress] (step ids, signal
	// names and deadlines — the shape of the file its author already has, not
	// values it computed), and the metadata beside [PendingActivity.LastFailure]
	// on the same message (an attempt count, a schedule, a phase word the engine
	// chose).

	if outs, ok := clone.Kind.(*GetResponse_Outputs); ok && outs.Outputs != nil {
		outs.Outputs.RunOutputs = redactRunOutputs(outs.Outputs.RunOutputs, sensitive, false)
		outs.Outputs = redactStepOutputs(outs.Outputs, carried)
	}

	return clone
}

// PromptWithheldSensitive is what a pending wait's prompt becomes when the
// run declares a sensitive output and the caller is not shown sensitive
// values: nothing proves the prompt does not read what that output withholds.
// Spelled like [PromptWithheldSecret], so it is unmistakably this system's
// annotation.
const PromptWithheldSensitive = "[prompt withheld: this run declares a sensitive output]"

// WithholdPendingWaitPrompts replaces every pending wait's prompt in
// response with [PromptWithheldSensitive], in place.
func WithholdPendingWaitPrompts(response *GetResponse) {
	for _, wait := range response.GetProgress().GetPendingWaits() {
		if wait.GetPrompt() != "" {
			wait.Prompt = PromptWithheldSensitive
			wait.PromptTruncated = false
		}
	}
}

// FailureWithheldMarker is what a failure sentence becomes when the redaction
// set could not be enumerated at all — a sensitive input this process could
// not read, or one too wide for [SensitiveValues]'s own bound. Nothing in
// the text is then provably safe, so none of it is printed, which is the same
// fail-closed answer flowtest's transcript gives for the same reason.
//
// A different sentence from the transcript's two markers, because a reader
// losing the *reason a run failed* is losing something else entirely and needs
// to be told which thing went missing.
const FailureWithheldMarker = "failure text withheld: a sensitive input could not be enumerated, so no part of this message is provably safe"

// WithholdUnrequestedTimelineFailures withholds, whole, every failure in a
// timeline the server revealed without being asked: a REVEALED answer the
// client did not accept by requesting it. A conformant server never sends
// one, so this is an anomaly, and failing closed on it costs nothing.
//
// Every other answer is rendered as the server sent it. A server that decided
// (WITHHELD, NONE_DECLARED) is trusted. An older server's UNSPECIFIED answer
// is rendered as a client before sensitive_disclosure rendered every answer,
// for the reason [RedactGetResponse] gives for its two failure texts: failure
// text is the only field that says why a run failed, and that server has
// already handed it to every caller allowed to read the run, so withholding it
// here would protect a terminal and nothing else.
func WithholdUnrequestedTimelineFailures(response *GetTimelineResponse, revealAccepted bool) {
	if response.GetSensitiveDisclosure() != SensitiveDisclosure_SENSITIVE_DISCLOSURE_REVEALED || revealAccepted {
		return
	}
	for _, entry := range response.GetEntries() {
		if entry.GetFailure() != "" {
			entry.Failure = FailureWithheldMarker
		}
	}
}

// WithholdGetResponseFailures replaces the two failure texts a [GetResponse]
// carries, the reason a failed run reports and each pending activity's last
// failure, with [FailureWithheldMarker], in place. It is for the answer
// [WithholdUnrequestedTimelineFailures] describes, on the Get side.
func WithholdGetResponseFailures(response *GetResponse) {
	if failed := response.GetError(); failed.GetMessage() != "" {
		failed.Message = FailureWithheldMarker
	}
	for _, pending := range response.GetPendingActivities() {
		if pending.GetLastFailure() != "" {
			pending.LastFailure = FailureWithheldMarker
		}
	}
}

// redactFailureText applies the same redaction to the two failure strings a
// [GetResponse] carries: the reason a failed run reports, and the last
// failure of a pending activity on a run still going.
//
// Both are named in [RedactGetResponse]'s comment as text that deliberately
// passes through, and that stays true of every call site holding no arguments
// to redact against — the fail-closed reading there is that a set cannot be
// built, and an empty [SensitiveValues] changes nothing. This is only for
// the caller that *can* build one.
//
// Each is held to the size it arrived at, or the entity-state allowance: a
// failure quoting a one-byte sensitive value many times would otherwise
// return several times its bounded size ([SensitiveValues.RedactTextWithin]).
//
// The response is mutated in place rather than cloned: every caller of this
// hands it a message [RedactGetResponse] has already cloned for them.
func RedactGetResponseFailures(response *GetResponse, sensitive SensitiveValues) *GetResponse {
	if response == nil || sensitive.Empty() {
		return response
	}

	if failed, ok := response.GetKind().(*GetResponse_Error); ok && failed.Error != nil {
		// The kind stays: it is a classification this binary chose from a
		// fixed vocabulary, not a value the workload put there, and it is the
		// only structured thing left to act on once the message is redacted.
		failed.Error.Message = sensitive.RedactTextWithin(failed.Error.GetMessage(), FailureWithheldMarker, RedactedEntityStateAllowance)
	}

	for _, pending := range response.GetPendingActivities() {
		if pending.GetLastFailure() == "" {
			continue
		}
		pending.LastFailure = sensitive.RedactTextWithin(pending.GetLastFailure(), FailureWithheldMarker, RedactedEntityStateAllowance)
	}

	return response
}

// CalleeDeclaresSensitiveValues reports whether any workflow a `call:` embeds
// in workflow declares a sensitive input or output. A caller passes an
// ordinary value into a callee's sensitive input, or reads a callee's
// sensitive output into its own ordinary one, by expression, and nothing here
// can trace which of the caller's values that made sensitive. A run for which
// this is true is withheld whole rather than by name.
func CalleeDeclaresSensitiveValues(workflow *Workflow) (bool, error) {
	for current, err := range specWorkflows(workflow) {
		if err != nil {
			return false, err
		}
		if current == workflow {
			continue
		}
		if len(sensitiveInputNames(current)) > 0 {
			return true, nil
		}
		for _, output := range current.GetDeclaredOutputs() {
			if output.GetSensitive() {
				return true, nil
			}
		}
	}
	return false, nil
}

// RunFailureSensitiveValues is the set a run's failure text is redacted with
// when the reader may not see its sensitive values: every value the run's own
// `sensitive:` inputs carry, bound as the engine binds them so a declared
// default is included.
//
// A callee's sensitive input is bound at the call, from an expression this
// cannot evaluate, so its value cannot be enumerated here. A run whose
// specification embeds a callee declaring one therefore gets the fail-closed
// set, which withholds failure text whole, as does a run whose inputs do not
// bind or whose specification cannot be walked.
//
// So does a run declaring a sensitive output, its own or a callee's. An
// output is computed by expression from ordinary inputs and step results, and
// the value it names can reach a failure (a URL a tolerated step quoted)
// before it is ever an output; which of the run's values those were cannot be
// enumerated from its inputs.
func RunFailureSensitiveValues(workflow *Workflow, inputs map[string]*Value) SensitiveValues {
	for current, err := range specWorkflows(workflow) {
		if err != nil {
			return WithheldSensitiveValues()
		}
		if current != workflow && len(sensitiveInputNames(current)) > 0 {
			return WithheldSensitiveValues()
		}
		for _, output := range current.GetDeclaredOutputs() {
			if output.GetSensitive() {
				return WithheldSensitiveValues()
			}
		}
	}

	names := sensitiveInputNames(workflow)
	if len(names) == 0 {
		return SensitiveValues{}
	}
	bound, err := BindRunInputs(workflow, inputs)
	if err != nil {
		return WithheldSensitiveValues()
	}
	return SensitiveInputValues(bound, names)
}
