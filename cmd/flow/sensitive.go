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
// that produced it — [v1.Workflow.DeclaredOutputs] — and that specification travels
// with the caller only when the caller just submitted it: `flow run`, `flow run
// local`, and the `flowstate_run_local` MCP tool all hold the parsed [*v1.Workflow]
// they started the run with, so redaction there is precise — only what the file
// itself marked `sensitive: true` is withheld, and nothing else changes.
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

// sensitiveTranscriptNames reports whether any declaration can put private data
// into the step transcript. Inputs matter here as well as outputs: loop and
// for_each failure records carry their bound input under `item`, even when the
// workflow declares no run outputs. The map's names are otherwise immaterial;
// [redactStepValues] deliberately withholds the whole transcript once it is
// non-empty. As with [sensitiveOutputNames], nil means there is no specification
// to consult and therefore selects the fail-closed path.
func sensitiveTranscriptNames(workflow *v1.Workflow) map[string]bool {
	names := sensitiveOutputNames(workflow)
	if names == nil {
		return nil
	}

	for _, declared := range workflow.GetDeclaredInputs() {
		if declared.GetSensitive() {
			names[declared.GetName()] = true
		}
	}

	return names
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

// stepTranscriptMarker is what every named value in the step transcript renders
// as once it is withheld — see [redactStepValues] for when that is.
const stepTranscriptMarker = "step transcript withheld: workflow declares a sensitive output"

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
// step — the moment any declared output is marked `sensitive: true` (or the
// fail-closed case: no specification to consult at all, same as
// [redactRunOutputsValues]). It does not attempt to say which step actually fed
// the sensitive output, because that is exactly the claim the traced version
// could not keep honestly. The cost is real and stated here rather than
// papered over: a caller reading `.outputs.stepValues` loses the transcript of
// a run that produced one sensitive output among many unrelated ones, not only
// the one value that mattered. What survives is the *shape* — which step ids
// ran, and which named outputs each produced — because that information is
// already implied by the workflow specification itself (an author who wrote the
// file already knows its step ids and output names); only the values change,
// to [stepTranscriptMarker], so `flow watch`'s step-progress display still shows
// a run advancing rather than going dark the moment a workflow declares anything
// sensitive.
func redactStepValues(values map[string]*v1.Node_Outputs, sensitive map[string]bool, reveal bool) map[string]*v1.Node_Outputs {
	if reveal || len(values) == 0 {
		return values
	}

	// sensitive == nil is the fail-closed case: withhold everything. A non-nil,
	// non-empty set means at least one declared output is sensitive: withhold
	// everything anyway, on purpose — see this function's own comment for why a
	// per-name trace is not attempted. Only a non-nil, *empty* set — a real
	// specification that declared nothing sensitive — leaves the transcript
	// untouched.
	if sensitive != nil && len(sensitive) == 0 {
		return values
	}

	redacted := make(map[string]*v1.Node_Outputs, len(values))
	for stepID, outputs := range values {
		named := outputs.GetNamedValues()
		redactedNamed := make(map[string]*v1.Value, len(named))
		for name := range named {
			redactedNamed[name] = redactedValue(stepTranscriptMarker)
		}
		redacted[stepID] = &v1.Node_Outputs{NamedValues: redactedNamed}
	}

	return redacted
}

// redactStepOutputs applies [redactStepValues] to one [*v1.Workflow_StepOutputs],
// leaving [v1.Workflow_StepOutputs.RunOutputs] to its own caller — see
// [redactGetResponse], which redacts that field separately so both places
// [v1.RunOutputs] travels stay in agreement.
func redactStepOutputs(outputs *v1.Workflow_StepOutputs, sensitive map[string]bool, reveal bool) *v1.Workflow_StepOutputs {
	if outputs == nil {
		return nil
	}

	outputs.StepValues = redactStepValues(outputs.GetStepValues(), sensitive, reveal)

	return outputs
}

// redactGetResponse returns a [*v1.GetResponse] with every declared run output this
// call site cannot vouch for replaced by its marker, in both places the answer
// travels, and with the step transcript withheld entirely when any of them
// applies — see [redactStepValues] for why the transcript gets the blunt
// treatment rather than a precise one.
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
	if response == nil || reveal || (response.GetRunOutputs() == nil && response.GetOutputs() == nil) {
		return response
	}

	sensitiveOutputs := sensitiveOutputNames(workflow)
	sensitiveTranscript := sensitiveTranscriptNames(workflow)

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

	clone.RunOutputs = redactRunOutputs(clone.RunOutputs, sensitiveOutputs, false)

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

	if outs, ok := clone.Kind.(*v1.GetResponse_Outputs); ok && outs.Outputs != nil {
		outs.Outputs.RunOutputs = redactRunOutputs(outs.Outputs.RunOutputs, sensitiveOutputs, false)
		outs.Outputs = redactStepOutputs(outs.Outputs, sensitiveTranscript, false)
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
