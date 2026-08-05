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

// redactGetResponse returns a [*v1.GetResponse] with every declared run output this
// call site cannot vouch for replaced by its marker, in both places the answer
// travels.
//
// Both, because server.go sets [v1.GetResponse.RunOutputs] and the nested
// [v1.Workflow_StepOutputs.RunOutputs] inside the completed-run oneof to the same
// run's answer — "one finished run reads the same document," which CLAUDE.md's
// "both execution drivers must agree" section states for the two drivers and this
// schema states for the two fields carrying one value. Redacting only one would
// leave a caller who reads `.outputs.runOutputs` instead of the top-level field
// seeing the real value.
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
	if response == nil || response.GetRunOutputs() == nil || reveal {
		return response
	}

	sensitive := sensitiveOutputNames(workflow)

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

	if outs, ok := clone.Kind.(*v1.GetResponse_Outputs); ok && outs.Outputs != nil {
		outs.Outputs.RunOutputs = redactRunOutputs(outs.Outputs.RunOutputs, sensitive, false)
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
			"Display etiquette only — the value already sits in the run's history exactly like "+
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
