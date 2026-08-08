package flowstatev1

// A [Diagnostic] carries structured *where* — Line, Column, Step, Field, Kind,
// Value — with the explicit argument that deriving a squiggle from Message text
// is how rewording moves it (see the message's own comment in the schema). Until
// #241's P2, *what* was wrong was prose only, so an agent deciding "repair the
// file / retry the run / escalate to the operator" had to parse a sentence the
// project explicitly reserves the right to reword.
//
// Code closes that gap the same way Kind already does for a run's own failure:
// a short, stable identifier chosen from what the diagnostic *is* rather than
// from what it currently says.
//
// # Why a small, deliberately incomplete set
//
// The validator has dozens of diagnostic sites (pkg/flowstate/v1/flowfile), and
// inventing a code per site would be a second grammar to keep in step with the
// first — exactly the drift class CLAUDE.md's proto-first rule and its
// generated-docs rule both exist to prevent, applied one level down. So this
// covers only the classes an agent is actually expected to branch on: an
// unknown task, a reference that cannot resolve, a value whose type the schema
// rejects, a schema rule the value violates, a construct placed somewhere the
// grammar refuses it, and a spelling this grammar retired. Everything else is
// [DiagnosticCodeGeneral] — an honest fallback, not a code invented to look
// complete. The set grows by adding a class here when a real one is missing,
// not by pre-assigning one to every message in the file.
//
// # Why a plain string rather than a proto enum
//
// [Diagnostic.Code] travels as a string on the wire (see the schema comment on
// that field) for the same reason [RunResponse_Error]'s Kind does: the closed
// set lives here, as a Go value, because that is where the diagnostics that
// assign it already live — pkg/flowstate/v1/flowfile — and a proto enum would
// be a second, schema-owned definition of the same handful of names. What must
// not drift is which strings this build actually assigns, which is why the set
// is generated into docs/reference/diagnostics.md from [DiagnosticCodes] rather
// than written by hand there — see cmd/flow/docsgen.go's renderDiagnosticCodeReference,
// which follows the same pattern [documentedEnvironmentVariables] does: a live
// Go value read directly rather than a parallel listing kept beside it.
type DiagnosticCode string

const (
	// DiagnosticCodeGeneral marks a diagnostic whose class has not earned its
	// own code. The default for every diagnostic site that does not set one
	// explicitly — see [Diagnostic.Code] — so the field is never empty on the
	// wire and a caller never has to treat "" as a seventh meaning.
	DiagnosticCodeGeneral DiagnosticCode = "general"

	// DiagnosticCodeUnknownTask marks a step (or an `undo:`) naming a task no
	// build of this binary provides.
	DiagnosticCodeUnknownTask DiagnosticCode = "unknown-task"

	// DiagnosticCodeUnresolvedReference marks an expression reading a var, an
	// input, a step, or a run field that the workflow does not declare or that
	// has not produced outputs yet.
	DiagnosticCodeUnresolvedReference DiagnosticCode = "unresolved-reference"

	// DiagnosticCodeTypeMismatch marks a value whose type a field, an operator,
	// or a function cannot accept — a literal the task's schema rejects, or an
	// expression with no matching CEL overload.
	DiagnosticCodeTypeMismatch DiagnosticCode = "type-mismatch"

	// DiagnosticCodeConstraintViolation marks a value that has the right type
	// but violates a rule the schema declares on it — a string that is not a
	// URI where the field requires one, a map with more entries than the field
	// allows.
	DiagnosticCodeConstraintViolation DiagnosticCode = "constraint-violation"

	// DiagnosticCodePlacementRefusal marks a construct the grammar refuses at
	// this position — a `undo:` nested where compensation cannot be ordered, a
	// loop nested inside another loop.
	DiagnosticCodePlacementRefusal DiagnosticCode = "placement-refusal"

	// DiagnosticCodeRetiredKey marks a bare name that is the pre-rooting
	// spelling of a step reference — what `flow fix` rewrites — reported
	// distinctly from an unresolved reference because the fix is a rewrite
	// rather than a decision about what to write.
	DiagnosticCodeRetiredKey DiagnosticCode = "retired-key"

	// DiagnosticCodeSensitiveInLog marks an input declared `sensitive:` whose
	// value is written straight into a `log:` message — bare, or concatenated
	// into it — where it would land in run history and stdout in the clear. A
	// distinct class rather than a general refusal because the fix is specific:
	// log something derived from the value instead of the value itself.
	DiagnosticCodeSensitiveInLog DiagnosticCode = "sensitive-in-log"

	// DiagnosticCodeSensitiveInPrompt marks a `wait_for_signal:`'s `prompt:`
	// that reaches an input declared `sensitive:`, or that holds a secret
	// reference. A distinct class from the `log:` one it is a sibling of,
	// because the rule is deliberately wider: a log message is read by the run's
	// own operator, so that check refuses only direct surfacing, while a prompt
	// is rendered to whoever is being asked to approve - somebody handed a run
	// id rather than the file - so reaching the value at all is refused, derived
	// or not.
	DiagnosticCodeSensitiveInPrompt DiagnosticCode = "sensitive-in-prompt"
)

// DiagnosticCodeInfo is one entry of the registry [DiagnosticCodes] returns —
// the code plus the description docs/reference/diagnostics.md renders beside
// it.
type DiagnosticCodeInfo struct {
	// Code is the stable identifier, as it appears on the wire.
	Code DiagnosticCode

	// Description says what the code marks, for a reader deciding whether their
	// own diagnostic belongs under it.
	Description string
}

// DiagnosticCodes is the whole registry, in the fixed order the reference
// document renders them — general first, since it is the default every other
// code is carved out of, then the rest in the order #241 introduced them.
//
// This is the one place the set is declared. cmd/flow/docsgen.go's
// renderDiagnosticCodeReference reads it directly to generate
// docs/reference/diagnostics.md, and TestDiagnosticCodesAreAssigned (in the
// flowfile package, which is where every code in this list is actually used)
// checks the other direction — that nothing here goes unused and nothing used
// is missing from here — so the two cannot drift apart silently.
func DiagnosticCodes() []DiagnosticCodeInfo {
	return []DiagnosticCodeInfo{
		{
			Code: DiagnosticCodeGeneral,
			Description: "Every diagnostic whose class has not earned its own code. The default: " +
				"an honest fallback rather than a code invented to look complete.",
		},
		{
			Code:        DiagnosticCodeUnknownTask,
			Description: "A step, or an `undo:`, names a task no build of this binary provides.",
		},
		{
			Code: DiagnosticCodeUnresolvedReference,
			Description: "An expression reads a var, an input, a step, or a run field the workflow " +
				"does not declare, or a step whose outputs are not available yet.",
		},
		{
			Code: DiagnosticCodeTypeMismatch,
			Description: "A value's type is wrong for where it is written — a literal a task's " +
				"schema rejects, or an expression with no matching operator overload.",
		},
		{
			Code: DiagnosticCodeConstraintViolation,
			Description: "A value has the right type but violates a rule the schema declares on " +
				"it, such as a required shape, a pattern, or a bound on size.",
		},
		{
			Code: DiagnosticCodePlacementRefusal,
			Description: "A construct is refused at the position it is written — an `undo:` the " +
				"engine cannot order, a loop nested inside another loop.",
		},
		{
			Code: DiagnosticCodeRetiredKey,
			Description: "A bare name is the pre-rooting spelling of a step reference; `flow fix` " +
				"rewrites it rather than an author needing to decide what to write.",
		},
		{
			Code: DiagnosticCodeSensitiveInLog,
			Description: "An input declared `sensitive:` is written directly into a `log:` message, " +
				"where it would be recorded in run history and stdout in the clear; log a " +
				"value derived from it instead of the value itself.",
		},
		{
			Code: DiagnosticCodeSensitiveInPrompt,
			Description: "A `wait_for_signal:`'s `prompt:` reaches an input declared `sensitive:`, or " +
				"holds a secret reference; a prompt is rendered to whoever is being asked to " +
				"approve, so ask the question without that value in it.",
		},
	}
}
