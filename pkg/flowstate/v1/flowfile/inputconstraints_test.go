package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file is the author-time half of the constraint system: everything the
// `pkg/flowstate/v1` bind-time bite tests prove at BindRunInputs, proven again
// here through `flow validate` — with a line and a column, per this
// repository's diagnostics standard — so a mistake is caught while an author
// is still looking at the file rather than only when a run refuses it.

// minimalConstrainedInput wraps one input declaration body in a workflow that
// otherwise compiles cleanly, so a test can focus entirely on what one
// declaration says.
func constrainedInputWorkflow(decl string) string {
	return "edition: v2026.2\nname: t\ninputs:\n  x:\n" + decl + "\nsteps:\n  - id: a\n    log:\n      message: hi\n"
}

// TestAConstraintKeyMismatchedToTheDeclaredTypeIsReported is the load-time
// half of the fail-closed rule, reported with a position: a string constraint
// (min_len, here) on an int input is refused in the editor rather than
// silently never firing.
func TestAConstraintKeyMismatchedToTheDeclaredTypeIsReported(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow("    type: int\n    min_len: 1\n"))
	assert.Contains(t, got, "x")
	assert.Contains(t, got, "string input")
}

// TestPatternIsRefusedWithARemedy proves the retired `pattern:` key is
// reported by name — not as an unknown key, which would send an author
// looking for a typo they did not make — and that the remedy echoes their own
// regular expression back inside a copy-pasteable `must: this.matches(...)`.
func TestPatternIsRefusedWithARemedy(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow("    type: string\n    pattern: \"^(us|eu)-\"\n"))
	assert.Contains(t, got, "x")
	assert.Contains(t, got, "`pattern:` is removed")
	assert.Contains(t, got, "must: this.matches(r'^(us|eu)-')")
}

// TestPatternWithASingleQuoteFallsBackToADoubleQuotedLiteral proves the
// remedy still parses as CEL when the author's own regex contains a single
// quote — the one character a raw `r'...'` literal cannot hold — by switching
// to `r"..."` instead.
func TestPatternWithASingleQuoteFallsBackToADoubleQuotedLiteral(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow(`    type: string
    pattern: "[^']+"
`))
	assert.Contains(t, got, `must: this.matches(r"[^']+")`)
}

// TestAnInvalidRegexInMustIsReportedAgainstAnExample is the closest this
// grammar comes, post-pattern:, to pattern:'s own unconditional "this regex
// will never compile" diagnostic.
//
// The two are not quite the same guarantee, and this test exists to record
// the difference rather than hide it: pattern:'s regexp.Compile ran in
// [v1.CheckInputConstraintShape], against the declaration alone, so a bad
// regex was reported even on an input with no example and no default.
// must:'s regex only reaches Go's regexp package inside `matches()`, which
// CEL evaluates rather than type-checks — [v1.CompileMustExpression] parses
// and type-checks the expression itself (proven not to regress by
// TestAMustThatDoesNotCompileIsReported below) but does not evaluate it, so
// an unusable regex compiles clean until something evaluates the
// expression. `flow validate` still catches it at author time whenever the
// input carries a literal to check the constraint against — an example, a
// default — which is the shape every declaration in this repository's own
// examples has: an input worth constraining is documented with one.
func TestAnInvalidRegexInMustIsReportedAgainstAnExample(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow(
		"    type: string\n    must: \"this.matches('[')\"\n    example: anything\n"))
	assert.Contains(t, got, "x")
	assert.Contains(t, got, "example")
}

// TestAMustThatDoesNotCompileIsReported proves a must: is compiled and
// type-checked when the file is validated, not only discovered the first
// time a run happens to submit a value against it.
func TestAMustThatDoesNotCompileIsReported(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow("    type: string\n    must: \"this + \"\n"))
	assert.Contains(t, got, "x")
}

// TestAMustReferencingNowIsReported is the purity requirement, proven at
// author time: a constraint reading the clock is refused before a run ever
// exists to disagree about what it means on replay.
func TestAMustReferencingNowIsReported(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow("    type: string\n    must: \"this == now\"\n"))
	assert.Contains(t, got, "now")
}

// TestAMustThatDoesNotReturnABoolIsReported catches a must: written as a
// value rather than a predicate.
func TestAMustThatDoesNotReturnABoolIsReported(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow("    type: int\n    must: \"this + 1\"\n"))
	assert.Contains(t, got, "bool")
}

// TestAStaleLiteralExampleIsReported is #177's acceptance spelling: an
// example that violates its own declaration's constraint is a diagnostic, not
// a value nobody notices went wrong.
func TestAStaleLiteralExampleIsReported(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow(
		"    type: string\n    must: \"this.matches('^(us|eu)-')\"\n    example: mars-east-1\n"))
	assert.Contains(t, got, "example")
	assert.Contains(t, got, "must satisfy")
}

// TestAConformingExampleIsSilent is the other direction: an example that
// satisfies its own declaration reports nothing.
func TestAConformingExampleIsSilent(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow(
		"    type: string\n    must: \"this.matches('^(us|eu)-')\"\n    example: us-east-1\n"))
	assert.Empty(t, got)
}

// TestAConformingDefaultAgainstConstraintsIsSilent proves the constraint layer
// composes with the existing default check rather than replacing it: a
// default that satisfies both its type and its own constraint is not
// reported.
func TestAConformingDefaultAgainstConstraintsIsSilent(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow(
		"    type: int\n    default: 3\n    min: 1\n    max: 50\n"))
	assert.Empty(t, got)
}

// TestAStaleLiteralDefaultAgainstAConstraintIsReported is the default-side
// mirror of the example test: a default is part of the specification too, so
// a default that violates the declaration's own constraint is exactly as much
// of a mistake as a bad type is.
func TestAStaleLiteralDefaultAgainstAConstraintIsReported(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow(
		"    type: int\n    default: 0\n    min: 1\n"))
	assert.Contains(t, got, "x")
	assert.Contains(t, got, "must be >=")
}

// TestAnOutputMustThatDoesNotCompileIsReported is the output-side mirror: a
// workflow's own output contract is checked when the file is validated too,
// even though the value it will be checked against does not exist yet.
func TestAnOutputMustThatDoesNotCompileIsReported(t *testing.T) {
	t.Parallel()

	got := diagnose(t, "edition: v2026.2\nname: t\nsteps:\n  - id: a\n    log:\n      message: hi\n"+
		"outputs:\n  answer:\n    value: ${1}\n    must: \"this == now\"\n")
	assert.Contains(t, got, "answer")
	assert.Contains(t, got, "now")
}

// TestACallArgumentViolatingTheCalleesConstraintIsReported proves the call
// boundary is one of the enforcement points, not only submit: a literal
// with: argument that satisfies the callee's declared type but not its
// must: is refused at the call site — the typed-function feel extending to
// preconditions.
func TestACallArgumentViolatingTheCalleesConstraintIsReported(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", `edition: v2026.2
name: callee
inputs:
  region:
    type: string
    required: true
    must: this.matches('^(us|eu)-')
steps:
  - id: a
    log:
      message: ${inputs.region}
`)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: c
    call: ./callee.yaml
    with:
      region: mars-east-1
`)

	ds := mustValidate(t, caller)
	require.NotEmpty(t, ds, "an argument violating the callee's must: was accepted")
	assert.Contains(t, ds.Error(), "region")
	assert.Contains(t, ds.Error(), "must satisfy")
}
