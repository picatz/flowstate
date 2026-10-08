package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
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
	return "edition: v2026.4\nname: t\ninputs:\n  x:\n" + decl + "\nsteps:\n  - id: a\n    log:\n      message: hi\n"
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

// TestMinIsRefusedWithARemedy proves the retired `min:` key is reported by
// name — not as an unknown key — and that the remedy echoes the author's own
// number back inside a copy-pasteable `must: this >= N`.
func TestMinIsRefusedWithARemedy(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow("    type: int\n    min: 1\n"))
	assert.Contains(t, got, "x")
	assert.Contains(t, got, "`min:` is removed")
	assert.Contains(t, got, "must: this >= 1")
}

// TestMaxIsRefusedWithARemedy is TestMinIsRefusedWithARemedy's mirror for
// `max:`, whose remedy uses `<=` rather than `>=`.
func TestMaxIsRefusedWithARemedy(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow("    type: int\n    max: 50\n"))
	assert.Contains(t, got, "x")
	assert.Contains(t, got, "`max:` is removed")
	assert.Contains(t, got, "must: this <= 50")
}

// TestMinRemedyEchoesTheAuthorsOwnNumberVerbatim proves the diagnostic does
// not round the author's number through float64 on its way into the
// remedy — the exact conversion that made `min:` lossy on `type: int` in the
// first place (see TestIntPrecisionMinMaxBug). A remedy that reproduced the
// bug it exists to fix would be worse than none.
func TestMinRemedyEchoesTheAuthorsOwnNumberVerbatim(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow("    type: int\n    min: 9007199254740993\n"))
	assert.Contains(t, got, "must: this >= 9007199254740993")
}

// TestUniqueIsRefusedWithARemedy proves the retired `unique:` key is
// reported by name, with the fixed `must: this == this.distinct()` remedy —
// unlike `min:`/`max:`/`pattern:`, `unique:`'s remedy needs nothing from the
// value the author wrote, since `unique: true` has exactly one meaning.
func TestUniqueIsRefusedWithARemedy(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow("    type: list(dyn)\n    unique: true\n"))
	assert.Contains(t, got, "x")
	assert.Contains(t, got, "`unique:` is removed")
	assert.Contains(t, got, "must: this == this.distinct()")
}

// TestAnInvalidRegexInMustIsReportedOnceWithoutAnExample pins that a regex
// literal in `must:` is refused when the declaration is validated, whether or
// not a default or example happens to evaluate it, and that a declaration
// carrying both is still one mistake and one diagnostic (#1556).
func TestAnInvalidRegexInMustIsReportedOnceWithoutAnExample(t *testing.T) {
	t.Parallel()

	for name, extra := range map[string]string{
		"nothing evaluates it":  "",
		"a default and example": "    default: anything\n    example: anything\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := diagnose(t, constrainedInputWorkflow(
				"    type: string\n    must: \"this.matches('[')\"\n"+extra))
			assert.Equal(t, 1, strings.Count(got, "invalid matches argument"), got)
			assert.NotContains(t, got, "ERROR:")
			assert.NotContains(t, got, "<input>")
		})
	}
}

// TestATypeMismatchInMustIsOneSentenceReportedOnce is the type-check half of
// #1556: cel-go's multi-line rendering is replaced by a sentence and a column.
func TestATypeMismatchInMustIsOneSentenceReportedOnce(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow(
		"    type: string\n    must: this > 1\n    default: acme\n    example: acme\n"))
	assert.Equal(t, 1, strings.Count(got, "found no matching overload"), got)
	assert.Contains(t, got, "(column 6 of the expression)")
	assert.NotContains(t, got, "ERROR:")
	assert.NotContains(t, got, "<input>")
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
		"    type: int\n    default: 3\n    must: \"this >= 1 && this <= 50\"\n"))
	assert.Empty(t, got)
}

// TestAStaleLiteralDefaultAgainstAConstraintIsReported is the default-side
// mirror of the example test: a default is part of the specification too, so
// a default that violates the declaration's own constraint is exactly as much
// of a mistake as a bad type is.
func TestAStaleLiteralDefaultAgainstAConstraintIsReported(t *testing.T) {
	t.Parallel()

	got := diagnose(t, constrainedInputWorkflow(
		"    type: int\n    default: 0\n    must: \"this >= 1\"\n"))
	assert.Contains(t, got, "x")
	assert.Contains(t, got, "must satisfy")
}

// TestAnOutputMustThatDoesNotCompileIsReported is the output-side mirror: a
// workflow's own output contract is checked when the file is validated too,
// even though the value it will be checked against does not exist yet.
func TestAnOutputMustThatDoesNotCompileIsReported(t *testing.T) {
	t.Parallel()

	got := diagnose(t, "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    log:\n      message: hi\n"+
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
	writeFile(t, dir, "callee.yaml", `edition: v2026.4
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
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.4
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

// TestAMustCompileErrorIsATypeMismatchReportedOnceBesideOtherShapeErrors pins
// that the dedupe does not depend on the declaration's other constraints being
// valid, and that the diagnostic carries the stable code.
func TestAMustCompileErrorIsATypeMismatchReportedOnceBesideOtherShapeErrors(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		extra string
		want  int
	}{
		"alone": {want: 1},
		// The earlier shape error is the declaration's one diagnostic; the must:
		// failure waits behind it rather than being repeated on the default and example.
		"beside an earlier shape error": {extra: "    min_items: 1\n", want: 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			src := constrainedInputWorkflow(
				"    type: string\n" + test.extra + "    must: this > 1\n    default: acme\n    example: acme\n")
			ds, err := flowfile.ValidateSource([]byte(src))
			require.NoError(t, err)

			var reports int
			for _, d := range ds {
				if strings.Contains(d.Message, "found no matching overload") {
					reports++
					assert.Equal(t, v1.DiagnosticCodeTypeMismatch, d.Code, d.Message)
				}
			}
			assert.Equal(t, test.want, reports)
		})
	}
}
