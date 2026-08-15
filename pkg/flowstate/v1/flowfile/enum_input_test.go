package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// enumSpellingSource is the spelling slice B ships — the one in the issue and
// in docs/DSL.md's design note — written once so every test here exercises
// the identical shape a compiled contract, not whatever a test happens to
// accept.
const enumSpellingSource = `edition: v2026.3
name: t
inputs:
  environment:
    type: enum
    values: [staging, production]
    required: true
    example: staging
steps:
  - id: a
    log:
      message: ${inputs.environment}
`

// TestEnumInputParses pins what the spelling in the issue compiles to.
func TestEnumInputParses(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(enumSpellingSource))
	require.NoError(t, err)
	require.Len(t, wf.GetDeclaredInputs(), 1)

	environment := wf.GetDeclaredInputs()[0]
	require.Equal(t, "environment", environment.GetName())
	require.Equal(t, v1.InputDeclaration_TYPE_ENUM, environment.GetType())
	require.Equal(t, []string{"staging", "production"}, environment.GetValues())
	require.True(t, environment.GetRequired())
	require.Equal(t, "staging", environment.GetExample().GetLiteral().GetStringValue())

	require.Empty(t, flowfile.Validate(wf), "the spelling this feature ships under does not validate")
}

// findDiagnostic returns the first diagnostic reported against field, so a
// test can assert its text and position rather than merely that the set is
// non-empty.
func findDiagnostic(ds flowfile.Diagnostics, field string) (flowfile.Diagnostic, bool) {
	for _, d := range ds {
		if d.Field == field {
			return d, true
		}
	}
	return flowfile.Diagnostic{}, false
}

// TestEnumValuesBesideNonEnumType pins the first of the four required
// diagnostics: `values:` written beside a type that is not enum is reported
// at the `values:` key's own position, not at the declaration as a whole.
func TestEnumValuesBesideNonEnumType(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
inputs:
  environment:
    type: string
    values: [staging, production]
steps:
  - id: a
    log:
      message: hi
`

	// The position a correct diagnostic points to, read independently of the
	// diagnostic under test: the span [declaredInput] itself records for
	// inputs.environment.values, from a file with no diagnostics to trip over.
	wf, positions, err := flowfile.Parse([]byte(src))
	require.NoError(t, err, "the file has one semantic mistake, not a syntax one, so it should still parse")
	require.Equal(t, []string{"staging", "production"}, wf.GetDeclaredInputs()[0].GetValues(),
		"values: is read regardless of whether the type it sits beside is enum")
	want, ok := positions.At("inputs.environment.values")
	require.True(t, ok, "no position was recorded for inputs.environment.values")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	d, ok := findDiagnostic(ds, "inputs.environment.values")
	require.True(t, ok, "no diagnostic landed on inputs.environment.values; got %+v", ds)
	require.Equal(t,
		`input "environment" declares values but is declared string; values apply only to an enum input`,
		d.Message)
	require.Equal(t, want.Start.Line, d.Line)
	require.Equal(t, want.Start.Column, d.Column)
}

// TestEnumWithNoValues pins the second required diagnostic: `type: enum`
// with no `values:` is reported at the declaration, since there is no
// values: line to point at instead.
func TestEnumWithNoValues(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
inputs:
  environment:
    type: enum
steps:
  - id: a
    log:
      message: hi
`

	wf, positions, err := flowfile.Parse([]byte(src))
	require.NoError(t, err, "the file has one semantic mistake, not a syntax one, so it should still parse")
	require.Empty(t, wf.GetDeclaredInputs()[0].GetValues())
	want, ok := positions.At("inputs.environment")
	require.True(t, ok, "no position was recorded for inputs.environment")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	d, ok := findDiagnostic(ds, "inputs.environment")
	require.True(t, ok, "no diagnostic landed on inputs.environment; got %+v", ds)
	require.Equal(t,
		`input "environment" is declared enum but declares no values; an enum needs at least one member to be a closed set of anything`,
		d.Message)
	require.Equal(t, want.Start.Line, d.Line)
	require.Equal(t, want.Start.Column, d.Column)
}

// TestEnumValuesWrittenAsScalar pins the third required diagnostic:
// `values:` written as a scalar rather than a list is a parse-time shape
// mistake, refused before the file compiles at all.
func TestEnumValuesWrittenAsScalar(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
inputs:
  environment:
    type: enum
    values: staging
steps:
  - id: a
    log:
      message: hi
`

	_, _, err := flowfile.Parse([]byte(src))
	require.Error(t, err, "a scalar values: should be refused rather than silently accepted as a one-member list")

	var ds flowfile.Diagnostics
	require.True(t, asDiagnostics(err, &ds), "error was not flowfile.Diagnostics: %v", err)

	d, ok := findDiagnostic(ds, "inputs.environment.values")
	require.True(t, ok, "no diagnostic landed on inputs.environment.values; got %+v", ds)
	require.Equal(t,
		"must be a list of the values this input may hold, like [staging, production], but a string was written here",
		d.Message)
	require.Equal(t, 6, d.Line, "values: is written on line 6 of this source")
	require.NotZero(t, d.Column)
}

// TestEnumDefaultNotAMember pins the fourth required diagnostic: a `default:`
// outside the declared values is refused with the declaration's own choices
// named verbatim, and a nearest-spelling suggestion when the typo is close
// enough to be worth offering.
func TestEnumDefaultNotAMember(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
inputs:
  environment:
    type: enum
    values: [staging, production]
    default: stagng
steps:
  - id: a
    log:
      message: hi
`

	wf, positions, err := flowfile.Parse([]byte(src))
	require.NoError(t, err, "a default of the wrong value is a semantic mistake, not a syntax one")
	require.Equal(t, "stagng", wf.GetDeclaredInputs()[0].GetDefault().GetLiteral().GetStringValue())
	want, ok := positions.At("inputs.environment.default")
	require.True(t, ok, "no position was recorded for inputs.environment.default")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	d, ok := findDiagnostic(ds, "inputs.environment.default")
	require.True(t, ok, "no diagnostic landed on inputs.environment.default; got %+v", ds)
	require.Equal(t,
		`input "environment" is "stagng", which is not one of the values environment declares: "staging", "production"; did you mean "staging"?`,
		d.Message)
	require.Equal(t, want.Start.Line, d.Line)
	require.Equal(t, want.Start.Column, d.Column)
}

// TestEnumExampleNotAMember is [TestEnumDefaultNotAMember] for `example:`,
// which is checked through the identical membership path but prefixes its
// own message with "example:" to say which of the two fields is wrong — see
// [v1.CheckInputExample].
func TestEnumExampleNotAMember(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
inputs:
  environment:
    type: enum
    values: [staging, production]
    example: stagng
steps:
  - id: a
    log:
      message: hi
`

	wf, positions, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)
	require.Equal(t, "stagng", wf.GetDeclaredInputs()[0].GetExample().GetLiteral().GetStringValue())
	want, ok := positions.At("inputs.environment.example")
	require.True(t, ok, "no position was recorded for inputs.environment.example")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	d, ok := findDiagnostic(ds, "inputs.environment.example")
	require.True(t, ok, "no diagnostic landed on inputs.environment.example; got %+v", ds)
	require.Equal(t,
		`example: input "environment" is "stagng", which is not one of the values environment declares: "staging", "production"; did you mean "staging"?`,
		d.Message)
	require.Equal(t, want.Start.Line, d.Line)
	require.Equal(t, want.Start.Column, d.Column)
}

// TestEnumMustStaysLegalBesideValues pins the design record's warning: `must:`
// stays legal alongside `values:`, redundant-but-legal, so the two-step
// migration (add values:, then delete the now-redundant must:) works.
func TestEnumMustStaysLegalBesideValues(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
inputs:
  environment:
    type: enum
    values: [staging, production]
    must: this in ["staging", "production"]
steps:
  - id: a
    log:
      message: hi
`

	wf, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf), "must: alongside values: should stay legal")
}

// TestOneMemberEnumIsLegal pins the design record's judgment call: a domain
// of one is a domain, and refusing it would be a diagnostic nobody asked for.
func TestOneMemberEnumIsLegal(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
inputs:
  environment:
    type: enum
    values: [production]
steps:
  - id: a
    log:
      message: hi
`

	wf, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf), "a one-member enum should validate cleanly")
}

// TestEnumInputRoundTripsByteExact pins that Marshal is an exact inverse for
// an enum input carrying default:, example: and must: alongside values: —
// the combination where ordering of emitted keys is where a round trip
// breaks, per the rewriter lesson in CLAUDE.md: byte comparison, not "it
// still validates".
func TestEnumInputRoundTripsByteExact(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
inputs:
  environment:
    type: enum
    values: [staging, production]
    required: true
    default: staging
    description: which environment to deploy to
    example: production
    sensitive: false
    must: this != ""
steps:
  - id: a
    log:
      message: ${inputs.environment}
`

	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)

	written, err := flowfile.Marshal(wf)
	require.NoError(t, err)

	again, _, err := flowfile.Parse(written)
	require.NoError(t, err)

	round, err := flowfile.Marshal(again)
	require.NoError(t, err)

	// Byte comparison of two Marshal calls, one round further apart, which is
	// what pins the property without having to hand-predict this emitter's
	// exact formatting choices: if Marshal were not an exact inverse of what
	// it itself just wrote, the second pass would move something the first
	// pass could not have — reordered keys, dropped values, a different
	// quoting choice — and the two byte strings would differ.
	require.Equal(t, string(written), string(round), "a second round trip changed the document")

	require.Len(t, again.GetDeclaredInputs(), 1)
	environment := again.GetDeclaredInputs()[0]
	require.Equal(t, v1.InputDeclaration_TYPE_ENUM, environment.GetType())
	require.Equal(t, []string{"staging", "production"}, environment.GetValues())
	require.Equal(t, "staging", environment.GetDefault().GetLiteral().GetStringValue())
	require.Equal(t, "production", environment.GetExample().GetLiteral().GetStringValue())
	require.Equal(t, `this != ""`, environment.GetMust())
}

// TestEnumValuesDuplicateMember pins the durable-vs-local disagreement PR
// #621's last review finding named: a duplicate member in `values:` used to
// validate cleanly at author time and only get refused at durable submission,
// through [v1.Validate] on the complete request. It is now refused here too,
// at the `values:` key's own position — a duplicate is a property of the list
// as a whole rather than of one member, since protovalidate's own
// repeated.unique rule does not say *which* member is the repeat, so this
// lands on the list rather than pretending to a member-level position it does
// not have.
func TestEnumValuesDuplicateMember(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
inputs:
  environment:
    type: enum
    values: [staging, production, staging]
steps:
  - id: a
    log:
      message: hi
`

	wf, positions, err := flowfile.Parse([]byte(src))
	require.NoError(t, err, "a duplicate value is a semantic mistake, not a syntax one")
	require.Equal(t, []string{"staging", "production", "staging"}, wf.GetDeclaredInputs()[0].GetValues())
	want, ok := positions.At("inputs.environment.values")
	require.True(t, ok, "no position was recorded for inputs.environment.values")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	d, ok := findDiagnostic(ds, "inputs.environment.values")
	require.True(t, ok, "no diagnostic landed on inputs.environment.values; got %+v", ds)
	require.Equal(t,
		`input "environment" value 2 ("staging") repeats one already declared; an enum's values must be distinct`,
		d.Message)
	require.Equal(t, want.Start.Line, d.Line)
	require.Equal(t, want.Start.Column, d.Column)
}

// TestEnumValuesEmptyMember pins the second of the four boundary violations:
// an empty string as a member is refused, and — unlike the duplicate case
// above — protovalidate's string.min_len rule does identify which member
// failed, so the diagnostic lands on that member's own position
// (`values[1]`) rather than on the list as a whole.
func TestEnumValuesEmptyMember(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
inputs:
  environment:
    type: enum
    values: [staging, ""]
steps:
  - id: a
    log:
      message: hi
`

	wf, positions, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)
	require.Equal(t, []string{"staging", ""}, wf.GetDeclaredInputs()[0].GetValues())
	want, ok := positions.At("inputs.environment.values[1]")
	require.True(t, ok, "no position was recorded for inputs.environment.values[1]")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	d, ok := findDiagnostic(ds, "inputs.environment.values[1]")
	require.True(t, ok, "no diagnostic landed on inputs.environment.values[1]; got %+v", ds)
	require.Equal(t,
		`input "environment" value 1 is empty; every enum value must be at least 1 character`,
		d.Message)
	require.Equal(t, want.Start.Line, d.Line)
	require.Equal(t, want.Start.Column, d.Column)
}

// TestEnumValuesOverlongMember pins the third boundary violation: a member
// over 128 characters is refused, at that member's own position, one
// character past the bound [TestEnumValuesReachedBoundary] proves is
// otherwise accepted.
func TestEnumValuesOverlongMember(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", 129)
	src := fmt.Sprintf(`edition: v2026.3
name: t
inputs:
  environment:
    type: enum
    values: [staging, %s]
steps:
  - id: a
    log:
      message: hi
`, long)

	wf, positions, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)
	require.Equal(t, []string{"staging", long}, wf.GetDeclaredInputs()[0].GetValues())
	want, ok := positions.At("inputs.environment.values[1]")
	require.True(t, ok, "no position was recorded for inputs.environment.values[1]")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	d, ok := findDiagnostic(ds, "inputs.environment.values[1]")
	require.True(t, ok, "no diagnostic landed on inputs.environment.values[1]; got %+v", ds)
	require.Equal(t,
		`input "environment" value 1 is 129 characters, over the 128 an enum value may hold`,
		d.Message)
	require.Equal(t, want.Start.Line, d.Line)
	require.Equal(t, want.Start.Column, d.Column)
}

// TestEnumValuesOver64Members pins the fourth boundary violation: a 65th
// member is refused against the list as a whole, since protovalidate's
// repeated.max_items rule names a count rather than a member.
func TestEnumValuesOver64Members(t *testing.T) {
	t.Parallel()

	members := make([]string, 65)
	for i := range members {
		members[i] = fmt.Sprintf("v%d", i)
	}
	src := fmt.Sprintf(`edition: v2026.3
name: t
inputs:
  environment:
    type: enum
    values: [%s]
steps:
  - id: a
    log:
      message: hi
`, strings.Join(members, ", "))

	wf, positions, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)
	require.Len(t, wf.GetDeclaredInputs()[0].GetValues(), 65)
	want, ok := positions.At("inputs.environment.values")
	require.True(t, ok, "no position was recorded for inputs.environment.values")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	d, ok := findDiagnostic(ds, "inputs.environment.values")
	require.True(t, ok, "no diagnostic landed on inputs.environment.values; got %+v", ds)
	require.Equal(t,
		`input "environment" declares 65 values, but an enum may declare at most 64; trim the list, or split it into more than one input`,
		d.Message)
	require.Equal(t, want.Start.Line, d.Line)
	require.Equal(t, want.Start.Column, d.Column)
}

// TestEnumValuesReachedBoundary is the direction that makes the four tests
// above meaningful: a list at exactly the schema's own bound — 64 members,
// each exactly 128 characters, all distinct — validates cleanly. `<= bound`
// is also satisfied by a check that gives up early, per CLAUDE.md's rule
// about asserting a bound was *reached* rather than merely not exceeded; this
// is the test that would fail if [checkEnumValuesShape] (or the schema
// annotation it derives from) quietly used 63, or 127, or refused the
// boundary case itself.
func TestEnumValuesReachedBoundary(t *testing.T) {
	t.Parallel()

	members := make([]string, 64)
	for i := range members {
		// "v" plus 127 zero-padded digits: 128 characters, and unique per index
		// so this boundary case cannot also trip the distinctness rule.
		members[i] = fmt.Sprintf("v%0127d", i)
	}
	src := fmt.Sprintf(`edition: v2026.3
name: t
inputs:
  environment:
    type: enum
    values: [%s]
steps:
  - id: a
    log:
      message: hi
`, strings.Join(members, ", "))

	wf, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)
	require.Len(t, wf.GetDeclaredInputs()[0].GetValues(), 64)
	for _, v := range wf.GetDeclaredInputs()[0].GetValues() {
		assert.Len(t, v, 128)
	}

	require.Empty(t, flowfile.Validate(wf), "a 64-member enum of 128-character values should validate cleanly")
}
