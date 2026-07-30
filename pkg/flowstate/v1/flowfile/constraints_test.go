package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/dynamicpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The schema states rules about a task's inputs, and `flow validate` used to check
// only their types.
//
// So a file declaring `method: FETCH` validated cleanly and failed at run time, with a
// message naming a Protobuf message and no line:
//
//	invalid flowstate.v1.Task.HTTP.Inputs: 2 rules violated:
//	- url: value must be a valid URI (string.uri)
//	- method: value does not match regex pattern `^(?i)(GET|POST|PUT|PATCH|DELETE)$`
//
// Everything in that sentence was knowable while the author was still looking at the
// file. What the validator lacked was not the information but the question.

// tooManyLogFields is a `log:` step with one more field than the schema allows.
func tooManyLogFields() string {
	var b strings.Builder
	b.WriteString("edition: v2026.2\nname: t\nsteps:\n  - id: s\n    log:\n      message: hi\n      fields:\n")
	for i := range 33 {
		fmt.Fprintf(&b, "        k%d: v\n", i)
	}

	return b.String()
}

// TestADeclaredRuleIsCheckedOnALiteral covers the rule kinds the schema puts on task
// inputs today — a string pattern, a string format, and a bounded map — because each
// reaches protovalidate by a different path, and a check handling one would look from
// the outside like it handled all three.
func TestADeclaredRuleIsCheckedOnALiteral(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		src   string
		input string
		says  string
	}{
		{
			name: "a string pattern",
			src: `edition: v2026.2
name: t
steps:
  - id: web
    http:
      method: FETCH
      url: https://example.com
`,
			input: "method",
			says:  "does not match regex pattern",
		},
		{
			name: "a string format",
			src: `edition: v2026.2
name: t
steps:
  - id: web
    http:
      method: GET
      url: not a uri at all
`,
			input: "url",
			says:  "must be a valid URI",
		},
		{
			name:  "a bounded map",
			src:   tooManyLogFields(),
			input: "fields",
			says:  "at most 32",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := diagnose(t, test.src)
			assert.Contains(t, got, fmt.Sprintf("input %q", test.input),
				"the diagnostic does not name the input the rule is on")
			assert.Contains(t, got, test.says,
				"the diagnostic does not carry what the schema objected to")
		})
	}
}

// TestARuleIsNotCheckedAgainstAnExpression is the direction that decides whether this
// check is worth having at all.
//
// An expression's value depends on step outputs that do not exist yet, so there is
// nothing for a rule to be checked against. Reporting one anyway would refuse a file
// that runs perfectly — which this repo's rule about diagnostics names as worse than
// missing the check entirely, because it teaches authors to stop reading the tool.
func TestARuleIsNotCheckedAgainstAnExpression(t *testing.T) {
	t.Parallel()

	require.Empty(t, diagnose(t, `edition: v2026.2
name: t
vars:
  verb: GET
steps:
  - id: web
    http:
      method: ${vars.verb}
      url: ${'https://example.com'}
`), "a value the validator cannot know was checked against the schema's rules anyway")
}

// TestAMissingRequiredInputIsReportedOnce is the trap this check builds for itself.
//
// The message the rules run against holds only the literal inputs, so every field
// supplied by an expression — and every field genuinely left out — is absent from it.
// protovalidate reports each of those as a required-field violation, which is true
// about that message and says nothing about the file.
//
// A required input really left out is already reported from the source, where there is
// a position to name. Two diagnostics about one mistake is how a validator becomes
// something people skim.
func TestAMissingRequiredInputIsReportedOnce(t *testing.T) {
	t.Parallel()

	got := diagnose(t, `edition: v2026.2
name: t
steps:
  - id: s
    http:
      method: GET
`)

	assert.Equal(t, 1, strings.Count(got, `requires input "url"`),
		"the missing input is reported more than once:\n%s", got)
	assert.NotContains(t, got, "does not accept it",
		"a field absent from the partial message was reported as a rule violation:\n%s", got)
}

// TestAWrongTypeIsNotAlsoAReportedRule keeps one mistake to one diagnostic from the
// other side.
//
// A literal the field cannot hold is reported as a type mismatch. Running the rules
// over it as well would answer a question nobody asked with a second sentence about the
// same line — and in the worst case with a rule about a value the field never accepted.
func TestAWrongTypeIsNotAlsoAReportedRule(t *testing.T) {
	t.Parallel()

	got := diagnose(t, `edition: v2026.2
name: t
steps:
  - id: s
    log:
      message: hi
      fields: not a mapping
`)

	assert.Contains(t, got, "expected a mapping", "the type mismatch stopped being reported")
	assert.NotContains(t, got, "does not accept it",
		"a value the field cannot hold was also run through the schema's rules:\n%s", got)
}

// TestTheDiagnosticCarriesTheSchemasOwnWords keeps the derivation honest.
//
// What makes this correct rather than lucky is that the rules are asked of
// protovalidate over the descriptor the registry carries, so a rule added to the schema
// is enforced the day it is added and one this package has never heard of is enforced
// too. The language server renders rules by hand for hover, which is a second reading
// of the same schema — and if this file did the same, the two would drift in the
// direction of the validator being wrong about a working file.
//
// So the assertion compares the validator's text against protovalidate's own, for the
// same message, rather than against a string written here. A hand-written table would
// fail this the moment its wording differed by a word.
func TestTheDiagnosticCarriesTheSchemasOwnWords(t *testing.T) {
	t.Parallel()

	def, known := v1.LookupTask("http")
	require.True(t, known)
	require.NotNil(t, def.Inputs)

	inputs := dynamicpb.NewMessage(def.Inputs)
	require.NoError(t, v1.PopulateLiterals(inputs, map[string]*v1.Value{
		"url":    v1.NewLiteral("https://example.com"),
		"method": v1.NewLiteral("FETCH"),
	}))

	var invalid *v1.ValidationError
	require.ErrorAs(t, v1.Validate(inputs), &invalid,
		"the fixture stopped breaking a rule, so this compares nothing")
	require.NotEmpty(t, invalid.Violations)

	got := diagnose(t, `edition: v2026.2
name: t
steps:
  - id: web
    http:
      method: FETCH
      url: https://example.com
`)

	for _, violation := range invalid.Violations {
		assert.Contains(t, got, violation.Message,
			"the validator's wording is its own rather than the schema's, so the two can drift")
	}
}

// TestPopulateLiteralsIgnoresEverythingElse pins the partial fill directly, because
// what it leaves out is what keeps the check from reporting things it cannot know.
func TestPopulateLiteralsIgnoresEverythingElse(t *testing.T) {
	t.Parallel()

	def, known := v1.LookupTask("http")
	require.True(t, known)

	inputs := dynamicpb.NewMessage(def.Inputs)
	require.NoError(t, v1.PopulateLiterals(inputs, map[string]*v1.Value{
		"method": v1.NewLiteral("GET"),
		"url":    v1.NewExpr(`"https://example.com"`),
	}))

	fields := def.Inputs.Fields()
	assert.True(t, inputs.Has(fields.ByName("method")), "a literal input was not carried")
	assert.False(t, inputs.Has(fields.ByName("url")),
		"an expression was filled in, so a rule would be checked against a value nobody knows yet")
}
