package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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
//	- url: must be a valid URI (string.uri)
//	- method: does not match regex pattern `^(?i)(GET|POST|PUT|PATCH|DELETE)$`
//
// Everything in that sentence was knowable while the author was still looking at the
// file. What the validator lacked was not the information but the question.

// tooManyLogFields is a `log:` step with one more field than the schema allows.
func tooManyLogFields() string {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nsteps:\n  - id: s\n    log:\n      message: hi\n      fields:\n")
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
			src: `edition: v2026.3
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
			src: `edition: v2026.3
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

	require.Empty(t, diagnose(t, `edition: v2026.3
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

	got := diagnose(t, `edition: v2026.3
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

	got := diagnose(t, `edition: v2026.3
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

	got := diagnose(t, `edition: v2026.3
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

// TestOneBadInputDoesNotSilenceTheOthers is the failure that made this check worse
// than not having one.
//
// The rules ran over a message holding every literal input at once, so a value the
// conversion refused took the whole message with it — and with it every violation the
// other inputs had. An `http` step written with both `method: FETCH` and
// `headers: {X-Count: 5}` validated *clean*: the method's mistake is plainly visible,
// and it went silent because of an unrelated key three lines below.
//
// That is worse than no check, because the file now looks examined.
func TestOneBadInputDoesNotSilenceTheOthers(t *testing.T) {
	t.Parallel()

	got := diagnose(t, `edition: v2026.3
name: t
steps:
  - id: web
    http:
      method: FETCH
      url: https://example.com
      headers:
        X-Count: 5
`)

	assert.Contains(t, got, `input "method"`,
		"a visible mistake went unreported because a different input could not be converted:\n%s", got)
	assert.Contains(t, got, `input "headers"`,
		"the value that could not be converted was not reported either:\n%s", got)
}

// TestAValueInsideAMapIsChecked is what the split turned from a silence into a
// diagnostic.
//
// The type check asks whether a map is a map; it does not look inside one. So a
// numeric value in a `map<string, string>` passed every compile-time check and failed
// at run time — and the conversion that refuses it is the only thing that knows.
// Since a conversion failure is now reported per input rather than dropped, it lands
// where the author can act on it, naming the key.
func TestAValueInsideAMapIsChecked(t *testing.T) {
	t.Parallel()

	got := diagnose(t, `edition: v2026.3
name: t
steps:
  - id: web
    http:
      method: GET
      url: https://example.com
      headers:
        X-Count: 5
`)

	assert.Contains(t, got, `input "headers"`, "a bad value inside a map was not reported")
	assert.Contains(t, got, `key "X-Count"`, "the diagnostic does not name the key at fault")
}

// TestNoTaskInputsMessageDeclaresACrossFieldRule checks the limitation the split
// creates, rather than leaving it as a sentence someone has to remember.
//
// Each input is validated in a message holding only that input, so a rule spanning two
// fields — protovalidate's message-level CEL — could not fire: neither message would
// hold both. No task's inputs declare one today. The day one does, this fails, and
// whoever adds it learns that the per-input pass needs a whole-message companion
// rather than discovering later that their rule never ran.
func TestNoTaskInputsMessageDeclaresACrossFieldRule(t *testing.T) {
	t.Parallel()

	for _, name := range v1.TaskNames() {
		def, known := v1.LookupTask(name)
		require.True(t, known)
		if def.Inputs == nil {
			continue
		}

		rules, _ := proto.GetExtension(def.Inputs.Options(), validate.E_Message).(*validate.MessageRules)
		assert.Empty(t, rules.GetCel(),
			"task %q declares a message-level rule, which the per-input check cannot evaluate; "+
				"it needs a whole-message pass alongside the per-input one", name)
	}
}
