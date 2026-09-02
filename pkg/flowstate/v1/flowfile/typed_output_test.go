package flowfile_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The `type:` half of the `outputs:` contract, from the file's side.
//
// The schema's side — that the value a run computes is checked against the
// declaration before it is reported — is a conformance case, because it is a
// claim about both drivers rather than about a document. What is here is only
// what a document can be wrong about on its own: a type that is not a type, a
// `values:` list beside something that is not an enum, and a declared type that
// contradicts what this validator can already see about `value:`.

// typedOutputSource builds a workflow whose single output declares outputs
// verbatim, so each test differs by exactly the declaration under test.
func typedOutputSource(declaration string) string {
	return fmt.Sprintf(`edition: v2026.3
name: t
inputs:
  release:
    type: string
    default: "1.0"
  count:
    type: int
    default: 1
steps:
  - id: a
    log:
      message: ${inputs.release}
outputs:
%s`, declaration)
}

// TestTypedOutputParses pins what the spelling compiles to, including the
// enum pairing, and that the round trip back to YAML is exact — which is what
// `flow fix --check` holds the corpus to.
func TestTypedOutputParses(t *testing.T) {
	t.Parallel()

	source := typedOutputSource(`  release:
    value: ${inputs.release}
    type: string
  channel:
    value: ${"stable"}
    type: enum
    values:
      - stable
      - beta
  untyped:
    value: ${inputs.count}
`)

	wf, _, err := flowfile.Parse([]byte(source))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf))
	require.Len(t, wf.GetDeclaredOutputs(), 3)

	assert.Equal(t, v1.InputDeclaration_TYPE_STRING, wf.GetDeclaredOutputs()[0].GetType())
	assert.Equal(t, v1.InputDeclaration_TYPE_ENUM, wf.GetDeclaredOutputs()[1].GetType())
	assert.Equal(t, []string{"stable", "beta"}, wf.GetDeclaredOutputs()[1].GetValues())

	// The whole point of the field being optional: an output that declares no
	// type is not a type mistake, it is a declaration that says nothing about
	// its shape, and it must stay that way through parse and marshal alike.
	assert.Equal(t, v1.InputDeclaration_TYPE_UNSPECIFIED, wf.GetDeclaredOutputs()[2].GetType())

	marshalled, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	assert.Equal(t, source, string(marshalled))
}

// TestTypedOutputRejectsAnUnknownType holds the diagnostic an author gets for a
// word that is not one of the types, since "not a type" is the mistake a person
// coming from a language with `str` or `boolean` makes first.
func TestTypedOutputRejectsAnUnknownType(t *testing.T) {
	t.Parallel()

	_, _, err := flowfile.Parse([]byte(typedOutputSource(`  release:
    value: ${inputs.release}
    type: text
`)))
	require.Error(t, err)

	var ds flowfile.Diagnostics
	require.True(t, asDiagnostics(err, &ds), "error was not flowfile.Diagnostics: %v", err)

	d, ok := findDiagnostic(ds, "outputs.release.type")
	require.True(t, ok, "no diagnostic against the type: %v", ds)
	assert.Contains(t, d.Message, `is "text", which is not a type an output can have`)
	assert.Contains(t, d.Message, "string")
}

// TestTypedOutputShapeRules holds the two set-facts a declaration can be wrong
// about on its own, which the schema cannot state per-field and so defers to
// here — the same pair [v1.CheckInputConstraintShape] holds for an input.
func TestTypedOutputShapeRules(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		declaration string
		field       string
		contains    string
	}{
		{
			name: "values beside a type that is not enum",
			declaration: `  release:
    value: ${inputs.release}
    type: string
    values:
      - stable
`,
			field:    "outputs.release.values",
			contains: "declares values but is declared string",
		},
		{
			name: "an enum with no values",
			declaration: `  release:
    value: ${inputs.release}
    type: enum
`,
			field:    "outputs.release",
			contains: "is declared enum but declares no values",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wf, _, err := flowfile.Parse([]byte(typedOutputSource(test.declaration)))
			require.NoError(t, err, "the mistake is a set-fact, so it must survive parsing to be validated")

			d, ok := findDiagnostic(flowfile.Validate(wf), test.field)
			require.True(t, ok, "no diagnostic against %s: %v", test.field, flowfile.Validate(wf))
			assert.Contains(t, d.Message, test.contains)
		})
	}
}

// TestTypedOutputReportsAKnowableMismatch is the `type-mismatch` half: a
// declared type that contradicts something this validator can already see.
func TestTypedOutputReportsAKnowableMismatch(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		declaration string
		contains    string
	}{
		{
			name: "a literal of the wrong type",
			declaration: `  release:
    value: 7
    type: string
`,
			contains: `output "release" is declared string but computed int`,
		},
		{
			name: "a mapping written where a scalar was declared",
			declaration: `  release:
    value:
      host: a
    type: string
`,
			contains: `output "release" is declared string but computed struct`,
		},
		{
			name: "a closed expression of the wrong type",
			declaration: `  release:
    value: ${1 + 2}
    type: string
`,
			contains: `output "release" is declared string, but this expression always produces int`,
		},
		{
			name: "a bare reference to an input of another type",
			declaration: `  release:
    value: ${inputs.count}
    type: string
`,
			contains: `output "release" is declared string, but this expression always produces int`,
		},
		{
			// #1404. A struct is the map a caller reads as a plain object, so a
			// map the checker already types with an int key is a declaration the
			// file contradicts — and the one mismatch the outer kind alone could
			// not see, because a map with any key at all is a map.
			name: "a map whose keys are not strings where a struct was declared",
			declaration: `  release:
    value: '${{1: "value"}}'
    type: struct
`,
			contains: `output "release" is declared struct, but this expression is typed as a map with int keys; a struct is a map with string keys`,
		},
		{
			// The same promise one level down, which is the direction a check on
			// the outer key type only would miss: the projection converts the
			// whole value, so a nested non-string key defeats it exactly as an
			// outer one does.
			name: "a nested map whose keys are not strings where a struct was declared",
			declaration: `  release:
    value: '${{"a": {true: "b"}}}'
    type: struct
`,
			contains: `output "release" is declared struct, but this expression is typed as a map with bool keys; a struct is a map with string keys`,
		},
		{
			// The other declared container, on the identical rule: the
			// projection converts a whole output and gives up on all of it, so
			// an int key one element down defeats the plain array `list`
			// promised exactly as an outer one defeats a struct.
			name: "a list of maps whose keys are not strings where a list was declared",
			declaration: `  release:
    value: '${[{1: "value"}]}'
    type: list
`,
			contains: `output "release" is declared list, but this expression is typed as a list holding a map with int keys; a list reads back as a plain array, whose maps have string keys`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			wf, _, err := flowfile.Parse([]byte(typedOutputSource(test.declaration)))
			require.NoError(t, err)

			d, ok := findDiagnostic(flowfile.Validate(wf), "outputs.release")
			require.True(t, ok, "no diagnostic against the declaration: %v", flowfile.Validate(wf))
			assert.Equal(t, v1.DiagnosticCodeTypeMismatch, d.Code,
				"an agent branches on the code, so a type mistake must not arrive as `general`")
			assert.Contains(t, d.Message, test.contains)
		})
	}
}

// TestTypedOutputStaysSilentWhereNothingIsKnowable is the direction that would
// make the check unusable if it were wrong: an expression whose type is not a
// fact about the document must not be guessed at.
//
// Both arms are correct workflows. `steps.a.value` is a value the run computes,
// and `inputs.anything` is declared but read through a further selection — the
// checker declares every referenced name `dyn` (celcheck.go), and `dyn` is the
// honest answer rather than a mismatch.
func TestTypedOutputStaysSilentWhereNothingIsKnowable(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.3
name: t
inputs:
  config:
    type: struct
    default:
      host: a
steps:
  - id: a
    value: ${inputs.config}
outputs:
  from_step:
    value: ${steps.a.value}
    type: int
  through_a_selection:
    value: ${inputs.config.host}
    type: int
`

	wf, _, err := flowfile.Parse([]byte(source))
	require.NoError(t, err)
	assert.Empty(t, flowfile.Validate(wf),
		"a type nothing in the document decides must not be reported as a mismatch")
}

// TestTypedOutputAcceptsAContainerWhoseKeysCouldBeStrings is the direction the
// #1404 refusal above would break if it were written as "not a string key":
// a struct or list output is the ordinary shape, and the checker types several
// common ones with a key it cannot decide.
//
// `${{}}` types as `map(dyn, dyn)` because there is no entry to infer a key
// from, `${[]}` as `list(dyn)`, and a mixed-key literal as `map(dyn, …)` — in
// each, which keys the value actually holds is a fact about the value rather
// than about the file, so the honest answer here is silence and
// [v1.CheckOutputValue] decides it at completion against what the run produced.
// The string-keyed arms are the shape every declared container in `examples/`
// has, and must stay silent for the ordinary reason.
func TestTypedOutputAcceptsAContainerWhoseKeysCouldBeStrings(t *testing.T) {
	t.Parallel()

	for _, declaration := range []string{
		`  release:
    value: '${{"host": "a"}}'
    type: struct
`,
		`  release:
    value: '${{}}'
    type: struct
`,
		`  release:
    value: '${{1: "a", "b": "c"}}'
    type: struct
`,
		`  release:
    value: '${[{"host": "a"}]}'
    type: list
`,
		`  release:
    value: '${[]}'
    type: list
`,
	} {
		t.Run(declaration, func(t *testing.T) {
			t.Parallel()

			wf, _, err := flowfile.Parse([]byte(typedOutputSource(declaration)))
			require.NoError(t, err)
			assert.Empty(t, flowfile.Validate(wf),
				"a key type the document does not decide must not be reported as a mismatch")
		})
	}
}
