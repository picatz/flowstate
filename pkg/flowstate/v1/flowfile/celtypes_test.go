package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Every file below is a mistake the run refuses with a sentence of its own, hours
// in on a durable run, and which `flow validate` said `ok` to while the checker
// declared every name `dyn` (#1634). The text of a refusal is pinned where it is
// the fix an author needs; the code always is, because an agent branches on it.
func TestTypedScopeRefusesWhatCannotRun(t *testing.T) {
	t.Parallel()

	const header = `edition: v2026.3
name: t
inputs:
  count:
    type: int
    default: 3
  port:
    type: int
    default: 80
  names:
    type: list
    default: [a, b]
  label:
    type: string
    default: x
  mode:
    type: enum
    values: [fast, slow]
    default: fast
`

	for _, test := range []struct {
		name     string
		steps    string
		contains string
	}{
		{
			name: "an int input iterated",
			steps: `
steps:
  - id: loop
    for_each:
      items: ${inputs.count}
      steps:
        - id: x
          log:
            message: hi
`,
			contains: "`items:` is the list to iterate, so this expression must be a list, but it is typed int",
		},
		{
			name: "an int input as a condition",
			steps: `
steps:
  - id: a
    if: ${inputs.count}
    log:
      message: hi
`,
			contains: "`if:` is a condition, so this expression must be a bool, but it is typed int",
		},
		{
			name: "a string literal as an until",
			steps: `
steps:
  - id: l
    loop:
      until: ${"done"}
      max_iterations: 2
      steps:
        - id: x
          log:
            message: hi
`,
			contains: "`until:` is a condition, so this expression must be a bool, but it is typed string",
		},
		{
			name: "a string method on an int input",
			steps: `
steps:
  - id: a
    value: ${inputs.port.startsWith("8")}
`,
			contains: "startsWith",
		},
		{
			name: "an int added to a string value step",
			steps: `
steps:
  - id: s
    value: ${"abc"}
  - id: n
    value: ${steps.s.value + 1}
`,
			contains: "no matching overload",
		},
		{
			name: "a value step read as the wrong type in a condition",
			steps: `
steps:
  - id: s
    value: ${"abc"}
  - id: a
    if: ${steps.s.value}
    log:
      message: hi
`,
			contains: "must be a bool, but it is typed string",
		},
		{
			name: "a list method on a string input",
			steps: `
steps:
  - id: a
    value: ${inputs.label.map(c, c)}
`,
			contains: "map",
		},
		{
			name: "an enum input is a string, not a number",
			steps: `
steps:
  - id: a
    value: ${inputs.mode + 1}
`,
			contains: "no matching overload",
		},
		{
			name: "a call-free number compared with a string",
			steps: `
steps:
  - id: a
    value: ${inputs.port == "80"}
`,
			contains: "no matching overload",
		},
		{
			name: "a value step through another",
			steps: `
steps:
  - id: s
    value: ${inputs.count}
  - id: t
    value: ${steps.s.value}
  - id: u
    value: ${steps.t.value.lowerAscii()}
`,
			contains: "lowerAscii",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ds := validateSource(t, header+test.steps)
			require.NotEmpty(t, ds, "validate said ok to a file that cannot run")
			assert.Contains(t, ds.Error(), test.contains)
			for _, d := range ds {
				assert.Equal(t, v1.DiagnosticCodeTypeMismatch, d.Code,
					"an agent branches on the code, so a type mistake must not arrive as `general`: %v", d)
			}
		})
	}
}

// TestTypedScopeIsSilentWhereTheFileDecidesNothing is the direction that would make
// the checker unusable if it were wrong. Each file runs.
func TestTypedScopeIsSilentWhereTheFileDecidesNothing(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name: "a list input iterated, a bool value step as a condition",
			source: `edition: v2026.3
name: t
inputs:
  names:
    type: list
    default: [a, b]
steps:
  - id: ok
    value: ${true}
  - id: a
    if: ${steps.ok.value}
    for_each:
      items: ${inputs.names}
      steps:
        - id: x
          log:
            message: hi
`,
		},
		{
			name: "a presence test and an optional read of a typed input",
			source: `edition: v2026.3
name: t
inputs:
  tag:
    type: string
steps:
  - id: a
    value: '${has(inputs.tag) ? inputs.tag : inputs.?tag.orValue("none")}'
`,
		},
		{
			name: "a struct input read through a selection",
			source: `edition: v2026.3
name: t
inputs:
  config:
    type: struct
    default:
      host: a
steps:
  - id: a
    value: ${inputs.config.host.lowerAscii() + string(inputs.config.port + 1)}
`,
		},
		{
			name: "a map value read for a key its literal never had",
			source: `edition: v2026.3
name: t
steps:
  - id: prefs
    value: '${{"volume": 7}}'
  - id: a
    value: ${steps.prefs.value.?muted.orValue("loud")}
`,
		},
		{
			name: "an id used twice in different loops is not typed from either",
			source: `edition: v2026.3
name: t
steps:
  - id: one
    for_each:
      items: ${[1, 2]}
      steps:
        - id: page
          value: ${1}
  - id: two
    for_each:
      items: ${["a", "b"]}
      steps:
        - id: page
          value: ${"x"}
        - id: use
          value: ${steps.page.value + "y"}
`,
		},
		{
			name: "a value read from a response stays dyn",
			source: `edition: v2026.3
name: t
steps:
  - id: s
    value: '${json_parse("{\"n\": 1}").n}'
  - id: u
    value: ${steps.s.value.size()}
`,
		},
		{
			name: "an optional is not a type to refuse as a list",
			source: `edition: v2026.3
name: t
steps:
  - id: prefs
    value: '${{"volume": 7}}'
  - id: loop
    for_each:
      items: ${steps.prefs.value.?stations.orValue(["a"])}
      steps:
        - id: x
          log:
            message: hi
`,
		},
		{
			name: "a value step that reads itself is left to the reference walk",
			source: `edition: v2026.3
name: t
steps:
  - id: s
    value: ${steps.s.value}
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			for _, d := range validateSource(t, test.source) {
				// A self-reference is the reference walk's own diagnostic; what must
				// not appear is a type one.
				assert.NotEqual(t, v1.DiagnosticCodeTypeMismatch, d.Code, "a type nothing in the file decides was refused: %v", d)
			}
		})
	}
}

// TestValueStepTypesReachLaterExpressions is #1636's second half: an output's
// declared type is checked against the type of the `value:` step it reads.
func TestValueStepTypesReachLaterExpressions(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.3
name: t
steps:
  - id: s
    value: ${"abc"}
outputs:
  n:
    type: int
    value: ${steps.s.value}
`
	ds := validateSource(t, source)
	d, ok := findDiagnostic(ds, "outputs.n")
	require.True(t, ok, "no diagnostic against the output: %v", ds)
	assert.Equal(t, v1.DiagnosticCodeTypeMismatch, d.Code)
	assert.Contains(t, d.Message, `output "n" is declared int, but this expression always produces string`)

	assert.Empty(t, validateSource(t, `edition: v2026.3
name: t
steps:
  - id: s
    value: ${"abc"}
outputs:
  n:
    type: string
    value: ${steps.s.value}
`))
}

// TestCallArgumentsAreCheckedAgainstTypedScope: a `with:` argument is evaluated in
// the caller's scope, so a value step's type is what the callee's declaration is
// held to.
func TestCallArgumentsAreCheckedAgainstTypedScope(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", `edition: v2026.3
name: callee
inputs:
  n:
    type: int
steps:
  - id: a
    value: ${inputs.n}
`)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: s
    value: ${"abc"}
  - id: c
    call: ./callee.yaml
    with:
      n: ${steps.s.value}
`)

	ds, err := flowfile.ValidateSourceFile(caller)
	require.NoError(t, err)
	require.NotEmpty(t, ds)
	assert.Contains(t, ds.Error(), "declared int by workflow")
	assert.Contains(t, ds.Error(), "always produces string")
}
