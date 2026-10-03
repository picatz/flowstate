package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestFixRetiresTheLegacyTypeWords is the edition boundary for #1640, compared
// byte for byte: `list`, `struct` and `float` become what they meant, on inputs
// and outputs, with comments and every other `type:` left alone.
func TestFixRetiresTheLegacyTypeWords(t *testing.T) {
	t.Parallel()

	in := `edition: v2026.3
name: retired
inputs:
  hosts:
    type: list # any list
    default: [a]
  limits:
    type: struct
  ratio:
    type: float
    default: 1.5
  label:
    type: string
steps:
  - id: s
    value: ${inputs.hosts}
outputs:
  all:
    type: list
    value: ${steps.s.value}
`
	want := `edition: v2026.4
name: retired
inputs:
  hosts:
    type: list(dyn) # any list
    default: [a]
  limits:
    type: map(string, dyn)
  ratio:
    type: double
    default: 1.5
  label:
    type: string
steps:
  - id: s
    value: ${inputs.hosts}
outputs:
  all:
    type: list(dyn)
    value: ${steps.s.value}
`
	require.Equal(t, want, fixed(t, in))

	// The rewrite is a fixed point, and the result compiles.
	require.Equal(t, want, fixed(t, want))
	_, _, err := flowfile.Parse([]byte(want))
	require.NoError(t, err)
}

// TestFixQuotesTheReplacementInAFlowMapping holds the YAML corner D1 records:
// inside a flow mapping the comma in `map(string, dyn)` ends the value, so the
// rewrite writes the quoted form.
func TestFixQuotesTheReplacementInAFlowMapping(t *testing.T) {
	t.Parallel()

	in := `edition: v2026.4
name: flow
inputs:
  limits: { type: struct }
  hosts: { type: list }
steps:
  - id: s
    value: ${inputs.hosts}
`
	want := `edition: v2026.4
name: flow
inputs:
  limits: { type: "map(string, dyn)" }
  hosts: { type: list(dyn) }
steps:
  - id: s
    value: ${inputs.hosts}
`
	require.Equal(t, want, fixed(t, in))
	_, _, err := flowfile.Parse([]byte(want))
	require.NoError(t, err)
}

// TestFixLeavesTypeExpressionsAndOtherTypeKeysAlone is the negative direction:
// a file already spelling its types, and a `type:` that is not a declaration's,
// come back byte for byte.
func TestFixLeavesTypeExpressionsAndOtherTypeKeysAlone(t *testing.T) {
	t.Parallel()

	in := `edition: v2026.4
name: alone
inputs:
  hosts:
    type: list(string)
  limits:
    type: map(string, int)
steps:
  - id: s
    value: ${inputs.hosts}
`
	require.Equal(t, in, fixed(t, in))
}

// TestFixMigratesQuotedAndCrowdedTypeWords covers the shapes a hand-written file
// takes: quoted retired words, and a flow mapping with many declarations on one
// line, which must migrate in one round rather than one per declaration.
func TestFixMigratesQuotedAndCrowdedTypeWords(t *testing.T) {
	t.Parallel()

	in := `edition: v2026.3
name: shapes
inputs:
  a:
    type: "list"
  b:
    type: 'struct'
  c: {type: float}
  d: {type: struct}
  e: {type: list}
steps:
  - id: s
    value: ${1}
`
	want := `edition: v2026.4
name: shapes
inputs:
  a:
    type: list(dyn)
  b:
    type: map(string, dyn)
  c: {type: double}
  d: {type: "map(string, dyn)"}
  e: {type: list(dyn)}
steps:
  - id: s
    value: ${1}
`
	require.Equal(t, want, fixed(t, in))

	crowded := "edition: v2026.3\nname: crowded\ninputs: {" +
		"a: {type: struct}, b: {type: struct}, c: {type: struct}, d: {type: struct}, " +
		"e: {type: struct}, f: {type: struct}, g: {type: struct}, h: {type: struct}}\n" +
		"steps:\n  - id: s\n    value: ${1}\n"
	out := fixed(t, crowded)
	require.NotContains(t, out, "type: struct")
	require.Contains(t, out, `h: {type: "map(string, dyn)"}`)
}

// TestRetiredWordDiagnosticOffersValidYAML pins that the spelling offered for
// `struct` survives being pasted into a flow mapping.
func TestRetiredWordDiagnosticOffersValidYAML(t *testing.T) {
	t.Parallel()

	ds := diagnose(t, "edition: v2026.4\nname: d\ninputs: {x: {type: struct}}\nsteps:\n  - id: s\n    value: ${1}\n")
	require.Contains(t, ds, `write "map(string, dyn)"`)
}
