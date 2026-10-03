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
