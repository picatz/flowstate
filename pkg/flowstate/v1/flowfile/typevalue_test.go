package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestATypeValueIsNotAnUnknownName is #2203. cel-go parses a type value — `int`
// in `type(x) == int`, a qualified `google.protobuf.Timestamp` — as an
// identifier, or a select chain over one, so it reached the reference walk
// looking like a name nobody bound, and a valid expression failed `flow
// validate` in every position the language has.
func TestATypeValueIsNotAnUnknownName(t *testing.T) {
	t.Parallel()

	const types = `type(1) == int && type("") == string && type([]) == list && type({}) == map && ` +
		`type(null) == null_type && type(1) != google.protobuf.Timestamp && ` +
		// A comprehension's binding selected through is the binding, even one
		// spelled like a type.
		`[{"a":1}].exists(m, m.a == 1) && [{"a":1}].exists(int, int.a == 1)`
	for name, src := range map[string]string{
		"a task input": `edition: v2026.3
name: t
steps:
  - id: s
    log:
      message: ${string(` + types + `)}
`,
		"a workflow var": `edition: v2026.3
name: t
vars:
  typed: ${` + types + `}
steps:
  - id: s
    log:
      message: ${string(vars.typed)}
`,
		"a step's own var": `edition: v2026.3
name: t
steps:
  - id: s
    vars:
      typed: ${` + types + `}
    log:
      message: ${string(typed)}
`,
		"a condition": `edition: v2026.3
name: t
steps:
  - id: s
    if: ${` + types + `}
    log:
      message: hi
`,
		"a value step": `edition: v2026.3
name: t
steps:
  - id: s
    value: ${` + types + `}
`,
		"an output": `edition: v2026.3
name: t
steps:
  - id: s
    value: ${1}
outputs:
  typed:
    value: ${type(steps.s.value) == int}
`,
		"a concurrency key": `edition: v2026.3
name: t
inputs:
  cluster:
    type: string
    required: true
concurrency:
  key: ${string(type(inputs.cluster) == string)}
  on_conflict: reject
steps:
  - id: s
    log:
      message: hi
`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.Empty(t, diagnose(t, src),
				"a type value the profile resolves was reported as an unknown name")
		})
	}
}

// TestAnUnknownNameBesideATypeIsStillUnknown is the negative direction: a
// name the environment does not resolve is reported as before, bare or as the
// first segment of a qualified name that names no type.
func TestAnUnknownNameBesideATypeIsStillUnknown(t *testing.T) {
	t.Parallel()

	using := func(expr string) string {
		return "edition: v2026.3\nname: t\nsteps:\n  - id: s\n    if: ${" + expr + "}\n    log:\n      message: hi\n"
	}

	require.Contains(t, diagnose(t, using("type(1) == nosuch")), `references unknown name "nosuch"`)
	require.Contains(t, diagnose(t, using("type(1) == google.protobuf.Nonsense")), `references unknown name "google"`,
		"a qualified name that resolves to no type was admitted")
	require.Contains(t, diagnose(t, using("int.nosuch == 1")), `references unknown name "int"`,
		"a selection through a type value was admitted as a qualified type")
}
