package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A profile's namespaced functions were reported as unknown names, everywhere.
//
// cel-go parses `regex.replace(s, a, b)` as a select over the identifier `regex`, so the
// qualifier reaches the reference walk looking exactly like a name nobody bound. Every
// use of one was refused — in a step input, in an `if:`, in a `vars:` value — with a
// diagnostic naming a step the author never wrote.
//
// The functions are documented, `flow tasks` prints them, and `flow validate` refused
// them. That is the failure mode a diagnostic can least afford: a tool that is wrong
// about working files is one people learn to run with their eyes closed.

// TestAProfileFunctionIsNotAnUnknownName covers every position an expression can be
// written in, because the check they share had no idea about namespaces and each of
// them reached it by a different path.
func TestAProfileFunctionIsNotAnUnknownName(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
	}{
		{
			name: "a task input",
			src: `edition: v2026.3
name: t
steps:
  - id: s
    log:
      message: ${regex.replace("ab", "a", "c")}
`,
		},
		{
			name: "a workflow var",
			src: `edition: v2026.3
name: t
vars:
  shouted: ${regex.replace("ab", "a", "c")}
steps:
  - id: s
    log:
      message: ${vars.shouted}
`,
		},
		{
			name: "a step's own var",
			src: `edition: v2026.3
name: t
steps:
  - id: s
    vars:
      biggest: ${math.greatest(1, 2)}
    log:
      message: ${string(biggest)}
`,
		},
		{
			name: "a condition",
			src: `edition: v2026.3
name: t
steps:
  - id: s
    if: ${math.greatest(1, 2) == 2}
    log:
      message: hi
`,
		},
		{
			name: "a loop's items expression",
			src: `edition: v2026.3
name: t
steps:
  - id: each
    for_each:
      items: ${lists.range(3)}
      as: n
      steps:
        - id: inner
          log:
            message: ${string(n)}
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Empty(t, diagnose(t, test.src),
				"a documented profile function was reported as an unknown name")
		})
	}
}

// TestAQualifierIsExemptAndALibraryNameIsNot is the distinction the first version of
// this got wrong in both directions at once.
//
// The exempt set was read from `v1.ExtensionLibraries()`, which are *registration* names
// rather than the qualifiers their functions hang from. They coincide often enough to
// look right — `regex`, `math`, `sets` — and then do not: `encoders` declares
// `base64.encode`, `protos` declares `proto.getExt`, `bindings` declares `cel.bind`.
//
// So a valid `${base64.encode(b)}` was still refused, and `${string(encoders)}` — a name
// that means nothing anywhere — was quietly accepted. Both are asserted here, because a
// set derived from the wrong thing passes a test that only checks one direction.
func TestAQualifierIsExemptAndALibraryNameIsNot(t *testing.T) {
	t.Parallel()

	using := func(expr string) string {
		return "edition: v2026.3\nname: t\nsteps:\n  - id: s\n    log:\n      message: ${" +
			expr + "}\n"
	}

	require.Empty(t, diagnose(t, using(`base64.encode(b"hi")`)),
		"a real qualifier from the encoders library was reported as an unknown name")

	require.Contains(t, diagnose(t, using("string(encoders)")), `references unknown name "encoders"`,
		"the library's registration name is not a qualifier and must not be exempt")
}

// TestTheExemptSetComesFromTheProfile keeps the derivation honest.
//
// What makes this correct rather than lucky is that the qualifiers are read off the
// environment's own declarations — the part of a declared function's name before its
// last dot — rather than from a list this package maintains. A library added to a
// profile is then covered the day it is added, and one whose qualifier differs from its
// name cannot be wrong again.
func TestTheExemptSetComesFromTheProfile(t *testing.T) {
	t.Parallel()

	env, err := v1.DefaultEvaluator().ProfileEnv(v1.CurrentProfile)
	require.NoError(t, err)

	qualifiers := map[string]bool{}
	for name := range env.Functions() {
		if at := strings.LastIndex(name, "."); at > 0 {
			qualifiers[name[:at]] = true
		}
	}
	require.NotEmpty(t, qualifiers, "the profile declares no namespaced functions, so this checks nothing")

	for qualifier := range qualifiers {
		src := "edition: v2026.3\nname: t\nsteps:\n  - id: s\n    log:\n      message: ${string(" +
			qualifier + ")}\n"

		// Whether the expression type-checks is CEL's business — `string(base64)` may
		// well not. What is pinned is that the reference walk does not call a
		// qualifier the profile declares an unknown step.
		require.NotContains(t, diagnose(t, src), "references unknown name",
			"the profile declares functions under %q and the validator calls it an unknown name", qualifier)
	}
}

// TestAnUnknownNameIsStillUnknown is the negative direction.
//
// An exemption is only worth having if it exempts exactly what it names. A bare word
// that is not a library, not a binding and not a root is still a mistake, and still gets
// the diagnostic that explains what a bare name can be.
func TestAnUnknownNameIsStillUnknown(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: s
    log:
      message: ${nonsense.thing()}
`

	require.Contains(t, diagnose(t, src), `references unknown name "nonsense"`,
		"exempting the profile's namespaces also exempted everything else")
}
