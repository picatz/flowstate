package flowfile_test

import (
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
			src: `edition: v2026.2
name: t
steps:
  - id: s
    log:
      message: ${regex.replace("ab", "a", "c")}
`,
		},
		{
			name: "a workflow var",
			src: `edition: v2026.2
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
			src: `edition: v2026.2
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
			src: `edition: v2026.2
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
			src: `edition: v2026.2
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

// TestEveryExtensionLibraryIsExemptDerivesFromTheBuild is what keeps the exemption from
// going stale.
//
// The set comes off `v1.ExtensionLibraries()` rather than being listed, so a library
// added to a profile is covered the day it is added. Asserting that here means the
// derivation cannot quietly become a hand-written list that agrees today.
func TestEveryExtensionLibraryIsExemptDerivesFromTheBuild(t *testing.T) {
	t.Parallel()

	libraries := v1.ExtensionLibraries()
	require.NotEmpty(t, libraries, "this build ships no extension libraries, so the test below checks nothing")

	for _, name := range libraries {
		src := "edition: v2026.2\nname: t\nsteps:\n  - id: s\n    log:\n      message: ${string(" +
			name + ")}\n"

		// Whether the *expression* is meaningful is CEL's business — `string(regex)`
		// may well not type-check. What is being pinned is that the reference walk
		// does not call the qualifier an unknown step.
		require.NotContains(t, diagnose(t, src), "references unknown name",
			"the library %q is shipped and its namespace is reported as an unknown name", name)
	}
}

// TestAnUnknownNameIsStillUnknown is the negative direction.
//
// An exemption is only worth having if it exempts exactly what it names. A bare word
// that is not a library, not a binding and not a root is still a mistake, and still gets
// the diagnostic that explains what a bare name can be.
func TestAnUnknownNameIsStillUnknown(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: t
steps:
  - id: s
    log:
      message: ${nonsense.thing()}
`

	require.Contains(t, diagnose(t, src), `references unknown name "nonsense"`,
		"exempting the profile's namespaces also exempted everything else")
}
