package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Every bound in this package is against input an outside party chooses — the
// language server compiles whatever an editor opens, and a server compiles
// whatever is submitted — so each one needs a test that reaches it.
//
// The rule these are written to is that a bound has to match the shape of the
// attack. Depth bounds do not stop breadth explosions; a bound on the values a
// walk descends into does not stop values produced without descending.

// The anchor, alias, and merge-key expansion bounds this file used to exercise
// are gone with the constructs themselves: the grammar is a strict subset of
// YAML that refuses all three, so the billion-laughs shape those tests fed is now
// refused on the presence of the construct, before any expansion, in
// strict_test.go's TestStrictYAMLRefusesBillionLaughsWithoutExpanding. A bound on
// an expansion that can no longer happen is a bound nothing reaches. See #653.

// The root is the one name rooting *creates* a collision for, which is worth
// stating plainly in a change that is otherwise about deleting collision rules.
//
// It has to be refused at compile time rather than left to resolve, because the
// runtime deliberately lets a step of this name win: a spec compiled before the
// root existed may contain one, and a worker replaying it must resolve the way it
// always did. That compatibility is only safe while no *new* file can create the
// situation.

// TestNothingMayBeCalledSteps covers every route a name reaches an expression's
// scope by, because closing one and leaving the others is how this kind of hole
// survives.
func TestNothingMayBeCalledSteps(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"a top-level step": `edition: v2026.3
name: t
steps:
  - id: steps
    log:
      message: hi
`,
		"a step inside a loop body": `edition: v2026.3
name: t
steps:
  - id: a
    for_each:
      items: ${[1]}
      steps:
        - id: steps
          log:
            message: hi
`,
		"a step inside a parallel branch": `edition: v2026.3
name: t
steps:
  - id: a
    parallel:
      - steps:
          - id: steps
            log:
              message: hi
`,
		// The other route into a body's scope. A bound name wins over the scope it
		// is bound into, so this hides every step from exactly the place rooted
		// references are written.
		"a loop iterator": `edition: v2026.3
name: t
steps:
  - id: a
    for_each:
      items: ${[1]}
      as: steps
      steps:
        - id: b
          log:
            message: hi
`,
	}

	for name, src := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(src))
			require.NoError(t, err, "the document is valid YAML; the name is a semantic problem")
			require.NotEmpty(t, ds, "%s called `steps` was accepted", name)
			assert.Contains(t, ds.Error(), "hide all",
				"the diagnostic has to say what goes wrong, not only that the name is taken")
		})
	}
}

// TestAStepCalledStepsWouldHaveFailedAtRunTime is the evidence the rule above is
// worth having, rather than a name reserved out of tidiness.
//
// Without the refusal this document validates clean and then dies on its third
// step with `no such key: other` — the shape of failure this repo cares most
// about, because nothing an author can see says why.
func TestAStepCalledStepsWouldHaveFailedAtRunTime(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: shadowed-root
steps:
  - id: steps
    log:
      message: i am a step called steps
  - id: other
    http:
      url: https://example.com
  - id: read
    log:
      message: ${steps.other.body}
`
	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.NotEmpty(t, ds)

	// The reference is *not* what is reported. It is correct; the id is the
	// problem, and a diagnostic on the reference would send an author to fix the
	// wrong line.
	assert.NotContains(t, ds.Error(), "unknown step",
		"the reference is fine; the id is what has to change")
}
