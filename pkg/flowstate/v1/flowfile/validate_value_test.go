package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The walks a node kind has to be added to, tested one by one.
//
// Adding a kind to the schema and to both drivers makes it *run*. What decides
// whether an author is told the truth about it is a different set of walks
// entirely, each of which switches over node kinds and each of which silently
// does nothing for a kind it has no arm for. Four of them had no `value:` arm
// when the kind landed, and every one of the four failed the same way: silence
// on a file that is wrong.

// TestValidateReportsATypeErrorInAValue is the type-check walk
// ([checkNodeExpressions]).
//
// `${1 + true}` has no overload and can never evaluate. Every other expression
// position in the language reports it at validate time; without an arm this one
// said "ok" and left the failure to a run, where a value that several later steps
// read fails all of them at once.
func TestValidateReportsATypeErrorInAValue(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte(`edition: v2026.2
name: w
steps:
  - id: v
    value: ${1 + true}
`))
	require.NoError(t, err)
	require.NotEmpty(t, ds, "a value whose expression cannot type-check was accepted")
	require.Contains(t, ds.Error(), "no matching overload")
}

// TestValidateReportsAnUnknownOutputOnAValue is the output-set walk
// ([unknownStepOutput]).
//
// A `value:` is the one kind whose output set the *grammar* fixes, so this is the
// one answer that needs nothing looked up to be exact, and the one where a false
// diagnostic is impossible. Silence here is worse than elsewhere for the same
// reason: a value exists to be read from several places, so a name nothing
// produces resolves to nothing in every branch built on it.
func TestValidateReportsAnUnknownOutputOnAValue(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte(`edition: v2026.2
name: w
steps:
  - id: v
    value: ${1 + 1}
  - id: reads
    log:
      message: ${string(steps.v.typo)}
`))
	require.NoError(t, err)
	require.NotEmpty(t, ds, "a reference to an output a value does not produce was accepted")
	require.Contains(t, ds.Error(), `has no output "typo"`)
	require.Contains(t, ds.Error(), "steps.v.value",
		"the diagnostic does not name the one output a value produces")
}

// TestValidateAcceptsAValuesOwnOutput is the other direction, and the one that
// makes the check above worth having rather than merely loud: the name a value
// does produce must not be reported.
func TestValidateAcceptsAValuesOwnOutput(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte(`edition: v2026.2
name: w
steps:
  - id: v
    value: ${1 + 1}
  - id: reads
    log:
      message: ${string(steps.v.value)}
`))
	require.NoError(t, err)
	require.Empty(t, ds, "reading a value's own output was refused:\n%s", ds.Error())
}

// TestValidateAcceptsErrorOnAToleratedValue pins the exception, which is the half
// of this rule that would have been wrong if it had been guessed at.
//
// `retry:` is refused on this kind because a deterministic expression gains
// nothing from a second attempt. `continue_on_error:` is *not* refused and is not
// meaningless: a value's expression can fail at run time (a division by zero, a
// reference to a step that was skipped), and both drivers then record `error` in
// place of `value` exactly as they do for any other tolerated step. So `error` is
// a real output here, and reporting it would be the false diagnostic this check's
// own design forbids.
func TestValidateAcceptsErrorOnAToleratedValue(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte(`edition: v2026.2
name: w
steps:
  - id: v
    continue_on_error: true
    value: ${1 / 0}
  - id: reads
    if: ${has(steps.v.error)}
    log:
      message: it failed
`))
	require.NoError(t, err)
	require.Empty(t, ds, "`error` on a tolerated value was reported as unknown:\n%s", ds.Error())
}

// TestValidateReportsErrorOnAnUntoleratedValue is the negative direction of the
// exception above: `error` exists *because* of the policy, so without the policy
// it is a name nothing produces, and the diagnostic says which key is missing
// rather than only that the name is wrong.
func TestValidateReportsErrorOnAnUntoleratedValue(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte(`edition: v2026.2
name: w
steps:
  - id: v
    value: ${1 + 1}
  - id: reads
    if: ${has(steps.v.error)}
    log:
      message: it failed
`))
	require.NoError(t, err)
	require.NotEmpty(t, ds, "`error` on a value that tolerates nothing was accepted")
	require.Contains(t, ds.Error(), "continue_on_error",
		"the diagnostic does not say what would make `error` exist")
}
