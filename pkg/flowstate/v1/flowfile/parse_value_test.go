package flowfile_test

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestParseValueStep is the reachability half of `value:`: a file someone writes
// compiles to the node kind, with the expression where the schema puts it.
//
// A test that built `&v1.Node{Kind: &v1.Node_Value{...}}` in Go would prove the
// engine can run one and say nothing about whether an author can reach it, which
// is the mistake CLAUDE.md records twice.
func TestParseValueStep(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: w
inputs:
  amount:
    type: int
    required: true
steps:
  - id: over
    value: ${inputs.amount >= 100}
  - id: shout
    if: ${steps.over.value}
    log:
      message: large
`

	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)

	require.Len(t, wf.GetSteps(), 2)
	kind, ok := wf.GetSteps()[0].GetKind().(*v1.Node_Value)
	require.True(t, ok, "the first step did not compile to a value node, got %T", wf.GetSteps()[0].GetKind())
	require.NotNil(t, kind.Value.GetExpr(), "a value's expression did not compile as an expression")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.Empty(t, ds, "a workload built out of a value was refused:\n%s", ds.Error())
}

// TestValueIsFenceOptional checks that a bare string under `value:` is expression
// source, the way it is under the workflow's own `outputs:` `value:`.
//
// The two positions spell the same word and must mean the same thing in both, or
// an author carries a rule between them that is not true.
func TestValueIsFenceOptional(t *testing.T) {
	t.Parallel()

	fenced, _, err := flowfile.Parse([]byte(`edition: v2026.2
name: w
steps:
  - id: v
    value: ${1 + 1}
`))
	require.NoError(t, err)

	bare, _, err := flowfile.Parse([]byte(`edition: v2026.2
name: w
steps:
  - id: v
    value: 1 + 1
`))
	require.NoError(t, err)

	require.Equal(t,
		fenced.GetSteps()[0].GetValue().GetExpr().String(),
		bare.GetSteps()[0].GetValue().GetExpr().String(),
		"the fenced and bare spellings of one value compiled to different expressions")
}

// TestValueRefusesPolicy is the refusal half: the three properties that mean
// nothing on a pure expression are reported, each on its own key with a position.
//
// A step that schedules nothing has nothing to bound, nothing to attempt again,
// and no effect to take back. Accepting any of the three silently would leave an
// author believing they had asked for something.
func TestValueRefusesPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a timeout on a value",
			src: `edition: v2026.2
name: w
steps:
  - id: v
    value: ${1 + 1}
    timeout: 5m
`,
			want: "does nothing on a `value:` step",
		},
		{
			name: "a retry on a value",
			src: `edition: v2026.2
name: w
steps:
  - id: v
    value: ${1 + 1}
    retry:
      attempts: 3
`,
			want: "does nothing on a `value:` step",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := flowfile.Parse([]byte(test.src))
			require.Error(t, err, "policy that does nothing on a value was accepted")
			require.Contains(t, err.Error(), test.want)

			// Every diagnostic names a position, which is what makes it
			// actionable in an editor rather than merely true.
			require.Regexp(t, `\d+:\d+:`, err.Error(),
				"the diagnostic does not name a line and column")
		})
	}
}

// TestValueRefusesUndo is the third refusal, which arrives by a different route:
// [v1.CheckUndoPlacement] owns it, so both drivers refuse it too, and the
// validator is what gives it a position on the `undo:` key.
func TestValueRefusesUndo(t *testing.T) {
	t.Parallel()

	src := []byte(`edition: v2026.2
name: w
steps:
  - id: v
    value: ${1 + 1}
    undo:
      log:
        message: undone
`)

	ds, err := flowfile.ValidateSource(src)
	require.NoError(t, err)
	require.NotEmpty(t, ds, "`undo:` on a value was accepted")
	require.Contains(t, ds.Error(), "a value computes an expression and changes nothing outside the run")
}

// TestValueRefusesASecretReference keeps `value:` on the right side of the rule
// that decides where a reference may be written: the workflow evaluates this, and
// what the workflow evaluates is written to durable history.
//
// The same refusal `vars:` gets, for the same reason, and this is the position
// that would quietly reopen it: a value is the newest expression position in the
// language and the one most obviously shaped like a place to put a token.
func TestValueRefusesASecretReference(t *testing.T) {
	t.Parallel()

	_, _, err := flowfile.Parse([]byte(`edition: v2026.2
name: w
steps:
  - id: v
    value: ${secret('env:TOKEN')}
`))
	require.Error(t, err, "a secret reference in a value was accepted")
	require.True(t,
		strings.Contains(err.Error(), "secret"),
		"the refusal does not mention what was refused: %s", err)
}

// TestValueSkippedThenReferencedIsUnresolved checks the composition rule at the
// validator, where an author meets it: a value carrying an `if:` may not run, and
// a reference to a step that has not run is an unresolved reference, not an empty
// value.
//
// The engine half of this is a shared driver case ([tests.ValueCases]); this is
// the half that says so before the run.
func TestValueSkippedThenReferencedIsUnresolved(t *testing.T) {
	t.Parallel()

	// Forward, rather than skipped: the validator cannot know an `if:` will be
	// false, so what it can report about this shape is a reference written above
	// the value it names. The skipped case is the engine's, and is shared.
	ds, err := flowfile.ValidateSource([]byte(`edition: v2026.2
name: w
steps:
  - id: reads
    log:
      message: ${string(steps.later.value)}
  - id: later
    value: ${1 + 1}
`))
	require.NoError(t, err)
	require.NotEmpty(t, ds, "a reference to a value declared later was accepted")
}

// TestValueRoundTripsThroughMarshal proves `flow fmt` and `flow fix` cannot lose a
// value: the bytes a formatter writes compile to the same specification.
//
// Compared as a compiled specification rather than as text, which is the standard
// CLAUDE.md sets for a rewriter: asserting the output still validates is what let
// two corruption bugs through.
func TestValueRoundTripsThroughMarshal(t *testing.T) {
	t.Parallel()

	src := []byte(`edition: v2026.2
name: w
steps:
  - id: shape
    value: "${ {'regions': ['eu', 'us']} }"
  - id: guarded
    if: ${size(steps.shape.value.regions) == 2}
    value: ${steps.shape.value.regions[0]}
`)

	first, _, err := flowfile.Parse(src)
	require.NoError(t, err)

	written, err := flowfile.Marshal(first)
	require.NoError(t, err)
	require.Contains(t, string(written), "value:", "the formatter dropped the value key")

	second, _, err := flowfile.Parse(written)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(first, second, protocmp.Transform()),
		"a value did not survive a round trip")
}
