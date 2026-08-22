package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `labels:` is the spelling for a schema field that existed from the beginning
// and that, until this round, nothing could set: no Flowfile key, no memo entry,
// no filter variable. These are the tests that keep it reachable from a file
// somebody writes, which is the only sense in which it is a capability at all.

const labelled = `edition: v2026.3
name: nightly-etl
labels:
  team: payments
  cost-center: cc-1234
steps:
  - id: gather
    log:
      message: gathering
`

func TestLabelsCompileFromTheFile(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(labelled))
	require.NoError(t, err)

	require.Equal(t, map[string]string{
		"team":        "payments",
		"cost-center": "cc-1234",
	}, workflow.GetLabels())

	// And the compiled workflow is one the validator accepts, because a spelling
	// the parser reads and the validator refuses is not a spelling.
	require.Empty(t, v1.Validate(workflow))
}

// TestAWorkflowWithNoLabelsCarriesNone keeps the zero case honest: absence, not
// an empty map written into the specification.
func TestAWorkflowWithNoLabelsCarriesNone(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: unlabelled
steps:
  - id: gather
    log:
      message: gathering
`))
	require.NoError(t, err)
	require.Empty(t, workflow.GetLabels())
}

// TestLabelsSurviveTheRoundTrip is the property `flow fmt` and the language
// server both depend on: Marshal is the inverse of Unmarshal, byte for byte on a
// file already in the written form.
//
// Byte comparison rather than "it still validates", which is what let two
// separate `flow fix` corruptions through: a file that computes something else
// validates perfectly.
func TestLabelsSurviveTheRoundTrip(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(labelled))
	require.NoError(t, err)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)

	// Directly under `name:`, where the parser reads it and an author writes it,
	// and with the keys sorted rather than in the order the file happened to
	// write them — a Go map has no order, so sorting is what makes this
	// reproducible at all.
	require.Equal(t, `edition: v2026.3
name: nightly-etl
labels:
  cost-center: cc-1234
  team: payments
steps:
- id: gather
  log:
    message: gathering
`, string(written))

	// And it reads back as the same workflow, which is the property the byte
	// comparison above is a proxy for.
	back, err := flowfile.Unmarshal(written)
	require.NoError(t, err)
	require.Equal(t, workflow.GetLabels(), back.GetLabels())

	// Twice, because the keys come out of a Go map, which has no order: a
	// formatter that emitted them in iteration order would pass the comparison
	// above roughly half the time and rewrite the file on every second run.
	for range 8 {
		again, err := flowfile.Marshal(workflow)
		require.NoError(t, err)
		require.Equal(t, string(written), string(again),
			"formatting the same workflow twice produced different bytes")
	}
}

// TestLabelsAreBounded pins that a label map is bounded, because a label travels:
// into the run's memo, into Temporal's history, and into a CEL activation the
// server builds per execution while scanning a listing.
//
// The bound is the schema's, so this asserts the refusal rather than the number:
// the validator is where a specification's shape is decided, and a parser-side
// copy of the limit is the same value written down twice.
func TestLabelsAreBounded(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString("edition: v2026.3\nname: too-many\nlabels:\n")
	for i := range 200 {
		source.WriteString("  key-")
		source.WriteString(strings.Repeat("0", 1))
		source.WriteString(string(rune('a'+i%26)) + string(rune('a'+i/26)))
		source.WriteString(": value\n")
	}
	source.WriteString("steps:\n  - id: gather\n    log:\n      message: gathering\n")

	workflow, err := flowfile.Unmarshal([]byte(source.String()))
	require.NoError(t, err)
	require.Greater(t, len(workflow.GetLabels()), 64,
		"the test did not build a label map larger than the schema's bound")

	require.Error(t, v1.Validate(workflow),
		"a workflow declaring more labels than the schema allows was accepted")
}
