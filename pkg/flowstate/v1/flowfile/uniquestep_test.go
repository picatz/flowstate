package flowfile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUniqueStepPathRefusesAnIdTwoBodiesDeclare pins that a step id declared in
// two sibling bodies, which validation permits, resolves to no single line.
func TestUniqueStepPathRefusesAnIdTwoBodiesDeclare(t *testing.T) {
	t.Parallel()

	_, positions, err := ParseAt([]byte(`edition: v2026.4
name: t
steps:
  - id: only
    log:
      message: hi
  - id: first
    for_each:
      items: ${[1]}
      steps:
        - id: body
          log:
            message: a
  - id: second
    for_each:
      items: ${[1]}
      steps:
        - id: body
          log:
            message: b
`), "t.yaml")
	require.NoError(t, err)

	_, ok := positions.UniqueStepPath("only")
	assert.True(t, ok, "an id declared once resolves")

	_, ok = positions.UniqueStepPath("body")
	assert.False(t, ok, "an id declared in two bodies is ambiguous")
	_, ok = positions.StepPath("body")
	assert.True(t, ok, "StepPath still answers with the first, for diagnostics")

	_, ok = positions.UniqueStepPath("missing")
	assert.False(t, ok)
	_, ok = (*Positions)(nil).UniqueStepPath("only")
	assert.False(t, ok)
}
