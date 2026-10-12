package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnUnexplainedExpressionFailureStillShowsWhereItBroke pins #1568: a
// failure whose sentence is complete (a division by zero) prints the step's
// `--> file:line:col` like the explained classes, with the sentence unchanged
// and no caret, since no operator position is known, and reports it in JSON.
func TestAnUnexplainedExpressionFailureStillShowsWhereItBroke(t *testing.T) {
	t.Parallel()

	const flowfile = `edition: v2026.4
name: divide
steps:
  - id: boom
    log:
      message: '${string(1 / 0)}'
`

	_, stderr, err := runLocal(t, flowfile)
	require.Error(t, err)
	assert.Contains(t, stderr+err.Error(), `step "boom": input "message": evaluate expression: division by zero`)
	assert.Contains(t, stderr+err.Error(), ":4:5")
	assert.Contains(t, stderr+err.Error(), "\n    --> ")
	assert.NotContains(t, stderr+err.Error(), "^")

	stdout, _, err := runLocal(t, flowfile, "--output", "json")
	require.Error(t, err)
	assert.Contains(t, stdout, `"step": "boom"`)
	assert.Contains(t, stdout, `"line": 4`)
}
