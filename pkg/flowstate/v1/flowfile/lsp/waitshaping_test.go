package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWaitShapingUnresolvedReferenceUnderlinesTheExpression is #318's squiggle
// half: the validator's diagnostic about a `wait_for_signal:` `outputs:` entry
// names the field as `outputs.<name>`, and the model resolves that to the
// shaping entry it already tracks — so the underline covers the expression the
// author wrote, not the step id lines above it. The error sits in the last of
// three entries because a first-entry fixture can pass by accident.
func TestWaitShapingUnresolvedReferenceUnderlinesTheExpression(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: waitshape
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
      outputs:
        first: ${payload.a}
        second: ${sender}
        ok: ${p}
`
	c := newClient(t)
	c.initialize()
	params := c.open("file:///wait-shaping.yaml", src)

	require.Len(t, params.Diagnostics, 1, "got %v", messages(params.Diagnostics))
	d := params.Diagnostics[0]
	assert.Contains(t, d.Message, `references unknown name "p"`)
	assert.Equal(t, "${p}", textInRange(src, d.Range),
		"the squiggle does not cover the offending entry's expression")
}
