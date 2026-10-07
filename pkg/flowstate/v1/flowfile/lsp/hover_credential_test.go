package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHoverOnCredentialReferences covers what an author cannot see from the text:
// what a ${credential('target')} marker names, why it may only be a whole task
// input, and that a call the compiler refuses is not described as if it worked.
func TestHoverOnCredentialReferences(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	t.Run("a well-formed reference", func(t *testing.T) {
		const src = `name: credentials
steps:
  - id: a
    log:
      message: ${credential('anthropic')}
edition: v2026.4
`
		const uri = "file:///credential-ok.yaml"
		require.Empty(t, messages(c.open(uri, src).Diagnostics))

		pos := positionOf(t, src, "credential('anthropic')", 3)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		text := hoverText(got)
		assert.Contains(t, text, "anthropic")
		assert.Contains(t, text, "federation target")
		assert.Contains(t, text, "never enters workflow history")

		require.NotNil(t, got.Range)
		assert.Equal(t, "credential('anthropic')", textInRange(src, *got.Range))
	})

	t.Run("a reference the compiler refuses is reported, not described as working", func(t *testing.T) {
		const src = `name: credentials
steps:
  - id: a
    log:
      message: ${credential('')}
edition: v2026.4
`
		const uri = "file:///credential-bad.yaml"
		require.NotEmpty(t, messages(c.open(uri, src).Diagnostics))

		pos := positionOf(t, src, "credential('')", 3)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		assert.Contains(t, hoverText(got), "not usable as written")
		assert.Contains(t, hoverText(got), "must not be empty")
	})

	t.Run("a misplaced reference is a diagnostic", func(t *testing.T) {
		const src = `name: credentials
steps:
  - id: a
    log:
      message: ${'Bearer ' + credential('anthropic')}
edition: v2026.4
`
		const uri = "file:///credential-misplaced.yaml"
		got := messages(c.open(uri, src).Diagnostics)
		require.NotEmpty(t, got)
		assert.Contains(t, got[0], "whole value of a task input")
	})
}
