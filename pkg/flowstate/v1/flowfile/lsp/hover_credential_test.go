package lsp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestHoverOnCredentialReferences covers what an author cannot see from the text:
// what a ${credential('target')} marker names, why it may only be a whole task
// input, and that a call the compiler refuses is not described as if it worked.
func TestHoverOnCredentialReferences(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	t.Run("a well-formed reference", func(t *testing.T) {
		const task = "test_credential_hover_probe"
		require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
			Name:         task,
			Inputs:       (&v1.Task_Log_Inputs{}).ProtoReflect().Descriptor(),
			SecretInputs: []string{"message"},
			Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
				return nil, nil
			},
		}))
		t.Cleanup(func() { v1.DefaultRegistry().Unregister(task) })

		const src = `name: credentials
steps:
  - id: a
    test_credential_hover_probe:
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

	t.Run("a lookalike call is not described as the marker", func(t *testing.T) {
		for _, call := range []string{"mycredential('x')", "client.credential('x')"} {
			src := "name: credentials\nsteps:\n  - id: a\n    log:\n      message: ${" + call + "}\nedition: v2026.4\n"
			uri := "file:///credential-lookalike.yaml"
			c.open(uri, src)

			pos := positionOf(t, src, call, 3)
			got := c.hover(uri, pos.Line, pos.Character)
			if got != nil {
				assert.NotContains(t, hoverText(got), "federation target", call)
			}
		}
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
