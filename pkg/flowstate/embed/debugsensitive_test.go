package embed

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDebugRefusesASensitiveWorkflowUnlessRevealed: a debugger is a reveal —
// the session narrates each step's values and an inspection reaches anything
// in scope, unredacted — so a workflow that declares a sensitive input is not
// debugged unless the caller authorizes disclosure, as `flow run local
// --debug` and `flow dap` refuse it without --reveal-sensitive.
func TestDebugRefusesASensitiveWorkflowUnlessRevealed(t *testing.T) {
	workflow, diags, err := Compile([]byte(`
edition: v2026.3
name: sensitive
inputs:
  token:
    type: string
    sensitive: true
steps:
  - id: echo
    value: ${inputs.token}
`))
	require.NoError(t, err, "diags=%v", diags)
	inputs := RunOptions{Inputs: map[string]any{"token": "hunter2"}}

	var narrated []string
	_, err = Debug(context.Background(), workflow, DebugOptions{
		RunOptions: inputs, Continue: true, Output: func(text string) { narrated = append(narrated, text) },
	})
	require.Error(t, err, "a workflow declaring a sensitive input was debugged without authorization")
	assert.Contains(t, err.Error(), "RevealSensitive")
	assert.Empty(t, narrated, "the refused run narrated before it was refused")

	debugging, err := Debug(context.Background(), workflow, DebugOptions{RunOptions: inputs, Continue: true, RevealSensitive: true})
	require.NoError(t, err)
	require.NoError(t, debugging.Close())
	outputs, err := debugging.Wait(context.Background())
	require.NoError(t, err)
	echoed, ok := StepOutputString(outputs, "echo", "value")
	require.True(t, ok)
	assert.Equal(t, "hunter2", echoed)
}
