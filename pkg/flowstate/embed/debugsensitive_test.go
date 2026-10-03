package embed

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestDebugRefusesASensitiveWorkflowUnlessRevealed: a debugger is a reveal —
// the session narrates each step's values and an inspection reaches anything
// in scope, unredacted — so a workflow that declares a sensitive input is not
// debugged unless the caller authorizes disclosure, as `flow run local
// --debug` and `flow dap` refuse it without --reveal-sensitive.
func TestDebugRefusesASensitiveWorkflowUnlessRevealed(t *testing.T) {
	workflow, diags, err := Compile([]byte(`
edition: v2026.4
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

// TestDebugRefusesAWorkflowWhoseDeclarationsCannotBeRead: a workflow built in
// memory can nest calls past what a specification is scanned to, so whether it
// declares anything sensitive cannot be told. Disclosure is authorized, never
// assumed: it is refused without RevealSensitive, as one that declares
// something is.
func TestDebugRefusesAWorkflowWhoseDeclarationsCannotBeRead(t *testing.T) {
	deep := &v1.Workflow{Name: "leaf", Steps: []*v1.Node{{Id: "leaf", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}}}
	for i := range v1.MaxStructureDepth + 2 {
		deep = &v1.Workflow{Name: fmt.Sprintf("level%d", i), Steps: []*v1.Node{
			{Id: "down", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: deep}}},
		}}
	}
	_, err := v1.DeclaresSensitiveValues(deep)
	require.Error(t, err, "the chain did not pass the scan's bound, so this proves nothing")

	_, err = Debug(context.Background(), deep, DebugOptions{Continue: true})
	require.Error(t, err, "a workflow whose declarations could not be read was debugged without authorization")
	assert.Contains(t, err.Error(), "could not be inspected")
}
