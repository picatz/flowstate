package lsp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	decisionv1 "github.com/picatz/flowstate/pkg/flowstate/decision/v1"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestEnumValuesAreOfferedFromTheTasksDescriptors pins that the names an
// expression may write for an enum come from the output descriptor of a task the
// file runs, and are offered only to a file that runs one.
func TestEnumValuesAreOfferedFromTheTasksDescriptors(t *testing.T) {
	const task = "test_enum_completion_probe"

	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name:    task,
		Inputs:  (&v1.Task_Log_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: (&decisionv1.Answer{}).ProtoReflect().Descriptor(),
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return nil, nil
		},
	}))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(task) })

	c := newClient(t)
	c.initialize()

	with := `edition: v2026.4
name: c
steps:
  - id: a
    ` + task + `:
      message: hi
  - id: b
    log:
      message: ${steps.a.calibration == CALIBRATION_S|}
`
	src, pos := splitCursor(t, with)
	c.open("file:///enum-with.yaml", src)
	got := c.complete("file:///enum-with.yaml", pos.Line, pos.Character)
	assert.Contains(t, labels(got.Items), "CALIBRATION_SELF_REPORTED")
	item := findItem(got.Items, "CALIBRATION_SELF_REPORTED")
	require.NotNil(t, item)
	assert.Contains(t, item.Detail, "Calibration = 2")

	without := `edition: v2026.4
name: c
steps:
  - id: b
    log:
      message: ${CALIBRATION_S|}
`
	src, pos = splitCursor(t, without)
	c.open("file:///enum-without.yaml", src)
	got = c.complete("file:///enum-without.yaml", pos.Line, pos.Character)
	assert.NotContains(t, labels(got.Items), "CALIBRATION_SELF_REPORTED",
		"a file that runs no task with the enum is not taught its names")
}
