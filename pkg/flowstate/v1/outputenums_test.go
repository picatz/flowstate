package flowstatev1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOutputEnumsComeFromTheStepsTaskDescriptors pins where the names come from:
// the output descriptor of a task the workflow runs, and no other.
func TestOutputEnumsComeFromTheStepsTaskDescriptors(t *testing.T) {
	registry := NewRegistry()
	require.NoError(t, registry.Register(TaskDef{
		Name:    "probe",
		Inputs:  (&Task_Log_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: (&Value_Error{}).ProtoReflect().Descriptor(),
		Fn: func(context.Context, map[string]*Value, *Scope) (*Node_Outputs, error) {
			return nil, nil
		},
	}))

	step := func(id, task string) *Node {
		return &Node{Id: id, Kind: &Node_Task{Task: &Task{Name: task}}}
	}

	used := OutputEnumsOf(&Workflow{Steps: []*Node{step("a", "probe")}}, registry)
	value, ok := used.Value("CODE_NOT_FOUND")
	require.True(t, ok)
	assert.EqualValues(t, int64(Value_Error_CODE_NOT_FOUND), value.Number)
	assert.Equal(t, "Code", string(value.Enum.Name()))

	enum, ok := used.FieldEnum("code")
	require.True(t, ok)
	assert.Equal(t, value.Enum.FullName(), enum.FullName())

	_, ok = used.FieldEnum("message")
	assert.False(t, ok, "a field that is not an enum is not judged as one")

	unused := OutputEnumsOf(&Workflow{Steps: []*Node{step("a", "log")}}, registry)
	assert.True(t, unused.Empty(), "a task the registry does not hold names nothing")
	_, ok = unused.Value("CODE_NOT_FOUND")
	assert.False(t, ok)
}
