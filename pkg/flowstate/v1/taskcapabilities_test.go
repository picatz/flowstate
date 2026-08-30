package flowstatev1_test

import (
	"context"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

func inertTask(name string) v1.TaskDef {
	return v1.TaskDef{
		Name: name,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return &v1.Node_Outputs{}, nil
		},
	}
}

func TestResolveTaskCapabilitiesOverwritesCallerStateAndFailsClosed(t *testing.T) {
	wf := &v1.Workflow{
		Name: "resolve-task-capabilities",
		Steps: []*v1.Node{
			{Id: "one", Kind: &v1.Node_Task{Task: &v1.Task{Name: "test.one"}}},
			{Id: "two", Kind: &v1.Node_Task{Task: &v1.Task{Name: "test.two"}}},
		},
		ResolvedTaskCapabilities: &v1.ResolvedTaskCapabilities{
			SchemaVersion: 99,
			TaskNames:     []string{"caller.claim"},
		},
	}
	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(inertTask("test.one")))

	err := v1.ResolveTaskCapabilities(wf, registry)
	require.ErrorContains(t, err, "test.two")
	require.Nil(t, wf.GetResolvedTaskCapabilities(),
		"a caller-supplied or partly resolved snapshot survived refusal")

	require.NoError(t, registry.Register(inertTask("test.two")))
	require.NoError(t, v1.ResolveTaskCapabilities(wf, registry))
	require.Equal(t, v1.CurrentTaskCapabilitySchemaVersion,
		wf.GetResolvedTaskCapabilities().GetSchemaVersion())
	require.Equal(t, []string{"test.one", "test.two"},
		wf.GetResolvedTaskCapabilities().GetTaskNames())

	pinned, present, err := v1.PinnedTaskCapabilities(wf)
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, []string{"test.one", "test.two"}, pinned)
}

func TestPinnedTaskCapabilitiesDistinguishesOldAbsentState(t *testing.T) {
	wf := &v1.Workflow{Name: "old", Steps: []*v1.Node{{
		Id: "step", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
	}}}

	names, present, err := v1.PinnedTaskCapabilities(wf)
	require.NoError(t, err)
	require.False(t, present)
	require.Nil(t, names)

	wf.ResolvedTaskCapabilities = &v1.ResolvedTaskCapabilities{}
	_, present, err = v1.PinnedTaskCapabilities(wf)
	require.True(t, present)
	require.ErrorContains(t, err, "schema version 0")
}
