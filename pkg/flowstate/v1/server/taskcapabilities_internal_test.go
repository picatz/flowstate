package server

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

func TestValidateSpecificationBindsTaskCapabilities(t *testing.T) {
	s := &FlowstateServer{}
	wf := &v1.Workflow{Name: "known-task", Steps: []*v1.Node{{
		Id: "say", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
	}}}

	require.NoError(t, s.validateSpecification(wf))
	require.Equal(t, []string{"log"}, wf.GetResolvedTaskCapabilities().GetTaskNames())
	require.Equal(t, v1.CurrentTaskCapabilitySchemaVersion,
		wf.GetResolvedTaskCapabilities().GetSchemaVersion())
}

func TestValidateSpecificationRefusesUnavailableTaskAndCallerSnapshot(t *testing.T) {
	s := &FlowstateServer{}
	wf := &v1.Workflow{
		Name: "unknown-task",
		Steps: []*v1.Node{{
			Id: "missing", Kind: &v1.Node_Task{Task: &v1.Task{Name: "missing.task"}},
		}},
		ResolvedTaskCapabilities: &v1.ResolvedTaskCapabilities{
			SchemaVersion: v1.CurrentTaskCapabilitySchemaVersion,
			TaskNames:     []string{"missing.task"},
		},
	}

	err := s.validateSpecification(wf)
	require.ErrorContains(t, err, "missing.task")
	require.Nil(t, wf.GetResolvedTaskCapabilities(),
		"the caller's capability claim was trusted after control-plane refusal")
}
