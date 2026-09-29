package server

import (
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

func TestValidateSpecificationBoundsTheProgramBeforeWalkingRequirements(t *testing.T) {
	s := &FlowstateServer{}
	wf := &v1.Workflow{
		Name: "oversized-before-task-walk",
		Steps: []*v1.Node{
			{Id: "large", Kind: &v1.Node_Value{Value: v1.NewLiteral(strings.Repeat("x", v1.MaxSpecBytes))}},
			{Id: "missing", Kind: &v1.Node_Task{Task: &v1.Task{Name: "missing.task"}}},
		},
	}

	err := s.validateSpecification(wf)
	require.ErrorContains(t, err, "byte limit")
	require.NotContains(t, err.Error(), "missing.task",
		"the recursive requirement walk ran before the untrusted program was bounded")
}

func TestTaskCapabilityAttestationDoesNotHideACallerSuppliedSnapshot(t *testing.T) {
	submitted := &v1.Workflow{Name: "attestation"}
	executed := proto.Clone(submitted).(*v1.Workflow)
	executed.ResolvedTaskCapabilities = &v1.ResolvedTaskCapabilities{
		SchemaVersion: v1.CurrentTaskCapabilitySchemaVersion,
	}
	require.True(t, specificationAsSubmitted(submitted, executed),
		"the control plane's own attestation changed executable-specification identity")

	submitted.ResolvedTaskCapabilities = &v1.ResolvedTaskCapabilities{SchemaVersion: 99}
	require.False(t, specificationAsSubmitted(submitted, executed),
		"overwriting a caller-supplied attestation was reported as unchanged")
}

// TestADeploymentCopyFromOtherBytesIsTheSubmittedProgram: a root's source
// digest records which bytes a client compiled from, not anything the program
// does, so a deployment-owned copy that differs only there is still the
// program submitted. Anything else differing is not.
func TestADeploymentCopyFromOtherBytesIsTheSubmittedProgram(t *testing.T) {
	submitted := &v1.Workflow{Name: "sourced", SourceDigest: v1.ContentDigest([]byte("name: sourced\n"))}
	executed := proto.CloneOf(submitted)
	executed.SourceDigest = ""
	require.True(t, specificationAsSubmitted(submitted, executed),
		"a copy compiled from no file was reported as a substitution")
	require.Equal(t, submitted.GetSourceDigest(), v1.ContentDigest([]byte("name: sourced\n")), "the comparison changed its argument")

	executed.Description = proto.String("another program")
	require.False(t, specificationAsSubmitted(submitted, executed),
		"a copy that differs beyond its source digest was reported as submitted")
}
