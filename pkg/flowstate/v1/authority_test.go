package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

func TestCredentialTargetsAreCheckedBeforeExecution(t *testing.T) {
	credentialTask := func(target string) *v1.Node {
		return &v1.Node{Id: "call", Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
			"credential": v1.NewValue(target),
		}}}}
	}

	workflow := &v1.Workflow{Name: "w", Steps: []*v1.Node{{
		Id: "loop", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewValue([]any{"one"}), Body: []*v1.Node{credentialTask("aws-prod")},
		}},
	}}}
	require.NoError(t, v1.ValidateCredentialTargets(workflow, []string{"aws-prod", "partner"}))
	require.ErrorContains(t, v1.ValidateCredentialTargets(workflow, []string{"partner"}),
		`step "call": credential target "aws-prod" is not configured`)
}
