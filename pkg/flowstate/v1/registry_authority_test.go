package flowstatev1_test

import (
	"context"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

func TestRegistryRequiresCredentialInputsToCarryAuthority(t *testing.T) {
	registry := v1.NewRegistry()
	err := registry.Register(v1.TaskDef{
		Name:             "cloud-call",
		CredentialInputs: []string{"credential"},
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return nil, nil
		},
	})
	require.ErrorContains(t, err, `credential input "credential" is not an authority input`)
}
