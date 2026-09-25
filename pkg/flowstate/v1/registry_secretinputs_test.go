package flowstatev1_test

import (
	"context"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

func TestRegistryRequiresRequiredSecretInputsToBeSecretInputs(t *testing.T) {
	registry := v1.NewRegistry()
	def := v1.TaskDef{
		Name:                 "probe",
		RequiredSecretInputs: []string{"dsn"},
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return nil, nil
		},
	}

	err := registry.Register(def)
	require.ErrorContains(t, err,
		`task "probe" requires input "dsn" to be a secret reference but does not declare it in secret_inputs`)

	def.SecretInputs = []string{"dsn"}
	require.NoError(t, registry.Register(def),
		"a required secret input the task also declares as one it resolves was refused")
}
