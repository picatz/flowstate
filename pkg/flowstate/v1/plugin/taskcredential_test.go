package plugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// The host does not mint a credential for a plugin task yet, so a credential
// reference reaching one is refused rather than forwarded: a plugin handed a name
// it cannot resolve is a host contract nobody agreed to. These pin the refusal
// that the host resolution replaces for a declared input, in the negative
// direction first.

func TestResolvePluginSecretInputsRefusesACredentialReference(t *testing.T) {
	t.Parallel()

	ref := flowstatev1.NewCredentialRef("anthropic")

	for name, inputs := range map[string]map[string]*flowstatev1.Value{
		"whole, in an input the task declared as a secret input": {"api_key": ref},
		"whole, in an undeclared input":                          {"other": ref},
		"nested in a mapping": {"api_key": flowstatev1.NewStructureMap(map[string]*flowstatev1.Value{
			"inner": ref,
		})},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			resolved, scrubber, err := resolvePluginSecretInputs(
				t.Context(), "example.task", []string{"api_key"}, nil, inputs, nil)
			require.Error(t, err)
			assert.Nil(t, resolved, "nothing is handed to the plugin")
			assert.Nil(t, scrubber)
			assert.Contains(t, err.Error(), "credential reference")

			var taskErr *flowstatev1.TaskError
			require.ErrorAs(t, err, &taskErr)
			assert.Equal(t, flowstatev1.ErrorKindInvalidInput, taskErr.Kind)
			assert.False(t, taskErr.Retryable())
		})
	}
}

func TestScrubPluginOutputsRefusesACredentialReference(t *testing.T) {
	t.Parallel()

	for name, value := range map[string]*flowstatev1.Value{
		"bare":   flowstatev1.NewCredentialRef("anthropic"),
		"nested": flowstatev1.NewStructureList(flowstatev1.NewCredentialRef("anthropic")),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := scrubPluginOutputs(secrets.NewScrubber(), &flowstatev1.Node_Outputs{
				NamedValues: map[string]*flowstatev1.Value{"leaked": value},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), `"leaked"`)
			assert.Contains(t, err.Error(), "credential reference")
		})
	}
}
