package plugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A plugin manifest's secret_inputs already gated whether the host would resolve
// a `${secret(...)}` reference into an input before the request crossed into the
// plugin process — see resolvePluginSecretInputs. What it did not do was reach
// [flowstatev1.TaskDef], so nothing describing the task afterward (the catalog,
// GetCatalog, `flow plugins`) could say the plugin asked to receive one. #712.

// TestAPluginsSecretInputsReachTheTaskDef is the mapping this fix adds: taskDef
// copying manifest.GetSecretInputs() onto TaskDef.SecretInputs, so a description
// built from the def can see what enforcement already knew.
func TestAPluginsSecretInputsReachTheTaskDef(t *testing.T) {
	t.Parallel()

	def, err := (&Plugin{name: "example"}).taskDef(&pluginv1.TaskManifest{
		Name:          "commit_push",
		Summary:       "writes a commit to a branch",
		InputMessage:  "flowstate.v1.Task.Log.Inputs",
		OutputMessage: "flowstate.v1.Task.Log.Outputs",
		SecretInputs:  []string{"token"},
		NeedsScope:    true,
	}, Config{})
	require.NoError(t, err)

	require.Equal(t, []string{"token"}, def.SecretInputs,
		"the manifest declared secret_inputs and the task definition does not carry it")
	require.True(t, def.NeedsPrevOutputs,
		"the manifest declared needs_scope and the task definition does not carry it")

	described := flowstatev1.DescribeTask(def)

	assert.Equal(t, []string{"token"}, described.GetSecretInputs(),
		"a plugin's whole-value secret-accepting inputs are invisible in the task's description")
	assert.True(t, described.GetNeedsScope(),
		"a plugin task that receives every prior step's outputs reports otherwise in its description")
}

// TestAPluginThatDeclaresNoSecretInputsDescribesNone is the negative direction,
// so the test above is not simply asserting that a field exists.
func TestAPluginThatDeclaresNoSecretInputsDescribesNone(t *testing.T) {
	t.Parallel()

	def, err := (&Plugin{name: "example"}).taskDef(&pluginv1.TaskManifest{
		Name:          "quiet",
		Summary:       "says nothing about its inputs",
		InputMessage:  "flowstate.v1.Task.Log.Inputs",
		OutputMessage: "flowstate.v1.Task.Log.Outputs",
	}, Config{})
	require.NoError(t, err)

	described := flowstatev1.DescribeTask(def)

	assert.Empty(t, described.GetSecretInputs())
	assert.False(t, described.GetNeedsScope())
}
