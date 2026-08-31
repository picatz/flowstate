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
		Name:                 "commit_push",
		Summary:              "writes a commit to a branch",
		InputMessage:         "flowstate.v1.Task.Log.Inputs",
		OutputMessage:        "flowstate.v1.Task.Log.Outputs",
		SecretInputs:         []string{"token"},
		RequiredSecretInputs: []string{"token"},
		NeedsScope:           true,
	}, Config{})
	require.NoError(t, err)

	require.Equal(t, []string{"token"}, def.SecretInputs,
		"the manifest declared secret_inputs and the task definition does not carry it")
	require.Equal(t, []string{"token"}, def.RequiredSecretInputs,
		"the manifest declared required_secret_inputs and the task definition does not carry it")
	require.True(t, def.NeedsPrevOutputs,
		"the manifest declared needs_scope and the task definition does not carry it")

	described := flowstatev1.DescribeTask(def)

	assert.Equal(t, []string{"token"}, described.GetSecretInputs(),
		"a plugin's whole-value secret-accepting inputs are invisible in the task's description")
	assert.Equal(t, []string{"token"}, described.GetRequiredSecretInputs(),
		"a plugin's required whole-value secret inputs are invisible in the task's description")
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
	assert.Empty(t, described.GetRequiredSecretInputs())
	assert.False(t, described.GetNeedsScope())
}

func TestRequiredSecretInputsMustAlsoPermitHostResolution(t *testing.T) {
	t.Parallel()

	_, err := (&Plugin{name: "example"}).taskDef(&pluginv1.TaskManifest{
		Name:                 "connect",
		InputMessage:         "flowstate.v1.Task.Log.Inputs",
		OutputMessage:        "flowstate.v1.Task.Log.Outputs",
		RequiredSecretInputs: []string{"token"},
	}, Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires")
	assert.Contains(t, err.Error(), "secret_inputs")
}

// TestPluginCatalogCarriesClaimsSchemaVersion is the same presence signal as
// TaskCatalog.ClaimsSchemaVersion, on the other message a build's task
// claims travel in.
//
// TaskCatalog.ClaimsSchemaVersion was added so a remote GetCatalog reader
// could tell "this task claims nothing" from "this server predates the claim
// fields entirely." PluginCatalog answers the identical question for `flow
// plugins -o json`'s output, which is written to disk or piped to another
// process rather than read in the process that built it — so it needs the
// same signal, and a Host built from a real plugin has to actually set it
// rather than leave the field declared and unpopulated.
func TestPluginCatalogCarriesClaimsSchemaVersion(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "ok")))

	got := host.Catalog().GetClaimsSchemaVersion()
	require.Equal(t, flowstatev1.CurrentClaimsSchemaVersion, got,
		"a PluginCatalog built by this host does not carry the current claims schema version, "+
			"so a saved or piped `flow plugins -o json` output could not be told apart from a "+
			"pre-#712 build's response by anything reading it later")
}
