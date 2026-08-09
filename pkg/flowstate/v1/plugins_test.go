package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

func plugin(name, version, schema string) *v1.PluginDescription {
	return &v1.PluginDescription{Name: name, Version: version, ProtocolVersion: 2, TaskSchemaDigest: schema, DistributionDigest: "sha256:binary"}
}

func TestResolvePluginsPinsCompatibleVersion(t *testing.T) {
	wf := &v1.Workflow{PluginRequirements: []*v1.PluginRequirement{{Name: "slack", MinimumVersion: "v2.1.0"}}}
	require.NoError(t, v1.ResolvePlugins(wf, &v1.PluginCatalog{Plugins: []*v1.PluginDescription{plugin("slack", "v2.1.7", "sha256:schema")}}))
	require.Equal(t, "v2.1.7", wf.GetResolvedPlugins()[0].GetVersion())
}

func TestResolvePluginsRefusesMissingOldAndWrongMajor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		catalog *v1.PluginCatalog
	}{
		{"missing", &v1.PluginCatalog{}},
		{"old", &v1.PluginCatalog{Plugins: []*v1.PluginDescription{plugin("slack", "v2.0.9", "sha256:schema")}}},
		{"major", &v1.PluginCatalog{Plugins: []*v1.PluginDescription{plugin("slack", "v3.0.0", "sha256:schema")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := &v1.Workflow{PluginRequirements: []*v1.PluginRequirement{{Name: "slack", MinimumVersion: "v2.1.0"}}}
			require.Error(t, v1.ResolvePlugins(wf, tc.catalog))
			require.Empty(t, wf.GetResolvedPlugins())
		})
	}
}

func TestReplayGuardRefusesChangedDescriptorAtSameVersion(t *testing.T) {
	wf := &v1.Workflow{PluginRequirements: []*v1.PluginRequirement{{Name: "slack", MinimumVersion: "v2.1.0"}}}
	require.NoError(t, v1.ResolvePlugins(wf, &v1.PluginCatalog{Plugins: []*v1.PluginDescription{plugin("slack", "v2.1.0", "sha256:old")}}))
	err := v1.CheckResolvedPlugins(wf, &v1.PluginCatalog{Plugins: []*v1.PluginDescription{plugin("slack", "v2.1.0", "sha256:new")}})
	require.ErrorContains(t, err, "replay contract")
}
