package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// CheckPluginRequirements is ResolvePlugins without the pin, for an offline
// reader of a portable catalog that omits the distribution hash. It must not
// loosen anything but that: the version and availability refusals are the same,
// and it writes nothing.
func TestCheckPluginRequirementsSkipsOnlyTheCompletenessOfThePin(t *testing.T) {
	portable := &v1.PluginDescription{
		Name: "slack", Version: "v2.1.0", ProtocolVersion: 2,
		TaskSchemaDigest: "sha256:schema", ClaimsDigest: "sha256:claims",
	}
	catalog := catalogOf(portable)
	catalog.ClaimsSchemaVersion = v1.CurrentClaimsSchemaVersion

	wf := requires("slack", "v2.1.0")
	require.ErrorContains(t, v1.ResolvePlugins(wf, catalog), "is incomplete",
		"a submission still refuses an entry it cannot pin")

	wf = requires("slack", "v2.1.0")
	require.NoError(t, v1.CheckPluginRequirements(wf, catalog))
	require.Empty(t, wf.GetResolvedPlugins(), "a check selects nothing")

	stale := catalogOf(portable)
	stale.ClaimsSchemaVersion = v1.CurrentClaimsSchemaVersion + 1

	for name, tc := range map[string]struct {
		catalog *v1.PluginCatalog
		want    string
	}{
		"missing": {&v1.PluginCatalog{}, "not installed"},
		"old":     {catalogOf(&v1.PluginDescription{Name: "slack", Version: "v2.0.9"}), "below the v2.1.0"},
		"major":   {catalogOf(&v1.PluginDescription{Name: "slack", Version: "v3.0.0"}), "different contract"},
		"claims":  {stale, "claims schema version"},
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorContains(t, v1.CheckPluginRequirements(requires("slack", "v2.1.0"), tc.catalog), tc.want)
		})
	}

	// The call tree is walked as ResolvePlugins walks it.
	require.ErrorContains(t,
		v1.CheckPluginRequirements(calls("c", requires("slack", "v2.1.0")), &v1.PluginCatalog{}), "not installed")
}
