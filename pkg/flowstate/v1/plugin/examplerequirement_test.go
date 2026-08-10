package plugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The `plugins:` block, from the file all the way to a launched process.
//
// Every other test of plugin requirements builds a [flowstatev1.PluginCatalog]
// in Go, which proves a resolver and, per CLAUDE.md's rule about a capability
// nobody can reach, says nothing about whether an author can write one down and
// have it mean anything. This one takes the requirement out of the shipped
// example, and the catalog out of a plugin process this test built and launched.
func TestThePluginExampleDeclaresItsRequirement(t *testing.T) {
	t.Parallel()

	workflow, _, err := flowfile.Parse(readPluginExample(t))
	require.NoError(t, err, "the plugin example does not compile")

	require.Len(t, workflow.GetPluginRequirements(), 1,
		"the plugin example no longer declares the plugin it needs, so nothing proves the "+
			"`plugins:` grammar reaches a specification")
	require.Equal(t, "example", workflow.GetPluginRequirements()[0].GetName())

	host := exampleHost(t)
	catalog := host.Catalog()

	// The deployment the example asks for, running. Resolution against it is what
	// a server does before a run of this file is accepted.
	require.NoError(t, flowstatev1.ResolvePlugins(workflow, catalog),
		"the example's own requirement does not resolve against the plugin it names")

	pinned := workflow.GetResolvedPlugins()
	require.Len(t, pinned, 1)
	assert.Equal(t, "example", pinned[0].GetName())
	assert.NotZero(t, pinned[0].GetProtocolVersion(), "the pin carries no negotiated protocol version")
	assert.NotEmpty(t, pinned[0].GetTaskSchemaDigest(), "the pin carries no task schema digest")
	assert.NotEmpty(t, pinned[0].GetDistributionDigest(), "the pin carries no distribution digest")

	// And the worker holding exactly this plugin is admitted, which is the other
	// half of the same fact: the tuple a submission records is one a worker
	// reproduces rather than one nothing can match.
	assert.NoError(t, flowstatev1.CheckResolvedPlugins(workflow, catalog))

	// The negative direction, from the same file: a floor this deployment does not
	// meet is refused rather than run against whatever is installed.
	tooNew, _, err := flowfile.Parse(readPluginExample(t))
	require.NoError(t, err)
	tooNew.PluginRequirements[0].MinimumVersion = "v99.0.0"
	require.ErrorContains(t, flowstatev1.ResolvePlugins(tooNew, catalog), "different contract")
}
