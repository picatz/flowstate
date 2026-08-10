package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func describedPlugin(name, version, schema string) *v1.PluginDescription {
	return &v1.PluginDescription{Name: name, Version: version, ProtocolVersion: 2, TaskSchemaDigest: schema, DistributionDigest: "sha256:binary"}
}

// catalogOf is a deployment holding exactly these plugins.
func catalogOf(plugins ...*v1.PluginDescription) *v1.PluginCatalog {
	return &v1.PluginCatalog{Plugins: plugins}
}

// requires is a workflow declaring one requirement and nothing else.
func requires(name, minimum string) *v1.Workflow {
	return &v1.Workflow{Name: name + "-user", PluginRequirements: []*v1.PluginRequirement{{Name: name, MinimumVersion: minimum}}}
}

// calls wraps a workflow in a caller that reaches it through a `call:` step, the
// way flowfile/call.go compiles one.
func calls(id string, callee *v1.Workflow) *v1.Workflow {
	return &v1.Workflow{
		Name:  "caller-of-" + callee.GetName(),
		Steps: []*v1.Node{{Id: id, Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}}},
	}
}

func TestResolvePluginsPinsCompatibleVersion(t *testing.T) {
	wf := requires("slack", "v2.1.0")
	require.NoError(t, v1.ResolvePlugins(wf, catalogOf(describedPlugin("slack", "v2.1.7", "sha256:schema"))))
	require.Equal(t, "v2.1.7", wf.GetResolvedPlugins()[0].GetVersion())
}

// A plugin advertising its version without the Flowfile's v prefix still
// resolves, which every plugin in this tree does ("0.1.0" in plugins/git). The
// grammar is a rule about what an author writes, not about what a third-party
// manifest says, and enforcing it on both sides would refuse every real plugin.
func TestResolvePluginsAcceptsABareAdvertisedVersion(t *testing.T) {
	wf := requires("git", "v0.1.0")
	require.NoError(t, v1.ResolvePlugins(wf, catalogOf(describedPlugin("git", "0.1.0", "sha256:schema"))))
	require.Equal(t, "0.1.0", wf.GetResolvedPlugins()[0].GetVersion())

	// And the Flowfile grammar is still the strict one.
	require.False(t, v1.ValidPluginVersion("0.1.0"))
	require.True(t, v1.ValidPluginVersion("v0.1.0"))
}

func TestResolvePluginsRefusesMissingOldAndWrongMajor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		catalog *v1.PluginCatalog
		want    string
	}{
		{"missing", &v1.PluginCatalog{}, "not installed"},
		{"nothing installed at all", nil, "not installed"},
		{"old", catalogOf(describedPlugin("slack", "v2.0.9", "sha256:schema")), "below the v2.1.0"},
		{"major", catalogOf(describedPlugin("slack", "v3.0.0", "sha256:schema")), "different contract"},
		{
			"incomplete catalog entry",
			catalogOf(&v1.PluginDescription{Name: "slack", Version: "v2.1.0"}),
			"is incomplete",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := requires("slack", "v2.1.0")
			err := v1.ResolvePlugins(wf, tc.catalog)
			require.ErrorContains(t, err, tc.want)
			require.Empty(t, wf.GetResolvedPlugins(), "a refused submission must not be left partly pinned")
		})
	}
}

// TestResolvePluginsOverwritesACallerSuppliedSelection is the forged-tuple
// direction: `resolved_plugins` is documented as the control plane's own
// selection, and an RPC client can put anything it likes in the field.
//
// Both halves matter. A caller that names a plugin the deployment does not have
// must not have that selection survive, and a caller that names one it *does*
// have must not get to choose the digest it will be checked against, because otherwise
// the replay contract is whatever the submitter said it was.
func TestResolvePluginsOverwritesACallerSuppliedSelection(t *testing.T) {
	t.Run("a selection for requirements that do not exist is discarded", func(t *testing.T) {
		wf := &v1.Workflow{
			Name: "no-requirements",
			ResolvedPlugins: []*v1.ResolvedPlugin{{
				Name: "slack", Version: "v9.9.9", ProtocolVersion: 2,
				TaskSchemaDigest: "sha256:forged", DistributionDigest: "sha256:forged",
			}},
		}

		require.NoError(t, v1.ResolvePlugins(wf, catalogOf(describedPlugin("slack", "v2.1.0", "sha256:schema"))))
		require.Empty(t, wf.GetResolvedPlugins(),
			"a workflow requiring nothing is pinned to nothing, whatever the caller sent")
	})

	t.Run("a forged tuple is replaced by the deployment's own", func(t *testing.T) {
		wf := requires("slack", "v2.1.0")
		wf.ResolvedPlugins = []*v1.ResolvedPlugin{{
			Name: "slack", Version: "v2.1.0", ProtocolVersion: 99,
			TaskSchemaDigest: "sha256:forged", DistributionDigest: "sha256:forged",
		}}

		require.NoError(t, v1.ResolvePlugins(wf, catalogOf(describedPlugin("slack", "v2.1.0", "sha256:schema"))))

		pinned := wf.GetResolvedPlugins()[0]
		require.Equal(t, uint32(2), pinned.GetProtocolVersion())
		require.Equal(t, "sha256:schema", pinned.GetTaskSchemaDigest())
		require.Equal(t, "sha256:binary", pinned.GetDistributionDigest())
	})
}

// TestResolvePluginsWalksTheCallTree covers the callee's `plugins:` block, which
// travels inside its caller's specification and used to be ignored entirely.
func TestResolvePluginsWalksTheCallTree(t *testing.T) {
	catalog := catalogOf(describedPlugin("slack", "v2.1.0", "sha256:schema"))

	t.Run("a callee's requirement is pinned where it is declared", func(t *testing.T) {
		callee := requires("slack", "v2.1.0")
		caller := calls("notify", callee)

		require.NoError(t, v1.ResolvePlugins(caller, catalog))
		require.Empty(t, caller.GetResolvedPlugins(), "the caller requires nothing itself")
		require.Len(t, callee.GetResolvedPlugins(), 1)
		require.Equal(t, "slack", callee.GetResolvedPlugins()[0].GetName())
	})

	t.Run("a callee requiring a plugin the deployment lacks refuses the whole submission", func(t *testing.T) {
		caller := calls("notify", requires("pagerduty", "v1.0.0"))

		err := v1.ResolvePlugins(caller, catalog)
		require.ErrorContains(t, err, `required plugin "pagerduty" is not installed`)
		require.ErrorContains(t, err, `step "notify" calls workflow`,
			"the refusal should say which call carries the requirement")
	})

	t.Run("a requirement three calls down is still reached", func(t *testing.T) {
		deep := calls("a", calls("b", calls("c", requires("slack", "v2.1.0"))))
		require.NoError(t, v1.ResolvePlugins(deep, catalog))

		pins, err := v1.PinnedPlugins(deep)
		require.NoError(t, err)
		require.Len(t, pins, 1)
	})

	t.Run("a diamond of calls pins each arm and reports one contract", func(t *testing.T) {
		// The same callee reached twice. Each copy is its own message, since a call
		// embeds a workflow rather than referring to one, so both are pinned and
		// the flattened contract still names the plugin once.
		diamond := &v1.Workflow{
			Name: "diamond",
			Steps: []*v1.Node{
				{Id: "left", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: requires("slack", "v2.1.0")}}},
				{Id: "right", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: requires("slack", "v2.1.0")}}},
			},
		}

		require.NoError(t, v1.ResolvePlugins(diamond, catalog))

		pins, err := v1.PinnedPlugins(diamond)
		require.NoError(t, err)
		require.Len(t, pins, 1, "one plugin, however many arms reach it")
	})
}

// TestResolvePluginsWalksLoopsAndParallelBranches covers the other two shapes a
// `call:` can hide inside, which a walk over top-level steps alone would miss.
func TestResolvePluginsWalksLoopsAndParallelBranches(t *testing.T) {
	catalog := catalogOf(describedPlugin("slack", "v2.1.0", "sha256:schema"))
	callee := requires("slack", "v2.1.0")

	inLoop := &v1.Workflow{
		Name: "in-a-loop",
		Steps: []*v1.Node{{
			Id: "each",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Body: []*v1.Node{{Id: "notify", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}}},
			}},
		}},
	}
	require.NoError(t, v1.ResolvePlugins(inLoop, catalog))
	pins, err := v1.PinnedPlugins(inLoop)
	require.NoError(t, err)
	require.Len(t, pins, 1, "a call in a loop body carries its requirement too")

	inBranch := &v1.Workflow{
		Name: "in-a-branch",
		Steps: []*v1.Node{{
			Id: "both",
			Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
				Branches: []*v1.Parallel_Branch{{
					Steps: []*v1.Node{{Id: "notify", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: requires("slack", "v2.1.0")}}}},
				}},
			}},
		}},
	}
	require.NoError(t, v1.ResolvePlugins(inBranch, catalog))
	pins, err = v1.PinnedPlugins(inBranch)
	require.NoError(t, err)
	require.Len(t, pins, 1, "a call in a parallel branch carries its requirement too")
}

func TestReplayGuardRefusesChangedDescriptorAtSameVersion(t *testing.T) {
	wf := requires("slack", "v2.1.0")
	require.NoError(t, v1.ResolvePlugins(wf, catalogOf(describedPlugin("slack", "v2.1.0", "sha256:old"))))

	err := v1.CheckResolvedPlugins(wf, catalogOf(describedPlugin("slack", "v2.1.0", "sha256:new")))
	require.ErrorContains(t, err, "replay contract")
	require.ErrorContains(t, err, "task schema digest", "the refusal should name the field that differs")
	require.ErrorContains(t, err, "sha256:new", "and what this worker has")
	require.ErrorContains(t, err, "sha256:old", "and what the run expects")
}

// TestReplayGuardRefusesEveryMismatchedField is the negative direction, one field
// at a time: a worker that differs in any part of the tuple must refuse the run.
func TestReplayGuardRefusesEveryMismatchedField(t *testing.T) {
	pinned := func(t *testing.T) *v1.Workflow {
		t.Helper()
		wf := requires("slack", "v2.1.0")
		require.NoError(t, v1.ResolvePlugins(wf, catalogOf(describedPlugin("slack", "v2.1.0", "sha256:schema"))))

		return wf
	}

	for _, tc := range []struct {
		name   string
		worker *v1.PluginDescription
		want   string
	}{
		{
			"a newer version this run was not pinned to",
			describedPlugin("slack", "v2.2.0", "sha256:schema"),
			"version is v2.2.0 here",
		},
		{
			"a different protocol version",
			&v1.PluginDescription{Name: "slack", Version: "v2.1.0", ProtocolVersion: 3, TaskSchemaDigest: "sha256:schema", DistributionDigest: "sha256:binary"},
			"protocol version is 3 here",
		},
		{
			"the same manifest from different bytes",
			&v1.PluginDescription{Name: "slack", Version: "v2.1.0", ProtocolVersion: 2, TaskSchemaDigest: "sha256:schema", DistributionDigest: "sha256:swapped"},
			"distribution digest is sha256:swapped here",
		},
		{
			"the plugin missing altogether",
			describedPlugin("pagerduty", "v1.0.0", "sha256:schema"),
			"no such plugin installed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, v1.CheckResolvedPlugins(pinned(t), catalogOf(tc.worker)))
			require.ErrorContains(t, v1.CheckResolvedPlugins(pinned(t), catalogOf(tc.worker)), tc.want)
		})
	}

	t.Run("a worker with no plugins at all", func(t *testing.T) {
		require.ErrorContains(t, v1.CheckResolvedPlugins(pinned(t), nil), "no such plugin installed")
	})
}

// TestReplayGuardWalksTheCallTree is the same refusal for a requirement the top
// level never mentions.
func TestReplayGuardWalksTheCallTree(t *testing.T) {
	callee := requires("slack", "v2.1.0")
	caller := calls("notify", callee)
	require.NoError(t, v1.ResolvePlugins(caller, catalogOf(describedPlugin("slack", "v2.1.0", "sha256:schema"))))

	require.NoError(t, v1.CheckResolvedPlugins(caller, catalogOf(describedPlugin("slack", "v2.1.0", "sha256:schema"))))
	require.ErrorContains(t,
		v1.CheckResolvedPlugins(caller, catalogOf(describedPlugin("slack", "v2.1.0", "sha256:other"))),
		"replay contract",
		"a callee's pin is the run's pin: a worker that cannot reproduce it must refuse")
}

// TestPinnedPluginsRefusesAnUnresolvedSpecification is the fail-closed direction
// of the worker's own check: a specification carrying requirements and no
// selection never passed through a control plane that knows about plugins, so
// there is nothing to check and the run must not proceed.
func TestPinnedPluginsRefusesAnUnresolvedSpecification(t *testing.T) {
	_, err := v1.PinnedPlugins(requires("slack", "v2.1.0"))
	require.ErrorContains(t, err, "was never resolved against a deployment")

	// Including when the unresolved half is a callee.
	_, err = v1.PinnedPlugins(calls("notify", requires("slack", "v2.1.0")))
	require.ErrorContains(t, err, "was never resolved against a deployment")

	// And a selection that does not line up with what is required, which is a
	// hand-built specification rather than anything ResolvePlugins can produce.
	crossed := requires("slack", "v2.1.0")
	crossed.ResolvedPlugins = []*v1.ResolvedPlugin{{Name: "pagerduty", Version: "v1.0.0"}}
	_, err = v1.PinnedPlugins(crossed)
	require.ErrorContains(t, err, `is pinned to "pagerduty" in its place`)
}

// TestPinnedPluginsRefusesTwoContractsForOnePlugin covers the specification that
// cannot be satisfied by any single worker: two callees pinned to different
// builds of one plugin. Choosing either would be choosing silently.
func TestPinnedPluginsRefusesTwoContractsForOnePlugin(t *testing.T) {
	left := requires("slack", "v2.1.0")
	require.NoError(t, v1.ResolvePlugins(left, catalogOf(describedPlugin("slack", "v2.1.0", "sha256:one"))))

	right := requires("slack", "v2.1.0")
	require.NoError(t, v1.ResolvePlugins(right, catalogOf(describedPlugin("slack", "v2.1.0", "sha256:two"))))

	both := &v1.Workflow{
		Name: "two-contracts",
		Steps: []*v1.Node{
			{Id: "left", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: left}}},
			{Id: "right", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: right}}},
		},
	}

	_, err := v1.PinnedPlugins(both)
	require.ErrorContains(t, err, "two different ways")
}

// TestPluginWalksAreBounded asserts the depth bound is reached rather than merely
// not exceeded: a specification nested past what the walk checks to is refused,
// because a check that cannot decide must not allow.
func TestPluginWalksAreBounded(t *testing.T) {
	deep := requires("slack", "v2.1.0")
	for range 64 {
		deep = calls("down", deep)
	}

	require.ErrorContains(t, v1.ResolvePlugins(deep, catalogOf(describedPlugin("slack", "v2.1.0", "sha256:schema"))),
		"past what a specification is checked to")

	_, err := v1.PinnedPlugins(deep)
	require.ErrorContains(t, err, "past what a specification is checked to")
}
