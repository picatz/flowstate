package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func describedPlugin(name, version, schema string) *v1.PluginDescription {
	return &v1.PluginDescription{
		Name: name, Version: version, ProtocolVersion: 2,
		TaskSchemaDigest: schema, DistributionDigest: "sha256:binary", ClaimsDigest: "sha256:claims",
	}
}

// catalogOf is a deployment holding exactly these plugins, reporting the
// current build's claims schema version — what a real catalog from this
// commit's Host.Catalog() actually reports, and what resolveOne now requires
// before it will mint a new pin (#763 review: a new pin left at the zero
// value would permanently exempt that run's replay guard from the version
// check). Tests exercising a specific other version, including zero to
// simulate a pre-#763 catalog, use [catalogWithClaimsSchemaVersion] instead.
func catalogOf(plugins ...*v1.PluginDescription) *v1.PluginCatalog {
	return &v1.PluginCatalog{Plugins: plugins, ClaimsSchemaVersion: v1.CurrentClaimsSchemaVersion}
}

// catalogWithClaimsSchemaVersion is catalogOf with a specific claims schema
// version asserted instead of the current build's, for tests that need one
// other than today's default — including zero, a pre-#763 catalog's shape.
func catalogWithClaimsSchemaVersion(version uint32, plugins ...*v1.PluginDescription) *v1.PluginCatalog {
	return &v1.PluginCatalog{Plugins: plugins, ClaimsSchemaVersion: version}
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

// TestReplayGuardAcceptsALegacyPinWithNoClaimsDigest is #763's P1: an
// in-flight durable run pinned before ClaimsDigest existed carries an empty
// one, and a worker on this commit must accept it for an otherwise-unchanged
// plugin rather than refusing the run — the old-writer/new-reader direction,
// not just new-writer/new-reader.
//
// Built by hand rather than through ResolvePlugins, because that is exactly
// the point: ResolvePlugins today always produces a ClaimsDigest (see
// pinOf), so the only way to get the shape an old pin actually has is to
// construct it directly, the way protojson.Unmarshal would reading it back
// out of a workflow history recorded before this field existed.
func TestReplayGuardAcceptsALegacyPinWithNoClaimsDigest(t *testing.T) {
	t.Parallel()

	legacyPin := []*v1.ResolvedPlugin{{
		Name: "slack", Version: "v2.1.0", ProtocolVersion: 2,
		TaskSchemaDigest:   "sha256:schema",
		DistributionDigest: "sha256:binary",
		// ClaimsDigest deliberately unset: this is what a pin recorded before
		// #712's ClaimsDigest field existed decodes as.
	}}

	// The worker's live catalog: same descriptors, same distribution — an
	// unchanged plugin — but it now reports a ClaimsDigest, because every
	// worker built from this commit computes one.
	worker := catalogOf(describedPlugin("slack", "v2.1.0", "sha256:schema"))

	require.NoError(t, v1.CheckPluginsAvailable(legacyPin, worker),
		"a run pinned before ClaimsDigest existed was refused by a worker reporting one for an unchanged plugin")
}

// TestReplayGuardStillRefusesAClaimsDigestMismatchOnANewPin is the positive
// direction beside it: once a pin has recorded a claims digest, the guard
// enforces it exactly like every other field in the tuple.
func TestReplayGuardStillRefusesAClaimsDigestMismatchOnANewPin(t *testing.T) {
	t.Parallel()

	wf := requires("slack", "v2.1.0")
	require.NoError(t, v1.ResolvePlugins(wf, catalogOf(describedPlugin("slack", "v2.1.0", "sha256:schema"))))

	worker := &v1.PluginCatalog{Plugins: []*v1.PluginDescription{{
		Name: "slack", Version: "v2.1.0", ProtocolVersion: 2,
		TaskSchemaDigest: "sha256:schema", DistributionDigest: "sha256:binary",
		ClaimsDigest: "sha256:different-claims",
	}}}

	err := v1.CheckResolvedPlugins(wf, worker)
	require.ErrorContains(t, err, "claims digest",
		"the run was pinned with a claims digest and a worker reporting a different one was not refused")
}

// TestResolvePluginsPinsClaimsSchemaVersion is the P1 a Codex review on #763
// named: ClaimsDigest hashes only the claim fields' values, not which schema
// version they were computed under, so a run pinned under one version and a
// worker computing byte-identical values under a later version — one that is
// allowed to redefine what an existing field *means*, not only add one — must
// still be told apart. This is the write side: resolving against a catalog
// asserting a version pins that version onto the run.
func TestResolvePluginsPinsClaimsSchemaVersion(t *testing.T) {
	t.Parallel()

	wf := requires("slack", "v2.1.0")
	require.NoError(t, v1.ResolvePlugins(wf,
		catalogWithClaimsSchemaVersion(v1.CurrentClaimsSchemaVersion, describedPlugin("slack", "v2.1.0", "sha256:schema"))))

	require.Equal(t, v1.CurrentClaimsSchemaVersion, wf.GetResolvedPlugins()[0].GetClaimsSchemaVersion(),
		"resolving against a catalog with a claims schema version did not pin it onto the run")
}

// TestResolvePluginsRefusesAnUnknownClaimsSchemaVersion is the Codex finding
// on #763's own version-pinning fix: sameResolvedPlugin's leniency for a zero
// ClaimsSchemaVersion exists for a pin made before this field existed, not
// for one resolveOne mints today. A new pin left at zero (or at a version
// this build did not compute its ClaimsDigest under) would permanently
// exempt that run's replay guard from the check
// TestReplayGuardStillRefusesAClaimsSchemaVersionMismatchOnANewPin proves
// exists — so resolveOne must refuse to create one rather than pin it and
// leave the check silently inert for that run's whole lifetime.
func TestResolvePluginsRefusesAnUnknownClaimsSchemaVersion(t *testing.T) {
	t.Parallel()

	wf := requires("slack", "v2.1.0")
	err := v1.ResolvePlugins(wf, catalogWithClaimsSchemaVersion(0, describedPlugin("slack", "v2.1.0", "sha256:schema")))
	require.ErrorContains(t, err, "claims schema version",
		"a catalog reporting an unknown (zero) claims schema version was resolved into a new pin "+
			"instead of being refused, which would leave that run's replay guard permanently unable "+
			"to check claims schema version")

	// A future version this build was not compiled to understand is refused
	// identically — TaskDescriptionClaimsKnown's own reasoning (#763, both
	// rounds) applies here too: reading is not the only side that must know
	// what a version means, minting a pin under a version this build cannot
	// interpret is exactly as unsafe.
	err = v1.ResolvePlugins(requires("slack", "v2.1.0"),
		catalogWithClaimsSchemaVersion(v1.CurrentClaimsSchemaVersion+1, describedPlugin("slack", "v2.1.0", "sha256:schema")))
	require.ErrorContains(t, err, "claims schema version")
}

// TestReplayGuardAcceptsALegacyPinWithNoClaimsSchemaVersion is the same
// old-writer/new-reader case as
// [TestReplayGuardAcceptsALegacyPinWithNoClaimsDigest], for the version field
// alongside it: a run pinned before ClaimsSchemaVersion existed carries zero,
// and a worker on this commit must accept that for an otherwise-unchanged
// plugin rather than refusing the run.
func TestReplayGuardAcceptsALegacyPinWithNoClaimsSchemaVersion(t *testing.T) {
	t.Parallel()

	legacyPin := []*v1.ResolvedPlugin{{
		Name: "slack", Version: "v2.1.0", ProtocolVersion: 2,
		TaskSchemaDigest:   "sha256:schema",
		DistributionDigest: "sha256:binary",
		ClaimsDigest:       "sha256:claims",
		// ClaimsSchemaVersion deliberately unset: this is what a pin recorded
		// before this field existed decodes as.
	}}

	worker := catalogWithClaimsSchemaVersion(v1.CurrentClaimsSchemaVersion, describedPlugin("slack", "v2.1.0", "sha256:schema"))

	require.NoError(t, v1.CheckPluginsAvailable(legacyPin, worker),
		"a run pinned before ClaimsSchemaVersion existed was refused by a worker reporting one for an unchanged plugin")
}

// TestReplayGuardStillRefusesAClaimsSchemaVersionMismatchOnANewPin is the
// positive direction beside it, and the finding's actual payload: a run
// pinned under one schema version must be refused by a worker on a different
// one, even when every other field of the tuple — including ClaimsDigest —
// matches exactly, because that is precisely the case a schema-version
// redefinition produces: identical serialized claim values, different
// meaning.
func TestReplayGuardStillRefusesAClaimsSchemaVersionMismatchOnANewPin(t *testing.T) {
	t.Parallel()

	wf := requires("slack", "v2.1.0")
	require.NoError(t, v1.ResolvePlugins(wf,
		catalogWithClaimsSchemaVersion(v1.CurrentClaimsSchemaVersion, describedPlugin("slack", "v2.1.0", "sha256:schema"))))

	// The worker's live catalog: identical descriptors and identical
	// ClaimsDigest — describedPlugin always writes "sha256:claims" — but a
	// newer claims schema version, the shape a meaning-redefining bump takes.
	worker := catalogWithClaimsSchemaVersion(v1.CurrentClaimsSchemaVersion+1, describedPlugin("slack", "v2.1.0", "sha256:schema"))

	err := v1.CheckResolvedPlugins(wf, worker)
	require.ErrorContains(t, err, "claims schema version",
		"the run was pinned to the current claims schema version and a worker reporting a different version with an "+
			"identical claims digest was not refused; a digest match alone cannot tell two schema "+
			"versions apart when a version bump redefines a field's meaning without changing its "+
			"serialized value")
}
