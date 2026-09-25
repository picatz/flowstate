package plugin

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A catalog is a task definition written down, and until #710 it was written
// down lossily: [flowstatev1.TaskDescription] rendered a task's messages into
// TaskField lists and dropped the descriptors they were read from, so anything
// rebuilding a definition from a catalog got a task whose schema nothing could
// validate against.
//
// These are round-trip tests rather than field-presence tests, because the
// failure they exist to prevent is not "a field is empty" — it is a validator
// loaded from a catalog accepting a file the launching validator refuses.
// [TestACatalogLoadedValidatorRefusesWhatTheLaunchingOneRefuses] is that
// statement directly; the rest are the facts it rests on.

// widgetManifest is one task declaring a message this binary has never seen
// and every claim a task can make.
//
// The descriptors matter: a task whose messages are flowstate's own round-trips
// on the strength of the name alone (see
// [TestACatalogCarriesNoDescriptorForAMessageEveryBuildHas]), which would prove
// nothing about carrying a schema this process cannot look up.
func widgetManifest(t *testing.T) *pluginv1.TaskManifest {
	t.Helper()

	descriptor := mustMarshal(t, &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{widgetFile()},
	})

	return &pluginv1.TaskManifest{
		Name:             "widget",
		Summary:          "a task whose schema this binary has never compiled",
		InputDescriptor:  descriptor,
		InputMessage:     "plugintest.v1.Widget",
		OutputDescriptor: descriptor,
		OutputMessage:    "plugintest.v1.Widget",

		// Every claim with security weight (#712), all set, so a rebuild that
		// drops one is a failing assertion rather than a value that happened
		// to match its zero.
		NeedsScope:       true,
		SecretInputs:     []string{"name"},
		ShapesOutputs:    true,
		DeferredInputs:   []string{"count"},
		ExpressionInputs: []string{"name"},
	}
}

// catalogOf renders one plugin's task defs into the catalog a reader gets,
// including the claims schema version a real host stamps on it.
func catalogOf(t *testing.T, name string, defs ...flowstatev1.TaskDef) *flowstatev1.PluginCatalog {
	t.Helper()

	described := make([]*flowstatev1.TaskDescription, 0, len(defs))
	for _, def := range defs {
		described = append(described, flowstatev1.DescribeTask(def))
	}

	return &flowstatev1.PluginCatalog{
		Plugins:             []*flowstatev1.PluginDescription{{Name: name, Tasks: described}},
		ClaimsSchemaVersion: flowstatev1.CurrentClaimsSchemaVersion,
	}
}

// TestACatalogRoundTripPreservesEveryClaim is the fidelity statement for the
// five fields with security weight.
//
// Each is asserted against the launched definition's own value rather than
// against a literal, so the test says "these agree" rather than "these are what
// I typed twice" — the same claim arriving from a manifest and from a catalog
// has to mean the same thing or a catalog-loaded validator is weaker than the
// one it stands in for.
func TestACatalogRoundTripPreservesEveryClaim(t *testing.T) {
	t.Parallel()

	launched, err := (&Plugin{name: "example"}).taskDef(widgetManifest(t), Config{}.withDefaults())
	require.NoError(t, err)

	defs, err := TaskDefsFromCatalog(catalogOf(t, "example", launched), Config{})
	require.NoError(t, err)
	require.Len(t, defs, 1)
	rebuilt := defs[0]

	assert.Equal(t, launched.Name, rebuilt.Name, "the task's name")
	assert.Equal(t, launched.Summary, rebuilt.Summary, "the task's summary")

	assert.Equal(t, launched.NeedsPrevOutputs, rebuilt.NeedsPrevOutputs,
		"needs_scope: a task rebuilt from a catalog does not report that it receives every prior "+
			"step's outputs, which is the largest trust jump a task definition can make")
	assert.Equal(t, launched.SecretInputs, rebuilt.SecretInputs,
		"secret_inputs: the inputs a host resolves a secret reference into are invisible after a catalog round trip")
	assert.Equal(t, launched.ShapesOutputs, rebuilt.ShapesOutputs,
		"shapes_outputs: a rebuilt task disagrees about whether `outputs:` replaces what it declares, "+
			"which is what decides whether the validator checks a step's output references at all")
	assert.Equal(t, launched.DeferredInputs, rebuilt.DeferredInputs,
		"deferred_inputs: an input the task evaluates itself would be checked against a scope the workflow has")
	assert.Equal(t, launched.ExpressionInputs, rebuilt.ExpressionInputs,
		"expression_inputs: an input that must be written as `${...}` would accept a literal after a round trip")
}

// TestACatalogRoundTripPreservesTheInputSchema is the fact the claims rest on:
// a rebuilt definition carries the same *descriptor*, not a rendering of one.
//
// Every schema-driven check in the validator asks the descriptor rather than
// the rendering — validateTaskInputs returns nothing at all when def.Inputs is
// nil — so a round trip that keeps the security-relevant claims and loses the descriptor is
// a validator that reports nothing about a task's inputs and says so nowhere.
func TestACatalogRoundTripPreservesTheInputSchema(t *testing.T) {
	t.Parallel()

	launched, err := (&Plugin{name: "example"}).taskDef(widgetManifest(t), Config{}.withDefaults())
	require.NoError(t, err)

	defs, err := TaskDefsFromCatalog(catalogOf(t, "example", launched), Config{})
	require.NoError(t, err)
	rebuilt := defs[0]

	require.NotNil(t, rebuilt.Inputs, "the rebuilt task has no input schema, so nothing can be validated against it")
	require.NotNil(t, rebuilt.Outputs, "the rebuilt task has no output schema")

	assert.Equal(t, launched.Inputs.FullName(), rebuilt.Inputs.FullName())
	assert.Equal(t, launched.Outputs.FullName(), rebuilt.Outputs.FullName())
	assert.Equal(t, fieldNamesOf(launched.Inputs), fieldNamesOf(rebuilt.Inputs),
		"the rebuilt task's input fields differ from the launched one's")

	// And describing the rebuilt task produces the description it was rebuilt
	// from, which is the round trip closing: a catalog written by a reader that
	// loaded one is the catalog it loaded, so a document can be checked in and
	// regenerated without a diff.
	assert.True(t,
		proto.Equal(flowstatev1.DescribeTask(launched), flowstatev1.DescribeTask(rebuilt)),
		"describing a rebuilt task did not produce the description it was rebuilt from:\n%v\n!=\n%v",
		flowstatev1.DescribeTask(launched), flowstatev1.DescribeTask(rebuilt))
}

// TestACatalogCarriesNoDescriptorForAMessageEveryBuildHas is the ordinary case
// and the reason the catalog does not grow by a schema per task: a task whose
// messages are flowstate's own travels as two names.
//
// Asserted rather than left implicit because the cost is what makes carrying
// descriptors at all defensible. Every built-in task is this case.
func TestACatalogCarriesNoDescriptorForAMessageEveryBuildHas(t *testing.T) {
	t.Parallel()

	launched, err := (&Plugin{name: "example"}).taskDef(&pluginv1.TaskManifest{
		Name:          "log_ish",
		Summary:       "a task reusing a message the engine compiled in",
		InputMessage:  "flowstate.v1.Task.Log.Inputs",
		OutputMessage: "flowstate.v1.Task.Log.Outputs",
	}, Config{})
	require.NoError(t, err)

	described := flowstatev1.DescribeTask(launched)
	assert.Empty(t, described.GetInputDescriptor(),
		"a message every build has was serialized into the catalog anyway, which is the whole schema per task")
	assert.Equal(t, "flowstate.v1.Task.Log.Inputs", described.GetInputMessage(),
		"and the name it would be resolved by is missing, so it cannot be rebuilt either")

	defs, err := TaskDefsFromCatalog(catalogOf(t, "example", launched), Config{})
	require.NoError(t, err)
	require.NotNil(t, defs[0].Inputs, "a name-only description did not rebuild")
	assert.Equal(t, launched.Inputs.FullName(), defs[0].Inputs.FullName())
}

// TestACatalogLoadedValidatorRefusesWhatTheLaunchingOneRefuses is what all of
// this is for.
//
// #710's second acceptance clause is that a misspelled input to a plugin task
// fails at the author's terminal. The offline catalog it asks for is only worth
// having if the answer is the same one the launching validator gives, so this
// validates one file against a definition rebuilt from a catalog and against
// the definition a launched plugin produced, and compares the diagnostics
// themselves rather than merely counting them.
//
// The two definitions are registered under two plugin names because the default
// registry has no scoped view to validate against and a task name is a key; the
// task name is normalized back out of the diagnostics before they are compared,
// so the comparison is of what was said and not of which name it was said
// about.
func TestACatalogLoadedValidatorRefusesWhatTheLaunchingOneRefuses(t *testing.T) {
	launchedDef, err := (&Plugin{name: "launched"}).taskDef(widgetManifest(t), Config{}.withDefaults())
	require.NoError(t, err)

	// The same manifest, described and rebuilt under a second plugin's name:
	// one task arriving two ways, which is the comparison.
	catalogSource, err := (&Plugin{name: "catalogged"}).taskDef(widgetManifest(t), Config{}.withDefaults())
	require.NoError(t, err)

	defs, err := TaskDefsFromCatalog(catalogOf(t, "catalogged", catalogSource), Config{})
	require.NoError(t, err)
	rebuilt := defs[0]

	// Into the default registry, which is the one the validator asks. Not
	// removed afterwards, for the reason the neighbouring plugin tests give: a
	// name is a plugin's, and a Go test binary is one process per package.
	require.NoError(t, flowstatev1.DefaultRegistry().Replace(launchedDef))
	require.NoError(t, flowstatev1.DefaultRegistry().Replace(rebuilt))

	for _, file := range []struct {
		name   string
		source func(task string) string
		expect string
	}{
		{
			name:   "a misspelled input",
			source: widgetSource,
			expect: `has no such input`,
		},
		{
			name: "a literal where the task requires an expression",
			source: func(task string) string {
				return `edition: v2026.3
name: t
steps:
  - id: make
    ` + task + `:
      name: a literal
`
			},
			expect: `has to be written as one`,
		},
	} {
		t.Run(file.name, func(t *testing.T) {
			launchedDiags, err := flowfile.ValidateSource([]byte(file.source("launched.widget")))
			require.NoError(t, err)

			catalogDiags, err := flowfile.ValidateSource([]byte(file.source("catalogged.widget")))
			require.NoError(t, err)

			launchedText := diagnosticText(launchedDiags)
			require.Contains(t, launchedText, file.expect,
				"the launching validator did not report what this case is about, so the comparison below proves nothing")

			assert.Equal(t,
				strings.ReplaceAll(launchedText, "launched.widget", "<task>"),
				strings.ReplaceAll(diagnosticText(catalogDiags), "catalogged.widget", "<task>"),
				"a validator loaded from a catalog disagrees with the one that launched the plugin")
		})
	}
}

// TestTaskDefsFromCatalogRefusesAnUnreadableClaimsSchemaVersion is the
// fail-closed direction, and the reason the rebuild takes a whole catalog
// rather than a task at a time.
//
// proto3 gives a bool and a repeated string field no presence, so a catalog
// written before the claim fields existed decodes as a task that needs no
// scope, takes no secret and defers nothing — every one of the five read at its
// weakest, by silence. ClaimsSchemaVersion is the presence signal for the set,
// and a rebuild that ignored it would produce exactly the quietly-weaker
// validator this whole change exists to prevent.
func TestTaskDefsFromCatalogRefusesAnUnreadableClaimsSchemaVersion(t *testing.T) {
	t.Parallel()

	launched, err := (&Plugin{name: "example"}).taskDef(widgetManifest(t), Config{}.withDefaults())
	require.NoError(t, err)

	catalog := catalogOf(t, "example", launched)

	for _, version := range []uint32{
		// A catalog from a build that predates the claim fields entirely.
		0,
		// And one from a build that is ahead of this one, which is refused for
		// the mirror-image reason: a version bump can redefine a field as well
		// as add one, so "newer than mine" is not safe either (#763 review).
		flowstatev1.CurrentClaimsSchemaVersion + 1,
	} {
		catalog.ClaimsSchemaVersion = version

		_, err := TaskDefsFromCatalog(catalog, Config{})
		require.ErrorIs(t, err, ErrCatalogClaims,
			"a catalog reporting claims schema version %d was rebuilt anyway, so its silence on a "+
				"claim field was read as the task not making it", version)
	}
}

// TestACatalogCannotNameATaskItsPluginCouldNotProvide is the direction a
// document has and a launch does not (#710).
//
// A host chooses the name a task is registered under: it qualifies every
// manifest's bare name with the plugin's own, so `example.widget` is the only
// shape a launch produces and `http` is unreachable that way. A catalog is a
// document and says whatever its author typed — and
// [flowstatev1.Registry.Register] replaces rather than refuses, so a catalog
// naming a task `http` would put a definition carrying the document's own
// descriptors, unable to execute, where the built-in was.
//
// Both directions, because the refusal is only meaningful against the
// acceptance: the qualified name this same host produced still rebuilds.
func TestACatalogCannotNameATaskItsPluginCouldNotProvide(t *testing.T) {
	t.Parallel()

	launched, err := (&Plugin{name: "example"}).taskDef(widgetManifest(t), Config{}.withDefaults())
	require.NoError(t, err)
	require.Equal(t, "example.widget", launched.Name,
		"the host no longer qualifies a task with its plugin's name, which is what this test checks a document against")

	_, err = TaskDefsFromCatalog(catalogOf(t, "example", launched), Config{})
	require.NoError(t, err, "a catalog this host's own naming produced was refused")

	// The same tasks, listed under names no manifest could have produced. The
	// first three are the shapes a prefix-only check let through (#863 review):
	// each is well-formed as a string and refused by the protovalidate rules
	// the host applies to the two segments at launch.
	for _, tc := range []struct {
		name    string
		catalog *flowstatev1.PluginCatalog
	}{
		{
			// The task is qualified — by a plugin name that is not one. The
			// schema's manifest name is lowercase.
			name:    "an uppercase plugin segment",
			catalog: namedTaskCatalog("Example", "Example.foo"),
		},
		{
			name:    "an uppercase task segment",
			catalog: namedTaskCatalog("example", "example.Bad"),
		},
		{
			// A dot in the task segment is what the manifest's own pattern is
			// there to stop: a plugin cannot smuggle a qualifier of its own.
			name:    "a second qualifier inside the task segment",
			catalog: namedTaskCatalog("example", "example.foo.bar"),
		},
		{
			name:    "a task qualified by a different plugin",
			catalog: catalogOf(t, "somebody-else", launched),
		},
		{
			name:    "an unqualified name that would shadow a built-in",
			catalog: namedTaskCatalog("example", "http"),
		},
		{
			name:    "a plugin entry with no name at all",
			catalog: namedTaskCatalog("", "example.widget"),
		},
		{
			name:    "a plugin segment and nothing after it",
			catalog: namedTaskCatalog("example", "example."),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := TaskDefsFromCatalog(tc.catalog, Config{})
			require.ErrorIs(t, err, ErrCatalogTaskName,
				"a catalog naming a task its plugin could not have provided was rebuilt anyway, so a "+
					"document can register a definition over whatever already holds that name")
		})
	}

	// And the boundary the refusals above are only meaningful against: the
	// valid form, every character of it legal in its own segment.
	t.Run("the valid form", func(t *testing.T) {
		t.Parallel()

		defs, err := TaskDefsFromCatalog(namedTaskCatalog("ex-ample9", "ex-ample9.foo_bar9"), Config{})
		require.NoError(t, err,
			"a task name a manifest could have produced was refused, so the check is stricter than the launcher")
		require.Len(t, defs, 1)
		assert.Equal(t, "ex-ample9.foo_bar9", defs[0].Name)
	})
}

// namedTaskCatalog is one plugin entry listing one task, both named exactly as
// given — the shape a hand-edited document has and no host writes.
func namedTaskCatalog(plugin, task string) *flowstatev1.PluginCatalog {
	return &flowstatev1.PluginCatalog{
		ClaimsSchemaVersion: flowstatev1.CurrentClaimsSchemaVersion,
		Plugins: []*flowstatev1.PluginDescription{{
			Name:  plugin,
			Tasks: []*flowstatev1.TaskDescription{{Name: task, Summary: "a task from a document"}},
		}},
	}
}

// TestACatalogCannotDefineOneTaskTwice is the other thing a document can be
// that a live host cannot (#863 review).
//
// [Plugin.checkManifest] refuses a manifest that provides one task twice, and
// across plugins the host's qualification makes a collision unreachable. A
// catalog can do both, and [flowstatev1.Registry.Register] replaces rather than
// refuses — so without this the definition a file is checked against is
// whichever one appears last in the document.
//
// Both shapes, and the refusal names both sources: with two entries under one
// plugin name, the task name alone does not say where to look.
func TestACatalogCannotDefineOneTaskTwice(t *testing.T) {
	t.Parallel()

	launched, err := (&Plugin{name: "example"}).taskDef(widgetManifest(t), Config{}.withDefaults())
	require.NoError(t, err)
	described := flowstatev1.DescribeTask(launched)

	other, err := (&Plugin{name: "example"}).taskDef(&pluginv1.TaskManifest{
		Name:          "widget",
		Summary:       "the same name, a different task",
		InputMessage:  "flowstate.v1.Task.Log.Inputs",
		OutputMessage: "flowstate.v1.Task.Log.Outputs",
	}, Config{})
	require.NoError(t, err)
	require.Equal(t, described.GetName(), flowstatev1.DescribeTask(other).GetName(),
		"the two definitions do not share a name, so nothing below is a duplicate")

	for _, tc := range []struct {
		name    string
		catalog *flowstatev1.PluginCatalog
	}{
		{
			name: "one plugin listing it twice",
			catalog: &flowstatev1.PluginCatalog{
				ClaimsSchemaVersion: flowstatev1.CurrentClaimsSchemaVersion,
				Plugins: []*flowstatev1.PluginDescription{{
					Name:  "example",
					Tasks: []*flowstatev1.TaskDescription{described, flowstatev1.DescribeTask(other)},
				}},
			},
		},
		{
			name: "two plugin entries under one name",
			catalog: &flowstatev1.PluginCatalog{
				ClaimsSchemaVersion: flowstatev1.CurrentClaimsSchemaVersion,
				Plugins: []*flowstatev1.PluginDescription{
					{Name: "example", Tasks: []*flowstatev1.TaskDescription{described}},
					{Name: "example", Tasks: []*flowstatev1.TaskDescription{flowstatev1.DescribeTask(other)}},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			defs, err := TaskDefsFromCatalog(tc.catalog, Config{})
			require.ErrorIs(t, err, ErrCatalogDuplicateTask,
				"a catalog defining one task twice rebuilt anyway, so which definition a file is "+
					"checked against is decided by the order the document lists them in")
			assert.Empty(t, defs,
				"a refused catalog handed back definitions, which a caller would register")
			assert.Contains(t, err.Error(), "example.widget",
				"the refusal does not name the duplicated task: %v", err)
			assert.Equal(t, 2, strings.Count(err.Error(), `"example"`),
				"the refusal does not name both plugin entries that defined it: %v", err)
		})
	}
}

// TestACatalogTaskRefusesToExecute is the boundary the rebuild does not cross.
//
// A catalog describes a task; it carries no way to run one, and the plugin
// binary may not exist on the machine reading it — on a browser authoring
// surface there is no such thing. The registry refuses a definition with no
// function at all, so the choice is between a function that fails closed and
// one that does something surprising.
func TestACatalogTaskRefusesToExecute(t *testing.T) {
	t.Parallel()

	launched, err := (&Plugin{name: "example"}).taskDef(widgetManifest(t), Config{}.withDefaults())
	require.NoError(t, err)

	defs, err := TaskDefsFromCatalog(catalogOf(t, "example", launched), Config{})
	require.NoError(t, err)

	require.NotNil(t, defs[0].Fn, "the registry refuses a definition with no function")

	_, err = defs[0].Fn(t.Context(), nil, nil)
	require.ErrorIs(t, err, ErrCatalogOnly)

	var taskErr *flowstatev1.TaskError
	require.ErrorAs(t, err, &taskErr)
	assert.False(t, taskErr.Retryable(),
		"executing a catalog-loaded task is retried, and no number of attempts makes a plugin "+
			"appear in a process that was never going to launch one")
}

// TestTheDescriptorFieldsStayOutOfTheTaskSchemaDigest guards the replay
// contract every in-flight durable run is pinned to.
//
// PluginDescription.task_schema_digest is embedded in a run's ResolvedPlugin at
// submission and compared exactly at every segment boundary, where a mismatch
// is a non-retryable failure. Descriptor bytes re-serialized by two builds of
// one unchanged plugin are not guaranteed identical, so folding them into that
// digest would fail runs whose plugin did not change. The shape they describe
// is already covered there, through the `inputs` and `outputs` renderings
// derived from them.
func TestTheDescriptorFieldsStayOutOfTheTaskSchemaDigest(t *testing.T) {
	t.Parallel()

	launched, err := (&Plugin{name: "example"}).taskDef(widgetManifest(t), Config{}.withDefaults())
	require.NoError(t, err)

	described := flowstatev1.DescribeTask(launched)
	require.NotEmpty(t, described.GetInputDescriptor(),
		"this task's descriptor is not being carried, so the assertion below would hold trivially")

	withoutDescriptors := proto.Clone(described).(*flowstatev1.TaskDescription)
	withoutDescriptors.InputDescriptor = nil
	withoutDescriptors.InputMessage = ""
	withoutDescriptors.OutputDescriptor = nil
	withoutDescriptors.OutputMessage = ""

	digested := func(one *flowstatev1.TaskDescription) []byte {
		raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(
			&flowstatev1.PluginDescription{Tasks: []*flowstatev1.TaskDescription{
				flowstatev1.TaskDescriptionSansClaims(one),
			}})
		require.NoError(t, err)
		return raw
	}

	assert.Equal(t, digested(withoutDescriptors), digested(described),
		"carrying a task's descriptors changed what task_schema_digest hashes, which fails every "+
			"in-flight run pinned to a plugin whose descriptors did not change")
}

// TestARealHostsCatalogRebuildsEveryTaskItRegistered walks the whole path a
// reader takes: a launched plugin, the catalog its host prints, and the
// definitions rebuilt from that catalog with nothing launched.
//
// The tests above build a description from a def directly. This one goes
// through [Host.Catalog], which is what `flow plugins --output json` serializes
// and therefore what a catalog document on disk actually contains.
func TestARealHostsCatalogRebuildsEveryTaskItRegistered(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "ok")))

	launched := host.TaskDefs()
	require.NotEmpty(t, launched, "the fixture plugin registered no tasks, so this proves nothing")

	rebuilt, err := TaskDefsFromCatalog(host.Catalog(), Config{})
	require.NoError(t, err)
	require.Len(t, rebuilt, len(launched))

	for i, def := range launched {
		assert.Equal(t, def.Name, rebuilt[i].Name)
		require.NotNil(t, rebuilt[i].Inputs, "task %q rebuilt with no input schema", def.Name)
		assert.Equal(t, def.Inputs.FullName(), rebuilt[i].Inputs.FullName())
		assert.Equal(t, def.NeedsPrevOutputs, rebuilt[i].NeedsPrevOutputs)
		assert.Equal(t, def.ShapesOutputs, rebuilt[i].ShapesOutputs)
	}
}

// TestASingleFileDescriptorRoundTripsAtExactlyTheBound is the boundary the
// framing used to move.
//
// A plugin may ship the raw FileDescriptorProto form, and the host accepts it
// when its encoded length is within MaxDescriptorBytes — including when the
// bound is exactly that length. Describing the launched task used to wrap the
// same file in a FileDescriptorSet, whose tag and length prefix made the
// catalog's copy two bytes longer than the descriptor the host had just
// accepted, so rebuilding it under the identical Config refused a task that
// launched fine. The bound was refusing its own output (#854 review).
//
// At exactly the limit, because that is the only place the two bytes are
// visible: with any slack at all the old behaviour passes.
func TestASingleFileDescriptorRoundTripsAtExactlyTheBound(t *testing.T) {
	t.Parallel()

	// The bare form, which is what the manifest carries and what the bound is
	// measured against on the way in.
	bare := mustMarshal(t, widgetFile())

	exact := Config{}.withDefaults()
	exact.MaxDescriptorBytes = len(bare)

	manifest := &pluginv1.TaskManifest{
		Name:             "widget",
		Summary:          "a task shipping one file, exactly at the bound",
		InputDescriptor:  bare,
		InputMessage:     "plugintest.v1.Widget",
		OutputDescriptor: bare,
		OutputMessage:    "plugintest.v1.Widget",
	}

	launched, err := (&Plugin{name: "example"}).taskDef(manifest, exact)
	require.NoError(t, err, "the host refused a descriptor exactly at its own bound, so the round trip below proves nothing")

	described := flowstatev1.DescribeTask(launched)
	require.NotEmpty(t, described.GetInputDescriptor())
	assert.LessOrEqual(t, len(described.GetInputDescriptor()), exact.MaxDescriptorBytes,
		"describing the task produced more descriptor bytes than the bound that admitted it")

	defs, err := TaskDefsFromCatalog(catalogOf(t, "example", launched), exact)
	require.NoError(t, err,
		"a catalog built from a task this very Config launched was refused by that same Config")
	require.NotNil(t, defs[0].Inputs)
	assert.Equal(t, launched.Inputs.FullName(), defs[0].Inputs.FullName())

	// And the bound is a real one rather than a formality: one byte less and
	// the same descriptor is refused, on the way in and out alike.
	tooTight := exact
	tooTight.MaxDescriptorBytes = len(bare) - 1

	_, err = (&Plugin{name: "example"}).taskDef(manifest, tooTight)
	require.ErrorIs(t, err, ErrDescriptor,
		"a descriptor one byte over the bound was accepted, so the bound above was not the deciding number")
}

// TestTaskDefsFromCatalogBoundsTheWholeDocument is the breadth half of the
// bound, and the finding CLAUDE.md's own doctrine caught pointed back at us:
// MaxDescriptorBytes and MaxDescriptorFiles bound *one* descriptor, and a
// catalog's author chooses how many descriptors arrive.
//
// Each bound is asserted to be *reached* as well as not exceeded — a catalog
// exactly at the bound rebuilds, one past it refuses — because "refuses when
// over" is also satisfied by a reader that refuses everything, and a bound
// nothing reaches is a bound nothing tests.
func TestTaskDefsFromCatalogBoundsTheWholeDocument(t *testing.T) {
	t.Parallel()

	// One described task, repeated under names of its own: these bounds are
	// about how many arrive, not about what any one of them says. The names
	// have to differ because a catalog defining one task twice is refused
	// before any bound is reached ([checkOneDefinitionPer]), and a bounds test
	// that tripped over that refusal would be asserting the wrong sentence.
	launched, err := (&Plugin{name: "example"}).taskDef(widgetManifest(t), Config{}.withDefaults())
	require.NoError(t, err)
	described := flowstatev1.DescribeTask(launched)

	catalogWith := func(plugins, tasksPerPlugin int) *flowstatev1.PluginCatalog {
		catalog := &flowstatev1.PluginCatalog{
			ClaimsSchemaVersion: flowstatev1.CurrentClaimsSchemaVersion,
		}
		for p := range plugins {
			name := fmt.Sprintf("example%d", p)
			one := &flowstatev1.PluginDescription{Name: name}
			for task := range tasksPerPlugin {
				renamed := proto.Clone(described).(*flowstatev1.TaskDescription)
				renamed.Name = fmt.Sprintf("%s.widget%d", name, task)
				one.Tasks = append(one.Tasks, renamed)
			}
			catalog.Plugins = append(catalog.Plugins, one)
		}
		return catalog
	}

	descriptorBytes := len(described.GetInputDescriptor()) + len(described.GetOutputDescriptor())
	require.NotZero(t, descriptorBytes, "this task carries no descriptors, so the byte bound below is untested")

	for _, bound := range []struct {
		name    string
		cfg     Config
		atBound *flowstatev1.PluginCatalog
		past    *flowstatev1.PluginCatalog
	}{
		{
			name:    "plugins",
			cfg:     Config{MaxCatalogPlugins: 3},
			atBound: catalogWith(3, 1),
			past:    catalogWith(4, 1),
		},
		{
			name: "tasks across every plugin",
			// Two plugins of three tasks is six, which is over a bound of five
			// while neither plugin is near it on its own — the aggregate is the
			// resource, not any one plugin's share of it.
			cfg:     Config{MaxCatalogTasks: 5},
			atBound: catalogWith(1, 5),
			past:    catalogWith(2, 3),
		},
		{
			name:    "descriptor bytes in total",
			cfg:     Config{MaxCatalogDescriptorBytes: descriptorBytes * 3},
			atBound: catalogWith(3, 1),
			past:    catalogWith(2, 2),
		},
	} {
		t.Run(bound.name, func(t *testing.T) {
			t.Parallel()

			defs, err := TaskDefsFromCatalog(bound.atBound, bound.cfg)
			require.NoError(t, err,
				"a catalog exactly at the %s bound was refused, so the bound is not reachable and "+
					"the refusal below says nothing", bound.name)
			require.NotEmpty(t, defs)

			_, err = TaskDefsFromCatalog(bound.past, bound.cfg)
			require.ErrorIs(t, err, ErrCatalogTooLarge,
				"a catalog past the %s bound was rebuilt anyway", bound.name)
			assert.Contains(t, err.Error(), "Config.MaxCatalog",
				"the refusal does not name the bound an operator would raise")
		})
	}
}

// widgetSource is one file naming a task, with an input the task does not
// declare. `nam` is a misspelling of `name`, which is what #710's second
// acceptance clause is about.
func widgetSource(task string) string {
	return `edition: v2026.3
name: t
steps:
  - id: make
    ` + task + `:
      nam: ${vars.who}
`
}

// fieldNamesOf lists a message's field names in declaration order.
func fieldNamesOf(md protoreflect.MessageDescriptor) []string {
	if md == nil {
		return nil
	}

	fields := md.Fields()
	names := make([]string, 0, fields.Len())
	for i := range fields.Len() {
		names = append(names, string(fields.Get(i).Name()))
	}

	return names
}
