package plugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A plugin could not say whether it shapes its outputs, and three surfaces
// guessed from a name.
//
// The `outputs:` exemption used to key on the *presence of an input called
// `outputs`*: the validator stood down from checking a step's output references,
// and the language server dropped the descriptor's names from completion and
// said they had been replaced. Nothing in the plugin contract reserved the name,
// and plugin execution has no shaping path at all — it returns the outputs the
// plugin's descriptor declares. So for a plugin with an ordinary input by that
// name, two authoring surfaces agreed with each other and disagreed with what
// the run produced, which is the arrangement nothing flags (#324).
//
// Shaping is declared now, on the manifest and on the TaskDef it becomes. These
// tests check it where it has an effect rather than where it is stored: a
// declaration nothing reads is the thing being fixed.

// TestAPluginInputNamedOutputsIsAnOrdinaryInput is the direction that was wrong.
func TestAPluginInputNamedOutputsIsAnOrdinaryInput(t *testing.T) {
	const task = "ordinary.fetch"

	def, err := (&Plugin{name: "ordinary"}).taskDef(&pluginv1.TaskManifest{
		Name:    "fetch",
		Summary: "a task with an ordinary input that happens to be called outputs",
		// The http task's messages, because they are the shape this is about:
		// an input named `outputs` beside a declared set of outputs.
		InputMessage:  "flowstate.v1.Task.HTTP.Inputs",
		OutputMessage: "flowstate.v1.Task.HTTP.Outputs",
		// And nothing said about shaping, which is every plugin written so far.
	}, Config{})
	require.NoError(t, err)
	require.False(t, def.ShapesOutputs, "a manifest that says nothing declares no shaping")

	// Into the default registry, which is the one the validator asks. Not
	// restored afterwards, for the reason the neighbouring plugin tests give: a
	// registry has no way to remove a task, the name is a plugin's, and a Go test
	// binary is one process per package.
	require.NoError(t, flowstatev1.DefaultRegistry().Replace(def))
	assert.False(t, flowstatev1.TaskShapesOutputs(task))

	ds, err := flowfile.ValidateSource([]byte(shapingSource(task)))
	require.NoError(t, err)

	assert.Contains(t, diagnosticText(ds), `has no output "nonsense"`,
		"an ordinary input named `outputs` shapes nothing, so the outputs the plugin declares still describe the step")
}

// TestAPluginThatDeclaresShapingIsTreatedAsShaping is the other direction: a
// plugin whose executor really does read `outputs:` as a replacement gets the
// same standing the built-in http task has.
func TestAPluginThatDeclaresShapingIsTreatedAsShaping(t *testing.T) {
	const task = "shaper.fetch"

	def, err := (&Plugin{name: "shaper"}).taskDef(&pluginv1.TaskManifest{
		Name:           "fetch",
		Summary:        "a task that evaluates outputs as a replacement",
		InputMessage:   "flowstate.v1.Task.HTTP.Inputs",
		OutputMessage:  "flowstate.v1.Task.HTTP.Outputs",
		DeferredInputs: []string{"outputs"},
		ShapesOutputs:  true,
	}, Config{})
	require.NoError(t, err)
	require.True(t, def.ShapesOutputs, "the manifest declared it and the task definition does not carry it")

	require.NoError(t, flowstatev1.DefaultRegistry().Replace(def))
	assert.True(t, flowstatev1.TaskShapesOutputs(task))

	ds, err := flowfile.ValidateSource([]byte(shapingSource(task)))
	require.NoError(t, err)

	text := diagnosticText(ds)
	assert.Contains(t, text, `has no output "nonsense"`,
		"a shaping task answers for its shaped names exactly, which is the point of writing them down")
	assert.Contains(t, text, "replaces what the "+task+" task produces",
		"and it says so in the words shaping uses")
}

// shapingSource is one file, used by both directions, so the difference between
// them is the declaration and nothing else.
func shapingSource(task string) string {
	return `edition: v2026.3
name: t
steps:
  - id: fetch
    ` + task + `:
      url: https://example.com
      outputs:
        id: ${response.json.id}
  - id: after
    log:
      message: ${steps.fetch.nonsense}
`
}
