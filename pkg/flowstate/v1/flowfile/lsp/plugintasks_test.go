package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A plugin's task is a task like any other, and the only thing that decides
// whether this server has heard of one is which registry it was handed.
//
// Both directions are asserted here, because only one of them is new. The
// default — a server nobody passed a registry to answering `unknown` for a
// dotted name — is what every editor does today and what an author sees when
// `flow lsp` is started without `--plugin-dir`. It has to keep doing that: a
// server that quietly recognised a task the worker running the file does not
// have would be worse than the squiggle, since the file would then be wrong
// only in production.

// pluginTaskName is spelled the way discovery spells one: the plugin's name, a
// dot, then the task's. Nothing registers it globally, so it is a name no other
// test in this package can be relying on.
const pluginTaskName = "example.greet"

const pluginTaskSummary = "Greet somebody by name."

// registryWithAPluginTask returns the built-in tasks plus one standing in for a
// plugin's.
//
// The descriptors are borrowed from a built-in rather than declared here. What
// this file is about is which registry an answer is read from, and a task whose
// shape came from somewhere other than a TaskDef would be testing a path the
// server does not have — a plugin's descriptors arrive reconstructed into
// exactly this shape, which is the whole point of the plugin protocol shipping
// them.
func registryWithAPluginTask(t *testing.T) *v1.Registry {
	t.Helper()

	shape, ok := v1.LookupTask("log")
	require.True(t, ok, "the log task is the shape this stands in for and it is not registered")

	registry := v1.NewRegistry()
	for _, def := range v1.DefaultRegistry().All() {
		require.NoError(t, registry.Register(def))
	}

	require.NoError(t, registry.Register(v1.TaskDef{
		Name:    pluginTaskName,
		Summary: pluginTaskSummary,
		Inputs:  shape.Inputs,
		Outputs: shape.Outputs,
		Fn:      shape.Fn,
	}))

	_, known := v1.LookupTask(pluginTaskName)
	require.False(t, known,
		"%s is in the default registry, so nothing below distinguishes a threaded "+
			"registry from a task that was always there", pluginTaskName)

	return registry
}

// pluginStepSource is a file naming that task, with the cursor markers the
// completion cases need supplied separately.
const pluginStepSource = `name: plugin-aware
steps:
  - id: greet
    example.greet:
      message: hello
edition: v2026.2
`

// TestAPluginTaskIsUnknownToAServerWithNoRegistry is the direction that must not
// regress.
//
// A process that launched no plugins cannot know their tasks, and saying so is
// the honest answer rather than a gap: the same one `flow validate` gives in a
// terminal, from the same validator.
func TestAPluginTaskIsUnknownToAServerWithNoRegistry(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const uri = "file:///unknown-plugin-task.yaml"
	published := c.open(uri, pluginStepSource)

	var reported bool
	for _, d := range published.Diagnostics {
		if strings.Contains(d.Message, pluginTaskName) {
			reported = true
		}
	}
	assert.True(t, reported,
		"a task nothing registered drew no diagnostic, so an author gets no signal at all: %v",
		published.Diagnostics)

	// And it is offered nowhere, which is the same rule read forwards: a name the
	// validator refuses must not be a name completion suggests.
	src, pos := splitCursor(t, "name: c\nsteps:\n  - |\n")
	c.open("file:///unknown-plugin-task-completion.yaml", src)

	got := labels(c.complete("file:///unknown-plugin-task-completion.yaml", pos.Line, pos.Character).Items)
	assert.NotContains(t, got, pluginTaskName,
		"a task this build has never heard of is offered as a step key")
}

// TestAThreadedRegistryMakesAPluginTaskCompletableAndHoverable is the seam.
//
// `flow lsp --plugin-dir` launches a host, registers what it found, and hands the
// registry to the server; this is that hand-off with the launching left out, so
// the assertion is about the seam rather than about a subprocess. The plugin's
// end of it — a real binary, discovered and reconstructed into TaskDefs — is
// asserted in the plugin package, against the real example plugin.
//
// Note what is *not* changed: no per-task code, no table, nothing naming
// `example.greet` in this package outside a test. That is the package's stated
// principle, and a plugin's task is the case that tests it, since it is a task
// nothing here could have been written to know about.
func TestAThreadedRegistryMakesAPluginTaskCompletableAndHoverable(t *testing.T) {
	t.Parallel()

	c := newClientFor(t, &FlowfileServer{Logger: discardLogger(), Tasks: registryWithAPluginTask(t)})
	c.initialize()

	t.Run("completion offers it as a step key", func(t *testing.T) {
		src, pos := splitCursor(t, "name: c\nsteps:\n  - |\n")
		const uri = "file:///threaded-completion.yaml"
		c.open(uri, src)

		assert.Contains(t, labels(c.complete(uri, pos.Line, pos.Character).Items), pluginTaskName,
			"the registry the server was given has the task and completion did not offer it")
	})

	t.Run("completion offers its inputs under it", func(t *testing.T) {
		src, pos := splitCursor(t, "name: c\nsteps:\n  - id: a\n    "+pluginTaskName+":\n      |\n")
		const uri = "file:///threaded-inputs.yaml"
		c.open(uri, src)

		// From the descriptor, which is the only place the keys under a task's own
		// name ever come from — the line scanner has to recognise the key as a
		// task first, and that question is now the given registry's to answer.
		assert.Contains(t, labels(c.complete(uri, pos.Line, pos.Character).Items), "message",
			"the task's inputs were not read from the registry the server was given")
	})

	t.Run("hover describes it", func(t *testing.T) {
		const uri = "file:///threaded-hover.yaml"
		c.open(uri, pluginStepSource)

		// Line 3, the `example.greet:` key.
		hover := c.hover(uri, 3, 6)
		require.NotNil(t, hover, "hovering a task the server knows about produced nothing")

		var content string
		for _, part := range hover.Contents {
			content += part.Value
		}
		assert.Contains(t, content, pluginTaskSummary,
			"hover did not read the summary from the registry the server was given")
	})
}
