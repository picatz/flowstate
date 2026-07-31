package plugin

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Everything in this package was tested and none of it was reachable.
//
// The tests beside this one are good tests of the wrong thing. They build the
// real plugin, launch it, reconstruct its descriptors, and call `def.Fn` — which
// proves the executor works and says nothing about whether an author can get
// there. That is the distinction CLAUDE.md draws about waiting and about secrets,
// met a third time: a test that reaches into a registry it made for itself cannot
// see that the engine reads a different one.
//
// So this file starts from a file somebody writes. It compiles a Flowfile whose
// step key is a plugin's task, asks the validator to accept it, and runs it — all
// against [flowstatev1.DefaultRegistry], because that is the registry every
// lookup in the engine actually consults and the one nothing was registering
// into.
//
// # Why this is one test and not several
//
// Registration into the default registry is a one-way door: there is no
// Unregister, so a worker opens one host and holds it until the process exits.
// This test does the same, which means exactly one test in this binary may do
// the global registration. Deliberately not parallel, for the same reason.

const pluginWorkflow = `edition: v2026.2
name: plugin-reachable
vars:
  who: world
steps:
  - id: greet
    example.greet:
      name: ${vars.who}
      greeting: Hello
  - id: report
    log:
      message: ${steps.greet.message}
`

func TestAFlowfileCanNameAPluginTask(t *testing.T) {
	host := exampleHost(t)

	// The premise. Before registration the task is not a task, and the file below
	// is a file naming something that does not exist — which is what every author
	// got, and what makes the assertions after registration mean something.
	_, unregistered := flowstatev1.LookupTask("example.greet")
	require.False(t, unregistered,
		"`example.greet` is already in the default registry before this test registered it, "+
			"so nothing below distinguishes a working seam from a task that was always there")

	before, err := flowfile.ValidateSource([]byte(pluginWorkflow))
	require.NoError(t, err)
	require.NotEmpty(t, before,
		"the validator accepted a step naming a task no registry holds")

	require.Contains(t, diagnosticText(before), "example.greet",
		"the diagnostic does not name the task the author wrote, so it cannot teach them anything")

	// The seam. One call, against the registry the engine reads.
	require.NoError(t, host.Register(flowstatev1.DefaultRegistry(), nil))

	t.Run("the validator accepts it", func(t *testing.T) {
		diags, err := flowfile.ValidateSource([]byte(pluginWorkflow))
		require.NoError(t, err)
		assert.Empty(t, diags,
			"a plugin's task is registered and `flow validate` still refuses a file that uses it")
	})

	t.Run("its schema is checked like a built-in's", func(t *testing.T) {
		// The claim ARCHITECTURE.md makes about plugins is that tooling cannot
		// tell them apart from built-ins, and this is where that becomes true or
		// does not: the descriptors the plugin shipped are what these diagnostics
		// come from.
		unknown, err := flowfile.ValidateSource([]byte(strings.Replace(
			pluginWorkflow, "greeting: Hello", "greetng: Hello", 1)))
		require.NoError(t, err)
		assert.NotEmpty(t, unknown,
			"an input the plugin does not declare was accepted, so a typo runs and does nothing")

		// A type, not just a name. The field is `string` in a schema this build
		// has never compiled, and the only way the validator knows that is the
		// descriptor set the plugin sent over its socket.
		wrongType, err := flowfile.ValidateSource([]byte(strings.Replace(
			pluginWorkflow, "greeting: Hello", "greeting: 42", 1)))
		require.NoError(t, err)
		assert.NotEmpty(t, wrongType,
			"a number was accepted for an input the plugin declares as a string")
	})

	t.Run("a later host is not confused by the first one's registration", func(t *testing.T) {
		// An earlier shape of `bind` asked the mutable registry whether a name
		// was a built-in, so once one host had registered, the next host in the
		// same process was refused over a conflict that did not exist — found by
		// this file breaking every other test in the package. The dotted name
		// now makes the collision unrepresentable, and this holds the property
		// the hard way: a second host after a global registration.
		second := exampleHost(t)

		_, ok := second.Lookup("example")
		assert.True(t, ok,
			"a second host refused to launch a plugin because an earlier host had "+
				"registered its tasks")
	})

	t.Run("a plugin cannot spell a built-in's name", func(t *testing.T) {
		// The dot is what enforces it: every plugin task carries one, no
		// built-in does, so installing a plugin cannot change what an existing
		// workflow's `http:` step does — structurally, rather than by a check
		// that could misfire.
		assert.True(t, flowstatev1.IsBuiltinTask("http"))
		assert.True(t, flowstatev1.IsBuiltinTask("log"))
		assert.False(t, flowstatev1.IsBuiltinTask("example.greet"))

		def, ok := flowstatev1.LookupTask("example.greet")
		require.True(t, ok)
		assert.Contains(t, def.Name, ".",
			"a registered plugin task carries no dot, so nothing structurally "+
				"separates it from the built-in namespace")
	})

	t.Run("the run reaches the plugin process", func(t *testing.T) {
		workflow, _, err := flowfile.Parse([]byte(pluginWorkflow))
		require.NoError(t, err)

		outputs, err := flowstatev1.Run(t.Context(), workflow)
		require.NoError(t, err, "a workflow naming a plugin task did not run")

		// The value came out of another process, through the socket, and back
		// into a step output a later step read. Asserting on the greeting rather
		// than on presence: an empty output would satisfy a nil check while
		// proving the request never arrived.
		message := outputs.GetStepValues()["greet"].GetNamedValues()["message"]
		assert.Equal(t, "Hello, world!", message.GetLiteral().GetStringValue(),
			"the step ran and did not produce what the plugin computes")
	})
}

// diagnosticText joins diagnostics into one string to assert against.
func diagnosticText(diags flowfile.Diagnostics) string {
	var b strings.Builder
	for _, d := range diags {
		b.WriteString(d.Message)
		b.WriteString("\n")
	}

	return b.String()
}
