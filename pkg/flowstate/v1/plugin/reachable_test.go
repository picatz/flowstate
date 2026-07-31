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
// Unregister, so a second host opening afterwards would find `example_greet`
// present and refuse it as a built-in — [Host.bind] checks
// [flowstatev1.LookupTask] to stop a plugin shadowing the engine's own tasks, and
// cannot tell a built-in from another host's plugin. That is correct behaviour
// for a worker, which opens one host and holds it until it exits, and it means
// exactly one test in this binary may do the global registration.
//
// Deliberately not parallel, for the same reason.

const pluginWorkflow = `edition: v2026.2
name: plugin-reachable
vars:
  who: world
steps:
  - id: greet
    example_greet:
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
	_, unregistered := flowstatev1.LookupTask("example_greet")
	require.False(t, unregistered,
		"`example_greet` is already in the default registry before this test registered it, "+
			"so nothing below distinguishes a working seam from a task that was always there")

	before, err := flowfile.ValidateSource([]byte(pluginWorkflow))
	require.NoError(t, err)
	require.NotEmpty(t, before,
		"the validator accepted a step naming a task no registry holds")

	require.Contains(t, diagnosticText(before), "example_greet",
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

	t.Run("a later host is not told its task is a built-in", func(t *testing.T) {
		// Found by this file breaking every other test in the package, which is a
		// worse way to find it than it looks: the symptom was five unrelated
		// failures, and the cause was one question asked of the wrong thing.
		//
		// `bind` refuses a plugin task that shadows a built-in, and asked the
		// *registry* — which is mutable, and which the host above had just added
		// to. So the second host to open in a process was told `example_greet` is
		// "a built-in task", which is both false and unfixable by whoever read it.
		// [flowstatev1.IsBuiltinTask] answers from a frozen set instead.
		second := exampleHost(t)

		_, ok := second.Lookup("example")
		assert.True(t, ok,
			"a second host refused to launch a plugin because an earlier host had "+
				"registered its tasks")
	})

	t.Run("a plugin still cannot shadow a real built-in", func(t *testing.T) {
		// The other direction, which the fix above must not have loosened: the
		// refusal exists so that installing a plugin cannot change what an
		// existing workflow's `http:` step does.
		assert.True(t, flowstatev1.IsBuiltinTask("http"))
		assert.True(t, flowstatev1.IsBuiltinTask("log"))
		assert.False(t, flowstatev1.IsBuiltinTask("example_greet"),
			"a plugin's task is reported as a built-in, so a later plugin of the same "+
				"name would be refused with a reason that names the wrong conflict")
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
