package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The house gate for #436, and it is deliberately not a Go test that builds a
// node: it runs the real `flow run local`, over the example Flowfile that ships
// in the tree, against the example plugin compiled from this repository. That
// is the whole of what the issue asked for, because everything short of it was
// already true before. The plugin protocol had tests, the engine had tests,
// and a Flowfile naming a plugin task still could not be rehearsed.
//
// CLAUDE.md states the rule these tests answer to: a capability lands when a
// Flowfile can express it, `flow validate` accepts it, and an example exercises
// it. `examples/plugins/greet/workflow.yaml` is that example, and this is what
// runs it in CI.

// exampleGreetWorkflow is the shipped example, from disk, by the same path the
// README tells a reader to type.
const exampleGreetWorkflow = "../../examples/plugins/greet/workflow.yaml"

// localSecretPolicy writes the auth policy a run needs before any secret
// provider may be read.
//
// Needed here rather than optional, and for a reason that is itself parity: the
// example plugin advertises a secrets backend as well as a task, so bringing it
// up registers a scheme, and a process holding a secret provider with no access
// policy is refused. `flow worker --plugin-dir` over the same directory refuses
// on the same line of the same function ([runtimePolicy], reached from
// workerRuntime), which is the point.
func localSecretPolicy(t *testing.T) string {
	t.Helper()

	// The policy the example ships, not a copy of it: the README tells a
	// person to point at this exact file, so the test that proves the
	// walkthrough works has to consume the same bytes they will.
	path, err := filepath.Abs(filepath.Join("..", "..", "examples", "plugins", "greet", "auth.yaml"))
	require.NoError(t, err)
	require.FileExists(t, path, "the README points at this policy; it has to ship")

	return path
}

// runLocalFile runs `flow run local` over a file already on disk, which is what
// the example tests need and what [runLocal]'s temp-file form cannot give them.
func runLocalFile(t *testing.T, path string, extra ...string) (stdout, stderr string, err error) {
	t.Helper()

	root := newRootCommand()
	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"run", "local", path}, extra...))

	err = execute(t.Context(), root)

	return out.String(), errOut.String(), err
}

// TestRunLocalTakesThePluginFlags is the flag half, mirroring
// [TestTheMCPServerTakesThePluginFlags]: `flow run local` was the last
// execution verb without them, so a Flowfile using a plugin task could be
// validated, run durably and invoked a task at a time through `flow task run`,
// and the rehearsal alone answered that the task did not exist.
func TestRunLocalTakesThePluginFlags(t *testing.T) {
	t.Parallel()

	local, _, err := newRootCommand().Find([]string{"run", "local"})
	require.NoError(t, err)
	require.Equal(t, "local", local.Name())

	for _, name := range []string{"plugin-dir", "plugin", "plugin-scheme", "allow-insecure-plugin-dir"} {
		assert.NotNil(t, local.Flags().Lookup(name),
			"`flow run local` does not take --%s, so a rehearsal cannot be told about a plugin", name)
	}
}

// TestRunLocalExecutesAPluginTaskFromAnExample is the end-to-end gate: the
// shipped example file, the real plugin binary, the real command.
//
// What each assertion is for:
//
//   - `greeting` proves the step dispatched across a process boundary and its
//     answer came back into the run's scope under the step's id.
//   - `length` proves the descriptors travelled: it is an integer in a schema
//     this build has never compiled, and an output the engine could only shape
//     from what the plugin shipped.
//   - `authenticated` proves the secret reached the plugin's task, and the two
//     containment assertions prove the material did not reach the run's output
//     document or the account stream on its way there.
func TestRunLocalExecutesAPluginTaskFromAnExample(t *testing.T) {
	const material = "greet-token-material"

	dir := buildExamplePluginDir(t)
	t.Setenv("FLOWSTATE_SECRET_GREET_TOKEN", material)

	stdout, stderr, err := runLocalFile(t, exampleGreetWorkflow,
		"--plugin-dir", dir,
		"--secret-env", "GREET_TOKEN",
		"--auth-policy", localSecretPolicy(t),
		"--output", "json")
	require.NoError(t, err, stderr)

	decoder := json.NewDecoder(strings.NewReader(stdout))
	decoder.UseNumber()

	var response struct {
		RunOutputs map[string]any `json:"runOutputs"`
	}
	require.NoError(t, decoder.Decode(&response), "stdout is not a run document:\n%s", stdout)

	outputs := response.RunOutputs
	require.Contains(t, outputs, "greeting")
	assert.Equal(t, "Hello, world!", outputs["greeting"])

	// A number, rather than protojson's string-encoded int64: the run document
	// writes a value the way a `jq` reader spells one. What matters is still that
	// the field is a whole number at all, which json.Number carries exactly.
	assert.Equal(t, json.Number("13"), outputs["length"],
		"the plugin's own integer output did not arrive as an integer")
	assert.Equal(t, true, outputs["authenticated"],
		"the plugin task did not receive the secret the Flowfile routed to it")

	assert.NotContains(t, stdout, material, "the secret reached the run's outputs")
	assert.NotContains(t, stderr, material, "the secret reached the account stream")
}

// TestRunLocalRefusesAPluginRequirementItCannotSatisfy is the `plugins:` half:
// the requirement surface was durable-only, so a file pinning a plugin was
// resolved against a deployment catalog at submit and against nothing at all
// locally.
//
// The two refusals are [v1.ResolvePlugins]'s own, which is the same function
// server.go's pinPlugins calls, so this asserts the sentences a server gives
// rather than a local paraphrase of them.
func TestRunLocalRefusesAPluginRequirementItCannotSatisfy(t *testing.T) {
	const requiresExample = `edition: v2026.3
name: needs-a-plugin
plugins:
  example: v%s
steps:
  - id: note
    log:
      message: this step needs no plugin at all
`

	t.Run("not installed", func(t *testing.T) {
		t.Parallel()

		_, stderr, err := runLocal(t, strings.Replace(requiresExample, "v%s", "v0.1.0", 1))
		require.Error(t, err)
		assert.Contains(t, err.Error(),
			`required plugin "example" is not installed on this deployment`)
		assert.Contains(t, err.Error(), "drop it from `plugins:`", stderr)
	})

	t.Run("installed below the floor the file sets", func(t *testing.T) {
		dir := buildExamplePluginDir(t)

		_, stderr, err := runLocalFile(t, writeWorkflow(t, "workflow.yaml", strings.Replace(requiresExample, "v%s", "v0.9.0", 1)),
			"--plugin-dir", dir, "--auth-policy", localSecretPolicy(t))
		require.Error(t, err, stderr)
		assert.Contains(t, err.Error(),
			`plugin "example" is 0.1.0 on this deployment, below the v0.9.0 the file requires`)
	})
}
