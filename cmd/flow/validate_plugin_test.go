package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// #724 and #710, from the outside: `flow validate` and `flow tasks` were the two
// surfaces in the tree that read a task name and could never be told what a
// plugin provides, so a file naming `example.greet` read as a mistake to the one
// verb CLAUDE.md's landing rule names, and the listing that documents itself as
// "the catalog, which plugins already extend" listed the built-ins.
//
// Every test here that launches a plugin drives the built `flow` binary as a
// subprocess rather than calling the functions in this package, and that is not
// incidental. Registering
// a host into [v1.DefaultRegistry] is a one-way door — there is no Unregister —
// so an in-process test that launches the example plugin puts `example.greet`
// into this test binary's registry for the rest of the run, where the next test
// asserting an absence finds it. A subprocess gets a registry of its own, which
// is also the only way to observe the thing being asserted: the exit status and
// the bytes a person or a CI job actually reads.

// runFlowCapturing runs the built binary's validate verb and returns what it
// wrote and whether it succeeded.
func runFlowCapturing(t *testing.T, bin string, args ...string) (output string, err error) {
	t.Helper()

	cmd := runFlowBin(bin, args...)
	out, err := cmd.CombinedOutput()

	return string(out), err
}

// TestTheAuthoringVerbsTakeThePluginFlags is the flag half, mirroring
// [TestTheMCPServerTakesThePluginFlags] and [TestRunLocalTakesThePluginFlags]:
// these were the last commands in the tree that read a task definition without
// them.
//
// `fix` is in the list for a reason of its own rather than for symmetry — see
// [newFixCommand], where the walk's own branches on a task's declared
// capabilities are what a plugin's task cannot answer unless the plugin is up.
func TestTheAuthoringVerbsTakeThePluginFlags(t *testing.T) {
	t.Parallel()

	for _, verb := range []string{"validate", "tasks", "fix"} {
		t.Run(verb, func(t *testing.T) {
			t.Parallel()

			var cmd *cobra.Command
			for _, c := range newRootCommand().Commands() {
				if c.Name() == verb {
					cmd = c

					break
				}
			}
			require.NotNil(t, cmd, "there is no %s command", verb)

			for _, name := range []string{"plugin-dir", "plugin", "plugin-scheme", "allow-insecure-plugin-dir"} {
				assert.NotNil(t, cmd.Flags().Lookup(name),
					"`flow %s` does not take --%s, so it cannot be told what a plugin provides", verb, name)
			}
		})
	}
}

// TestAPinnedPluginWithNowhereToLookIsRefused is the review finding on #835,
// and it is the "silently half-configured" failure this whole path exists to
// prevent, arriving through the one flag that promises the opposite.
//
// `--plugin NAME` documents itself as "a name with no binary is an error",
// because a deployment that pinned a set expects that set. The host keeps that
// promise and the host never ran: [pluginFlags.configured] asks only whether
// there are directories, so `--plugin example` with no --plugin-dir and no
// $FLOWSTATE_PLUGIN_DIR took [startPlugins]'s early return, launched nothing,
// and said nothing. On `flow validate` and `flow tasks` that is the unknown-task
// answer this branch set out to stop giving; on `flow fix` it is worse, because
// the rewriter then treats that plugin's tasks as ordinary ones and its walk
// branches on exactly the declarations it no longer has.
//
// Asserted per verb rather than once against [pluginFlagsOf], because what is
// being claimed is that no command surface reaches its work first — and each
// verb is written separately even though they share the seam. It is also
// asserted as a *usage* error: nothing ran, the command line was wrong, and
// docs/CLI.md's three-value exit status says so with 2.
func TestAPinnedPluginWithNowhereToLookIsRefused(t *testing.T) {
	t.Parallel()

	// A real file, so that a verb which got as far as its work would succeed
	// rather than fail for a second reason and look like it had refused.
	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: fine
steps:
  - id: hi
    log:
      message: hello
`), 0o600))

	for _, args := range [][]string{
		{"validate", "--plugin", "example", path},
		{"tasks", "--plugin", "example"},
		{"fix", "--check", "--plugin", "example", path},
		{"plugins", "--plugin", "example"},
	} {
		t.Run(args[0], func(t *testing.T) {
			t.Parallel()

			root := newRootCommand()
			var out, errOut strings.Builder
			root.SetOut(&out)
			root.SetErr(&errOut)
			root.SetArgs(args)

			err := execute(t.Context(), root)
			require.Error(t, err,
				"`flow %s` accepted a pinned plugin with nowhere to look for it, and so "+
					"ran having launched nothing", strings.Join(args, " "))

			assert.Contains(t, err.Error(), "example",
				"the refusal does not name the plugin that was pinned")
			assert.Contains(t, err.Error(), "--plugin-dir",
				"the refusal does not say what to pass instead")
			assert.Equal(t, exitCodeUsage, exitCodeFor(err),
				"a command line asking for two things that do not fit was not "+
					"reported as an invocation mistake")
		})
	}
}

// TestValidateAcceptsAPluginTaskGivenThePluginDir is the acceptance clause of
// both issues: the shipped example, the real plugin binary, the real command,
// and the *exit status*, which is the whole of what a CI job reads.
//
// Both directions in one test, in this order, because each is only meaningful
// against the other. A pass with --plugin-dir proves nothing on its own if the
// file would have passed anyway; a failure without it proves nothing on its own
// if the file is simply broken.
func TestValidateAcceptsAPluginTaskGivenThePluginDir(t *testing.T) {
	bin := buildFlowBinary(t)
	dir := buildExamplePluginDir(t)

	// Without: the installation question, not a spelling one. This is the
	// diagnostic the greet example's README quotes and the behaviour that must
	// not change — whether a plugin is installed is a deployment's decision, so
	// a checker that has not been told about one says what it does not know
	// rather than passing the file silently.
	output, err := runFlowCapturing(t, bin, "validate", exampleGreetWorkflow)
	require.Error(t, err, "a file naming an unregistered plugin task validated clean:\n%s", output)
	assert.Contains(t, output, `no plugin task "example.greet" is registered here`,
		"the plugin-free answer is not the installation-question diagnostic:\n%s", output)

	// With: the same file, checked against the plugin that provides the task,
	// and it is fine — which is what makes the landing rule reachable for a
	// plugin task at all.
	output, err = runFlowCapturing(t, bin, "validate", "--plugin-dir", dir, exampleGreetWorkflow)
	assert.NoError(t, err, "the shipped plugin example still does not validate against its own plugin:\n%s", output)
	assert.Contains(t, output, "ok", "validate said nothing about the file it checked:\n%s", output)
}

// TestValidateChecksAPluginTasksInputsAgainstItsDescriptors is #710's second
// acceptance clause, and the one that makes this worth more than silencing a
// diagnostic: a plugin's task manifest carries the descriptors for its inputs,
// the host reconstructs them, and the validator then holds a plugin task to the
// same standard it holds `http` to. Before this, a typo'd input to a plugin task
// was discovered at the worker.
//
// The misspelling is of an input the example plugin's own descriptor declares,
// so the diagnostic can only come from schema this build has never compiled.
func TestValidateChecksAPluginTasksInputsAgainstItsDescriptors(t *testing.T) {
	bin := buildFlowBinary(t)
	dir := buildExamplePluginDir(t)

	path := filepath.Join(t.TempDir(), "typo.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: greet-with-a-typo
steps:
  - id: hi
    example.greet:
      nmae: world
`), 0o600))

	output, err := runFlowCapturing(t, bin, "validate", "--plugin-dir", dir, path)
	require.Error(t, err, "a misspelled input to a plugin task validated clean:\n%s", output)
	assert.Contains(t, output, "nmae",
		"the diagnostic does not name the input that was misspelled:\n%s", output)
}

// TestValidateReportsAPluginThatWillNotStart is the degraded-mode decision, and
// it is the reason this is not simply `addPluginFlags` and a call.
//
// `flow validate` runs in an editor's neighbourhood and in CI, so the tempting
// answer to a plugin that will not launch is to carry on with the registry
// there is. That answer reports every one of that plugin's tasks as an unknown
// task: a diagnostic about the *file*, drawn from something that went wrong
// with a *process*, and false. CLAUDE.md is explicit that a false diagnostic is
// worse than a missing one, so the failure is reported as what it is and nothing
// is validated.
//
// What is asserted is therefore both halves: the plugin is named, and the
// unknown-task sentence is absent.
func TestValidateReportsAPluginThatWillNotStart(t *testing.T) {
	bin := buildFlowBinary(t)

	// A file discovery accepts and the handshake cannot: it is executable, it is
	// named the way a plugin is named, and it exits without announcing itself.
	// The host's own handshake bound is what makes this quick rather than a
	// ten-second wait, and it is the same bound a worker runs under.
	dir := t.TempDir()
	broken := filepath.Join(dir, plugin.BinaryPrefix+"broken")
	require.NoError(t, os.WriteFile(broken, []byte("#!/bin/sh\nexit 1\n"), 0o700))

	output, err := runFlowCapturing(t, bin, "validate", "--plugin-dir", dir, exampleGreetWorkflow)
	require.Error(t, err, "a plugin that would not start did not fail the command:\n%s", output)

	assert.Contains(t, output, "broken",
		"the failure does not name the plugin that would not start:\n%s", output)
	assert.NotContains(t, output, "no plugin task",
		"a plugin that failed to launch was reported as the file's tasks being unknown:\n%s", output)
}

// TestFixWritesNothingWhenAPluginWillNotStart is the same decision on the one
// verb that changes files, where getting it wrong costs more than a wrong
// message: a rewriter that had already converted half a directory when its
// plugin failed to come up would have rewritten those files against a task set
// the author did not ask for. So the launch happens before a file is read, and
// the bytes on disk are what this asserts — the standard every other `flow fix`
// test in this package holds it to.
func TestFixWritesNothingWhenAPluginWillNotStart(t *testing.T) {
	bin := buildFlowBinary(t)

	plugins := t.TempDir()
	broken := filepath.Join(plugins, plugin.BinaryPrefix+"broken")
	require.NoError(t, os.WriteFile(broken, []byte("#!/bin/sh\nexit 1\n"), 0o700))

	// A file `flow fix` has real work to do on, so "unchanged" is a claim about
	// the rewriter having been stopped rather than about there being nothing to
	// write.
	path := filepath.Join(t.TempDir(), "old.yaml")
	require.NoError(t, os.WriteFile(path, []byte(oldStyleGreeter), 0o600))

	output, err := runFlowCapturing(t, bin, "fix", "--plugin-dir", plugins, path)
	require.Error(t, err, "a plugin that would not start did not stop the rewrite:\n%s", output)
	assert.Contains(t, output, "broken",
		"the failure does not name the plugin that would not start:\n%s", output)

	after, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	assert.Equal(t, oldStyleGreeter, string(after),
		"a file was rewritten by a run that could not bring its plugins up")
}

// TestTasksListsWhatAPluginProvidesWithProvenance is #724's other half. The
// index has to hold the task, and the reader has to be able to tell whose code
// it is: which plugin, which build, which file on this machine.
func TestTasksListsWhatAPluginProvidesWithProvenance(t *testing.T) {
	bin := buildFlowBinary(t)
	dir := buildExamplePluginDir(t)

	// Without --plugin-dir, this build's own tasks and nothing else — the
	// listing is honest about being a smaller catalog rather than silently
	// implying it is the whole of one.
	output, err := runFlowCapturing(t, bin, "tasks")
	require.NoError(t, err, output)
	assert.NotContains(t, output, "example.greet",
		"a plugin's task is listed by a process that launched no plugins:\n%s", output)

	output, err = runFlowCapturing(t, bin, "tasks", "--plugin-dir", dir)
	require.NoError(t, err, output)

	assert.Contains(t, output, "example.greet",
		"a task the plugin host registered is missing from the listing:\n%s", output)
	assert.Contains(t, output, "from the example plugin",
		"the listing does not say which plugin the task came from:\n%s", output)
	assert.Contains(t, output, filepath.Join(dir, plugin.BinaryPrefix+"example"),
		"the listing does not say which binary provided the task:\n%s", output)

	// And the detail page for one of them, which is the page somebody reads
	// while deciding to write the step.
	output, err = runFlowCapturing(t, bin, "tasks", "example.greet", "--plugin-dir", dir)
	require.NoError(t, err, output)
	assert.Contains(t, output, "Provided by the example plugin",
		"the detail page does not say where the task came from:\n%s", output)

	// The machine shape carries it too, by carrying the task at all: `flow tasks
	// -o json` is what an agent reads, and a catalog that omitted the plugin's
	// tasks would be the same blindness in the other format.
	output, err = runFlowCapturing(t, bin, "tasks", "--plugin-dir", dir, "-o", "json")
	require.NoError(t, err, output)
	assert.Contains(t, output, `"example.greet"`,
		"the JSON catalog omits the plugin's task:\n%s", output)
}

// TestTasksReportsAPluginThatWillNotStart is the same degraded-mode decision on
// the listing verb, and it fails the same way for a reason worth stating: a
// catalog printed as though it were complete, with the tasks somebody asked to
// see missing from it, is a wrong answer rather than a smaller one.
func TestTasksReportsAPluginThatWillNotStart(t *testing.T) {
	bin := buildFlowBinary(t)

	dir := t.TempDir()
	broken := filepath.Join(dir, plugin.BinaryPrefix+"broken")
	require.NoError(t, os.WriteFile(broken, []byte("#!/bin/sh\nexit 1\n"), 0o700))

	output, err := runFlowCapturing(t, bin, "tasks", "--plugin-dir", dir)
	require.Error(t, err, "a plugin that would not start did not fail the listing:\n%s", output)
	assert.Contains(t, output, "broken",
		"the failure does not name the plugin that would not start:\n%s", output)
}
