package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// #710's offline half, from the outside: the same questions
// [TestValidateAcceptsAPluginTaskGivenThePluginDir] asks with the plugin
// binaries present, asked with the binaries *deleted* and a document in their
// place.
//
// Every test here that uses a catalog builds it with `flow plugins --output
// json` and then removes the directory it was built from, which is the whole
// claim: a browser authoring surface, a server-side Validate RPC and a CI
// runner with no plugins installed all need the answer to come from the
// document alone, and a test that leaves the binaries on disk cannot tell the
// two sources apart.

// pluginCatalogFor writes a catalog for the example plugin and returns its
// path, with no plugin binary left anywhere the verb under test could find.
//
// The plugin is copied out of the shared build directory into one this test
// owns, precisely so it can be removed afterwards: [buildExamplePluginDir] is
// built once for the whole process and every other subprocess test in this
// package still needs it.
func pluginCatalogFor(t *testing.T, bin string) string {
	t.Helper()

	source := buildExamplePluginDir(t)

	dir := t.TempDir()
	binary := filepath.Join(dir, plugin.BinaryPrefix+"example")

	data, err := os.ReadFile(filepath.Join(source, plugin.BinaryPrefix+"example"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(binary, data, 0o700))

	// The writer half, and the only writer there is: `flow plugins --output
	// json` emits the catalog this reads back, so the document under test is
	// the shipped one rather than a fixture written by hand to match.
	res := runFlowBinary(t, bin, "plugins", "--plugin-dir", dir, "--output", "json")
	require.NoError(t, res.Err, "writing the catalog:\n%s", res.Output())
	require.Contains(t, res.Stdout, "example.greet",
		"the catalog `flow plugins -o json` wrote does not carry the plugin's task:\n%s", res.Stdout)

	path := filepath.Join(t.TempDir(), "plugins.lock.json")
	require.NoError(t, os.WriteFile(path, []byte(res.Stdout), 0o600))

	// And the binaries go away. Everything below has only the document.
	require.NoError(t, os.RemoveAll(dir))

	return path
}

// TestTheOfflineVerbsTakeThePluginCatalogFlag is the flag half, and it asserts
// both directions.
//
// The verbs that read a task definition without ever running it take the flag.
// The verbs that *execute* must not, and that is the more interesting half: a
// definition rebuilt from a catalog carries a function that refuses to run
// ([plugin.ErrCatalogOnly]), so a worker registering one would accept a step it
// can only ever fail. A flag present there would be a way to configure that.
func TestTheOfflineVerbsTakeThePluginCatalogFlag(t *testing.T) {
	t.Parallel()

	find := func(t *testing.T, path ...string) *cobra.Command {
		t.Helper()

		commands := newRootCommand().Commands()
		var cmd *cobra.Command
		for _, name := range path {
			cmd = nil
			for _, c := range commands {
				if c.Name() == name {
					cmd = c

					break
				}
			}
			require.NotNil(t, cmd, "there is no %s command", strings.Join(path, " "))
			commands = cmd.Commands()
		}

		return cmd
	}

	for _, verb := range [][]string{{"validate"}, {"tasks"}, {"fix"}, {"compile"}, {"schedule", "create"}} {
		t.Run(strings.Join(verb, " "), func(t *testing.T) {
			t.Parallel()

			assert.NotNil(t, find(t, verb...).Flags().Lookup(pluginCatalogFlag),
				"`flow %s` cannot be told what a plugin provides without launching one", strings.Join(verb, " "))
		})
	}

	for _, verb := range [][]string{{"worker"}, {"run", "local"}, {"task", "run"}, {"dap"}} {
		t.Run("not on "+strings.Join(verb, " "), func(t *testing.T) {
			t.Parallel()

			assert.Nil(t, find(t, verb...).Flags().Lookup(pluginCatalogFlag),
				"`flow %s` takes --%s, and a task rebuilt from a catalog cannot execute: "+
					"that flag configures a process to accept steps it can only fail",
				strings.Join(verb, " "), pluginCatalogFlag)
		})
	}
}

// TestValidateAcceptsAPluginTaskGivenASavedCatalog is #710's first acceptance
// clause through the mechanism that requires no process launch — the third
// clause — in one test, because each is only meaningful with the other.
//
// The plugin binary is deleted before `flow validate` runs. A pass here can
// therefore only have come from the document.
func TestValidateAcceptsAPluginTaskGivenASavedCatalog(t *testing.T) {
	bin := buildFlowBinary(t)
	catalog := pluginCatalogFor(t, bin)

	// Without: the installation question, unchanged. Whether a plugin is
	// installed is a deployment's decision, and with neither flag this process
	// has not been told.
	output, err := runFlowCapturing(t, bin, "validate", exampleGreetWorkflow)
	require.Error(t, err, "a file naming an unregistered plugin task validated clean:\n%s", output)
	assert.Contains(t, output, `no plugin task "example.greet" is registered here`,
		"the answer with no catalog is not the installation-question diagnostic:\n%s", output)

	// With: the same file, checked against a document, on a machine with no
	// plugin binary on it at all.
	output, err = runFlowCapturing(t, bin, "validate", "--"+pluginCatalogFlag, catalog, exampleGreetWorkflow)
	assert.NoError(t, err, "the shipped plugin example does not validate against its own catalog:\n%s", output)
	assert.Contains(t, output, "ok", "validate said nothing about the file it checked:\n%s", output)
}

// TestACatalogChecksAPluginTasksInputsAgainstItsDescriptors is #710's second
// acceptance clause, and the reason the catalog had to carry descriptors
// (#854): a misspelled input fails at the author's terminal, with no plugin on
// the machine.
//
// The same misspelling [TestValidateChecksAPluginTasksInputsAgainstItsDescriptors]
// makes with the plugin launched, so the two mechanisms are asserted to give
// the same answer to the same file rather than each being asserted alone.
func TestACatalogChecksAPluginTasksInputsAgainstItsDescriptors(t *testing.T) {
	bin := buildFlowBinary(t)
	catalog := pluginCatalogFor(t, bin)

	path := filepath.Join(t.TempDir(), "typo.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: greet-with-a-typo
steps:
  - id: hi
    example.greet:
      nmae: world
`), 0o600))

	output, err := runFlowCapturing(t, bin, "validate", "--"+pluginCatalogFlag, catalog, path)
	require.Error(t, err, "a misspelled input to a plugin task validated clean against its catalog:\n%s", output)
	assert.Contains(t, output, "nmae",
		"the diagnostic does not name the input that was misspelled:\n%s", output)
	assert.Contains(t, output, "name",
		"the diagnostic does not offer the input the catalog's descriptor declares:\n%s", output)
}

// TestACatalogAnswersAPluginsRequirementBlock is the other thing a catalog is,
// beyond a set of task schemas: it records which plugins at which versions,
// which is what a file's `plugins:` block resolves against.
//
// The launched form of this is
// [TestValidateChecksPluginRequirementsAgainstTheCatalog]. Asserting it again
// here is not duplication: the catalog reaching [validatePluginRequirements] is
// a separate wire from the task definitions reaching the registry, and a
// version floor that resolved against a launched catalog and silently passed
// against a saved one is the same false green in a new place.
func TestACatalogAnswersAPluginsRequirementBlock(t *testing.T) {
	bin := buildFlowBinary(t)
	catalog := pluginCatalogFor(t, bin)

	dir := t.TempDir()

	ok := filepath.Join(dir, "fine.yaml")
	require.NoError(t, os.WriteFile(ok, []byte(`edition: v2026.3
name: needs-the-plugin-it-has
plugins:
  example: v0.1.0
steps:
  - id: hi
    example.greet:
      name: world
      greeting: Hello
`), 0o600))

	output, err := runFlowCapturing(t, bin, "validate", "--"+pluginCatalogFlag, catalog, ok)
	require.NoError(t, err, "a requirement the catalog satisfies was refused:\n%s", output)

	tooNew := filepath.Join(dir, "too-new.yaml")
	require.NoError(t, os.WriteFile(tooNew, []byte(`edition: v2026.3
name: needs-a-newer-plugin
plugins:
  example: v99.0.0
steps:
  - id: hi
    example.greet:
      name: world
      greeting: Hello
`), 0o600))

	output, err = runFlowCapturing(t, bin, "validate", "--"+pluginCatalogFlag, catalog, tooNew)
	require.Error(t, err,
		"a file requiring a plugin version the catalog does not carry validated clean, "+
			"though both drivers refuse it:\n%s", output)
	assert.Contains(t, output, "example",
		"the diagnostic does not name the plugin whose version did not resolve:\n%s", output)
}

// TestTasksListsWhatACatalogHolds is the listing verb's half. A catalog *is* a
// listing, and the provenance lines describe the machine the document was
// written on, which is what it records.
func TestTasksListsWhatACatalogHolds(t *testing.T) {
	bin := buildFlowBinary(t)
	catalog := pluginCatalogFor(t, bin)

	output, err := runFlowCapturing(t, bin, "tasks", "--"+pluginCatalogFlag, catalog)
	require.NoError(t, err, output)

	assert.Contains(t, output, "example.greet",
		"a task the catalog carries is missing from the listing:\n%s", output)
	assert.Contains(t, output, "from the example plugin",
		"the listing does not say which plugin the task came from:\n%s", output)

	// And the detail page, built from descriptors this binary never compiled
	// and no process on this machine served.
	output, err = runFlowCapturing(t, bin, "tasks", "example.greet", "--"+pluginCatalogFlag, catalog)
	require.NoError(t, err, output)
	assert.Contains(t, output, "Provided by the example plugin",
		"the detail page does not say where the task came from:\n%s", output)
}

// TestACatalogAndAPluginDirAreRefusedTogether is the both-flags decision.
//
// Two sources of one fact, and nothing here can tell which the caller meant:
// merging them would have to decide what happens when the document and the
// binaries disagree about a task's schema, and every answer to that is a
// deployment's answer being invented by an authoring verb. So the command line
// says which source it means.
//
// Asserted as a *usage* error on every verb that takes both, because nothing
// ran: docs/CLI.md's three-value exit status spells that 2. It is asserted per
// verb rather than once against [pluginFlagsOf] for the reason
// [TestAPinnedPluginWithNowhereToLookIsRefused] gives — the claim is that no
// command surface reaches its work first.
func TestACatalogAndAPluginDirAreRefusedTogether(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	path := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: fine
steps:
  - id: hi
    log:
      message: hello
`), 0o600))

	// A file that would load, so the refusal is about the pair of flags rather
	// than about the document.
	catalog := filepath.Join(dir, "plugins.lock.json")
	require.NoError(t, os.WriteFile(catalog, []byte(`{"claimsSchemaVersion": 1}`), 0o600))

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"validate", []string{"validate", "--" + pluginCatalogFlag, catalog, "--plugin-dir", dir, path}},
		{"tasks", []string{"tasks", "--" + pluginCatalogFlag, catalog, "--plugin-dir", dir}},
		{"fix", []string{"fix", "--check", "--" + pluginCatalogFlag, catalog, "--plugin-dir", dir, path}},
		{"compile", []string{"compile", "--" + pluginCatalogFlag, catalog, "--plugin-dir", dir, path}},
		{"a pinned plugin", []string{"validate", "--" + pluginCatalogFlag, catalog, "--plugin", "example", path}},
	} {
		args := tc.args
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// In process, as the sibling refusal test is: this happens before
			// anything is launched or read, so there is no registry to poison.
			res := runFlow(t, args...)
			require.Error(t, res.Err,
				"`flow %s` accepted two sources for what a plugin provides:\n%s",
				strings.Join(args, " "), res.Output())

			assert.Contains(t, res.Output(), "--"+pluginCatalogFlag,
				"the refusal does not name the catalog flag:\n%s", res.Output())
			assert.Equal(t, exitCodeUsage, res.ExitCode,
				"a command line naming two sources of one fact was not reported as an "+
					"invocation mistake:\n%s", res.Output())

			// And the sentence is about the command line rather than about a
			// plugin that would not start: nothing was launched, so an account
			// of a failed launch would be a description of something that never
			// happened.
			assert.NotContains(t, res.Output(), "would not start",
				"a refusal made before anything launched is reported as a launch failure:\n%s",
				res.Output())
		})
	}
}

// TestValidateRefusesACatalogItCannotRead is the fail-closed half, and it is
// the same decision #835 made for a plugin that will not launch: nothing is
// checked against a half-read catalog.
//
// The tempting alternative — carry on with the registry there is — reports
// every task the catalog was carrying as an unknown task, which is a diagnostic
// about the *file* drawn from something that went wrong with a *document*, and
// false. So both halves are asserted: the catalog is named, and the
// unknown-task sentence is absent.
func TestValidateRefusesACatalogItCannotRead(t *testing.T) {
	bin := buildFlowBinary(t)

	dir := t.TempDir()

	notJSON := filepath.Join(dir, "not-a-catalog.json")
	require.NoError(t, os.WriteFile(notJSON, []byte("{\"nope\": 1}\n"), 0o600))

	// A catalog whose claim fields this build cannot read, which is the
	// fail-open direction plugin.ErrCatalogClaims exists to close: proto3 gives
	// those fields no presence, so a version this build does not know decodes
	// as a task that needs no scope and takes no secret.
	wrongVersion := filepath.Join(dir, "from-the-future.json")
	require.NoError(t, os.WriteFile(wrongVersion, []byte(`{"claimsSchemaVersion": 9999}`), 0o600))

	// A name a host could not have produced. Every task a launch registers is
	// qualified with its plugin's name, so a catalog naming a task `http` is a
	// document asking to be registered over a built-in.
	shadowing := filepath.Join(dir, "shadowing.json")
	require.NoError(t, os.WriteFile(shadowing, []byte(
		`{"claimsSchemaVersion": 1, "plugins": [{"name": "example", "tasks": [{"name": "http"}]}]}`), 0o600))

	for _, path := range []string{
		filepath.Join(dir, "absent.json"),
		notJSON,
		wrongVersion,
		shadowing,
		dir, // a directory has no size a bound could be checked against
	} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			output, err := runFlowCapturing(t, bin, "validate", "--"+pluginCatalogFlag, path, exampleGreetWorkflow)
			require.Error(t, err, "a catalog that could not be read did not fail the command:\n%s", output)

			assert.Contains(t, output, filepath.Base(path),
				"the failure does not name the catalog that could not be read:\n%s", output)
			assert.NotContains(t, output, "no plugin task",
				"a catalog that failed to load was reported as the file's tasks being unknown:\n%s", output)
		})
	}
}

// TestValidateRefusesACatalogThatDefinesOneTaskTwice is the #863 review's
// second finding at the surface an author uses.
//
// An edited catalog can list one qualified task twice, and a registry keeps one
// definition per name — so without a refusal, which definition this file is
// checked against is decided by the order the lines appear in. A live host
// cannot produce such a document: a manifest providing a task twice is refused
// at launch, and the host's qualification makes a cross-plugin collision
// unreachable.
//
// What is asserted is the whole rule: the command fails, both plugin entries
// are named so an author knows which two to look at, and *nothing was
// registered* — which shows as the plugin-free installation question rather
// than as the file passing against whichever definition won.
func TestValidateRefusesACatalogThatDefinesOneTaskTwice(t *testing.T) {
	bin := buildFlowBinary(t)
	catalog := pluginCatalogFor(t, bin)

	saved, err := os.ReadFile(catalog)
	require.NoError(t, err)

	// The document's own plugin entry, duplicated: two entries named "example",
	// each listing example.greet. Written by editing the catalog `flow plugins`
	// produced, because that is how one of these comes to exist.
	var doc map[string]any
	require.NoError(t, json.Unmarshal(saved, &doc))

	plugins, ok := doc["plugins"].([]any)
	require.True(t, ok && len(plugins) == 1, "the catalog does not hold exactly one plugin: %v", doc["plugins"])
	doc["plugins"] = []any{plugins[0], plugins[0]}

	edited, err := json.Marshal(doc)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "doubled.json")
	require.NoError(t, os.WriteFile(path, edited, 0o600))

	output, err := runFlowCapturing(t, bin, "validate", "--"+pluginCatalogFlag, path, exampleGreetWorkflow)
	require.Error(t, err, "a catalog defining one task twice was accepted:\n%s", output)

	assert.Contains(t, output, "example.greet",
		"the refusal does not name the task that was defined twice:\n%s", output)
	assert.Equal(t, 2, strings.Count(output, `"example"`),
		"the refusal does not name both plugin entries that defined it:\n%s", output)
	assert.NotContains(t, output, "no plugin task",
		"a catalog that failed to load was reported as the file's tasks being unknown, "+
			"which means definitions from it reached the registry:\n%s", output)
}

// TestFixWritesNothingWhenACatalogWillNotLoad is that decision on the one verb
// that changes files, where getting it wrong costs more than a wrong message.
//
// The load happens before a file is read, so what this asserts is the bytes on
// disk — the standard every other `flow fix` test in this package is held to,
// and the same one [TestFixWritesNothingWhenAPluginWillNotStart] applies to the
// launching form.
func TestFixWritesNothingWhenACatalogWillNotLoad(t *testing.T) {
	bin := buildFlowBinary(t)

	catalog := filepath.Join(t.TempDir(), "not-a-catalog.json")
	require.NoError(t, os.WriteFile(catalog, []byte("this is not json at all\n"), 0o600))

	// A file `flow fix` has real work to do on, so "unchanged" is a claim about
	// the rewriter having been stopped rather than about there being nothing to
	// write.
	path := filepath.Join(t.TempDir(), "old.yaml")
	require.NoError(t, os.WriteFile(path, []byte(oldStyleGreeter), 0o600))

	output, err := runFlowCapturing(t, bin, "fix", "--"+pluginCatalogFlag, catalog, path)
	require.Error(t, err, "a catalog that would not load did not stop the rewrite:\n%s", output)
	assert.Contains(t, output, filepath.Base(catalog),
		"the failure does not name the catalog that could not be read:\n%s", output)

	after, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	assert.Equal(t, oldStyleGreeter, string(after),
		"a file was rewritten by a run that could not read its catalog")
}

// TestACatalogLosesToNothingItWasNotPointedAt is the absent direction, and it
// is what keeps the default behaviour the one every invocation in the tree has
// today: with no --plugin-catalog and no --plugin-dir, a step naming a plugin
// task still gets the installation question, and no file on disk is read.
//
// Written with a catalog sitting in the working directory under the name the
// issue's own example uses, because "nothing is read" is otherwise a claim
// about an absence that no test would notice breaking.
func TestACatalogLosesToNothingItWasNotPointedAt(t *testing.T) {
	bin := buildFlowBinary(t)
	catalog := pluginCatalogFor(t, bin)

	dir := t.TempDir()

	saved, err := os.ReadFile(catalog)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "plugins.lock.json"), saved, 0o600))

	path := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: greet
steps:
  - id: hi
    example.greet:
      name: world
      greeting: Hello
`), 0o600))

	// The command's working directory is the one holding the catalog, which is
	// the only way "it was not read" is a claim rather than a tautology.
	command := flowBinaryCommand(bin, "validate", path)
	command.Dir = dir

	res := runFlowBinaryWith(t, command)
	require.Error(t, res.Err,
		"a catalog nobody named was consulted, so this verb reads a file the caller did not ask it to:\n%s",
		res.Output())
	assert.Contains(t, res.Output(), `no plugin task "example.greet" is registered here`,
		"the answer with no flag is not the installation-question diagnostic:\n%s", res.Output())
}
