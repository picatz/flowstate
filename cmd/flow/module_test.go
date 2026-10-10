package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// moduleFile is a module in canonical form, so `flow fmt --check` accepts it as
// written.
const moduleFile = `edition: v2026.4
name: ids
types:
  Uuid:
    type: string
    must: isUuid(this)
errors:
  NotFound:
    description: The customer does not exist.
functions:
  isUuid:
    params:
      s: string
    returns: bool
    body: ${s.matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")}
`

func writeModuleFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ids.yaml")
	require.NoError(t, os.WriteFile(path, []byte(moduleFile), 0o600))
	return path
}

// TestAModuleIsAcceptedByTheAuthoringCommands states the positive half: a module
// validates, formats to itself and lints clean, as a first-class file.
func TestAModuleIsAcceptedByTheAuthoringCommands(t *testing.T) {
	t.Parallel()

	path := writeModuleFile(t)
	for _, args := range [][]string{
		{"validate", path},
		{"fmt", "--check", path},
		{"lint", "--strict", path},
		{"validate", filepath.Dir(path)},
		{"fmt", "--check", filepath.Dir(path)},
	} {
		res := runFlow(t, args...)
		assert.NoError(t, res.Err, "%v: %s%s", args, res.Stdout, res.Stderr)
	}
}

// TestAModuleIsRefusedWhereAWorkflowWouldRun is the negative half: every command
// that would execute or compile the file for submission names it a module and
// does not run it.
func TestAModuleIsRefusedWhereAWorkflowWouldRun(t *testing.T) {
	t.Parallel()

	path := writeModuleFile(t)
	for _, args := range [][]string{
		{"run", "local", path},
		{"compile", path},
		{"run", path},
	} {
		res := runFlow(t, args...)
		require.Error(t, res.Err, "%v ran a module", args)
		assert.Contains(t, res.Stdout+res.Stderr+res.Err.Error(),
			"is a module (no steps); import it with use:, don't run it", "%v", args)
	}
}

// TestTheMCPRunToolRefusesAModule covers the one entry that takes source
// instead of a path.
func TestTheMCPRunToolRefusesAModule(t *testing.T) {
	t.Parallel()

	_, err := parseFlowfileSource([]byte(moduleFile))
	require.Error(t, err)
	assert.ErrorIs(t, err, v1.ErrModule)
}

// shippedModules finds every module the examples corpus ships, by what a file is
// rather than by where it sits: a Flowfile that compiles and has no steps
// ([v1.IsModule]). Test suites and files that are not Flowfiles at all fall out of
// [flowfile.LooksLikeFlowfile], the filter the commands' own directory walks use.
//
// A file named `workflow.yaml` that does not compile is left to the harnesses that
// own it: the plugin examples name tasks the built-in registry lacks, and are not
// modules. A module cannot hide among them, because the second return is every
// `workflow.yaml` that parses as one.
func shippedModules(t *testing.T) (modules, misnamed []string) {
	t.Helper()

	err := filepath.WalkDir(filepath.Join("..", "..", "examples"), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml") {
			return err
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !flowfile.LooksLikeFlowfile(source) || flowfile.LooksLikeFlowfileTest(source) {
			return nil
		}
		wf, _, err := flowfile.ParseFile(path)
		if err != nil || !v1.IsModule(wf) {
			return nil //nolint:nilerr // not a module; the harness that owns the file reports it
		}
		if d.Name() == "workflow.yaml" {
			misnamed = append(misnamed, path)
			return nil
		}
		modules = append(modules, path)
		return nil
	})
	require.NoError(t, err)

	return modules, misnamed
}

// TestEveryShippedModuleIsCheckedAndNeverRun holds the examples corpus to the one
// rule a module file needs from it. The run harnesses (`TestEveryOfflineExampleRuns`,
// `TestEveryNetworkedExampleRuns`, `TestEveryExampleRunsDurably`) execute each
// `examples/*/workflow.yaml`, and a steps-less file cannot run, so a module is never
// named that; the commands CI points at the whole directory (`flow fix --check`,
// `flow lint --strict`, `flow fmt --check`, `flow validate`) take it as a file to
// check, and the commands that would execute it refuse it by name.
func TestEveryShippedModuleIsCheckedAndNeverRun(t *testing.T) {
	t.Parallel()

	modules, misnamed := shippedModules(t)
	assert.Empty(t, misnamed,
		"a module named workflow.yaml is picked up by every run harness, which cannot run it; name it for what it declares")

	// The starter library is the floor: a walk that finds none of it is a walk that
	// stopped looking, and every claim below would hold of nothing.
	for _, want := range []string{"ids.yaml", "numbers.yaml", "errors.yaml"} {
		assert.Contains(t, modules, filepath.Join("..", "..", "examples", "lib", want))
	}
	assert.Contains(t, modules, filepath.Join("..", "..", "examples", "use-modules", "lib", "ids.yaml"))

	for _, path := range modules {
		t.Run(filepath.ToSlash(path), func(t *testing.T) {
			t.Parallel()

			for _, args := range [][]string{
				{"validate", path},
				{"fmt", "--check", path},
				{"lint", "--strict", path},
			} {
				res := runFlow(t, args...)
				assert.NoError(t, res.Err, "%v: %s%s", args, res.Stdout, res.Stderr)
			}

			res := runFlow(t, "run", "local", path)
			require.Error(t, res.Err, "a module ran")
			assert.Contains(t, res.Stdout+res.Stderr+res.Err.Error(),
				"is a module (no steps); import it with use:, don't run it")
		})
	}
}
