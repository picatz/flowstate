package flowtest_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// `testdefaults.yaml` (#1072 slice 3): the fixture every suite in one
// directory shares. The chain is directory → file defaults → entry → row,
// each level filling what the level below did not state.

func writeDirWorkflow(t *testing.T, dir string) {
	t.Helper()

	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: shared
inputs:
  who:
    type: string
steps:
  - id: greet
    log:
      message: ${inputs.who}
outputs: {}
`)
}

// TestADirectoryStatesTheFixtureOnce: two suites beside one testdefaults.yaml
// both inherit its workflow, stub, vars and check — the whole point.
func TestADirectoryStatesTheFixtureOnce(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDirWorkflow(t, dir)
	writeFile(t, filepath.Join(dir, "testdefaults.yaml"), `
vars:
  caller: team-a
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
  check:
    - "!run.failed"
`)
	first := filepath.Join(dir, "one.test.yaml")
	writeFile(t, first, `
tests:
  - name: inherits everything
    inputs: {who: "${vars.caller}"}
    expect:
      ran: [greet]
      check:
        - inputs.who == vars.caller
`)
	second := filepath.Join(dir, "two.test.yaml")
	writeFile(t, second, `
tests:
  - name: the sibling inherits too
    inputs: {who: someone}
    expect:
      ran: [greet]
`)

	for _, path := range []string{first, second} {
		report := flowtest.RunFile(path)
		require.Empty(t, report.GetRefused(), path)
		require.Len(t, report.GetCases(), 1, path)
		assert.True(t, report.GetCases()[0].GetPassed(), "%s: %v / %v",
			path, report.GetCases()[0].GetError(), report.GetCases()[0].GetFailures())
	}
}

// TestAFileBeatsItsDirectory: the one direction, on a var and the workflow at
// once — the file's own values win, and the directory fills the rest.
func TestAFileBeatsItsDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDirWorkflow(t, dir)
	writeFile(t, filepath.Join(dir, "other.yaml"), `
edition: v2026.3
name: own
steps:
  - id: own_step
    log:
      message: hi
outputs: {}
`)
	writeFile(t, filepath.Join(dir, "testdefaults.yaml"), `
vars:
  who: from-directory
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  who: from-file
defaults:
  workflow: ./other.yaml
tests:
  - name: the file's own values win
    expect:
      ran: [own_step]
      check:
        - vars.who == 'from-file'
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	assert.True(t, report.GetCases()[0].GetPassed(),
		"%v / %v", report.GetCases()[0].GetError(), report.GetCases()[0].GetFailures())
}

// TestADirectorysOwnVarsResolveItsOwnDefaults: a testdefaults.yaml may
// reference its own vars in its own defaults, because the fold happens before
// resolution — the issue's sketch, working.
func TestADirectorysOwnVarsResolveItsOwnDefaults(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDirWorkflow(t, dir)
	writeFile(t, filepath.Join(dir, "testdefaults.yaml"), `
vars:
  fallbackWho: shared-default
defaults:
  workflow: ./workflow.yaml
  inputs: {who: "${vars.fallbackWho}"}
  stubs:
    - task: log
      returns: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the inherited input arrived resolved
    expect:
      ran: [greet]
      check:
        - inputs.who == 'shared-default'
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	assert.True(t, report.GetCases()[0].GetPassed(),
		"%v / %v", report.GetCases()[0].GetError(), report.GetCases()[0].GetFailures())
}

// TestNoUpwardWalk: a suite in a subdirectory does not inherit a parent
// directory's file. Two possible sources, both visible in one ls, is the
// bound that keeps "where did this come from" answerable.
func TestNoUpwardWalk(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	writeFile(t, filepath.Join(parent, "testdefaults.yaml"), `
defaults:
  workflow: ./workflow.yaml
`)
	sub := filepath.Join(parent, "sub")
	writeDirWorkflow(t, mkdir(t, sub))
	path := filepath.Join(sub, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: inherits nothing from above
    expect:
      ran: [greet]
`)

	report := flowtest.RunFile(path)
	require.Contains(t, report.GetRefused(), "names no workflow",
		"the parent's workflow default must not reach a subdirectory's suite")
}

// TestATestdefaultsFileDeclaringTestsIsRefused: almost certainly a suite
// saved under the wrong name, refused with the field named.
func TestATestdefaultsFileDeclaringTestsIsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDirWorkflow(t, dir)
	writeFile(t, filepath.Join(dir, "testdefaults.yaml"), `
defaults:
  workflow: ./workflow.yaml
tests:
  - name: does not belong here
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: never runs
    expect:
      ran: [greet]
`)

	report := flowtest.RunFile(path)
	require.Contains(t, report.GetRefused(), "tests")
	require.Contains(t, report.GetRefused(), "testdefaults.yaml")
}

// TestNoDirectoryFileMeansNothingChanges: the overwhelmingly common shape.
func TestNoDirectoryFileMeansNothingChanges(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDirWorkflow(t, dir)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: stands alone
    workflow: ./workflow.yaml
    inputs: {who: someone}
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [greet]
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	assert.True(t, report.GetCases()[0].GetPassed(), "%v / %v",
		report.GetCases()[0].GetError(), report.GetCases()[0].GetFailures())
}

func mkdir(t *testing.T, dir string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	return dir
}
