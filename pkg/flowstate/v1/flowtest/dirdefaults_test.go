package flowtest_test

import (
	"fmt"
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

func TestLoadSourceAtWithDefaultsUsesTheProvidedLiveBytes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "suite.test.yaml")
	suite := []byte("defaults:\n  workflow: ./workflow.yaml\ntests:\n  - name: x\n    expect: {failed: false}\n")

	_, err := flowtest.LoadSourceAtWithDefaults(suite, path, []byte("defaults:\n  stubs:\n    - returns: {}\n"))
	require.Error(t, err)
	var diagnostics *flowtest.Diagnostics
	require.ErrorAs(t, err, &diagnostics)
	require.NotEmpty(t, diagnostics.Problems)
	assert.Equal(t, filepath.Join(dir, flowtest.DirDefaultsName), diagnostics.Problems[0].File)
	assert.Positive(t, diagnostics.Problems[0].Line)
	assert.Contains(t, diagnostics.Problems[0].Message, "names neither a task nor a step")

	_, err = flowtest.LoadSourceAtWithDefaults(suite, path, []byte("defaults:\n  stubs:\n    - task: log\n      returns: {}\n"))
	require.NoError(t, err)
}

func TestLoadDirDefaultsSourceUsesTheStrictDefaultsShape(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), flowtest.DirDefaultsName)
	require.NoError(t, flowtest.LoadDirDefaultsSource([]byte("defaults: {}\n"), path))

	err := flowtest.LoadDirDefaultsSource([]byte("tests: []\n"), path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown field "tests"`)
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

// A var testdefaults.yaml states is printed by every suite in the directory,
// and taint is a fact about one suite's `secrets:`, so a suite whose taint
// closure reaches a directory var is refused rather than trusted to be the
// only reader (#2080). The four tests below are the rule's three shapes and
// its positive control.

// TestASuiteNamingADirectoryVarAsASecretIsRefused is #2080's first leak at
// its source: the suite names the directory's var from `secrets:` directly.
func TestASuiteNamingADirectoryVarAsASecretIsRefused(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-dirseed-5521"

	dir := t.TempDir()
	writeDirWorkflow(t, dir)
	writeFile(t, filepath.Join(dir, flowtest.DirDefaultsName), "vars:\n  token: "+secret+"\n")
	path := filepath.Join(dir, "a.test.yaml")
	writeFile(t, path, `
tests:
  - name: names the directory's var as a secret
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: ${vars.token}
    expect: {failed: false}
`)

	_, err := flowtest.Load(path)
	require.Error(t, err, "a directory var on a path to a secret must refuse the suite (#2080)")
	assert.Contains(t, err.Error(), flowtest.DirDefaultsName)
	assert.Contains(t, err.Error(), "vars.token")
	assert.Contains(t, err.Error(), `secrets["env:TOKEN"] references`, "the refusal names the chain")
	assert.Contains(t, err.Error(), "move vars.token into the suite's own vars: and remove it from testdefaults.yaml", "the refusal names the remedy")
	assert.NotContains(t, err.Error(), secret, "the refusal names a path, never the value")

	// Reported in both documents: at the directory var, where the text is,
	// and at the suite's own `secrets:` entry, so an editor showing only the
	// suite shows why it is refused.
	var diagnostics *flowtest.Diagnostics
	require.ErrorAs(t, err, &diagnostics)
	files := map[string]string{}
	for _, problem := range diagnostics.Problems {
		files[problem.File] = problem.Field
		assert.Positive(t, problem.Line, "%s is positioned", problem.File)
	}
	assert.Contains(t, files, filepath.Join(dir, flowtest.DirDefaultsName))
	assert.Equal(t, "tests[0].secrets.env:TOKEN", files[path], "the suite's copy of the refusal sits at the entry that seeds the path")
}

// TestASuiteAliasOfADirectoryVarNamedAsASecretIsRefused is the transitive
// shape: the suite's own var reads the directory's, and `secrets:` names the
// alias. The closure reaches the directory var backward, so the refusal is
// the same, with the hop in its path.
func TestASuiteAliasOfADirectoryVarNamedAsASecretIsRefused(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-diralias-7730"

	dir := t.TempDir()
	writeDirWorkflow(t, dir)
	writeFile(t, filepath.Join(dir, flowtest.DirDefaultsName), "vars:\n  token: "+secret+"\n")
	path := filepath.Join(dir, "a.test.yaml")
	writeFile(t, path, `
vars:
  alias: ${vars.token}
tests:
  - name: names a local alias of the directory's var as a secret
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: ${vars.alias}
    expect: {failed: false}
`)

	_, err := flowtest.Load(path)
	require.Error(t, err, "an alias does not launder a directory var out of the rule (#2080)")
	assert.Contains(t, err.Error(), flowtest.DirDefaultsName)
	assert.Contains(t, err.Error(), "vars.token → vars.alias", "the refusal walks the alias back to the directory var")
	assert.NotContains(t, err.Error(), secret)
}

// TestANestedDirectoryVarOnASecretPathIsRefusedAtItsRoot is the structured
// shape: a fixture reference names a whole var, so a nested directory leaf
// reaches `secrets:` through a suite var that reads it. Provenance is per
// top-level name, so the refusal and its remedy name the root.
func TestANestedDirectoryVarOnASecretPathIsRefusedAtItsRoot(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-dirnested-1184"

	dir := t.TempDir()
	writeDirWorkflow(t, dir)
	writeFile(t, filepath.Join(dir, flowtest.DirDefaultsName), "vars:\n  request:\n    token: "+secret+"\n")
	path := filepath.Join(dir, "a.test.yaml")
	writeFile(t, path, `
vars:
  token: ${vars.request.token}
tests:
  - name: names a nested directory leaf as a secret
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: ${vars.token}
    expect: {failed: false}
`)

	_, err := flowtest.Load(path)
	require.Error(t, err, "a nested directory leaf on a path to a secret must refuse the suite (#2080)")
	assert.Contains(t, err.Error(), "vars.request.token is stated by "+flowtest.DirDefaultsName)
	assert.Contains(t, err.Error(), "move vars.request into the suite's own vars: and remove it from testdefaults.yaml")
	assert.NotContains(t, err.Error(), secret)
}

// TestASuiteStatingItsOwnSecretVarShadowsTheDirectory is the remedy the
// refusals above name, and the rule's positive control: the suite states
// `token` itself, so the value it withholds lives in the one file that
// withholds it — the directory's `token` is a different, harmless value — and
// the directory's other var, on no secret path, is read as it always was.
func TestASuiteStatingItsOwnSecretVarShadowsTheDirectory(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-dirshadow-6402"

	dir := t.TempDir()
	writeDirWorkflow(t, dir)
	writeFile(t, filepath.Join(dir, flowtest.DirDefaultsName), `
vars:
  token: directory-placeholder
  caller: team-a
`)
	path := filepath.Join(dir, "a.test.yaml")
	writeFile(t, path, `
vars:
  token: `+secret+`
tests:
  - name: withholds its own secret
    workflow: ./workflow.yaml
    inputs: {who: "${vars.caller}"}
    secrets:
      env:TOKEN: ${vars.token}
    stubs:
      - task: log
        returns: {}
    expect:
      check:
        - that: vars.token == 'nope'
          because: false on purpose, so the witness renders
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused(), "a suite stating its own secret-holding var is the remedy, and loads")
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed(), "the claim is false on purpose")

	rendered := fmt.Sprintf("%v %+v", c.GetFailures(), c.GetFailures())
	assert.Contains(t, rendered, "[redacted]", "#2041's withholding holds for the suite's own var")
	assert.NotContains(t, rendered, secret)
}

// TestASuiteCopyingADirectoryVarItNamesAsASecretIsRefused is the remedy
// followed as a copy rather than a move: the suite restates the directory's
// value in its own `vars:` and withholds that copy, but testdefaults.yaml
// still states it, so every other suite in the directory reads and prints it.
// Refused in the same words, and positioned in the suite, which wrote the
// copy.
func TestASuiteCopyingADirectoryVarItNamesAsASecretIsRefused(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-dircopy-3907"

	dir := t.TempDir()
	writeDirWorkflow(t, dir)
	writeFile(t, filepath.Join(dir, flowtest.DirDefaultsName), "vars:\n  token: "+secret+"\n")
	path := filepath.Join(dir, "a.test.yaml")
	writeFile(t, path, `
vars:
  token: `+secret+`
tests:
  - name: withholds a copy the directory still states
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: ${vars.token}
    expect: {failed: false}
`)

	_, err := flowtest.Load(path)
	require.Error(t, err, "a copy leaves the directory's value readable by every other suite (#2080)")
	var diagnostics *flowtest.Diagnostics
	require.ErrorAs(t, err, &diagnostics)
	require.NotEmpty(t, diagnostics.Problems)
	assert.Equal(t, path, diagnostics.Problems[0].File, "positioned in the suite, which wrote the copy")
	assert.Contains(t, err.Error(), "vars.token restates the value testdefaults.yaml gives it")
	assert.Contains(t, err.Error(), "move vars.token into the suite's own vars: and remove it from testdefaults.yaml")
	assert.NotContains(t, err.Error(), secret, "the refusal names a path, never the value")
}
