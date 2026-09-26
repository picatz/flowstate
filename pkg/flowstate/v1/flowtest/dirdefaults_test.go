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

// gatedSiblingWorkflow waits on one signal, so a scripted signal naming
// anything else is refused by checkSignalNames — the mismatch every test
// below drives, so its own error text has something to quote.
const gatedSiblingWorkflow = `edition: v2026.3
name: gated
steps:
  - id: gate
    wait_for_signal:
      name: approve
      timeout: 10s
outputs: {}
`

// TestASiblingsLocalAliasOfASharedVarIsTaintedToo is Codex's finding on this
// fix's own first pass: a directory scan seeded only from a sibling's
// *direct* `secrets:` references (secretHoldingVars alone) missed a shared
// var read only through the sibling's own local alias of it.
// `a.test.yaml` never names `vars.token` directly — it names `vars.alias`,
// its own computed var reading the directory's shared `token` — so a scan
// that read only `a.test.yaml`'s `secrets:` text found `alias` and never
// learned that `alias`, in turn, reads the shared `token` every sibling's
// own `vars:` carries the identical value of.
func TestASiblingsLocalAliasOfASharedVarIsTaintedToo(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-transitivealias-6631"

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), gatedSiblingWorkflow)
	writeFile(t, filepath.Join(dir, "testdefaults.yaml"), "vars:\n  token: "+secret+"\n")
	writeFile(t, filepath.Join(dir, "a.test.yaml"), "vars:\n"+
		"  alias: ${vars.token}\n"+
		"tests:\n"+
		"  - name: holds the secret through a local alias\n"+
		"    workflow: ./workflow.yaml\n"+
		"    secrets:\n"+
		"      env:TOKEN: ${vars.alias}\n")
	bPath := filepath.Join(dir, "b.test.yaml")
	writeFile(t, bPath, "tests:\n"+
		"  - name: the gate is signalled by the wrong name\n"+
		"    workflow: ./workflow.yaml\n"+
		"    signals:\n"+
		"      - name: ${vars.token}\n"+
		"        at: 1s\n"+
		"        payload: {}\n"+
		"    expect:\n"+
		"      ran: [gate]\n")

	report := flowtest.RunFile(bPath)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed(), "the scripted signal names no gate this workflow waits on")

	rendered := c.GetError()
	assert.NotContains(t, rendered, secret,
		"a shared var reached through a sibling's own local alias of it printed in full (#2080)")
	assert.Contains(t, rendered, "matches no gate",
		"the positive control: the mismatch itself must still be reported")
}

// TestATruncatedSiblingScanFailsClosedOnSharedVars is Codex's second finding:
// a directory scan that cannot visit every suite file in it must not answer
// as though it had. This directory holds one more sibling than the scan's
// own bound — 256, [maxSiblingCandidates]' own value, spelled out here since
// this file cannot import an unexported constant — which makes the scan
// incomplete deterministically, by count alone, whatever order the
// directory's own entries come back in.
//
// None of the 257 siblings actually references `token` in a `secrets:` of
// its own — the fail-closed answer this test pins does not depend on any one
// of them being the file that really would have leaked it, only on the scan
// being unable to certify that none of them is, the same standing an
// unreadable sensitive input already gets (CLAUDE.md, "fail closed"): every
// var the directory's shared vars: states is tainted once a scan of its
// siblings cannot be shown to be complete, not only the ones a scan that
// happened to finish would have found.
func TestATruncatedSiblingScanFailsClosedOnSharedVars(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-truncatedscan-7742"

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), gatedSiblingWorkflow)
	writeFile(t, filepath.Join(dir, "testdefaults.yaml"), "vars:\n  token: "+secret+"\n")

	// One more sibling than the scan's own bound. Each is a minimal,
	// otherwise-unrelated loadable suite: the scan has to actually open and
	// decode a candidate to tell it apart from a real secret-holding one, so
	// a syntactically invalid filler would prove nothing about the bound
	// this test is about.
	for i := range 257 {
		writeFile(t, filepath.Join(dir, fmt.Sprintf("sibling%03d.test.yaml", i)),
			"tests:\n  - name: filler\n    workflow: ./workflow.yaml\n    expect: {failed: true}\n")
	}

	targetPath := filepath.Join(dir, "target.test.yaml")
	writeFile(t, targetPath, "tests:\n"+
		"  - name: the gate is signalled by the wrong name\n"+
		"    workflow: ./workflow.yaml\n"+
		"    signals:\n"+
		"      - name: ${vars.token}\n"+
		"        at: 1s\n"+
		"        payload: {}\n"+
		"    expect:\n"+
		"      ran: [gate]\n")

	report := flowtest.RunFile(targetPath)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed(), "the scripted signal names no gate this workflow waits on")

	rendered := c.GetError()
	assert.NotContains(t, rendered, secret,
		"a shared var tainted only by a sibling past the scan's own bound printed in full (#2080)")
	assert.Contains(t, rendered, "matches no gate",
		"the positive control: the mismatch itself must still be reported")
}
