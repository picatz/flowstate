package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// What is checked here is the verb rather than the checks, which
// pkg/flowstate/v1/flowfile/lint_test.go owns: the walk, the two renderings, and
// the one thing about this command a reader has to be able to rely on — that a
// style finding does not fail anything unless the caller said it should.
//
// That last one is the property the whole tier rests on. Tier 4 warns and never
// blocks (docs/STYLE.md, Part II), and a lint that started failing builds on
// taste is how a language ends up refusing its own generators. So the exit
// status is asserted in both directions, over a corpus that actually has
// findings — an exit-code test over a clean file passes whichever way the code
// is written.

// lintFixture writes a Flowfile with one nested conditional in it, which is the
// smallest thing this command has something to say about.
func lintFixture(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.yaml")

	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: nested
inputs:
  amount:
    type: int
steps:
  - id: band
    value: '${inputs.amount > 100 ? "high" : (inputs.amount > 10 ? "medium" : "low")}'
`), 0o600))

	return path
}

// TestLintExitsZeroOnFindings is the contract of the whole tier: advice does not
// fail anything.
func TestLintExitsZeroOnFindings(t *testing.T) {
	res := runFlow(t, "lint", lintFixture(t))

	require.NoError(t, res.Err, "a style finding must not fail the command")
	assert.Contains(t, res.Stdout, "R5/nested-conditional")
	assert.Contains(t, res.Stdout, "exits 0",
		"the reader is told the finding is advice, not a refusal")
}

// TestLintStrictExitsNonZeroOnFindings is the opt-in the CI leg over examples/
// uses.
func TestLintStrictExitsNonZeroOnFindings(t *testing.T) {
	res := runFlow(t, "lint", "--strict", lintFixture(t))

	require.Error(t, res.Err, "--strict is what makes a finding a failure")
	assert.Contains(t, res.Stdout, "R5/nested-conditional",
		"the findings are printed before the failure, not instead of it")
}

// TestLintStrictExitsZeroOnACleanFile keeps --strict a statement about findings
// rather than about the flag.
func TestLintStrictExitsZeroOnACleanFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.yaml")

	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: clean
steps:
  - id: greet
    log:
      message: hello
`), 0o600))

	res := runFlow(t, "lint", "--strict", path)

	require.NoError(t, res.Err)
	assert.Contains(t, res.Stdout, "nothing to suggest")
}

// TestLintNamesAFileItCouldNotCheck is the false all-clear this must never
// give: a named file that does not compile was not checked, and a summary
// saying "nothing to suggest" about it is a green tick for a file nobody read
// (#865 review, Codex r3835040609).
func TestLintNamesAFileItCouldNotCheck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.yaml")

	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: broken
steps:
  - id: nope
    log:
      message: ${1 +}
`), 0o600))

	res := runFlow(t, "lint", path)

	require.NoError(t, res.Err, "advisory by default, even for a file it could not read")
	assert.Contains(t, res.Stdout, "not checked for style")
	assert.Contains(t, res.Stdout, path+":6:",
		"the reason carries its own position, in the form an editor can jump to")
	assert.Contains(t, res.Stdout, "0 file(s) checked",
		"the summary counts what was read, and reads as zero rather than as clean")
}

// TestLintStrictFailsOnANamedFileItCouldNotCheck is the decision `--strict`
// makes about that file.
//
// Two facts, two failures: a finding says the file is written a way the charter
// has an opinion about, and this says the file was never looked at. A strict
// check that stayed green on the second would go green exactly when somebody is
// mid-edit — the moment a corpus most needs the check.
func TestLintStrictFailsOnANamedFileItCouldNotCheck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.yaml")

	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: broken
steps:
  - id: nope
    log:
      message: ${1 +}
`), 0o600))

	res := runFlow(t, "lint", "--strict", path)

	require.Error(t, res.Err, "a named file that could not be checked fails a strict run")
	assert.Contains(t, res.Stdout, "not checked for style")
}

// TestLintStrictToleratesAWalkedFileItCouldNotCheck is the other half of that
// decision, and the reason it is a split rather than a rule.
//
// A directory walk picks up every `*.yaml` shaped like a Flowfile, which
// includes each `*.test.yaml` beside a workflow. Failing on those would make
// `--strict` unusable over `examples/`, where sixty of them sit, so a walked
// file is counted and a named one is named.
func TestLintStrictToleratesAWalkedFileItCouldNotCheck(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(`edition: v2026.3
name: clean
steps:
  - id: greet
    log:
      message: hello
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "broken.yaml"), []byte(`edition: v2026.3
name: broken
steps:
  - id: nope
    log:
      message: ${1 +}
`), 0o600))

	res := runFlow(t, "lint", "--strict", dir)

	require.NoError(t, res.Err, "a walked file that will not compile does not fail a strict run")
	assert.Contains(t, res.Stdout, "1 file(s) the walk found",
		"but it is counted, so the run cannot read as having checked everything")
}

// TestLintPositionsAreClickable pins the one rendering detail every consumer
// downstream matches on: `file:line:` with no space after the filename (#384).
func TestLintPositionsAreClickable(t *testing.T) {
	path := lintFixture(t)

	res := runFlow(t, "lint", path)
	require.NoError(t, res.Err)

	assert.Contains(t, res.Stdout, path+":8:",
		"a finding names its own file and line in the form an editor and a CI annotation match")
}

// TestLintJSONIsReadable checks the machine form carries what a job filters on,
// and that a file it could not compile is named rather than silently dropped.
func TestLintJSONIsReadable(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(`edition: v2026.3
name: nested
inputs:
  amount:
    type: int
steps:
  - id: band
    value: '${inputs.amount > 100 ? "high" : (inputs.amount > 10 ? "medium" : "low")}'
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "broken.yaml"), []byte(`edition: v2026.3
name: broken
steps:
  - id: nope
    log:
      message: ${1 +}
`), 0o600))

	res := runFlow(t, "lint", "-o", "json", dir)
	require.NoError(t, res.Err)

	var report lintReport
	require.NoError(t, json.Unmarshal([]byte(res.Stdout), &report),
		"the machine format should be JSON a program can read: %s", res.Stdout)

	require.Len(t, report.Files, 1)
	require.Len(t, report.Files[0].Findings, 1)

	finding := report.Files[0].Findings[0]
	assert.Equal(t, "R5/nested-conditional", finding.Rule)
	assert.Equal(t, 8, finding.Line)
	assert.Equal(t, "band", finding.Step)
	assert.NotEmpty(t, finding.Message)

	assert.Equal(t, 1, report.Totals.Findings)
	assert.Equal(t, 1, report.Totals.ByRule["R5/nested-conditional"])
	assert.Len(t, report.Skipped, 1,
		"a file that does not compile is named as skipped; `validate` is the verb for it")
}

// TestLintReadsTheShownCorpus is the reachability check CLAUDE.md asks for: the
// command runs over the files this repository teaches from, and answers.
//
// Deliberately not an assertion that the corpus is clean. It is not, and
// docs/STYLE.md Part III records what it holds and why the leg over it lands
// advisory. Asserting a number here would make this test the place that has to
// be edited every time an example is written, which is not what it is for.
func TestLintReadsTheShownCorpus(t *testing.T) {
	res := runFlow(t, "lint", "-o", "json", corpus)
	require.NoError(t, res.Err, "the corpus compiles, so the lint has an answer for it")

	var report lintReport
	require.NoError(t, json.Unmarshal([]byte(res.Stdout), &report))

	assert.Positive(t, report.Totals.Files, "the walk found Flowfiles to read")
}
