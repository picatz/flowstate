package lsp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A `*.test.yaml` in the editor (#1110 slice 1): checked by the loader
// `flow test` runs, never by the workflow grammar. The negative direction is
// the one that was live: before the document kind existed, a test file
// attached to this server drew a workflow's diagnostics — `tests:` an unknown
// key, no `steps:` — false squiggles on a correct file.

const validSuite = `
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
tests:
  - name: the case
    expect:
      ran: [hello]
`

// TestAValidTestFileDrawsNoDiagnostics is the false-diagnosis regression: a
// correct suite must be silent, which it cannot be if the workflow grammar is
// consulted at all.
func TestAValidTestFileDrawsNoDiagnostics(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	params := c.open("file:///suite.test.yaml", validSuite)
	assert.Empty(t, params.Diagnostics,
		"a correct test file drew diagnostics — the workflow grammar is leaking into the test language")
}

// TestATestFilesUnknownKeyIsPositioned: the loader's strict decode carries
// goccy's own [line:col], and the diagnostic lands on it — the same precision
// a workflow's YAML mistakes get.
func TestATestFilesUnknownKeyIsPositioned(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	text := strings.Replace(validSuite, "expect:", "expct:", 1)
	params := c.open("file:///typo.test.yaml", text)
	require.Len(t, params.Diagnostics, 1)
	d := params.Diagnostics[0]
	assert.Equal(t, codeTestFile, d.Code)
	assert.Contains(t, d.Message, `unknown field "expct"`)

	lines := strings.Split(text, "\n")
	require.Less(t, d.Range.Start.Line, len(lines))
	assert.Contains(t, lines[d.Range.Start.Line], "expct:",
		"the diagnostic is not on the line holding the mistake")
}

// TestASemanticRefusalAnchorsAtTheNamedTest: the loader's prose errors carry
// no position, so one naming a test anchors at that test's `name:` line — a
// deliberate slice-1 heuristic (#1110), and only where the name is
// unambiguous.
func TestASemanticRefusalAnchorsAtTheNamedTest(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	text := `
defaults:
  workflow: ./workflow.yaml
tests:
  - name: fine
    expect:
      ran: [hello]
  - name: replays wrongly
    trigger:
      webhook: stripe
      payload: ./delivery.json
      signature: sometimes
    expect:
      refused: true
`
	params := c.open("file:///semantic.test.yaml", text)
	require.Len(t, params.Diagnostics, 1)
	d := params.Diagnostics[0]
	assert.Equal(t, codeTestFile, d.Code)
	assert.Contains(t, d.Message, `"sometimes"`)

	lines := strings.Split(text, "\n")
	require.Less(t, d.Range.Start.Line, len(lines))
	assert.Contains(t, lines[d.Range.Start.Line], "replays wrongly",
		"the diagnostic did not anchor at the named test")
}

// TestTheWorkflowFeaturesStayQuietOnATestFile: none of these six answer a
// test document from the *workflow* grammar (#1110 item 8's whole point —
// a step's `for_each:`, a task's inputs, a Marshal-shaped format edit would
// all be wrong answers with confidence in a document that has no `steps:`
// at all). It no longer means every feature answers nothing: completion,
// hover and the outline now have real, narrower answers of their own for
// the test language, asserted here by their *absence of a workflow leak*
// rather than by blanket emptiness, and exercised on their own positive
// terms in testcompletion_test.go, testhover_test.go and
// testsymbols_test.go. Format and code actions still answer nothing at all
// — see their own docTest branches (format.go, codeaction.go) for why.
func TestTheWorkflowFeaturesStayQuietOnATestFile(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	c.open("file:///quiet.test.yaml", validSuite)

	// (4,8) sits inside the *key* "task" of a stub entry, not its value —
	// not a position hoverTestDocument answers (only a stub's task *value*
	// has a registry entry to look up) — and the workflow grammar has
	// nothing to say about it either.
	assert.Nil(t, c.hover("file:///quiet.test.yaml", 4, 8),
		"hover answered a position that is neither a stub's task value nor something the workflow grammar would know")

	// Completion at the same position offers the test language's own
	// stub-level keys (task, step, where, ...), never the workflow's. The
	// leak this asserts against is a workflow-only candidate, not the
	// presence of any candidate at all.
	got := labels(c.complete("file:///quiet.test.yaml", 4, 8).Items)
	for _, workflowOnly := range []string{"for_each", "loop", "parallel", "sleep", "wait_until", "steps"} {
		assert.NotContains(t, got, workflowOnly,
			"a workflow-only key %q leaked into a test file's completion", workflowOnly)
	}

	// The outline names the suite's one case — the test language's own
	// answer, never a workflow's steps.
	symbols := c.symbols("file:///quiet.test.yaml")
	require.Len(t, symbols, 1)
	assert.Equal(t, "the case", symbols[0].Name)

	assert.Empty(t, c.format("file:///quiet.test.yaml"),
		"formatting answered a test document — no flowtest analogue of flowfile.Marshal exists")
	assert.Empty(t, c.codeAction("file:///quiet.test.yaml", wholeOf(validSuite), nil, nil),
		"code actions answered a test document — nothing here computes one yet")
}

// TestTheDirectoryDefaultsFileIsNeverAWorkflow: `testdefaults.yaml` gets
// syntax feedback and nothing else in slice 1 — and above all, not the
// workflow grammar's opinion of a `defaults:` block.
func TestTheDirectoryDefaultsFileIsNeverAWorkflow(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	params := c.open("file:///testdefaults.yaml", `
vars:
  region: us-east-1
defaults:
  stubs:
    - task: log
      returns: {}
`)
	assert.Empty(t, params.Diagnostics,
		"a correct testdefaults.yaml drew diagnostics — the workflow grammar is leaking")

	broken := c.open("file:///dir2/testdefaults.yaml", "vars: [unclosed\n")
	require.Len(t, broken.Diagnostics, 1)
	assert.Equal(t, codeYAMLSyntax, broken.Diagnostics[0].Code,
		"a syntax error is still reported, as itself")
}

// TestABrokenDefaultsFileIsNotAnchoredOnTheSuite (Codex, #1109): a yaml.Error
// from the sibling testdefaults.yaml carries the DEFAULTS file's position, so
// mapping it onto this buffer lands a squiggle on an unrelated token, with
// the parser's bare message hiding which file is broken. It anchors at the
// suite's document start instead, the message carrying the sibling's path,
// position and excerpt — the one case the excerpt earns its place, since the
// editor is not showing that file.
func TestABrokenDefaultsFileIsNotAnchoredOnTheSuite(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "testdefaults.yaml"),
		[]byte("defaults:\n  stubs: [\n"), 0o600))

	params := c.open("file://"+dir+"/suite.test.yaml", validSuite)
	require.Len(t, params.Diagnostics, 1)
	d := params.Diagnostics[0]
	assert.Equal(t, codeTestFile, d.Code)
	assert.Contains(t, d.Message, "testdefaults.yaml",
		"the message must name the broken sibling file")
	assert.Equal(t, documentStart, d.Range,
		"a sibling file's own position must never index this buffer's lines")
}

// TestASuitesOwnErrorIsNotMistakenForTheDefaultsFile (Codex, #1109) is the
// negative direction of the test above, and it is the one that was wrong.
//
// Provenance was decided by looking for "testdefaults.yaml" in the error's
// prose, so an ordinary refusal from *this* suite that happened to quote that
// string — a case named after the file — was filed as a sibling's problem:
// anchored at the document start, shown whole, and bypassing the positioning
// every other loader error gets. The error is asked where it came from now.
func TestASuitesOwnErrorIsNotMistakenForTheDefaultsFile(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	// No sibling defaults file exists here at all, so anything blamed on one
	// is a misfiling by construction.
	dir := t.TempDir()

	params := c.open("file://"+dir+"/suite.test.yaml", `edition: v2026.3
tests:
  - name: testdefaults.yaml
    expect:
      failed: false
`)
	require.Len(t, params.Diagnostics, 1)

	d := params.Diagnostics[0]
	assert.Equal(t, codeTestFile, d.Code)
	assert.NotEqual(t, documentStart, d.Range,
		"a refusal about this suite's own case was filed as a sibling defaults file's, "+
			"so it lost the position every other loader error gets")
}

// TestADirectoryNamedAfterTheDefaultsFileIsStillOrdinary is the same
// misfiling reached through the path rather than the content: the loader
// prefixes its errors with the suite's path, so every suite under a directory
// whose name contains `testdefaults.yaml` matched the prose test.
func TestADirectoryNamedAfterTheDefaultsFileIsStillOrdinary(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	dir := filepath.Join(t.TempDir(), "testdefaults.yaml.d")
	require.NoError(t, os.MkdirAll(dir, 0o750))

	params := c.open("file://"+dir+"/suite.test.yaml", `edition: v2026.3
tests:
  - name: it runs
    expect:
      failed: false
`)
	require.Len(t, params.Diagnostics, 1)

	d := params.Diagnostics[0]
	assert.Equal(t, codeTestFile, d.Code)
	assert.NotEqual(t, documentStart, d.Range,
		"the directory's name decided where this suite's own diagnostic was anchored")
}
