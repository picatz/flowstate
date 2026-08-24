package lsp

import (
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

// TestTheWorkflowFeaturesStayQuietOnATestFile: hover, completion, symbols and
// the rest answer from the workflow grammar, so on a test document they answer
// nothing rather than something confidently wrong.
func TestTheWorkflowFeaturesStayQuietOnATestFile(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	c.open("file:///quiet.test.yaml", validSuite)

	assert.Nil(t, c.hover("file:///quiet.test.yaml", 4, 8),
		"hover answered a test document from the workflow grammar")
	assert.Empty(t, c.complete("file:///quiet.test.yaml", 4, 8).Items,
		"completion offered workflow keys inside a test file")
	assert.Empty(t, c.symbols("file:///quiet.test.yaml"),
		"the symbol tree described a test file as a workflow")
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
