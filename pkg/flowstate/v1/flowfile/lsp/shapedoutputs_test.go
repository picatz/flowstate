package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A task step's `outputs:` input replaces the task's declared outputs with
// author-defined names (issue #314). The validator has always known this — its
// output-reference check stands down the moment the input is present — but
// hover answered from the descriptor and told an author, falsely, that the
// task "does not declare" a name the file both validates and runs with, and
// completion offered the replaced names while never offering the real ones.
//
// Per the house rule the negative direction matters most here in reverse: the
// "does not declare" sentence is *true and load-bearing* on an unshaped step,
// so the fix must be provably confined to steps that shape.

// shapedOutputsFile shapes an http step into two author-named outputs through
// a CEL map literal — the paged-fan-out example's shape, single-quoted so the
// `: ` inside the literal stays inside the scalar. It is a file `flow
// validate` accepts, which the premise test below proves.
const shapedOutputsFile = `edition: v2026.2
name: shaped-outputs
steps:
  - id: pages
    http:
      method: GET
      url: https://catalog.invalid/records
      parse_json: true
      outputs: '${ {"records": response.json.records, "next_cursor": response.json.next_cursor} }'
  - id: report
    log:
      message: ${steps.pages.next_cursor}
`

// unshapedOutputsFile is the control: the same task with no shaping, and a
// reference to a name the task genuinely does not declare. It does not
// validate — the bogus reference is the point — but it parses, which is the
// level hover and completion answer at.
const unshapedOutputsFile = `edition: v2026.2
name: unshaped-outputs
steps:
  - id: web
    http:
      method: GET
      url: https://example.invalid/x
  - id: report
    log:
      message: ${steps.web.bogus}
`

// opaqueShapingFile shapes through an expression whose top level is not a map
// literal, so the output names are knowable only at run time.
const opaqueShapingFile = `edition: v2026.2
name: opaque-shaping
steps:
  - id: pages
    http:
      method: GET
      url: https://example.invalid/x
      parse_json: true
      outputs: ${response.json}
  - id: report
    log:
      message: ${steps.pages.records}
`

// TestTheShapedOutputsFileIsLegal is the premise: the false sentence was false
// precisely because the engine accepts this file, so every claim below is only
// worth anything if it still does.
func TestTheShapedOutputsFileIsLegal(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(shapedOutputsFile))
	require.NoError(t, err)
	assert.Empty(t, diags, "the fixture is meant to be a file the engine accepts")
}

// TestHoverOnAShapedOutputNameDescribesTheShaping is the defect itself: the
// name the shaping defines is described as the step's shaped output, and the
// descriptor-derived denial is gone.
func TestHoverOnAShapedOutputNameDescribesTheShaping(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///shaped-outputs.yaml"
	c.open(uri, shapedOutputsFile)

	at := positionOf(t, shapedOutputsFile, "message: ${steps.pages.next_cursor}", len("message: ${steps.pages.")+1)
	require.Positive(t, at.Line, "the fixture puts the reference mid-file; line 1 means the position fell back")
	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h, "hover says nothing about a shaped output the file validates with")
	text := hoverText(h)
	assert.Contains(t, text, "Shaped output of step `pages`",
		"the shaped name is not described as the step's own shaped output")
	assert.NotContains(t, text, "does not declare",
		"hover denies a name the validator accepts and the engine produces")
}

// TestCompletionAfterAShapedStepOffersTheShapedNames covers the other surface:
// after `steps.pages.` the menu holds the names the shaping defines and none
// of the declared names it removed.
func TestCompletionAfterAShapedStepOffersTheShapedNames(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///shaped-outputs-complete.yaml"
	c.open(uri, shapedOutputsFile)

	at := positionOf(t, shapedOutputsFile, "message: ${steps.pages.next_cursor}", len("message: ${steps.pages."))
	got := labels(c.complete(uri, at.Line, at.Character).Items)
	assert.Contains(t, got, "records", "a shaped name the step produces is not offered")
	assert.Contains(t, got, "next_cursor", "a shaped name the step produces is not offered")
	for _, replaced := range []string{"status_code", "headers", "body", "json"} {
		assert.NotContains(t, got, replaced,
			"completion offers %q, a declared output this step's shaping removed", replaced)
	}
}

// TestAnUnshapedStepKeepsTheDeclaredAnswer is the negative direction: without
// shaping, the "does not declare" sentence is true and must survive verbatim,
// and completion must keep offering the declared outputs.
func TestAnUnshapedStepKeepsTheDeclaredAnswer(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///unshaped-outputs.yaml"
	c.open(uri, unshapedOutputsFile)

	at := positionOf(t, unshapedOutputsFile, "message: ${steps.web.bogus}", len("message: ${steps.web.")+1)
	require.Positive(t, at.Line, "the fixture puts the reference mid-file; line 1 means the position fell back")
	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h, "hover says nothing about an undeclared output on an unshaped step")
	assert.Contains(t, hoverText(h),
		"The `http` task does not declare an output named `bogus`; it produces `status_code`, `headers`, `body`, `json`.",
		"the true denial on an unshaped step changed; it is load-bearing there")

	at = positionOf(t, unshapedOutputsFile, "message: ${steps.web.bogus}", len("message: ${steps.web."))
	got := labels(c.complete(uri, at.Line, at.Character).Items)
	for _, declared := range []string{"status_code", "headers", "body", "json"} {
		assert.Contains(t, got, declared,
			"an unshaped step stopped offering its declared output %q", declared)
	}
}

// TestOpaqueShapingClaimsNothing: when the shaping's names are not statically
// knowable, hover gives the honest generic answer — shaping replaced the
// declared outputs — without denying the name or fabricating a list, and
// completion offers no output names at all.
func TestOpaqueShapingClaimsNothing(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///opaque-shaping.yaml"
	c.open(uri, opaqueShapingFile)

	at := positionOf(t, opaqueShapingFile, "message: ${steps.pages.records}", len("message: ${steps.pages.")+1)
	require.Positive(t, at.Line, "the fixture puts the reference mid-file; line 1 means the position fell back")
	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h, "hover says nothing about an output of a step whose shaping is opaque")
	text := hoverText(h)
	assert.Contains(t, text, "replaces what the `http` task declares",
		"hover does not say the shaping replaced the declared outputs")
	assert.NotContains(t, text, "does not declare",
		"hover denies a name it cannot know anything about")
	assert.NotContains(t, text, "It names",
		"hover lists names for a shaping expression whose keys are not statically knowable")

	at = positionOf(t, opaqueShapingFile, "message: ${steps.pages.records}", len("message: ${steps.pages."))
	items := c.complete(uri, at.Line, at.Character).Items
	assert.Empty(t, items,
		"completion fabricates output names for a shaping whose keys only the run can know: %v", labels(items))
}
