package flowtest_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// File-level `vars:` (#1072 slice 2): literals stated once, referenced as
// whole-value `${vars.x}` in fixture positions (substituted at load) and as
// bare `vars.x` in checks (bound at evaluation). A stub's `vars.` stays the
// workflow's — the one deliberate asymmetry, pinned below.

// TestAVarReachesEveryFixturePosition: one value, referenced from inputs, a
// nested payload position, expect.outputs, and a check — all four agreeing
// about it is the DRY this exists for.
func TestAVarReachesEveryFixturePosition(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: echo
inputs:
  order:
    type: struct
steps:
  - id: keep
    value: ${inputs.order.region}
outputs:
  region:
    value: ${steps.keep.value}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  region: eu-west-1
tests:
  - name: the region is stated once
    workflow: ./workflow.yaml
    inputs:
      order:
        region: ${vars.region}
        items: [a, b]
    expect:
      ran: [keep]
      outputs:
        region: ${vars.region}
      check:
        - steps.keep.value == vars.region
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}

// TestAVarMayHoldAStructure, landing whole in a position that wants one — a
// payload fragment is the corpus's most-duplicated shape.
func TestAVarMayHoldAStructure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: shaped
inputs:
  order:
    type: struct
steps:
  - id: read
    value: ${inputs.order.id}
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  order: {id: ord_123, region: eu-west-1}
tests:
  - name: the structure lands whole
    workflow: ./workflow.yaml
    inputs:
      order: ${vars.order}
    expect:
      ran: [read]
      check:
        - steps.read.value == 'ord_123'
        - vars.order.region == 'eu-west-1'
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.True(t, report.GetCases()[0].GetPassed(),
		"%v / %v", report.GetCases()[0].GetError(), report.GetCases()[0].GetFailures())
}

// TestAStubsVarsAreTheWorkflowsNotTheFiles is the asymmetry's pin, in the
// position where it could actually be lost. A stub's `returns:` carries
// whole-value `${...}` fences evaluated at run time against the run's scope,
// where `vars.` means the workflow's own `vars:` block — the same spelling
// the loader substitutes in fixture positions. The workflow says hello; the
// file says goodbye; the stub's fenced return must evaluate at run time and
// answer hello, which fails if the loader substitutes the file's literal into
// the fence before the run ever sees it. The bare-CEL `where:` rides along,
// though substitution cannot reach it by construction (it rewrites fences
// only).
func TestAStubsVarsAreTheWorkflowsNotTheFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: spoken
vars:
  greeting: hello
steps:
  - id: say
    log:
      message: ${vars.greeting}
outputs:
  said:
    value: ${steps.say.echoed}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  greeting: goodbye
tests:
  - name: the stub speaks the run's language
    workflow: ./workflow.yaml
    stubs:
      - task: log
        where: vars.greeting == 'hello'
        returns:
          echoed: "${vars.greeting}"
    expect:
      ran: [say]
      outputs:
        said: hello
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.True(t, report.GetCases()[0].GetPassed(),
		"a stub's fenced return must read the workflow's vars at run time — %v / %v",
		report.GetCases()[0].GetError(), report.GetCases()[0].GetFailures())
}

// TestVarsReachRowsAndDefaults: substitution runs before tables expand and
// before defaults merge, so an inherited reference resolves exactly once and
// every level sees the literal.
func TestVarsReachRowsAndDefaults(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: chained
inputs:
  who:
    type: string
steps:
  - id: greet
    value: ${inputs.who}
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  caller: team-a
defaults:
  workflow: ./workflow.yaml
  inputs: {who: "${vars.caller}"}
tests:
  - name: greeting
    expect:
      check:
        - steps.greet.value == vars.caller
    cases:
      - name: inherits through both levels
      - name: a row overrides with its own reference
        inputs: {who: "${vars.caller}"}
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2)
	for _, c := range report.GetCases() {
		assert.True(t, c.GetPassed(), "%s: %v / %v", c.GetName(), c.GetError(), c.GetFailures())
	}
}

// The refusals, each naming its position.

func TestAnUnknownVarIsRefused(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  region: eu-west-1
tests:
  - name: misspelled
    workflow: ./workflow.yaml
    inputs: {r: "${vars.regoin}"}
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "names no")
	require.Contains(t, err.Error(), "regoin")
}

func TestAMixedVarReferenceIsRefused(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  host: api.example.com
tests:
  - name: templated
    workflow: ./workflow.yaml
    inputs: {url: "https://${vars.host}/v1"}
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixes text with a vars reference")
}

// TestAVarHoldingANestedExpressionIsRefused: the fence rule is a whole-value
// rule. `computed: ${vars.other}` is an expression and is evaluated (see
// computedvars_test.go); a fence *inside* a structure is neither a literal nor
// something anything evaluates, so it is refused — and says so in its own
// words rather than in the `defaults:` block's (#1072, repair 5).
func TestAVarHoldingANestedExpressionIsRefused(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  other: plain
  order: {id: "${vars.other}"}
tests:
  - name: never loads
    workflow: ./workflow.yaml
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "vars.order.id")
	require.Contains(t, err.Error(), "a var holds a literal, or one whole-value")
	require.NotContains(t, err.Error(), "`defaults:` is a fixture",
		"a var refused for a nested fence was told about the `defaults:` block until #1072")
}

func TestAVarWithAnUnaddressableNameIsRefused(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  api-host: x
tests:
  - name: never loads
    workflow: ./workflow.yaml
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "CEL identifier")
}

func TestANonStringVarInAStringPositionIsRefused(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  count: 3
tests:
  - name: a path that is a number
    workflow: ${vars.count}
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "takes a string")
}
