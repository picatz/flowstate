package flowtest_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// loopWorkflow fans out over two named services and reports the names of the
// iterations whose probe came back with one.
//
// The step's `outputs:` is a deferred input the http task evaluates itself, so
// nothing a stub sees in `inputs` says which iteration it is answering — both
// probes carry the identical url on purpose. The loop's own binding is the only
// thing that separates them, which is what these tests are about (#269).
const loopWorkflow = `
edition: v2026.3
name: loop-binding
steps:
  - id: checks
    for_each:
      items:
        - name: alpha
          url: https://example.invalid/probe
        - name: beta
          url: https://example.invalid/probe
      as: service
      steps:
        - id: probe
          continue_on_error: true
          http:
            method: GET
            url: ${service.url}
            outputs: '${ {"name": service.name} }'
outputs:
  reported:
    value: ${steps.checks.results.filter(r, has(r.probe.name)).map(r, r.probe.name)}
`

// TestStubSeesTheLoopBinding is the positive direction: a stub's `where:` can
// name the loop's binding and tell one iteration from another, and a stub's
// `returns:` can carry that iteration's own value forward — so a case over a
// loop asserts what the workflow computes rather than what one canned answer,
// repeated, distorts it into.
func TestStubSeesTheLoopBinding(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", loopWorkflow)
	writeFile(t, dir+"/loop.test.yaml", `
tests:
  - name: a where discriminates iterations
    workflow: ./workflow.yaml
    stubs:
      - task: http
        where: service.name == 'alpha'
        returns:
          name: alpha
      - task: http
        where: service.name == 'beta'
        returns:
          name: beta
    expect:
      outputs:
        reported: [alpha, beta]

  - name: one stub answers every iteration with that iteration's own value
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns:
          name: '${service.name}'
    expect:
      outputs:
        reported: [alpha, beta]

  - name: an expression nested inside a returned structure is evaluated too
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns:
          name: '${service.name}'
          detail:
            probed: ['${service.url}']
    expect:
      outputs:
        reported: [alpha, beta]
`)

	report := flowtest.RunFile(dir + "/loop.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 3)
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "%s: failures: %v", c.GetName(), c.GetFailures())
	}
}

// TestStubWhereDoesNotMatchAnotherIteration is the negative direction, which is
// the half that actually proves the binding is per-iteration: a `where:` naming
// the binding must *not* answer an iteration it does not name. Asserting only
// that a stub matches its own iteration is satisfied by a binding that matches
// everything (CLAUDE.md, "test that A cannot reach B").
//
// With no stub left to answer `beta`, that iteration reaches the unmatched-stub
// diagnostic — and because the loop body tolerates its failure, the run still
// finishes and reports `alpha` alone. A `where:` that leaked across iterations
// would report both.
func TestStubWhereDoesNotMatchAnotherIteration(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", loopWorkflow)
	writeFile(t, dir+"/loop.test.yaml", `
tests:
  - name: a where naming one iteration answers only that one
    workflow: ./workflow.yaml
    stubs:
      - task: http
        where: service.name == 'alpha'
        returns:
          name: alpha
    expect:
      outputs:
        reported: [alpha]
`)

	report := flowtest.RunFile(dir + "/loop.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(), "failures: %v", report.GetCases()[0].GetFailures())
}

// TestStubReturnsRejectsAMalformedExpression checks that a `returns:` value
// that tried to be an expression and is not one is refused when the file loads,
// naming the value — rather than being carried into the run as literal text
// that quietly asserts the wrong thing (CLAUDE.md, "diagnostics are a
// feature").
func TestStubReturnsRejectsAMalformedExpression(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", loopWorkflow)
	writeFile(t, dir+"/loop.test.yaml", `
tests:
  - name: a returns value that mixes text with an expression
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns:
          name: 'probe of ${service.name}'
    expect:
      outputs:
        reported: [alpha, beta]
`)

	report := flowtest.RunFile(dir + "/loop.test.yaml")
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), "mixes literal text with an expression")
	require.Contains(t, c.GetError(), "returns")
}

// unmatchedStubWorkflow is a single log step whose message is built from a
// non-sensitive input and a sensitive one, used by the tests below to check
// what an unmatched-stub failure prints about the invocation it could not
// answer (#386).
const unmatchedStubWorkflow = `
edition: v2026.3
name: greet
inputs:
  name:
    type: string
    required: true
  token:
    type: string
    required: true
    sensitive: true
steps:
  - id: greet
    log:
      message: ${"hello, " + inputs.name}
outputs: {}
`

// TestUnmatchedStubReportsTheInvocationInputs is the positive direction of
// #386: when a task is invoked with no matching stub, the failure names the
// invocation's own inputs and every tried stub's `where:` beside its verdict,
// instead of leaving the reader to reconstruct what the invocation carried.
func TestUnmatchedStubReportsTheInvocationInputs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", unmatchedStubWorkflow)
	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: the greeting uses the input it was given
    workflow: ./workflow.yaml
    inputs:
      name: flowstate
      token: shh-secret-value
    stubs:
      - task: log
        where: inputs.message == 'goodbye, flowstate'
    expect:
      outputs: {}
`)

	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())

	found := false
	for _, f := range c.GetFailures() {
		if f.GetField() != "expect.failed" {
			continue
		}
		msg := f.GetMessage()
		// The invocation's actual input, which the old message never showed.
		require.Contains(t, msg, `"hello, flowstate"`)
		// The where: clause that did not match, and its verdict.
		require.Contains(t, msg, "inputs.message == 'goodbye, flowstate'")
		require.Contains(t, msg, "-> false")
		found = true
	}
	require.True(t, found, "expected an expect.failed diagnostic showing the invocation's inputs; got %v", c.GetFailures())
}

// TestUnmatchedStubRedactsASensitiveInput is the negative direction: a value
// that traces back to an input the workflow declared `sensitive:` must not
// appear in the clear in an unmatched-stub failure, even though that failure
// text is exactly the kind of surface that ends up in CI logs (CLAUDE.md,
// "diagnostics are a feature" and the sensitive-input containment rules).
func TestUnmatchedStubRedactsASensitiveInput(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: greet
inputs:
  token:
    type: string
    required: true
    sensitive: true
steps:
  - id: greet
    log:
      message: ${inputs.token}
outputs: {}
`)
	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: the sensitive value never reaches the failure text
    workflow: ./workflow.yaml
    inputs:
      token: shh-secret-value
    stubs:
      - task: log
        where: inputs.message == 'goodbye, flowstate'
    expect:
      outputs: {}
`)

	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())

	found := false
	for _, f := range c.GetFailures() {
		if f.GetField() != "expect.failed" {
			continue
		}
		msg := f.GetMessage()
		require.NotContains(t, msg, "shh-secret-value")
		require.Contains(t, msg, "[redacted: message]")
		found = true
	}
	require.True(t, found, "expected an expect.failed diagnostic redacting the sensitive input; got %v", c.GetFailures())
}

// TestUnmatchedStubRedactsASensitiveValueNestedInAStructuredInput is the
// Codex-review follow-up to #386: `headers: {Authorization: ${inputs.token}}`
// carries the sensitive token as a leaf of a map, not as the whole `headers`
// input, so a redaction check comparing the whole map against the token by
// reflect.DeepEqual never matches and the credential prints in the clear one
// level down. The fix has to walk the structure rather than compare it whole.
func TestUnmatchedStubRedactsASensitiveValueNestedInAStructuredInput(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: headers-probe
inputs:
  token:
    type: string
    sensitive: true
    required: true
steps:
  - id: call
    http:
      url: https://example.invalid/probe
      headers:
        Authorization: ${inputs.token}
outputs: {}
`)
	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: header carries the sensitive token and no stub matches
    workflow: ./workflow.yaml
    inputs:
      token: shh-secret-value
    stubs:
      - task: http
        where: inputs.url == 'https://nope.invalid/'
    expect:
      outputs: {}
`)

	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())

	found := false
	for _, f := range c.GetFailures() {
		if f.GetField() != "expect.failed" {
			continue
		}
		msg := f.GetMessage()
		require.NotContains(t, msg, "shh-secret-value")
		require.Contains(t, msg, "[redacted]")
		found = true
	}
	require.True(t, found, "expected an expect.failed diagnostic redacting the nested sensitive value; got %v", c.GetFailures())
}

// A sensitive declaration can itself be structured. Selecting a leaf from it
// must preserve the declaration's sensitivity even though that scalar is not
// DeepEqual to the whole value stored in the run scope.
func TestUnmatchedStubRedactsALeafOfASensitiveStructuredInput(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: structured-credentials
inputs:
  creds:
    type: struct
    sensitive: true
    required: true
steps:
  - id: call
    http:
      url: https://example.invalid/probe
      headers:
        Authorization: ${inputs.creds.token}
outputs: {}
`)
	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: a selected credential field never reaches the failure text
    workflow: ./workflow.yaml
    inputs:
      creds:
        token: shh-structured-secret
    stubs:
      - task: http
        where: inputs.url == 'https://nope.invalid/'
    expect:
      outputs: {}
`)

	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	for _, f := range c.GetFailures() {
		if f.GetField() == "expect.failed" {
			require.NotContains(t, f.GetMessage(), "shh-structured-secret")
			require.Contains(t, f.GetMessage(), "[redacted]")
			return
		}
	}
	require.Fail(t, "expected an expect.failed diagnostic", "%v", c.GetFailures())
}

// TestUnmatchedStubRedactsASensitiveValueInsideAList is the same nested-leaf
// case for the other structured shape a native input can hold: an element of
// a list, not only a value of a map.
func TestUnmatchedStubRedactsASensitiveValueInsideAList(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: list-probe
inputs:
  token:
    type: string
    sensitive: true
    required: true
steps:
  - id: call
    http:
      url: https://example.invalid/probe
      json: '${ {"tokens": [inputs.token]} }'
outputs: {}
`)
	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: a list element carries the sensitive token and no stub matches
    workflow: ./workflow.yaml
    inputs:
      token: shh-secret-value
    stubs:
      - task: http
        where: inputs.url == 'https://nope.invalid/'
    expect:
      outputs: {}
`)

	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())

	found := false
	for _, f := range c.GetFailures() {
		if f.GetField() != "expect.failed" {
			continue
		}
		msg := f.GetMessage()
		require.NotContains(t, msg, "shh-secret-value")
		require.Contains(t, msg, "[redacted]")
		found = true
	}
	require.True(t, found, "expected an expect.failed diagnostic redacting the sensitive list element; got %v", c.GetFailures())
}

// TestUnmatchedStubRedactsASensitiveValueConcatenatedIntoAString is the
// substring backstop: a sensitive value wrapped in unrelated text by string
// concatenation is not equal to the whole rendered string, so the map/list
// walk alone cannot see it either; only a substring check on the rendered
// text catches it.
func TestUnmatchedStubRedactsASensitiveValueConcatenatedIntoAString(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: concat-probe
inputs:
  token:
    type: string
    sensitive: true
    required: true
steps:
  - id: call
    http:
      url: https://example.invalid/probe
      headers:
        Authorization: ${"Bearer " + inputs.token}
outputs: {}
`)
	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: a concatenated header carries the sensitive token and no stub matches
    workflow: ./workflow.yaml
    inputs:
      token: shh-secret-value
    stubs:
      - task: http
        where: inputs.url == 'https://nope.invalid/'
    expect:
      outputs: {}
`)

	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())

	found := false
	for _, f := range c.GetFailures() {
		if f.GetField() != "expect.failed" {
			continue
		}
		msg := f.GetMessage()
		require.NotContains(t, msg, "shh-secret-value")
		require.Contains(t, msg, "[redacted]")
		found = true
	}
	require.True(t, found, "expected an expect.failed diagnostic redacting the concatenated sensitive value; got %v", c.GetFailures())
}

// TestUnmatchedStubSurvivesAWhereEvaluationError is the Codex-review
// follow-up: a stub whose where: fails to evaluate used to abort the
// invocation immediately, which both hid a later stub that would have
// matched cleanly and made [stubVerdict.err] unreachable from a real
// evaluation failure: nothing ever built the diagnostic that would have
// rendered it. Two stubs are declared; the first's where: divides by zero,
// the second's where: is merely false, so the case proves both that the
// second stub was still tried and that the first's error is reported rather
// than swallowed.
func TestUnmatchedStubSurvivesAWhereEvaluationError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: greet
steps:
  - id: greet
    log:
      message: hello
outputs: {}
`)
	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: a broken where errors, and the second stub still doesn't match
    workflow: ./workflow.yaml
    stubs:
      - task: log
        where: 1 / (1 - 1) == 1
      - task: log
        where: inputs.message == 'goodbye'
    expect:
      outputs: {}
`)

	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())

	found := false
	for _, f := range c.GetFailures() {
		if f.GetField() != "expect.failed" {
			continue
		}
		msg := f.GetMessage()
		// Both stubs were tried, and the first's evaluation failure is
		// reported rather than the invocation aborting the moment it hit it.
		require.Contains(t, msg, "1 / (1 - 1) == 1")
		require.Contains(t, msg, "-> error:")
		require.Contains(t, msg, "division by zero")
		require.Contains(t, msg, "inputs.message == 'goodbye'")
		require.Contains(t, msg, "-> false")
		found = true
	}
	require.True(t, found, "expected an expect.failed diagnostic showing both stub verdicts; got %v", c.GetFailures())
}

// TestUnmatchedStubValueTruncatesOnARuneBoundary is the Codex-review
// follow-up: eliding an overlong value at a fixed byte offset can land in the
// middle of a multi-byte UTF-8 sequence, producing invalid UTF-8 that
// encoding/json (and so `-o json`'s protojson rendering) refuses to encode.
// The input is built so the 200-byte cut point falls inside a multi-byte
// rune: the rendered, quoted value is `"` + 197 ASCII bytes + the 3-byte
// character 中 (occupying bytes 198-200) + more filler, so a byte-200 cut
// lands after the character's first two bytes rather than on a boundary.
func TestUnmatchedStubValueTruncatesOnARuneBoundary(t *testing.T) {
	t.Parallel()

	overlong := strings.Repeat("a", 197) + "中" + strings.Repeat("b", 20)

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: greet
inputs:
  text:
    type: string
    required: true
steps:
  - id: greet
    log:
      message: ${inputs.text}
outputs: {}
`)
	writeFile(t, dir+"/workflow.test.yaml", fmt.Sprintf(`
tests:
  - name: an overlong value with a multibyte rune at the cut point
    workflow: ./workflow.yaml
    inputs:
      text: %q
    stubs:
      - task: log
        where: inputs.message == 'goodbye'
    expect:
      outputs: {}
`, overlong))

	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())

	found := false
	for _, f := range c.GetFailures() {
		if f.GetField() != "expect.failed" {
			continue
		}
		msg := f.GetMessage()
		require.True(t, utf8.ValidString(msg), "truncated failure message is not valid UTF-8: %q", msg)
		// The same report the way it would be marshaled for `-o json`, which
		// is what actually breaks when a cut lands mid-rune.
		encoded, err := json.Marshal(c.GetFailures())
		require.NoError(t, err, "a failure message split mid-rune must still be valid JSON text")
		require.True(t, utf8.Valid(encoded))
		found = true
	}
	require.True(t, found, "expected an expect.failed diagnostic carrying the truncated value; got %v", c.GetFailures())
}
