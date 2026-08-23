package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A workflow whose http step shapes its own outputs from the response — the
// expressions a `returns:` stub bypasses entirely, and the reason `response:`
// exists (#925).
const shapingWorkflow = `
edition: v2026.3
name: shaper
steps:
  - id: build
    http:
      method: POST
      url: https://ci.internal/build
      parse_json: true
      expect: ${response.status_code == 200}
      outputs:
        artifact: ${response.json.artifact}
outputs:
  artifact:
    value: ${steps.build.artifact}
`

// TestResponseStubRunsTheStepsOwnShaping is the case issue #925 exists for: a
// stub that answers with a raw response makes the step's own `outputs:` and
// `expect:` run for real. With `returns:`, those expressions are dead code in
// every green suite — this file's mapping could misspell `artifact` and no
// test would ever notice; the sibling test below proves the typo now fails.
func TestResponseStubRunsTheStepsOwnShaping(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", shapingWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: the mapping runs over the declared response
    workflow: ./workflow.yaml
    stubs:
      - step: build
        response:
          status_code: 200
          body: {artifact: checkout-1.2.3}
    expect:
      outputs:
        artifact: checkout-1.2.3
`))

	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}

// TestResponseStubCatchesAMappingTypo is the mutation half, run against a
// deliberately-broken mapping rather than by editing the harness: the exact
// defect `returns:` hides — a misspelled path in `outputs:` — fails loudly
// when the shaping actually runs, and the failure names the mapping.
func TestResponseStubCatchesAMappingTypo(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: shaper
steps:
  - id: build
    http:
      method: POST
      url: https://ci.internal/build
      parse_json: true
      outputs:
        artifact: ${response.json.artifcat}
outputs: {}
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: the typo the shaping bypass used to hide
    workflow: ./workflow.yaml
    stubs:
      - step: build
        response:
          status_code: 200
          body: {artifact: checkout-1.2.3}
    expect:
      failed: true
      error_contains: artifcat
`))

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(),
		"the misspelled mapping must fail the run, naming the typo: %v / %v", c.GetError(), c.GetFailures())
}

// TestResponseStubRunsExpect: the step's own `expect:` decides success over
// the declared response, so a 500 fails the step exactly as production would
// — through the production classification, not a stub's imitation of one.
func TestResponseStubRunsExpect(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", shapingWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: a 500 is refused by the step's own expect
    workflow: ./workflow.yaml
    stubs:
      - step: build
        response:
          status_code: 500
          body: {}
    expect:
      failed: true
      error_contains: "500"
`))

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}

// TestResponseOnATaskWithoutRawSemanticsIsRefused: only a task that defines
// what a raw response means can evaluate one; `log` defines none, and the
// refusal comes before the run, naming the spelling that exists.
func TestResponseOnATaskWithoutRawSemanticsIsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", ghostWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: a raw response aimed at log
    workflow: ./workflow.yaml
    stubs:
      - task: log
        response: {status_code: 200}
    expect:
      ran: [greet]
`))

	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Contains(t, c.GetError(), `task "log" does not evaluate a raw response`)
	require.Contains(t, c.GetError(), "use returns:")
}

// TestResponseWithReturnsIsRefusedAtLoad: one answer per stub, said once.
func TestResponseWithReturnsIsRefusedAtLoad(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", shapingWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: both stanzas at once
    workflow: ./workflow.yaml
    stubs:
      - step: build
        response: {status_code: 200}
        returns: {artifact: x}
    expect:
      outputs: {artifact: x}
`))

	require.Contains(t, report.GetRefused(), "declares both response and returns")
}

// TestResponseUnknownFieldIsRefused: a misspelled `staus_code:` silently
// defaulting to 200 would be the silent-nothing failure the diagnostics rule
// forbids, so the http task names its three fields and refuses the fourth.
func TestResponseUnknownFieldIsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", shapingWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: a field the task does not define
    workflow: ./workflow.yaml
    stubs:
      - step: build
        response:
          staus_code: 200
    expect:
      failed: true
      error_contains: '"staus_code" is not one of them'
`))

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}

// TestResponseStatusOutsideTheWireRangeIsRefused pins the Codex finding on
// #982: the decoder's int64 narrowed unchecked through the int32 the default
// outputs carry, so a stub declaring 4294967496 wrapped to 200 and could
// satisfy a success expectation — a test passing on a status no HTTP response
// could carry. Both directions of the range are refused before any response
// is constructed; the wrap-around value is the exact one from the finding,
// and without the range check the first case here passes.
func TestResponseStatusOutsideTheWireRangeIsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", shapingWorkflow)

	for name, code := range map[string]string{"wraps to 200": "4294967496", "negative": "-1", "two digits": "99"} {
		report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: `+name+`
    workflow: ./workflow.yaml
    stubs:
      - step: build
        response:
          status_code: `+code+`
          body: {artifact: x}
    expect:
      outputs:
        artifact: x
`))
		c := report.GetCases()[0]
		require.False(t, c.GetPassed(), "status_code %s must not produce a satisfiable response", code)
		var account string
		for _, f := range c.GetFailures() {
			account += f.GetMessage()
		}
		require.Contains(t, account, "must be a three-digit HTTP status",
			"status_code %s: got error=%q failures=%v", code, c.GetError(), c.GetFailures())
	}
}

// TestResponseHeadersAndStringBodyReachTheShaping: headers land in the
// response scope the shaping reads, a string body arrives verbatim, and an
// omitted status_code is the ordinary 200.
func TestResponseHeadersAndStringBodyReachTheShaping(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: echo
steps:
  - id: fetch
    http:
      method: GET
      url: https://api.internal/echo
      outputs:
        kind: ${response.headers["Content-Type"][0]}
        text: ${response.body}
outputs:
  kind:
    value: ${steps.fetch.kind}
  text:
    value: ${steps.fetch.text}
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: headers and body arrive as the task exposes them
    workflow: ./workflow.yaml
    stubs:
      - step: fetch
        response:
          body: plain text answer
          headers:
            Content-Type: text/plain
    expect:
      outputs:
        kind: text/plain
        text: plain text answer
`))

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}
