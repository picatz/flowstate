package flowtest_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// #1687: a `returns:` stub on a step that shapes its outputs used to become
// the step's outputs verbatim, the shaping never ran, and the first thing to
// notice was whatever read a shaped name two screens away — an `undo:` input,
// naming the expression and not the stub.

// shapedUndoWorkflow is the issue's own shape: a step that shapes its outputs
// and registers a compensation reading one of them.
const shapedUndoWorkflow = `
edition: v2026.3
name: reserve
vars:
  api: https://api.internal
steps:
  - id: reserve
    http:
      method: POST
      url: ${vars.api + "/post"}
      parse_json: true
      outputs:
        id: ${"res-" + json_parse(response.body)["order"]}
    undo:
      log:
        message: ${"releasing reservation " + steps.reserve.id}
  - id: done
    log:
      message: ${"reserved " + steps.reserve.id}
`

func TestARawResponseReturnedForAShapedStepIsRefusedAtTheStub(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", shapedUndoWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: the newcomer's stub
    workflow: ./workflow.yaml
    stubs:
      - task: http
        where: inputs.url.endsWith("/post")
        returns: {status_code: 200, body: '{"order": "ord-1"}'}
      - task: log
        returns: {}
    expect:
      ran: [reserve, done]
`))

	// Refused before the run, at the stub — a case-level error, the way a
	// stub aimed at a task with no raw semantics is — because every http step
	// in this workflow shapes its outputs, so no invocation could take the
	// raw response.
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.Empty(t, c.GetFailures(), "the case ran; the refusal was meant to come before the run:\n%v", c.GetFailures())
	require.Contains(t, c.GetError(), `stub 1 for task "http"`, "the refusal does not name the stub:\n%s", c.GetError())
	require.Contains(t, c.GetError(), `step "reserve" shapes its outputs from the response into id`)
	require.Contains(t, c.GetError(), "carries body, status_code — the task's own response fields")
	require.Contains(t, c.GetError(), "write response:", "the refusal does not name the key that runs the shaping")
}

// TestAShapedNameReturnedForAShapedStepIsTheStepsAnswer is the rule the shipped
// examples rely on: `returns:` supplies the shaped outputs, and one that carries
// a shaped name is exactly what a later step reads.
func TestAShapedNameReturnedForAShapedStepIsTheStepsAnswer(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", shapedUndoWorkflow)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: the shaped answer
    workflow: ./workflow.yaml
    stubs:
      - step: reserve
        returns: {id: res-ord-1}
      - task: log
        where: inputs.message == 'reserved res-ord-1'
        returns: {}
    expect:
      ran: [reserve, done]
`))

	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}

// TestAnAnswerNothingReadsIsNotRefused keeps the refusal to the mistake it is
// for: a case that does not care what a shaped step reported may return
// nothing, or a marker no step reads — examples/deployment-reconciler answers
// its shaped `converge` step with `accepted: true` and nothing downstream
// reads it — and neither is the raw response the shaping wanted.
func TestAnAnswerNothingReadsIsNotRefused(t *testing.T) {
	t.Parallel()

	for name, returns := range map[string]string{
		"nothing":  "{}",
		"a marker": "{accepted: true}",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: unread
steps:
  - id: reserve
    http:
      method: POST
      url: https://api.internal/post
      outputs:
        id: ${"res-" + response.body}
  - id: done
    log:
      message: reserved
`)
			report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: nothing reads the step
    workflow: ./workflow.yaml
    stubs:
      - step: reserve
        returns: `+returns+`
      - task: log
        returns: {}
    expect:
      ran: [reserve, done]
`))

			require.Empty(t, report.GetRefused())
			c := report.GetCases()[0]
			require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
		})
	}
}

// TestARawResponseIsRefusedWhenItLandsOnAShapedStep is the answer-time half:
// a task-form stub some unshaped http step could take is not refused at load,
// and is refused at the stub the moment it answers a shaped step's invocation.
func TestARawResponseIsRefusedWhenItLandsOnAShapedStep(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: mixed
steps:
  - id: ping
    http:
      url: https://api.internal/ping
  - id: reserve
    http:
      method: POST
      url: https://api.internal/post
      outputs:
        id: ${"res-" + response.body}
  - id: done
    log:
      message: ${steps.reserve.id}
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: one stub for both http steps
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns: {status_code: 200, body: ord-1}
      - task: log
        returns: {}
    expect:
      ran: [ping, reserve, done]
`))

	require.Empty(t, report.GetRefused(), "a stub the unshaped step could take was refused at load")
	c := report.GetCases()[0]
	require.False(t, c.GetPassed(), "the raw response was accepted as the shaped step's outputs")
	require.Empty(t, c.GetError(), "the case was refused before the run, though the unshaped step could have taken the stub")

	var messages []string
	for _, failure := range c.GetFailures() {
		messages = append(messages, failure.GetMessage())
	}
	failure := strings.Join(messages, "\n")
	require.Contains(t, failure, `stub 1 for task "http"`, "the failure does not name the stub: %s", failure)
	require.Contains(t, failure, `step "reserve" shapes its outputs from the response into id`)
}
