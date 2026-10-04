package flowtest_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// runInvocationCase runs one case body over the defaults fixture and returns
// the only case's result, so each test states just the claim it is about.
func runInvocationCase(t *testing.T, amount int, claims string) (passed bool, failures []string, refused string) {
	t.Helper()

	dir := t.TempDir()
	writeDefaultsWorkflow(t, dir)
	report := flowtest.RunFile(writeInline(t, dir, `
defaults:
  stubs:
    - task: log
      returns: {}
    - task: http
      returns: {tag: t}
tests:
  - name: the case
    workflow: ./workflow.yaml
    inputs: {amount: `+strconv.Itoa(amount)+`}
    expect:
      failed: false
      invocations:
`+claims))
	if report.GetRefused() != "" {
		return false, nil, report.GetRefused()
	}
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	for _, f := range c.GetFailures() {
		failures = append(failures, f.GetMessage())
	}

	return c.GetPassed(), failures, c.GetError()
}

func TestInvocationsCountsHoldAndFail(t *testing.T) {
	t.Parallel()

	passed, failures, _ := runInvocationCase(t, 1, `
        - {task: log, count: 1}
        - {task: http, at_least: 1, at_most: 1}
        - {step: small, count: 1}
        - {step: large, never: true}
`)
	assert.True(t, passed, "%v", failures)

	// The negative direction: each claim, made wrong, fails and says why.
	for claim, want := range map[string]string{
		`- {task: log, count: 2}`:                  `expected task "log" to be invoked 2 time(s), got 1`,
		`- {step: small, never: true}`:             `expected step "small" never to be invoked, but it was invoked 1 time(s)`,
		`- {task: http, at_least: 3}`:              `at least 3 time(s), got 1`,
		`- {step: large, at_most: 0, at_least: 0}`: ``,
	} {
		passed, failures, _ := runInvocationCase(t, 1, "        "+claim+"\n")
		if want == "" {
			assert.True(t, passed, "%v", failures)
			continue
		}
		assert.False(t, passed, claim)
		require.Len(t, failures, 1, claim)
		assert.Contains(t, failures[0], want)
	}
}

func TestInvocationsOrder(t *testing.T) {
	t.Parallel()

	passed, failures, _ := runInvocationCase(t, 1, "        - order: [announce, small]\n")
	assert.True(t, passed, "%v", failures)

	passed, failures, _ = runInvocationCase(t, 1, "        - order: [small, announce]\n")
	assert.False(t, passed)
	require.Len(t, failures, 1)
	assert.Contains(t, failures[0], "invoked in the order [announce, small]")

	// A step that never ran cannot be ordered: the claim fails, not skips.
	passed, failures, _ = runInvocationCase(t, 1, "        - order: [announce, large]\n")
	assert.False(t, passed)
	require.Len(t, failures, 1)
	assert.Contains(t, failures[0], `step "large"`)
}

func TestInvocationsRefusedBeforeTheRun(t *testing.T) {
	t.Parallel()

	for claim, want := range map[string]string{
		`- {task: log}`:                          "say how many",
		`- {count: 1}`:                           "exactly one of",
		`- {task: log, step: small, count: 1}`:   "exactly one of",
		`- {task: log, count: 1, never: true}`:   "alternatives",
		`- {task: log, count: -1}`:               "negative",
		`- {task: log, at_least: 3, at_most: 1}`: "exceeds",
		`- {order: [announce]}`:                  "at least two",
		`- {order: [announce, announce]}`:        "twice",
		`- {order: [announce, small], count: 1}`: "stands alone",
		`- {step: smal, count: 1}`:               `did you mean "small"`,
		`- {task: lgo, never: true}`:             `did you mean "log"`,
	} {
		passed, _, refused := runInvocationCase(t, 1, "        "+claim+"\n")
		assert.False(t, passed, claim)
		assert.Contains(t, refused, want, claim)
	}
}

// TestInvocationsCountAttemptsAndKeepCalleesApart pins the two counting rules
// the docs state: a retried step counts every attempt, and a step claim about
// the workflow under test never counts a callee's identically named step.
func TestInvocationsCountAttemptsAndKeepCalleesApart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/callee.yaml", `
edition: v2026.4
name: callee
steps:
  - id: notify
    log:
      message: from callee
`)
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.4
name: caller
steps:
  - id: notify
    log:
      message: from caller
  - id: invoke
    call: ./callee.yaml
  - id: flaky
    retry:
      attempts: 3
      interval: 1s
    log:
      message: flaky
  - id: left
    parallel:
      - steps:
          - id: a
            log: {message: a}
      - steps:
          - id: b
            log: {message: b}
`)
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: counts
    workflow: ./workflow.yaml
    stubs:
      - step: flaky
        times: 2
        fails: {message: no}
      - task: log
        returns: {}
    expect:
      failed: false
      invocations:
        - {step: notify, count: 1}
        - {task: log, count: 7}
        - {step: flaky, count: 3}
  - name: order inside a parallel block is refused
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      invocations:
        - order: [a, b]
`))
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2)
	assert.True(t, report.GetCases()[0].GetPassed(), "%v %s", report.GetCases()[0].GetFailures(), report.GetCases()[0].GetError())
	assert.Contains(t, report.GetCases()[1].GetError(), "not observable")
}

// TestAnEmptyInvocationsListIsNoClaim: unlike `ran: []`, an empty list names
// no target, so it asserts nothing and a row stating it keeps its entry's.
func TestAnEmptyInvocationsListIsNoClaim(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeDefaultsWorkflow(t, dir)
	alone := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: empty alone claims nothing
    workflow: ./workflow.yaml
    inputs: {amount: 1}
    expect: {invocations: []}
`))
	assert.Contains(t, alone.GetRefused(), "claims nothing")

	report := flowtest.RunFile(writeInline(t, dir, `
defaults:
  stubs:
    - {task: log, returns: {}}
    - {task: http, returns: {tag: t}}
tests:
  - name: tabled
    workflow: ./workflow.yaml
    inputs: {amount: 1}
    expect:
      invocations:
        - {task: log, count: 99}
    cases:
      - name: empty row
        expect: {invocations: []}
`))
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	assert.False(t, report.GetCases()[0].GetPassed(), "the row erased its entry's claim")
}
