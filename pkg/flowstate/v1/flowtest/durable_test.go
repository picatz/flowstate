package flowtest_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest/durable"
)

// durableRunner is what `flow test --driver both` hands the harness.
func durableRunner(ctx context.Context, wf *v1.Workflow, inputs map[string]*v1.Value, start time.Time, runtime v1.TaskRuntime) (flowtest.DurableResult, error) {
	res, err := durable.Run(ctx, wf, inputs, start, runtime)
	if res == nil {
		return flowtest.DurableResult{}, err
	}

	return flowtest.DurableResult{Outputs: res.Outputs, Segments: res.Segments}, err
}

func runBoth(t *testing.T, workflow, tests string) *flowtest.RunResult {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), workflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, tests)
	run := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{Durable: durableRunner})

	return &run
}

const durableWorkflow = `
edition: v2026.4
name: seams
inputs:
  who:
    type: string
    required: true
steps:
  - id: greeting
    value: ${"hello " + inputs.who}
  - id: shout
    log:
      message: ${steps.greeting.value}
  - id: after
    value: ${steps.greeting.value + "!"}
`

// A case that passes locally is run again with a Continue-As-New between every
// pair of steps, and passes there too when the state it carries is whole.
func TestACaseThatPassesLocallyAlsoPassesAcrossEverySeam(t *testing.T) {
	t.Parallel()

	run := runBoth(t, durableWorkflow, `
tests:
  - name: greets
    workflow: ./workflow.yaml
    inputs: {who: world}
    stubs: [{task: log, returns: {}}]
    expect:
      ran: [greeting, shout, after]
`)
	require.Len(t, run.Report.GetCases(), 1)
	c := run.Report.GetCases()[0]
	assert.True(t, c.GetPassed(), "%v", c.GetFailures())
	assert.Empty(t, c.GetWarnings(), "an eligible case reports no local-only note")
}

// The proof is not vacuous: a value that differs by design between the drivers
// (`run.local`) and is read by a later step fails the case at that step, naming
// the value, so a green here is a claim that could have been red.
func TestADriverDisagreementFailsTheCaseAtTheValueThatDiffers(t *testing.T) {
	t.Parallel()

	run := runBoth(t, `
edition: v2026.4
name: drivers
steps:
  - id: where
    value: ${run.local}
  - id: use
    log:
      message: ${string(steps.where.value)}
`, `
tests:
  - name: reads the driver
    workflow: ./workflow.yaml
    stubs: [{task: log, returns: {}}]
    expect:
      ran: [where, use]
`)
	c := run.Report.GetCases()[0]
	require.False(t, c.GetPassed(), "the drivers disagree about run.local, and the case must say so")
	require.Len(t, c.GetFailures(), 1)
	failure := c.GetFailures()[0]
	assert.Equal(t, "driver", failure.GetField())
	assert.Equal(t, "where", failure.GetStep())
	assert.Contains(t, failure.GetMessage(), "where.value changed")
}

// A case the durable driver cannot take stays local and says why: the proof is
// never silently skipped.
func TestACaseWithSignalsStaysLocalAndSaysSo(t *testing.T) {
	t.Parallel()

	run := runBoth(t, `
edition: v2026.4
name: gated
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
`, `
tests:
  - name: answered
    workflow: ./workflow.yaml
    signals:
      - name: go
        payload: {}
    expect:
      ran: [gate]
`)
	c := run.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v %v", c.GetError(), c.GetFailures())
	require.Len(t, c.GetWarnings(), 1)
	assert.Equal(t, "driver", c.GetWarnings()[0].GetField())
	assert.Contains(t, c.GetWarnings()[0].GetMessage(), "local only")
	assert.Contains(t, c.GetWarnings()[0].GetMessage(), "signals")
}

// A step-scoped stub answers on the durable driver as on the local one, so a
// file that stubs by step id is proved, not skipped.
func TestAStepScopedStubAnswersAcrossTheSeams(t *testing.T) {
	t.Parallel()

	run := runBoth(t, `
edition: v2026.4
name: stepped
steps:
  - id: first
    log:
      message: one
  - id: second
    log:
      message: two
`, `
tests:
  - name: by step
    workflow: ./workflow.yaml
    stubs:
      - step: first
        returns: {}
      - step: second
        returns: {}
    expect:
      ran: [first, second]
`)
	c := run.Report.GetCases()[0]
	assert.True(t, c.GetPassed(), "%v %v", c.GetError(), c.GetFailures())
	assert.Empty(t, c.GetWarnings())
}

// Without a runner nothing changes: the default is the local driver alone.
func TestWithoutADurableRunnerNoCaseIsProvedTwice(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), durableWorkflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: greets
    workflow: ./workflow.yaml
    inputs: {who: world}
    stubs: [{task: log, returns: {}}]
    expect:
      ran: [greeting]
`)
	run := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	require.NotEmpty(t, run.Report.GetCases())
	require.True(t, run.Report.GetCases()[0].GetPassed())
	assert.Empty(t, run.Report.GetCases()[0].GetWarnings())
}
