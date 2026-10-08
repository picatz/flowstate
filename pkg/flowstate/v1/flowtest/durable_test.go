package flowtest_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest/durable"
)

// durableRunner is what `flow test --driver both` hands the harness.
func durableRunner(ctx context.Context, req flowtest.DurableRequest) (flowtest.DurableResult, error) {
	signals := make([]durable.Signal, 0, len(req.Signals))
	for _, s := range req.Signals {
		signals = append(signals, durable.Signal{Name: s.Name, Offset: s.At, Payload: s.Payload, Sender: s.Sender})
	}
	res, err := durable.Run(ctx, durable.Request{
		Workflow: req.Workflow, Inputs: req.Inputs, Start: req.Start, Runtime: req.Runtime, Signals: signals,
	})
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

// The proof is not vacuous: when the durable run's retained value differs from
// the local one, the case fails at that step, naming the value, so a green here
// is a claim that could have been red.
func TestADriverDisagreementFailsTheCaseAtTheValueThatDiffers(t *testing.T) {
	t.Parallel()

	tamper := func(ctx context.Context, req flowtest.DurableRequest) (flowtest.DurableResult, error) {
		res, err := durableRunner(ctx, req)
		for _, outputs := range res.Outputs.GetStepValues() {
			for name := range outputs.GetNamedValues() {
				outputs.NamedValues[name] = v1.NewLiteral("tampered")
			}
		}

		return res, err
	}
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
      ran: [greeting, shout, after]
`)
	run := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{Durable: tamper})
	c := run.Report.GetCases()[0]
	require.False(t, c.GetPassed(), "the drivers disagree and the case must say so")
	require.NotEmpty(t, c.GetFailures())
	failure := c.GetFailures()[0]
	assert.Equal(t, "driver", failure.GetField())
	assert.Contains(t, failure.GetMessage(), "changed once the run continued as new")
}

// A scripted signal is replayed to the durable run: one that arrives before its
// gate is carried across the Continue-As-New, one due later arrives when the
// virtual clock reaches it, and the gate reads the same payload on both drivers.
func TestScriptedSignalsAreProvedAcrossTheSeams(t *testing.T) {
	t.Parallel()

	run := runBoth(t, `
edition: v2026.4
name: gated
steps:
  - id: before
    value: ${"one"}
  - id: early
    wait_for_signal:
      name: early
      timeout: 1h
  - id: between
    value: ${steps.early.payload.n + 1}
  - id: late
    wait_for_signal:
      name: late
      timeout: 1h
outputs:
  early:
    value: ${steps.early.payload.n}
  late:
    value: ${steps.late.payload.n}
  between:
    value: ${steps.between.value}
  late_timed_out:
    value: ${steps.late.timed_out}
`, `
tests:
  - name: both arrive
    workflow: ./workflow.yaml
    signals:
      - name: early
        payload: {n: 1}
      - name: late
        at: 10m
        payload: {n: 7}
    expect:
      outputs:
        early: 1
        late: 7
        between: 2
        late_timed_out: false
`)
	c := run.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v %v", c.GetError(), c.GetFailures())
	assert.Empty(t, c.GetWarnings(), "the case was proved durably, not left local")
}

// A workflow reading a fact the drivers answer differently by design stays local
// and says why, so the proof is never silently skipped.
func TestACaseReadingRunLocalStaysLocalAndSaysSo(t *testing.T) {
	t.Parallel()

	run := runBoth(t, `
edition: v2026.4
name: local-gated
steps:
  - id: gate
    if: ${run.local}
    value: ${"local"}
`, `
tests:
  - name: ran
    workflow: ./workflow.yaml
    expect:
      ran: [gate]
`)
	c := run.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v %v", c.GetError(), c.GetFailures())
	require.Len(t, c.GetWarnings(), 1)
	assert.Equal(t, "driver", c.GetWarnings()[0].GetField())
	assert.Contains(t, c.GetWarnings()[0].GetMessage(), "local only")
	assert.Contains(t, c.GetWarnings()[0].GetMessage(), "run.local")
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
