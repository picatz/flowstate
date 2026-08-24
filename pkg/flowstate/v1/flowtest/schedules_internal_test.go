package flowtest

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
)

// An internal test, because what it needs is a case whose observables really do
// depend on the schedule — and the local engine will not give it one.
//
// That is not a gap in this file, it is the state of the engine, and both halves
// are worth writing down. `parallel:` runs every branch and reports the first
// failure *by declaration index*, merges in declaration order, and appends each
// branch's undo log in declaration order; an `async:` step's work is frozen
// against the outputs visible at its launch. So a legal Flowfile whose
// observables move with the schedule does not exist to be used as a fixture
// here, which is exactly the property `--seeds` exists to keep true and exactly
// what makes the divergence *path* unreachable from a `*.test.yaml` on disk.
//
// A fake case is therefore what proves the reporting works: it stands in for the
// engine defect this feature exists to catch, so that the day one arrives, the
// path that reports it has been exercised rather than assumed.

// fakeCase is a case whose result depends on which scheduler its context
// carries, driving [scheduleAccumulator.run] without an engine.
type fakeCase struct {
	name  string
	runs  int
	seeds []uint64
}

// run is a [caseRun] that records the schedule it was called under and hands
// back a transcript naming it, so the accumulator has something to disagree
// about.
func (f *fakeCase) run(transcriptFor func(scheduler v1.Scheduler) string) caseRun {
	return func(ctx context.Context) (*v1.TestCase, *v1.Workflow, *v1.Workflow_StepOutputs, []TranscriptLine, error) {
		scheduler := v1.SchedulerFromContext(ctx)
		f.runs++
		if seeded, ok := scheduler.(*v1.SeededScheduler); ok {
			f.seeds = append(f.seeds, seeded.Seed())
		}

		marker := transcriptFor(scheduler)

		return &v1.TestCase{Name: f.name, Passed: true},
			&v1.Workflow{Name: "fake"},
			&v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"step": {NamedValues: map[string]*v1.Value{
					"marker": {Kind: &v1.Value_Literal{Literal: &expr.Value{
						Kind: &expr.Value_StringValue{StringValue: marker},
					}}},
				}},
			}},
			nil,
			nil
	}
}

// steady is the ordinary shape: one transcript, whatever the schedule.
func steady(v1.Scheduler) string { return "same" }

// TestNoSeedsRunsEachCaseExactlyOnce is the compatibility promise `--seeds`
// defaults to, checked as a count rather than as an output comparison: at zero
// seeds nothing is explored, no digest is computed, and a case runs the one time
// `flow test` has always run it.
func TestNoSeedsRunsEachCaseExactlyOnce(t *testing.T) {
	accumulator := newScheduleAccumulator(dst.Budget{})
	fake := &fakeCase{name: "a case"}

	result, spec, transcript, _ := accumulator.run(t.Context(), fake.run(steady))

	assert.Equal(t, 1, fake.runs, "a run with no seeds must run each case exactly once")
	require.NotNil(t, result)
	require.NotNil(t, spec)
	require.NotNil(t, transcript)
	assert.Nil(t, accumulator.result(),
		"a run that explored nothing must report no schedule line at all, rather than an empty one")
}

// TestSeedsRunTheBaselineAndOneScheduleEach pins the shape of the search: the
// written-order baseline plus exactly one schedule per seed, with the seeds
// walking upward from Seed0 so that `--seed0` moves the search rather than
// widening it.
func TestSeedsRunTheBaselineAndOneScheduleEach(t *testing.T) {
	accumulator := newScheduleAccumulator(dst.Budget{Schedules: 3, Seed0: 7})
	fake := &fakeCase{name: "a case"}

	accumulator.run(t.Context(), fake.run(steady))

	assert.Equal(t, 4, fake.runs, "three seeds must run the baseline and three schedules")
	assert.Equal(t, []uint64{7, 8, 9}, fake.seeds, "the search must walk upward from Seed0")

	report := accumulator.result()
	require.NotNil(t, report)
	assert.Equal(t, 3, report.Schedules)
	assert.Equal(t, 1, report.Cases)
	assert.Nil(t, report.Divergence)
}

// markerOf reads back the schedule marker a [fakeCase] recorded in its
// transcript.
func markerOf(transcript *v1.Workflow_StepOutputs) string {
	return transcript.GetStepValues()["step"].GetNamedValues()["marker"].GetLiteral().GetStringValue()
}

// TestADivergenceNamesTheFirstFailingSeed is the reporting path a real engine
// defect would take: a case whose transcript depends on the schedule is caught,
// attributed to the *first* seed that produced it, and carries both renderings
// so a person can see what moved.
func TestADivergenceNamesTheFirstFailingSeed(t *testing.T) {
	accumulator := newScheduleAccumulator(dst.Budget{Schedules: 4, Seed0: 1})
	fake := &fakeCase{name: "an order-dependent case"}

	// Every seeded schedule disagrees with written order, so the first one — seed
	// 1 — is the one that must be reported. A later seed being named would mean
	// the search kept walking past a failure it had already found.
	_, _, transcript, _ := accumulator.run(t.Context(), fake.run(func(scheduler v1.Scheduler) string {
		if scheduler == v1.WrittenOrder {
			return "written"
		}

		return "seeded"
	}))

	// The run handed back — the verdict the file reports and the transcript its
	// coverage is measured from — is the written-order one, and this is the
	// load-bearing half. If a seeded schedule's run could be the reported one,
	// `flow test --seeds 24` would report a different pass/fail set than `flow
	// test`, and an author could not tell a regression from a seed they drew.
	assert.Equal(t, "written", markerOf(transcript),
		"the reported run must be the written-order one, never a seeded schedule's")

	report := accumulator.result()
	require.NotNil(t, report)
	require.NotNil(t, report.Divergence, "a transcript that moves with the schedule must be reported")

	divergence := report.Divergence
	assert.Equal(t, "an order-dependent case", divergence.Case)
	assert.Equal(t, uint64(1), divergence.Seed, "the first failing seed is the one worth replaying")
	assert.NotEqual(t, divergence.WrittenOrder, divergence.Seeded,
		"a divergence must carry the two renderings it was decided on")
}

// TestCaseObservablesComparesContentAndNotDiagnosticOrder is why
// [caseObservables] sorts. assertExpectation builds the `expect.outputs`
// diagnostics by ranging over the case's own map of expectations, so one case
// can report one set of failures in two orders with no schedule involved.
// Reporting that as a divergence would be a false diagnostic, which for a check
// nobody can act on is worse than a missed one.
func TestCaseObservablesComparesContentAndNotDiagnosticOrder(t *testing.T) {
	first := &v1.TestCase{Failures: []*v1.Diagnostic{
		{Field: "expect.outputs", Value: "a", Message: "expected 1, got 2"},
		{Field: "expect.outputs", Value: "b", Message: "expected 3, got 4"},
	}}
	reordered := &v1.TestCase{Failures: []*v1.Diagnostic{
		{Field: "expect.outputs", Value: "b", Message: "expected 3, got 4"},
		{Field: "expect.outputs", Value: "a", Message: "expected 1, got 2"},
	}}
	different := &v1.TestCase{Failures: []*v1.Diagnostic{
		{Field: "expect.outputs", Value: "a", Message: "expected 1, got 2"},
		{Field: "expect.outputs", Value: "b", Message: "expected 3, got 5"},
	}}

	assert.Equal(t, caseObservables(first, nil).Error(), caseObservables(reordered, nil).Error(),
		"the order two diagnostics are listed in is the harness's map iteration, not a claim")
	assert.NotEqual(t, caseObservables(first, nil).Error(), caseObservables(different, nil).Error(),
		"a changed diagnostic is a changed observable")

	assert.NoError(t, caseObservables(&v1.TestCase{Passed: true}, nil),
		"a case that passed with nothing to say has no observable failure")

	// The run's own error is an observable in its own right: `expect.failed: true`
	// passes while the run errored, so a failure text that moved with the schedule
	// would otherwise be invisible — and `expect.error_contains` is read out of it.
	assert.ErrorContains(t, caseObservables(&v1.TestCase{Passed: true}, errors.New("boom")), "boom")
}
