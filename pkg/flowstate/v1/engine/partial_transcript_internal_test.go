package engine

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/testing/protocmp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// partialTranscriptProbe runs a workflow that is expected to fail and reports the
// transcript [runWorkflow] handed back with the failure, as a *success*.
//
// The inversion is what makes the assertion possible at all. Temporal drops a
// workflow's result whenever the workflow function returns an error, so a test
// executing [Run] on a failing workflow can read the failure and nothing else
// and the value under test here is precisely the thing beside the failure. Turning
// the failure into a result is therefore not a shortcut around the boundary; it is
// the only way to observe the durable driver's own answer to "what did this run
// do", which is the answer invariant 3 requires to match the local driver's.
//
// It is deliberately strict about the run having failed: a workflow that succeeded
// would otherwise be reported as a transcript that merely disagrees, which reads
// like the wrong bug.
func partialTranscriptProbe(ctx workflow.Context, st *v1.RunState) (*v1.Workflow_StepOutputs, error) {
	partial, err := runWorkflow(ctx, st)
	if err == nil {
		return nil, fmt.Errorf("expected the run to fail, it succeeded")
	}

	return partial, nil
}

// exhaustionSpanningProbe is [partialTranscriptProbe] for a run that must first
// Continue-As-New: a Continue-As-New error passes through untouched, so the
// harness can feed the carried state into a fresh segment by hand (the
// TestRunWorkflowLoopResultsAcrossCAN technique), and only a real failure is
// inverted into a readable result.
func exhaustionSpanningProbe(ctx workflow.Context, st *v1.RunState) (*v1.Workflow_StepOutputs, error) {
	partial, err := runWorkflow(ctx, st)
	if err == nil {
		return nil, fmt.Errorf("expected the run to fail, it succeeded")
	}
	var continued *workflow.ContinueAsNewError
	if errors.As(err, &continued) {
		return nil, err
	}

	return partial, nil
}

// TestRunWorkflowLoopExhaustionAcrossCAN drives a loop across a real
// Continue-As-New and then exhausts it on a resumed segment — the one path
// [v1.LoopExhaustedError.Truncated] exists for, and one no shared case can
// reach: every [tests.LoopExhaustionTranscriptCases] workflow exhausts within
// its first segment (a local run always does — it has no Continue-As-New at
// all), so this lives beside the durable driver's other CAN tests rather than
// in the shared set, exercised through [exhaustionSpanningProbe] because
// Temporal drops a failed workflow's result and the transcript beside the
// failure is exactly the value under test.
//
// Both halves of the honesty rule, so neither direction can regress silently:
//
//   - an UNREAD loop resumed past a Continue-As-New already dropped earlier
//     segments' iterations ([v1.LoopResumeResults]), so its exhaustion entry
//     must carry the `error` and omit `results` entirely — publishing the
//     surviving suffix would read as a short but complete account, the exact
//     lie [v1.LoopStateOutputsHonest] refuses on the completing path;
//   - a READ loop carried everything, so its exhaustion entry must keep the
//     whole account: every budgeted iteration, spanning every segment.
func TestRunWorkflowLoopExhaustionAcrossCAN(t *testing.T) {
	// Like loop_results_test.go's loopSpanningCAN, with an `until:` that never
	// holds: six budgeted iterations under StepsBudget 1, so the loop is
	// guaranteed to suspend before its budget is spent and the exhaustion is
	// guaranteed to happen on a resumed segment.
	exhaustsAcrossCAN := func(read bool) *v1.Workflow {
		wf := &v1.Workflow{
			Name:    "loop-can-exhaustion",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				{
					Id: "loop",
					Kind: &v1.Node_Loop{Loop: &v1.Loop{
						State:         "n",
						Initial:       v1.NewLiteral(int64(0)),
						Update:        v1.NewExpr("n + 1"),
						Until:         v1.NewExpr("false"),
						MaxIterations: 6,
						Body: []*v1.Node{{
							Id: "tick",
							Kind: &v1.Node_Task{Task: &v1.Task{
								Name:   "log",
								Inputs: map[string]*v1.Value{"message": v1.NewLiteral("tick")},
							}},
						}},
					}},
				},
			},
		}
		if read {
			wf.DeclaredOutputs = []*v1.OutputDeclaration{
				{Name: "count", Value: v1.NewExpr("size(steps.loop.results)")},
			}
		}
		return wf
	}

	for _, tc := range []struct {
		name string
		read bool
	}{
		{name: "an unread loop that dropped history omits results from its exhaustion entry", read: false},
		{name: "a read loop's exhaustion entry keeps every segment's iterations", read: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &v1.RunState{Workflow: exhaustsAcrossCAN(tc.read), StepsBudget: 1}

			var (
				out      v1.Workflow_StepOutputs
				got      bool
				segments int
			)
			for segments = 0; segments < 20; segments++ {
				testSuite := &testsuite.WorkflowTestSuite{}
				env := testSuite.NewTestWorkflowEnvironment()
				env.RegisterWorkflow(exhaustionSpanningProbe)
				env.OnActivity(Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(Task)
				env.OnActivity(TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(TaskInScope)
				env.OnActivity(WorkflowVars, mock.Anything, mock.Anything).Return(WorkflowVars)

				env.ExecuteWorkflow(exhaustionSpanningProbe, state)
				require.True(t, env.IsWorkflowCompleted())

				err := env.GetWorkflowError()
				var continued *workflow.ContinueAsNewError
				if errors.As(err, &continued) {
					next := &v1.RunState{}
					require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next))
					state = next
					continue
				}

				require.NoError(t, err, "the probe inverts the exhaustion into a result")
				require.NoError(t, env.GetWorkflowResult(&out))
				got = true
				break
			}

			require.True(t, got, "the run never exhausted within %d segments", segments)
			require.Greater(t, segments, 0,
				"the loop exhausted within its first segment, so this proves nothing "+
					"the single-segment shared cases did not already cover")

			entry := out.GetStepValues()["loop"]
			require.NotNil(t, entry, "the exhausted loop must have a transcript entry")
			require.Contains(t,
				entry.GetNamedValues()["error"].GetLiteral().GetStringValue(),
				"ran its full budget of 6 iterations",
				"the entry must carry the exhaustion sentence")

			results, present := entry.GetNamedValues()["results"]
			if tc.read {
				require.True(t, present, "a read loop's exhaustion entry must keep its account")
				require.Len(t, results.GetLiteral().GetListValue().GetValues(), 6,
					"every budgeted iteration, spanning every segment — a suffix here means "+
						"the resume dropped history a reader was entitled to")
			} else {
				require.False(t, present,
					"an unread loop that dropped history across a Continue-As-New must omit `results` "+
						"from its exhaustion entry entirely rather than publish a suffix nothing marks as partial")
			}
		})
	}
}

// TestRunWorkflowLoopExhaustionTranscript is the durable half of what an
// exhausted loop's transcript entry says (#157's question 3), run through
// [partialTranscriptProbe] for the reason every transcript case here is: the
// value under test rides beside a failure Temporal would otherwise drop.
//
// The local driver runs the identical [tests.LoopExhaustionTranscriptCases].
// What the pairing holds the two to: the iterations that ran are recorded under
// the failed loop's own `results` — a tolerated failure among them naming the
// state it carried under `item` — and an iteration the spent budget never let
// start has no entry at all, on either driver, so ran-and-failed and
// never-attempted cannot blur into each other on just one of them.
func TestRunWorkflowLoopExhaustionTranscript(t *testing.T) {
	for _, test := range tests.LoopExhaustionTranscriptCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(partialTranscriptProbe)
			env.OnActivity(Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(Task)
			env.OnActivity(TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(TaskInScope)
			env.OnActivity(WorkflowVars, mock.Anything, mock.Anything).Return(WorkflowVars)

			env.ExecuteWorkflow(partialTranscriptProbe, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))

			// Compared whole: an entry for an iteration the loop never ran is as
			// wrong as a missing entry for one it did.
			require.Empty(t, cmp.Diff(test.Expected, &out, protocmp.Transform()))
		})
	}
}

// TestRunWorkflowPartialTranscript is the durable half of what a failed run hands
// back about what it did (issue #453).
//
// The local driver runs the identical [tests.PartialTranscriptCases]. Pairing them
// is the whole point: the record both drivers accumulate as they walk was always
// there and never returned, so the risk in returning it was never "does it exist"
// but "do the two contain the same thing", and in particular whether the step that
// *ended* the run is in one and not the other, which is the one entry neither
// driver wrote before this.
func TestRunWorkflowPartialTranscript(t *testing.T) {
	for _, test := range tests.PartialTranscriptCases() {
		t.Run(test.Name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(partialTranscriptProbe)
			env.OnActivity(Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(Task)
			env.OnActivity(TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(TaskInScope)
			env.OnActivity(WorkflowVars, mock.Anything, mock.Anything).Return(WorkflowVars)

			env.ExecuteWorkflow(partialTranscriptProbe, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))

			// Compared whole for the reason the local half compares it whole: a
			// transcript carrying a step the run never reached is as wrong as one
			// missing a step it did.
			require.Empty(t, cmp.Diff(test.Expected, &out, protocmp.Transform()))
			require.Nil(t, out.GetRunOutputs(),
				"a run that failed produced no declared outputs")
		})
	}
}
