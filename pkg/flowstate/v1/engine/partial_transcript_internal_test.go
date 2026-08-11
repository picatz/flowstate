package engine

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
			env.OnActivity(Task, mock.Anything, mock.Anything, mock.Anything).Return(Task)
			env.OnActivity(TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(TaskInScope)
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
