package engine_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// TestRunWorkflowAsyncUnwind is the durable half of [tests.AsyncUnwindCases].
//
// Both claims were found by the schedule search over the local driver and both
// are claims about the execution model rather than about one driver, so this
// side runs them too — the corpus's own rule, and the check that matters here is
// that this driver already keeps them for reasons of its own: the scope's exit
// waits on the coroutine (asyncStep.wait) and the branch logs merge by branch
// index. A driver that stopped doing either would fail here, which is exactly
// what the local driver did before #477 slice 0.
//
// The interleaving is Temporal's rather than anything this test arranges, which
// is why the claims are written against [tests.UndoCase.UnorderedPrefix]: the
// concurrent work is a set and the unwind after it is a sequence.
func TestRunWorkflowAsyncUnwind(t *testing.T) {
	for index, outline := range tests.AsyncUnwindCases(undoPlaceholderBase) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := tests.NewUndoServer(t)
			test := tests.AsyncUnwindCases(base)[index]

			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(engine.Run)
			env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
			env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)
			env.OnActivity(engine.WorkflowVars, mock.Anything, mock.Anything).Return(engine.WorkflowVars)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})
			require.True(t, env.IsWorkflowCompleted())

			err := env.GetWorkflowError()
			require.Error(t, err, "the run was expected to fail")
			require.Contains(t, err.Error(), test.Summary,
				"the failure does not carry the account of what was compensated")

			tests.AssertRecorded(t, test, recorded())
		})
	}
}
