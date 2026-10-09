package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// TestTheDurableDriverRefusesAModule is the second driver's half of
// TestEveryEntryRefusesASpecWithNoSteps: a spec that reaches a worker with no
// steps fails the run and says it is a module, rather than completing empty.
func TestTheDurableDriverRefusesAModule(t *testing.T) {
	t.Parallel()

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	engine.Register(env)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name:           "ids",
		DeclaredErrors: []*v1.ErrorDeclaration{{Name: "NotFound"}},
	}})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "is a module (no steps); import it with use:, don't run it")
}
