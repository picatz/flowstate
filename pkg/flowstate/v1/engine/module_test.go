package engine_test

import (
	"testing"

	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestNoStepsCasesDurably runs the shared no-steps cases through the durable
// driver; module_test.go in the parent package runs the same cases locally.
func TestNoStepsCasesDurably(t *testing.T) {
	t.Parallel()

	conformance.AssertNoStepsCases(t, func(w *v1.Workflow) error {
		env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
		engine.Register(env)
		env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: w})
		return env.GetWorkflowError()
	})
}
