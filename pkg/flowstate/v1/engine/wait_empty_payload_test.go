package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

func TestEmptySignalPayloadCasesDurably(t *testing.T) {
	t.Parallel()

	conformance.AssertEmptySignalPayloadCases(t, func(workflow *v1.Workflow, signal string, payload *v1.Node_Outputs) (*v1.Workflow_StepOutputs, error) {
		env := newWaitEnv(t)
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(signal, &v1.SignalDelivery{Payload: payload})
		}, 0)
		env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: workflow})

		require.True(t, env.IsWorkflowCompleted())
		if err := env.GetWorkflowError(); err != nil {
			return nil, err
		}

		var outputs v1.Workflow_StepOutputs
		require.NoError(t, env.GetWorkflowResult(&outputs))
		return &outputs, nil
	})
}
