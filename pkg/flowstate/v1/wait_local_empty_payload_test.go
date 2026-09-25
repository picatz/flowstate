package flowstatev1_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

func TestEmptySignalPayloadCasesLocally(t *testing.T) {
	t.Parallel()

	conformance.AssertEmptySignalPayloadCases(t, func(workflow *v1.Workflow, signal string, payload *v1.Node_Outputs) (*v1.Workflow_StepOutputs, error) {
		signals := v1.NewLocalSignals()
		if err := signals.Deliver(signal, payload); err != nil {
			return nil, err
		}

		return v1.Run(v1.NewContextWithSignalWaiter(t.Context(), signals), workflow)
	})
}
