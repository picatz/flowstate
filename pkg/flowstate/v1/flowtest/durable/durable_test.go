package durable_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest/durable"
)

func logStep(id, message string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
		Name:   "log",
		Inputs: map[string]*v1.Value{"message": v1.NewLiteral(message)},
	}}}
}

// A run takes one segment per step: the budget forces a Continue-As-New between
// every pair, which is the whole point of the driver.
func TestARunContinuesAsNewBetweenEveryPairOfSteps(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{Name: "w", Profile: v1.CurrentProfile, Steps: []*v1.Node{
		logStep("a", "x"), logStep("b", "y"), logStep("c", "z"),
	}}
	res, err := durable.Run(t.Context(), wf, nil, v1.TaskRuntime{})
	require.NoError(t, err)
	assert.Equal(t, 3, res.Segments, "three steps, one per segment")
	assert.Contains(t, res.Outputs.GetStepValues(), "c", "the last step is always retained")
}

// A workflow that fails reports the failure and how far it got, so a caller can
// say where the seam run stopped.
func TestAFailingRunReportsItsErrorAndTheSegmentsItTook(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{Name: "w", Profile: v1.CurrentProfile, Steps: []*v1.Node{
		logStep("a", "x"),
		{Id: "boom", Kind: &v1.Node_Task{Task: &v1.Task{Name: "no-such-task"}}},
	}}
	res, err := durable.Run(t.Context(), wf, nil, v1.TaskRuntime{})
	require.Error(t, err)
	require.NotNil(t, res)
	assert.GreaterOrEqual(t, res.Segments, 1)
}
