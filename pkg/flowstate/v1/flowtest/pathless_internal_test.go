package flowtest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPathlessRefusalCoversBothPathsACaseNames: the trigger payload is the
// second path a case can name, and it gets the same answer a relative
// workflow does — checked only after the workflow, which a refused case never
// reaches past. All-absolute is the shape [Run] with no directory exists for.
func TestPathlessRefusalCoversBothPathsACaseNames(t *testing.T) {
	t.Parallel()

	relWorkflow := &Test{Workflow: "./workflow.yaml"}
	err := pathlessRefusal(relWorkflow)
	require.Error(t, err)
	require.Contains(t, err.Error(), `workflow "./workflow.yaml"`)

	relPayload := &Test{
		Workflow: "/somewhere/workflow.yaml",
		Trigger:  &TriggerDelivery{Payload: "deliveries/one.json"},
	}
	err = pathlessRefusal(relPayload)
	require.Error(t, err)
	require.Contains(t, err.Error(), `trigger payload "deliveries/one.json"`)

	allAbsolute := &Test{
		Workflow: "/somewhere/workflow.yaml",
		Trigger:  &TriggerDelivery{Payload: "/somewhere/deliveries/one.json"},
	}
	require.NoError(t, pathlessRefusal(allAbsolute))

	noPayload := &Test{Workflow: "/somewhere/workflow.yaml", Trigger: &TriggerDelivery{}}
	require.NoError(t, pathlessRefusal(noPayload), "a trigger that replays nothing names no path to refuse")
}
