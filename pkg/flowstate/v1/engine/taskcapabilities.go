package engine

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Kept stable because histories record the activity type name. It is scheduled
// only for workflows whose control plane wrote a resolved task snapshot, so an
// old history never gains a command during replay.
const checkTaskCapabilitiesActivity = "CheckTaskCapabilities"

// CheckTaskCapabilities is the worker-side fleet-drift guard. The required
// names came from the immutable workflow snapshot; the available names were
// frozen from this worker's Registry when its activities were registered.
func (a taskActivities) CheckTaskCapabilities(_ context.Context, required []string) error {
	if err := v1.CheckTaskCapabilitiesAvailable(required, a.configured.taskNames); err != nil {
		return temporal.NewNonRetryableApplicationError(
			err.Error(), v1.ErrorKindUnknownTask.String(), err)
	}
	return nil
}

// admitTaskCapabilities performs one replay-safe segment admission. Deriving
// and validating requirements is pure workflow-side work; reading worker
// availability happens only in the activity.
func admitTaskCapabilities(ctx workflow.Context, wf *v1.Workflow) error {
	required, pinned, err := v1.PinnedTaskCapabilities(wf)
	if err != nil {
		return &ErrRunFailed{Message: err.Error(), Kind: v1.ErrorKindInvalidInput}
	}
	if !pinned {
		return nil
	}

	return workflow.ExecuteActivity(withSummary(ctx, taskCapabilityAdmissionSummary),
		checkTaskCapabilitiesActivity, required).Get(ctx, nil)
}
