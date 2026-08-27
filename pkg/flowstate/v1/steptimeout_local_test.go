package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestStepTimeoutReachesTheTaskLocal is one of the two driver callers
// [conformance.StepTimeoutTaskDef] asks for: the local driver turns a step's
// `timeout:` into a deadline on the context it dispatches the task with, which
// is what lets `plugin.Plugin.callContext` trust that deadline instead of
// capping every plugin call at thirty seconds (#1130).
// TestStepTimeoutReachesTheTaskDurable in engine/step_timeout_test.go is the
// other.
//
// Registered on a private [v1.Registry] rather than the process-global one, the
// way runPluginIdentityLocal does, so this test needs no coordination with
// anything else registering tasks for the life of the binary.
func TestStepTimeoutReachesTheTaskLocal(t *testing.T) {
	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(conformance.StepTimeoutTaskDef()))

	ctx := v1.NewContextWithRegistry(context.Background(), registry)

	out, err := v1.Run(ctx, conformance.StepTimeoutWorkflow("step-timeout-local", "call"))
	require.NoError(t, err)

	conformance.AssertStepTimeoutReachedTheTask(t, "the local driver", out.GetStepValues()["call"])
}
