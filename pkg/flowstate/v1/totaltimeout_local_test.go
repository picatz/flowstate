package flowstatev1_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTotalTimeoutEndsTheStepLocal is one of the two driver callers
// [conformance.TotalTimeoutTaskDef] asks for: the local driver turns a step's
// `total_timeout:` into the caused [context.WithTimeoutCause] budget
// runStepWithPolicy wraps its retry loop in, so a step ends on the budget with
// most of its attempt list unspent (#920).
// TestTotalTimeoutEndsTheStepDurable in engine/total_timeout_test.go is the
// other, over Temporal's ScheduleToCloseTimeout.
//
// Registered on a private [v1.Registry] rather than the process-global one, the
// way TestStepTimeoutReachesTheTaskLocal does, so this test needs no
// coordination with anything else registering tasks for the life of the binary.
func TestTotalTimeoutEndsTheStepLocal(t *testing.T) {
	var attempts atomic.Int64

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(conformance.TotalTimeoutTaskDef(&attempts)))

	ctx := v1.NewContextWithRegistry(context.Background(), registry)

	out, err := v1.Run(ctx, conformance.TotalTimeoutWorkflow("total-timeout-local", "poll"))
	require.NoError(t, err, "the budget expiring is an ordinary step failure `continue_on_error:` tolerates, not a cancellation of the run")

	conformance.AssertTotalTimeoutEndedTheStep(t, "the local driver", out.GetStepValues()["poll"], attempts.Load())
	conformance.AssertTotalTimeoutSuppressesWidening(t, "the local driver")
}
