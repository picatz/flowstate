package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestCheckPolicyPlacementRefusesTheRPCPathBypass is the reproduction Codex's
// review of #665 named: a hand-built Workflow submitted straight to the RPC
// boundary never passes through the Flowfile compiler, so a composite step
// carrying `timeout:`/`retry:` was accepted and both drivers silently ignored
// the policy. [v1.CheckPolicyPlacement] is the schema-side half of the bound
// the compiler enforces while parsing.
func TestCheckPolicyPlacementRefusesTheRPCPathBypass(t *testing.T) {
	t.Parallel()

	forEachWithTimeout := &v1.Workflow{
		Name: "wf",
		Steps: []*v1.Node{{
			Id: "each",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items: v1.NewLiteralList(),
				Body: []*v1.Node{{
					Id:   "inner",
					Kind: &v1.Node_Task{Task: &v1.Task{Name: "http"}},
				}},
			}},
			Policy: &v1.StepPolicy{Timeout: durationpb.New(0)},
		}},
	}

	err := v1.CheckPolicyPlacement(forEachWithTimeout)
	require.Error(t, err)
	require.Contains(t, err.Error(), "for_each")

	switchWithRetry := &v1.Workflow{
		Name: "wf",
		Steps: []*v1.Node{{
			Id: "route",
			Kind: &v1.Node_Switch{Switch: &v1.Switch{
				Value: v1.NewLiteral("x"),
			}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 3}},
		}},
	}

	err = v1.CheckPolicyPlacement(switchWithRetry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "switch")

	// A task carrying the identical policy is exactly what the keys are for.
	taskWithPolicy := &v1.Workflow{
		Name: "wf",
		Steps: []*v1.Node{{
			Id:     "call",
			Kind:   &v1.Node_Task{Task: &v1.Task{Name: "http"}},
			Policy: &v1.StepPolicy{Timeout: durationpb.New(0), Retry: &v1.RetryPolicy{MaxAttempts: 3}},
		}},
	}

	require.NoError(t, v1.CheckPolicyPlacement(taskWithPolicy))
}
