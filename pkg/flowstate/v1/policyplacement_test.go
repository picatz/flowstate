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

// TestCheckPolicyPlacementDescendsIntoCallees is Codex's follow-on finding on
// #665: [WalkWorkflow] deliberately does not descend into a `call:` step's
// inlined callee, so a hand-built Workflow whose embedded callee holds a
// misplaced policy several calls deep must be caught by this function's own
// bounded descent, the same way [v1.CheckStructureDepth] catches a structure
// nested inside a callee.
func TestCheckPolicyPlacementDescendsIntoCallees(t *testing.T) {
	t.Parallel()

	callee := &v1.Workflow{
		Name: "callee",
		Steps: []*v1.Node{{
			Id: "inner-fan",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items: v1.NewLiteralList(),
				Body: []*v1.Node{{
					Id:   "leaf",
					Kind: &v1.Node_Task{Task: &v1.Task{Name: "http"}},
				}},
			}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 3}},
		}},
	}

	caller := &v1.Workflow{
		Name: "caller",
		Steps: []*v1.Node{{
			Id:   "call-it",
			Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}},
		}},
	}

	err := v1.CheckPolicyPlacement(caller)
	require.Error(t, err, "a misplaced retry buried in a called workflow must be caught, not silently accepted")
	require.Contains(t, err.Error(), "for_each")

	// A callee with no policy problem of its own must not be flagged just for
	// being called.
	cleanCallee := &v1.Workflow{
		Name: "callee",
		Steps: []*v1.Node{{
			Id:     "leaf",
			Kind:   &v1.Node_Task{Task: &v1.Task{Name: "http"}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 3}},
		}},
	}
	cleanCaller := &v1.Workflow{
		Name: "caller",
		Steps: []*v1.Node{{
			Id:   "call-it",
			Kind: &v1.Node_Call{Call: &v1.Call{Workflow: cleanCallee}},
		}},
	}
	require.NoError(t, v1.CheckPolicyPlacement(cleanCaller))
}
