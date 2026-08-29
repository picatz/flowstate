package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// The durable half of policy_denial_metric_test.go (pkg/flowstate/v1), and
// the assertion #934's audit found missing: the denial counter is emitted
// from [v1.CheckTaskPolicy], a seam both drivers execute, and for months only
// the local driver ever read it back — the two-callers rule enforced by
// imitation. This file is the second caller: a real durable run under a
// denying policy, the counter read with the durable driver's own label.

// TestADeniedDurableDispatchIsCounted runs a workflow the deployment's policy
// refuses through the durable driver and reads the counter.
//
// Through a real durable run rather than by calling the check directly —
// TestTheDenialCounterLabelsTheDriverItWasToldAbout already pins the label
// mapping in both directions — because the claim here is the same one the
// local file makes about *its* driver: a refused dispatch is counted exactly
// once, with the durable label, on the path production actually takes. A
// direct call cannot see a denial counted per retry attempt, and the durable
// driver is the one with a retry loop to get that wrong in.
func TestADeniedDurableDispatchIsCounted(t *testing.T) {
	denying, err := v1.TaskPolicyConfig{Deny: []string{"true"}}.Policy()
	require.NoError(t, err)
	v1.SetDefaultTaskPolicy(denying)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	reader := conformance.RecordMetrics(t)

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	engine.Register(env, engine.TaskRuntimeConfig{})

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: &v1.Workflow{
			Name:    "denied-durably",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				{Id: "narrate", Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("never printed")},
				}}},
			},
		},
	})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(), "the policy denies every task")

	collected := conformance.CollectFlowstateMetrics(t, reader)

	points := collected[metricschema.InstrumentPolicyDenials]
	require.Len(t, points, 1, "the denial counter recorded %v", points)
	require.Equal(t, uint64(1), points[0].Count,
		"one refused dispatch, counted once — not once per retry attempt")
	require.Equal(t, map[string]string{
		metricschema.PolicySurface: metricschema.SurfaceTaskDispatch,
		metricschema.TaskName:      "log",
		metricschema.Driver:        metricschema.DriverDurable,
	}, points[0].Attributes)

	// And the execution the refusal replaced is a failure with the durable
	// label and the classification — the same second half the local file
	// asserts, because an operator's error rate must not omit the failures
	// the deployment itself caused on either driver.
	executions := collected[metricschema.InstrumentTaskExecutions]
	require.Len(t, executions, 1, "the execution counter recorded %v", executions)
	require.Equal(t, map[string]string{
		metricschema.TaskName:    "log",
		metricschema.Driver:      metricschema.DriverDurable,
		metricschema.TaskOutcome: metricschema.OutcomeError,
		metricschema.ErrorType:   v1.ErrorKindPolicyDenied.String(),
	}, executions[0].Attributes)
}
