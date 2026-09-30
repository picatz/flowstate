package engine_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// valueSteps returns `value:` steps named ids, each producing its own id.
func valueSteps(ids ...string) []*v1.Node {
	steps := make([]*v1.Node, len(ids))
	for i, id := range ids {
		steps[i] = &v1.Node{Id: id, Kind: &v1.Node_Value{Value: v1.NewLiteral(id)}}
	}

	return steps
}

// switchAfterAContinuedCall is a call whose callee continues as new after its
// third step, followed by a `switch:` whose arm has four steps.
func switchAfterAContinuedCall() *v1.Workflow {
	return &v1.Workflow{Name: "switch-after-a-continued-call", Profile: v1.CurrentProfile, Steps: []*v1.Node{
		{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: &v1.Workflow{
			Name: "child", Profile: v1.CurrentProfile, Steps: valueSteps("x", "y", "z", "w"),
		}}}},
		{Id: "route", Kind: &v1.Node_Switch{Switch: &v1.Switch{
			Value: v1.NewLiteral("go"),
			Cases: []*v1.Switch_Case{{Values: []*v1.Value{v1.NewLiteral("go")}, Steps: valueSteps("a1", "a2", "a3", "a4")}},
		}}},
	}}
}

// runAcrossSeams runs spec, resuming every continuation with a large budget,
// and returns the steps that have outputs at the end and how many segments ran.
// setup is applied to each segment's environment.
func runAcrossSeams(t *testing.T, spec *v1.Workflow, budget int, setup func(*testsuite.TestWorkflowEnvironment)) ([]string, int) {
	t.Helper()

	state := &v1.RunState{Workflow: spec, StepsBudget: int32(budget)}
	for segments := 1; segments <= 8; segments++ {
		env := newWaitEnv(t)
		setup(env)
		env.ExecuteWorkflow(engine.Run, state)
		require.True(t, env.IsWorkflowCompleted())

		var continued *workflow.ContinueAsNewError
		if !errors.As(env.GetWorkflowError(), &continued) {
			require.NoError(t, env.GetWorkflowError())
			var out v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&out))
			var steps []string
			for id := range out.GetStepValues() {
				steps = append(steps, id)
			}
			slices.Sort(steps)

			return steps, segments
		}
		next := &v1.RunState{}
		require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continued.Input, next))
		next.StepsBudget = 100
		state = next
	}
	require.FailNow(t, "the run kept continuing as new")

	return nil, 0
}

// A switch arm starts at its first step after a continuation taken inside a
// callee. The arm runs on the caller's executor, which still held the callee's
// saved position, and read it as its own: the arm began at the callee's next
// step and the ones before it never ran, with nothing to say so (#2238).
func TestASwitchArmAfterACalleeContinuationRunsEveryStep(t *testing.T) {
	t.Parallel()

	steps, segments := runAcrossSeams(t, switchAfterAContinuedCall(), 3, func(*testsuite.TestWorkflowEnvironment) {})

	require.Greater(t, segments, 1, "the run never continued as new, so this proves nothing")
	assert.Equal(t, []string{"a1", "a2", "a3", "a4", "nested", "route"}, steps)
}

// A history recorded before the fix started the arm where the callee stopped,
// and replays that way: the marker is what tells the two apart.
func TestAHistoryBeforeTheSwitchArmFixStartsTheArmWhereTheCalleeStopped(t *testing.T) {
	t.Parallel()

	steps, segments := runAcrossSeams(t, switchAfterAContinuedCall(), 3, func(env *testsuite.TestWorkflowEnvironment) {
		env.OnGetVersion(engine.SwitchArmResumeChange, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	})

	require.Greater(t, segments, 1, "the run never continued as new, so this proves nothing")
	assert.Equal(t, []string{"a4", "nested", "route"}, steps,
		"a history recorded before the fix was replayed into an arm that runs steps it did not")
}
