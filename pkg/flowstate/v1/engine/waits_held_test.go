package engine_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// gateFan returns a workflow parked on n gates at once, each in its own parallel
// branch and each named gate-<i>, which no summary bound can list past
// [v1.MaxPendingWaits].
func gateFan(n int) *v1.Workflow {
	branches := make([]*v1.Parallel_Branch, 0, n)
	for i := range n {
		branches = append(branches, &v1.Parallel_Branch{Steps: []*v1.Node{
			signalStep(fmt.Sprintf("hold_%d", i), fmt.Sprintf("gate-%d", i), 0),
		}})
	}

	return &v1.Workflow{
		Name:  "gate-fan",
		Steps: []*v1.Node{{Id: "fan", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: branches}}}},
	}
}

// askGates runs the progress query and the gate query for each name while the
// run is parked, and then ends the run: by signalling every gate when release is
// set, and by cancelling it otherwise, which is the cheaper way to end a run
// holding a thousand gates.
func askGates(t *testing.T, env *testsuite.TestWorkflowEnvironment, gates int, release bool, names ...string) (*v1.RunProgress, map[string]*v1.RunProgress) {
	t.Helper()

	summary := &v1.RunProgress{}
	found := map[string]*v1.RunProgress{}
	var asked bool

	env.RegisterDelayedCallback(func() {
		encoded, err := env.QueryWorkflow(engine.ProgressQuery)
		require.NoError(t, err)
		require.NoError(t, encoded.Get(summary))

		for _, name := range names {
			encoded, err := env.QueryWorkflow(engine.GateQuery, name)
			require.NoError(t, err)
			answer := &v1.RunProgress{}
			require.NoError(t, encoded.Get(answer))
			found[name] = answer
		}
		asked = true
	}, 30*time.Second)

	env.RegisterDelayedCallback(func() {
		if !release {
			env.CancelWorkflow()

			return
		}
		for i := range gates {
			env.SignalWorkflow(fmt.Sprintf("gate-%d", i), &v1.SignalDelivery{})
		}
	}, 45*time.Second)

	t.Cleanup(func() { assert.True(t, asked, "the queries never ran, so this asserted on empty answers") })

	return summary, found
}

// TestAGatePastTheSummaryBoundIsFoundByName: a run holding more than
// [v1.MaxPendingWaits] gates lists only that many, and the gate query still
// answers for the 65th and the last, with that gate's step and nothing of the
// run's position, and says a name nobody waits on is not open.
func TestAGatePastTheSummaryBoundIsFoundByName(t *testing.T) {
	t.Parallel()

	const gates = v1.MaxPendingWaits + 6

	env := newWaitEnv(t)
	last := fmt.Sprintf("gate-%d", gates-1)
	summary, found := askGates(t, env, gates, true, "gate-64", last, "gate-0", "no-such-gate")

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: gateFan(gates)})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.Len(t, summary.GetPendingWaits(), v1.MaxPendingWaits)
	require.True(t, summary.GetPendingWaitsTruncated())
	for _, wait := range summary.GetPendingWaits() {
		require.NotEqual(t, "gate-64", wait.GetSignalName(), "the 65th gate was in the summary, so this proves nothing about the lookup")
	}

	for name, step := range map[string]string{"gate-64": "hold_64", last: fmt.Sprintf("hold_%d", gates-1), "gate-0": "hold_0"} {
		answer := found[name]
		require.Len(t, answer.GetPendingWaits(), 1, name)
		assert.Equal(t, step, answer.GetPendingWaits()[0].GetStepId(), name)
		assert.Equal(t, name, answer.GetPendingWaits()[0].GetSignalName())
		assert.False(t, answer.GetPendingWaitsTruncated())
		assert.Empty(t, answer.GetStepId(), "the gate query answered with the run's position")
	}

	assert.Empty(t, found["no-such-gate"].GetPendingWaits())
	assert.False(t, found["no-such-gate"].GetPendingWaitsTruncated(),
		"a miss on a run that retains every gate it holds said it could not tell")
}

// TestAGatePastTheHeldBoundSaysItCannotTell: the retention bound is a work
// bound, so a gate past [v1.MaxHeldWaits] is not found and the answer says a miss
// is not proof, which the server reports as FailedPrecondition and never as
// closed. Gates held before it are still found.
func TestAGatePastTheHeldBoundSaysItCannotTell(t *testing.T) {
	t.Parallel()

	const gates = v1.MaxHeldWaits + 1

	env := newWaitEnv(t)
	inside, past := fmt.Sprintf("gate-%d", v1.MaxHeldWaits-1), fmt.Sprintf("gate-%d", v1.MaxHeldWaits)
	_, found := askGates(t, env, gates, false, "gate-0", inside, past)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: gateFan(gates)})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(), "the run was meant to end by cancellation")

	require.Len(t, found["gate-0"].GetPendingWaits(), 1)
	require.Len(t, found[inside].GetPendingWaits(), 1)
	assert.Empty(t, found[past].GetPendingWaits())
	assert.True(t, found[past].GetPendingWaitsTruncated())
}
