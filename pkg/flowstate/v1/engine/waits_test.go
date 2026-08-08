package engine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// A run parked on an approval was RUNNING and nothing else. Which gate, which
// signal name would open it, and whether it lapses on its own were all in the
// interpreter's own stack, so an operator holding a run id could not learn the
// name to send without going back to the file the run was compiled from.
//
// These ask the same query the position rides on, while the run is parked.

// TestAParkedRunSaysWhatItIsWaitingFor runs the shared table against the durable
// driver. The local driver runs the same one, which is what keeps a gate from
// describing itself differently in a rehearsal than in production.
func TestAParkedRunSaysWhatItIsWaitingFor(t *testing.T) {
	t.Parallel()

	for _, test := range tests.PendingWaitCases() {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			env := newWaitEnv(t)

			// Asked while the run is parked, which is the only moment any of
			// this is true: a query after the run has finished is answered from
			// a final state that is parked on nothing.
			parked, queryErr := askDuring(t, env, 30*time.Second)

			// Delivered after the question, so the run is still holding the
			// gate when the query lands and has been released by the time the
			// test asserts the run completed.
			for _, name := range test.Release {
				env.RegisterDelayedCallback(func() {
					env.SignalWorkflow(name, &v1.SignalDelivery{})
				}, 45*time.Second)
			}

			// Asked again once the gate has opened. The whole point of a live
			// answer is that it stops being true: a registry that only ever
			// grew would report a gate somebody already opened, which is worse
			// than reporting nothing.
			after, afterErr := askDuring(t, env, 90*time.Second)

			// The case's own workflow, plus a long sleep, so that the run is
			// still going when that second question lands. A query against a
			// finished run is answered from a final state and would report an
			// empty set for the wrong reason.
			spec := proto.Clone(test.Workflow).(*v1.Workflow)
			spec.Steps = append(spec.Steps, sleepStep("keep_running", 2*time.Hour))

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})

			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			require.NoError(t, *queryErr, "the run did not answer what it was parked on")
			tests.AssertPendingWaits(t, parked.GetPendingWaits(), test.Want)
			assert.False(t, parked.GetPendingWaitsTruncated(),
				"an answer with a handful of waits in it called itself truncated")

			require.NoError(t, *afterErr)
			assert.Empty(t, after.GetPendingWaits(),
				"the run kept reporting a gate that had already been opened")
		})
	}
}

// TestTwoGatesHeldAtOnceAreBothReported is the join direction, and the reason
// the parked waits are a set rather than a field on the position.
//
// A position is singular, so a `parallel:` block refuses to claim one and the
// query reports the block itself. Two branches parked on two different signals
// are two facts, both true at once, and an operator who is told about only one
// of them opens one gate and watches the run stay exactly where it was.
//
// Durable-driver only, and that is not an omission: the local driver runs
// branches sequentially (eval.go's runParallel), so it can never hold two gates
// at the same time to report them. See [tests.PendingWaitCases].
func TestTwoGatesHeldAtOnceAreBothReported(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	parked, queryErr := askDuring(t, env, 30*time.Second)

	for _, name := range []string{"left", "right"} {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(name, &v1.SignalDelivery{})
		}, 45*time.Second)
	}

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "two-gates",
		Signals: map[string]*v1.SignalPolicy{
			"left": {Allow: []*v1.SignalPolicyRule{{Subject: "https://idp.example#one"}}},
		},
		Steps: []*v1.Node{{
			Id: "both",
			Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
				{Steps: []*v1.Node{signalStep("left_gate", "left", time.Hour)}},
				{Steps: []*v1.Node{signalStep("right_gate", "right", 0)}},
			}}},
		}},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, *queryErr)

	tests.AssertPendingWaits(t, parked.GetPendingWaits(), []tests.WantWait{
		{
			StepID:     "left_gate",
			SignalName: "left",
			Policed:    true,
			// Bounded, and inside concurrent work, which is where the two
			// fields part company: the deadline is exact because the wait
			// computed it, and the path is empty because no one branch is the
			// run's position.
			HasDeadline: true,
		},
		{
			StepID:      "right_gate",
			SignalName:  "right",
			Policed:     false,
			HasDeadline: false,
		},
	})

	assert.Equal(t, "both", parked.GetStepId(),
		"the position stopped at the concurrent block, which is what makes the waits worth reporting separately")
}

// TestAGateThatNeverParkedIsNotReported pins the line both drivers draw at the
// same place: a wait resolved by a signal that arrived before the step was
// reached, and a wait whose bound had already lapsed, never blocked on anything
// and so were never gates anybody could act on.
//
// Reporting one would be worse than saying nothing: it names a step and a
// signal to an operator who would then send a signal to a run that had already
// walked past it.
func TestAGateThatNeverParkedIsNotReported(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	during, queryErr := askDuring(t, env, 30*time.Second)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: &v1.Workflow{
			Name: "lapsed-gate",
			Steps: []*v1.Node{
				// A bound of zero has already lapsed, so this resolves without
				// blocking, and the sleep after it is what the run is really on
				// when the query lands. Written as a `timeout:` of 0s rather
				// than as an absent one, which is the "already lapsed" case
				// rather than the "waits until somebody acts" case.
				{Id: "lapsed", Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "never"}},
					Timeout: durationpb.New(0),
				}}},
				sleepStep("pause", time.Hour),
			},
		},
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, *queryErr)

	assert.Empty(t, during.GetPendingWaits(),
		"a wait that never blocked was reported as a gate somebody could open")
	assert.Equal(t, "pause", during.GetStepId(),
		"the query landed somewhere other than the sleep, so the empty answer above proves nothing")
}

// TestACarriedSignalIsNotAGate is the other half of the line above: a signal
// that arrived before its step was reached is consumed without the run ever
// parking, so nothing is reported even for an instant.
func TestACarriedSignalIsNotAGate(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)
	during, queryErr := askDuring(t, env, 30*time.Second)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: &v1.Workflow{
			Name: "early-approval",
			Steps: []*v1.Node{
				signalStep("gate", "approve", time.Hour),
				sleepStep("pause", time.Hour),
			},
		},
		// Approved before the run reached the gate, carried in the run's own
		// state exactly as a suspend would have carried it.
		PendingSignals: []*v1.PendingSignal{{Name: "approve"}},
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, *queryErr)

	assert.Empty(t, during.GetPendingWaits(),
		"a gate an early signal had already answered was reported as still holding the run")
	assert.Equal(t, "pause", during.GetStepId(),
		"the query landed somewhere other than the sleep, so the empty answer above proves nothing")
}
