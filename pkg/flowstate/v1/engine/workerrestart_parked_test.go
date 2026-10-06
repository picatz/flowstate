package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// Worker restart while a run is parked.
//
// [TestWorkerRestartOverWorkflows] loses the worker between activities. A run
// held at a gate is the other state a worker can be lost in: nothing is
// executing, an open timer and a selector exist only in history, and the next
// worker has to rebuild both from it. Two ways to leave the gate are covered,
// since an interpreter can break one while replaying the other perfectly:
//
//   - answered: the first worker is stopped while the run is parked, the second
//     starts, and the signal arrives after.
//   - lapsed: the first worker is stopped while the run is parked and the
//     gate's bound lapses with no worker running at all; the second worker
//     starts to a timer already fired and resolves the wait through it.
//
// Each is judged against the same run with no restart.

// parkedGate is the workflow both variants run: a step, a bounded gate, a step.
func parkedGate(name string, bound time.Duration) *v1.RunState {
	says := func(id, message string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
			Name:   "log",
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral(message)},
		}}}
	}

	return &v1.RunState{Workflow: &v1.Workflow{
		Name: name,
		Steps: []*v1.Node{
			says("announce", "asking for approval"),
			{Id: "approval", Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Signal{Signal: &v1.Signal{
					Name: "deploy-approved",
					Outputs: map[string]*v1.Value{
						"approved": v1.NewExpr(`has(payload.approved) && payload.approved`),
						"lapsed":   v1.NewExpr(`timed_out`),
					},
				}},
				Timeout: durationpb.New(bound),
			}}},
			says("after", "the gate is open"),
		},
	}}
}

// runParked runs state to its gate on a first worker. With restart, that worker
// is stopped while the run is parked and a second takes over before the gate
// is left; lapse makes the gate leave through its timer, with no worker
// running when it fires. Without lapse the gate is answered by a signal.
func runParked(ctx context.Context, t *testing.T, temporal client.Client, id string, state *v1.RunState, restart, lapse bool) restartOutcome {
	t.Helper()

	first := newRestartWorker(temporal, firstWorkerIdentity, nil)
	second := newRestartWorker(temporal, secondWorkerIdentity, nil)
	require.NoError(t, first.Start())
	defer func() {
		first.Stop()
		second.Stop()
	}()

	run, err := temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        id,
		TaskQueue: engine.RunTaskQueueName,
	}, engine.Run, state)
	require.NoError(t, err)

	parkedOn(ctx, t, temporal, id, "approval")

	if restart {
		first.Stop()
		if lapse {
			// Nobody is polling: the bound lapses on the server alone, and the
			// second worker is started only once it has.
			waitForTimerFired(ctx, t, temporal, id, run.GetRunID())
		}
		require.NoError(t, second.Start())
	}

	if !lapse {
		// Sent after the restart, so the answer meets a run the second worker
		// rebuilt from history. The park is checked again first, through the
		// query the second worker now serves.
		signalWhenParked("approval", "deploy-approved", &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		})(ctx, t, temporal, id)
	}

	outcome := restartOutcome{workflowID: run.GetID(), runID: run.GetRunID(), outputs: &v1.Workflow_StepOutputs{}}
	outcome.runErr = run.Get(ctx, outcome.outputs)

	outcome.history = &historypb.History{}
	iter := temporal.GetWorkflowHistory(ctx, outcome.workflowID, outcome.runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)
		outcome.history.Events = append(outcome.history.Events, event)
	}
	outcome.resumedAt, outcome.boundary, outcome.boundaries = secondWorkerResume(outcome.history)

	return outcome
}

// waitForTimerFired returns once the run's history holds a fired timer. It
// reads history as a long poll, which blocks on the server until an event
// arrives, so nothing here spends wall-clock time of its own.
func waitForTimerFired(ctx context.Context, t *testing.T, temporal client.Client, id, runID string) {
	t.Helper()

	iter := temporal.GetWorkflowHistory(ctx, id, runID, true, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)
		if event.GetEventType() == enumspb.EVENT_TYPE_TIMER_FIRED {
			return
		}
	}
	t.Fatalf("run %s ended without its gate's timer firing", id)
}

func TestWorkerRestartWhileParkedAtAGate(t *testing.T) {
	for _, variant := range []struct {
		name  string
		bound time.Duration
		lapse bool
	}{
		// An hour: certainly still open when the signal arrives.
		{name: "answered after the restart", bound: time.Hour},
		{name: "lapsed while no worker ran", bound: 2 * time.Second, lapse: true},
	} {
		t.Run(variant.name, func(t *testing.T) {
			temporal := newTemporalNamespace(t)
			ctx, cancel := context.WithTimeout(t.Context(), exampleRunTimeout)
			defer cancel()

			baseline := runParked(ctx, t, temporal, "parked-baseline", parkedGate("parked-gate", variant.bound), false, variant.lapse)
			require.NoError(t, baseline.runErr)
			require.NotEmpty(t, baseline.outputs.GetStepValues(), "the undisturbed run produced no outputs to compare against")

			got := runParked(ctx, t, temporal, "parked-restarted", parkedGate("parked-gate", variant.bound), true, variant.lapse)
			require.NoError(t, got.runErr, "the restarted run failed")
			require.NotZero(t, got.resumedAt, "the second worker never ran a workflow task, so the restart was not realized")
			requireOutputs(t, baseline.outputs, got.outputs, "the run restarted while parked")

			// And the gate was left the way the variant says, so equality with
			// the baseline cannot be two runs that both took the wrong exit.
			approval := got.outputs.GetStepValues()["approval"].GetNamedValues()
			require.Equal(t, variant.lapse, approval["lapsed"].GetLiteral().GetBoolValue(), "lapsed")
			require.Equal(t, !variant.lapse, approval["approved"].GetLiteral().GetBoolValue(), "approved")
		})
	}
}
