package engine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestSignalBatchCasesDurably is the durable driver's half of the shared
// `wait_for_signals:` table — the local half is the v1 package's own
// TestSignalBatchCasesLocally, which runs the identical cases through a
// [v1.LocalSignals] queue.
//
// Both put the burst on the channel before the run's first drain looks at it,
// which is the shape [conformance.SignalBatchCases] documents. Here that is
// `SignalWorkflow` before `ExecuteWorkflow`: the test environment buffers a
// signal sent to a workflow that has not reached its wait exactly as a real
// server does, which is what makes "already arrived" expressible without a
// timing race in the test.
func TestSignalBatchCasesDurably(t *testing.T) {
	t.Parallel()

	conformance.AssertSignalBatchCases(t, func(t *testing.T, c conformance.SignalBatchCase) (*v1.Workflow_StepOutputs, error) {
		env := newWaitEnv(t)

		// In written order, which is the order `deliveries` has to report. The
		// environment appends to the channel in call order, so this is the
		// durable spelling of the local queue's own ordering guarantee.
		for _, payload := range c.Deliveries {
			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(c.SignalName, &v1.SignalDelivery{
					Payload: &v1.Node_Outputs{NamedValues: payload},
				})
			}, 0)
		}

		env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: c.Workflow})

		require.True(t, env.IsWorkflowCompleted(), "the run never finished")
		if err := env.GetWorkflowError(); err != nil {
			return nil, err
		}

		var outputs v1.Workflow_StepOutputs
		require.NoError(t, env.GetWorkflowResult(&outputs))

		return &outputs, nil
	})
}

// TestSignalBatchTakesCarriedSignalsBeforeTheChannel is the one claim about
// `wait_for_signals:` that the shared table cannot make, and the one it would be
// most expensive to be wrong about.
//
// A run that suspends drains whatever is still buffered into
// `RunState.pending_signals` — see [drainSignals] — so a batch reached *after* a
// Continue-As-New has two places to look, and they hold different generations of
// the same channel. The carried ones are older, and `deliveries` is ordered
// oldest first, so a drain that read the channel before the carry would report a
// burst in the wrong order across exactly one seam: a suspend. That is a defect
// no local run can produce, because a local run is a process that does not
// suspend, and no shared case can express, because the local half of it does not
// exist.
//
// So it is here, beside the single wait's own version of the same claim
// (TestWaitForSignalSurvivesContinueAsNew), and asserted at both ends: that the
// suspend genuinely carried the burst rather than dropping it, and that the
// resumed run reports it in the order the senders sent it.
func TestSignalBatchTakesCarriedSignalsBeforeTheChannel(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		Name: "batch-carried-across-a-suspend",
		Steps: []*v1.Node{
			logStep("one", "1"),
			logStep("two", "2"),
			{
				Id: "batch",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind: &v1.Wait_SignalBatch{SignalBatch: &v1.SignalBatch{
						Name: "order-placed",
						Outputs: map[string]*v1.Value{
							"ids":   v1.NewExpr("deliveries.map(delivery, delivery.payload.id)"),
							"taken": v1.NewExpr(v1.CountOutput),
						},
					}},
					Timeout: durationpb.New(time.Minute),
				}},
			},
			// Both shaped names are read by this condition, and that is load
			// bearing rather than thorough: an output field nothing downstream
			// names is legitimately prunable at Continue-As-New (see
			// compactOutputsForRemainingSteps), so a `taken`-only gate leaves
			// `ids` pruned and the ordering assertion below reads an empty list
			// that proves nothing. The single wait's own carry test references
			// `sender` for exactly this reason.
			gatedOn(logStep("processed", "processed"),
				`batch.taken == 3 && batch.ids == ["a", "b", "c"]`),
		},
	}

	// A budget of one step forces a suspend well before the drain is reached, so
	// the whole burst is carried rather than read off a live channel.
	first := newWaitEnv(t)
	first.RegisterDelayedCallback(func() {
		for _, id := range []string{"a", "b", "c"} {
			first.SignalWorkflow("order-placed", &v1.SignalDelivery{
				Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{"id": v1.NewLiteral(id)}},
			})
		}
	}, 0)

	first.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec, StepsBudget: 1})

	require.True(t, first.IsWorkflowCompleted())

	err := first.GetWorkflowError()
	require.Error(t, err, "the run did not suspend, so this test proves nothing")

	var continueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continueAsNew)

	var carried v1.RunState
	require.NoError(t,
		converter.GetDefaultDataConverter().FromPayloads(continueAsNew.Input, &carried),
		"could not read the state the suspended run carried")

	// The whole burst was drained off its channel and carried, in order, rather
	// than left on a channel that is about to be discarded.
	require.Len(t, carried.GetPendingSignals(), 3,
		"the burst that arrived before the drain was not carried across the suspend")
	for i, id := range []string{"a", "b", "c"} {
		require.Equal(t, id,
			carried.GetPendingSignals()[i].GetPayload().GetNamedValues()["id"].GetLiteral().GetStringValue(),
			"the carried burst is not in the order it arrived, at position %d", i)
	}

	outputs, runs := resumeToCompletion(t, &carried)
	require.Greater(t, runs, 1, "the run did not suspend again, so the carry was only tested once")

	batch := outputs.GetStepValues()["batch"]
	require.NotNil(t, batch, "the drain produced no outputs at all")

	ids := batch.GetNamedValues()["ids"].GetLiteral().GetListValue().GetValues()
	require.Len(t, ids, 3, "the resumed drain did not take the whole carried burst")
	for i, id := range []string{"a", "b", "c"} {
		require.Equal(t, id, ids[i].GetStringValue(),
			"the resumed drain reported the burst out of order at position %d; carried signals are older "+
				"than anything on the channel and must be taken first", i)
	}

	require.NotNil(t, outputs.GetStepValues()["processed"],
		"the step gated on the drain's count never ran, so the batch did not survive the suspend intact")
}
