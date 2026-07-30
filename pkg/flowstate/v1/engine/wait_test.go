package engine_test

import (
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// newWaitEnv returns a test environment with the engine's workflow and
// activities registered.
func newWaitEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

	return env
}

// logStep is a task step, for putting something either side of a wait.
//
// `log` rather than the `echo` this used to build: echo retired at edition v2026.2,
// and nothing here ever read what it produced. What every caller wants is a step that
// exists and runs, which is exactly what a log step is — present in the outputs with
// an empty entry when it ran, and absent when it did not. The message is kept because
// the cancellation tests identify a step by it (see newCancelEnv).
func logStep(id, message string) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Task{Task: &v1.Task{
			Name:   "log",
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral(message)},
		}},
	}
}

// gatedOn attaches a condition to a step, so a test can say what has to hold for
// the step to run at all.
//
// Presence in the run's outputs is what these tests read the condition's answer
// from: a step whose condition is false is absent rather than present and empty,
// which is the one bit a workflow can set from an expression now that no task
// returns a value of its own.
func gatedOn(node *v1.Node, condition string) *v1.Node {
	node.Condition = v1.NewExpr(condition)

	return node
}

// sleepStep waits for a duration.
func sleepStep(id string, d time.Duration) *v1.Node {
	return &v1.Node{
		Id:   id,
		Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Duration{Duration: durationpb.New(d)}}},
	}
}

// signalStep waits for a signal, optionally with a timeout.
func signalStep(id, name string, timeout time.Duration) *v1.Node {
	wait := &v1.Wait{Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: name}}}
	if timeout > 0 {
		wait.Timeout = durationpb.New(timeout)
	}
	return &v1.Node{Id: id, Kind: &v1.Node_Wait{Wait: wait}}
}

// TestRunWorkflowWait runs the shared wait cases against the durable driver.
//
// The local driver runs the same ones, which is what keeps a timer from meaning
// something different in a local run than it does in production.
func TestRunWorkflowWait(t *testing.T) {
	t.Parallel()

	for _, test := range tests.WaitCases() {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			env := newWaitEnv(t)
			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: test.Workflow})

			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var output v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&output))

			require.True(t,
				proto.Equal(test.ExpectedOutputs, &output),
				"outputs differ from the local driver's expectations:\n%s",
				cmp.Diff(test.ExpectedOutputs, &output, protocmp.Transform()),
			)
		})
	}
}

// TestWaitSleep checks a durable timer, including one long enough that nothing
// could plausibly stay up for it.
//
// The test environment advances workflow time rather than waiting, which is
// exactly the point: the workload is not running during the wait, so there is
// nothing for the test to wait on either.
func TestWaitSleep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		sleep time.Duration
	}{
		{name: "a moment", sleep: time.Second},
		{name: "an hour", sleep: time.Hour},
		{name: "a week, which is the point", sleep: 7 * 24 * time.Hour},
		{name: "no time at all", sleep: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			env := newWaitEnv(t)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
				Name: "sleeping",
				Steps: []*v1.Node{
					logStep("before", "starting"),
					sleepStep("pause", test.sleep),
					logStep("after", "done"),
				},
			}})

			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())

			var outputs v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&outputs))

			// The wait produced outputs, and reports that it was not cut short.
			pause := outputs.GetStepValues()["pause"]
			require.NotNil(t, pause, "the wait step recorded no outputs")
			require.False(t, pause.GetNamedValues()[v1.TimedOutOutput].GetLiteral().GetBoolValue(),
				"a completed sleep reports having timed out")

			// And the step after it ran, which is what makes the wait a wait
			// rather than an ending.
			require.NotNil(t, outputs.GetStepValues()["after"], "the step after the wait did not run")
		})
	}
}

// TestWaitForSignal checks the approval gate: a run blocks until something
// outside it says to proceed, and what the sender sent becomes the step's
// outputs.
func TestWaitForSignal(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)

	// Sent after the run has started and reached the gate. In workflow time this
	// is a person approving a deploy.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("deploy-approved", &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{
				"approved": v1.NewLiteral(true),
				"by":       v1.NewLiteral("someone@example.com"),
			},
		})
	}, time.Minute)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "gated",
		Steps: []*v1.Node{
			logStep("request", "requesting approval"),
			signalStep("approval", "deploy-approved", 0),
			logStep("deploy", "deploying"),
		},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	approval := outputs.GetStepValues()["approval"]
	require.NotNil(t, approval)

	// The sender's data is under `payload`, which is what makes
	// ${approval.payload.approved} the spelling and what keeps a sender from
	// naming anything outside it.
	require.True(t, payloadField(t, approval, "approved").GetBoolValue())
	require.Equal(t, "someone@example.com", payloadField(t, approval, "by").GetStringValue())

	// And not at the top level, which is the property being protected.
	require.NotContains(t, approval.GetNamedValues(), "approved",
		"a sender's key reached the step's own output namespace")
	require.False(t, approval.GetNamedValues()[v1.TimedOutOutput].GetLiteral().GetBoolValue())

	require.NotNil(t, outputs.GetStepValues()["deploy"], "the gated step did not run after approval")
}

// TestWaitForSignalTimeout checks that a lapsed approval is a normal outcome an
// author can branch on, not an error they have to tolerate.
func TestWaitForSignalTimeout(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)

	// Nothing signals. The gate lapses.
	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "gate-lapses",
		Steps: []*v1.Node{
			logStep("request", "requesting approval"),
			signalStep("approval", "deploy-approved", 24*time.Hour),
			// Runs only if the approval did not lapse, which is the whole point
			// of the outcome being an output rather than an error.
			gatedOn(logStep("deploy", "deploying"), "!approval.timed_out"),
		},
	}})

	require.True(t, env.IsWorkflowCompleted())
	// A timeout is not a failure: the run completed.
	require.NoError(t, env.GetWorkflowError())

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	approval := outputs.GetStepValues()["approval"]
	require.NotNil(t, approval)
	require.True(t, approval.GetNamedValues()[v1.TimedOutOutput].GetLiteral().GetBoolValue(),
		"a lapsed gate does not report having timed out")

	require.Nil(t, outputs.GetStepValues()["deploy"],
		"the gated step ran even though its approval lapsed")
}

// TestWaitTimeoutLeavesPayloadKeysAbsent is the durable half of a parity check.
//
// A wait that timed out carries no payload, so a condition naming a payload key
// fails the run with an unresolved reference rather than quietly evaluating to
// false — the engine's existing rule for referencing something that does not
// exist. The local driver does the same, and the companion test lives beside the
// local wait implementation. Both are here because "absent" and "false" being
// distinguishable is what keeps "nobody approved this" from reading as "someone
// rejected it".
func TestWaitTimeoutLeavesPayloadKeysAbsent(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "lapsed-then-referenced",
		Steps: []*v1.Node{
			signalStep("approval", "deploy-approved", time.Hour),
			// The obvious thing to write, and the thing that fails.
			gatedOn(logStep("deploy", "deploying"), "approval.payload.approved"),
		},
	}})

	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err, "a condition naming an absent payload key silently passed")
	require.Contains(t, err.Error(), "approved",
		"the error does not name the reference that could not be resolved")
}

// TestWaitForSignalArrivingEarly checks the case a real approver produces: they
// approve before the run has reached the gate.
//
// Temporal buffers the signal, so this works as long as nothing throws the buffer
// away — which is what the next test is about.
func TestWaitForSignalArrivingEarly(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)

	// Immediately, before the run has got anywhere near the gate.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("deploy-approved", &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		})
	}, 0)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "approved-in-advance",
		Steps: []*v1.Node{
			logStep("one", "1"),
			logStep("two", "2"),
			signalStep("approval", "deploy-approved", 0),
			logStep("deploy", "deploying"),
		},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	require.True(t, payloadField(t, outputs.GetStepValues()["approval"], "approved").GetBoolValue())
	require.NotNil(t, outputs.GetStepValues()["deploy"])
}

// TestWaitForSignalSurvivesContinueAsNew is the test this design exists for.
//
// A signal arrives while the run is on an earlier step. The step budget then
// forces the run to continue as new before it reaches the gate. Temporal drops
// whatever is still buffered on a channel a suspending run never read, so without
// draining those channels and carrying the payloads forward, the approval is lost
// and the resumed run waits forever — the worst failure available to a feature
// whose promise is that waiting is reliable.
func TestWaitForSignalSurvivesContinueAsNew(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		Name: "approved-then-suspended",
		Steps: []*v1.Node{
			logStep("one", "1"),
			logStep("two", "2"),
			signalStep("approval", "deploy-approved", 0),
			// Gated on what the approval carried, which is the user-visible
			// requirement: not merely that the gate opened, but that what the
			// approver sent is still readable by a later step several suspends
			// away.
			gatedOn(logStep("deploy", "deploying"), "approval.payload.approved"),
		},
	}

	// A budget of one step forces a suspend after the first, which is before the
	// gate.
	first := newWaitEnv(t)
	first.RegisterDelayedCallback(func() {
		first.SignalWorkflow("deploy-approved", &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		})
	}, 0)

	first.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec, StepsBudget: 1})

	require.True(t, first.IsWorkflowCompleted())

	// Suspending is reported as a Continue-As-New, carrying the state the next
	// run starts from.
	err := first.GetWorkflowError()
	require.Error(t, err, "the run did not suspend, so this test proves nothing")

	var continueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continueAsNew)

	var carried v1.RunState
	require.NoError(t,
		converter.GetDefaultDataConverter().FromPayloads(continueAsNew.Input, &carried),
		"could not read the state the suspended run carried")

	// The approval was drained off its channel and carried, rather than being
	// left on a channel that is about to be discarded.
	require.Len(t, carried.GetPendingSignals(), 1,
		"the signal that arrived before the gate was not carried across the suspend")
	require.Equal(t, "deploy-approved", carried.GetPendingSignals()[0].GetName())
	require.True(t,
		carried.GetPendingSignals()[0].GetPayload().GetNamedValues()["approved"].GetLiteral().GetBoolValue(),
		"the carried signal lost its payload")

	// The resumed runs consume it and never block, even though nothing signals
	// them at all. A budget of one step means several more suspends before the
	// gate is reached, so the approval has to survive being carried repeatedly —
	// which is the case a long workload actually presents.
	outputs, runs := resumeToCompletion(t, &carried)
	require.Greater(t, runs, 1, "the run did not suspend again, so the carry was only tested once")

	approval := outputs.GetStepValues()["approval"]
	require.NotNil(t, approval, "the gate's outputs were not carried to the step that needed them")
	require.True(t, payloadField(t, approval, "approved").GetBoolValue(),
		"the approval arrived but what the approver sent was lost")

	require.NotNil(t, outputs.GetStepValues()["deploy"],
		"the resumed run never got past the gate it had already been approved through")
}

// resumeToCompletion runs a carried state, following every further suspend, and
// returns the final outputs and how many runs it took.
//
// A real workload continues as new until it is done, so a test that follows only
// the first hop tests less than it looks like it does — anything carried across a
// suspend has to survive being carried again.
func resumeToCompletion(t *testing.T, state *v1.RunState) (*v1.Workflow_StepOutputs, int) {
	t.Helper()

	// Bounded, because a bug that suspends without making progress would
	// otherwise loop until the test timeout with no indication of why.
	const maxRuns = 20

	for run := 1; run <= maxRuns; run++ {
		env := newWaitEnv(t)
		env.ExecuteWorkflow(engine.Run, state)

		require.True(t, env.IsWorkflowCompleted(), "run %d did not finish", run)

		err := env.GetWorkflowError()
		if err == nil {
			var outputs v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&outputs))
			return &outputs, run
		}

		var continueAsNew *workflow.ContinueAsNewError
		require.ErrorAs(t, err, &continueAsNew, "run %d failed rather than suspending", run)

		var next v1.RunState
		require.NoError(t,
			converter.GetDefaultDataConverter().FromPayloads(continueAsNew.Input, &next),
			"could not read the state run %d carried", run)
		state = &next
	}

	t.Fatalf("the workload suspended %d times without finishing", maxRuns)

	return nil, 0
}

// TestWaitUntil checks the timer-to-a-moment form.
func TestWaitUntil(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		until    *v1.Value
		wantFail string
	}{
		{
			name:  "an RFC 3339 string",
			until: v1.NewLiteral("2030-01-01T00:00:00Z"),
		},
		{
			name:  "an expression producing a time",
			until: v1.NewExpr(`"2030-06-01T09:00:00Z"`),
		},
		{
			name:  "a moment already past, which a late run has to be able to catch up from",
			until: v1.NewLiteral("2000-01-01T00:00:00Z"),
		},
		{
			// The mistake most likely to be made, so it gets an answer that says
			// what to use instead rather than hanging forever.
			name:     "a condition, which cannot change while the run waits",
			until:    v1.NewLiteral(true),
			wantFail: "wait_for_signal",
		},
		{
			name:     "something that is not a time at all",
			until:    v1.NewLiteral("next tuesday"),
			wantFail: "RFC 3339",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			env := newWaitEnv(t)

			env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
				Name: "until",
				Steps: []*v1.Node{
					logStep("before", "starting"),
					{Id: "pause", Kind: &v1.Node_Wait{Wait: &v1.Wait{
						Kind: &v1.Wait_Until{Until: test.until},
					}}},
					logStep("after", "done"),
				},
			}})

			require.True(t, env.IsWorkflowCompleted())

			if test.wantFail != "" {
				err := env.GetWorkflowError()
				require.Error(t, err, "an unusable wait_until was accepted")
				require.Contains(t, err.Error(), test.wantFail,
					"the diagnostic does not say what to do instead")
				return
			}

			require.NoError(t, env.GetWorkflowError())

			var outputs v1.Workflow_StepOutputs
			require.NoError(t, env.GetWorkflowResult(&outputs))
			require.NotNil(t, outputs.GetStepValues()["after"],
				"the step after the wait did not run")
		})
	}
}

// TestWaitRejectsMeaninglessTimeout checks a diagnostic rather than a behavior: a
// timeout on a sleep does nothing, and an author who wrote one believed it did.
func TestWaitRejectsMeaninglessTimeout(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "sleep-with-timeout",
		Steps: []*v1.Node{{
			Id: "pause",
			Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind:    &v1.Wait_Duration{Duration: durationpb.New(time.Hour)},
				Timeout: durationpb.New(time.Minute),
			}},
		}},
	}})

	require.True(t, env.IsWorkflowCompleted())

	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "the duration is already how long it waits")
}

// TestSignalNames checks the static enumeration the signal carry depends on.
//
// If a signal inside a loop body or a parallel branch were missed, its channel
// would not be drained before a suspend, and that signal would be the one that
// gets lost.
func TestSignalNames(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		Steps: []*v1.Node{
			logStep("a", "a"),
			signalStep("top", "top-level", 0),
			{
				Id: "loop",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items: v1.NewLiteralList("x", "y"),
					Body:  []*v1.Node{signalStep("in-loop", "per-item", 0)},
				}},
			},
			{
				Id: "branches",
				Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
					Branches: []*v1.Parallel_Branch{
						{Steps: []*v1.Node{signalStep("in-branch", "branch-signal", 0)}},
						// A repeat, which must not be listed twice.
						{Steps: []*v1.Node{signalStep("again", "top-level", 0)}},
					},
				}},
			},
		},
	}

	require.Equal(t,
		[]string{"top-level", "per-item", "branch-signal"},
		v1.SignalNames(spec))
}

// payloadField reads one entry out of a wait's `payload` mapping.
//
// A signal sender's data is rooted under one key rather than spread across the
// step's outputs, so reading it is a lookup inside a map — see v1.PayloadOutput
// for why it is not spread.
func payloadField(t *testing.T, outputs *v1.Node_Outputs, name string) *expr.Value {
	t.Helper()

	payload := outputs.GetNamedValues()[v1.PayloadOutput].GetLiteral().GetMapValue()
	require.NotNil(t, payload, "the wait produced no payload mapping")

	for _, entry := range payload.GetEntries() {
		if entry.GetKey().GetStringValue() == name {
			return entry.GetValue()
		}
	}

	t.Fatalf("the payload has no %q; it holds %d entries", name, len(payload.GetEntries()))
	return nil
}
