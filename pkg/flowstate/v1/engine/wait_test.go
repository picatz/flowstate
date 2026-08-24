package engine_test

import (
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// newWaitEnv returns a test environment with the engine's workflow and
// activities registered.
func newWaitEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()

	env.RegisterWorkflow(engine.Run)
	env.OnActivity(engine.Task, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.Task)
	env.OnActivity(engine.TaskWithPrev, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskWithPrev)
	env.OnActivity(engine.TaskInScope, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(engine.TaskInScope)

	// A workflow's own `vars:` are evaluated in an activity, so a wait whose
	// expression reads one - a gate whose `prompt:` names `vars.target` - needs
	// this registered or the run fails before it ever parks. Registered here
	// rather than in the one test that needs it, because "this environment runs
	// the engine" is what every caller of this helper means, and a caller
	// discovering an unregistered activity learns nothing about waiting.
	env.RegisterActivity(engine.WorkflowVars)

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

	for _, test := range conformance.WaitCases() {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			env := newWaitEnv(t)
			env.ExecuteWorkflow(engine.Run,
				&v1.RunState{Workflow: test.Workflow, Inputs: test.Inputs})

			require.True(t, env.IsWorkflowCompleted())

			// The same two fields the local driver's caller reads, for the same
			// reason: a case with inputs and a case that must fail are both in
			// this set now, and a caller that skipped either would report
			// agreement it never checked.
			if test.ExpectFailure {
				require.Error(t, env.GetWorkflowError(),
					"the wait was expected to fail the run, as it does locally")
				return
			}
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

// testSignalDelivery builds the [v1.SignalDelivery] the server would send for
// a signal carrying payload, attested to subject.
//
// Every durable signal test builds this rather than a bare [v1.Node_Outputs],
// because that bare shape is no longer what travels the wire: `FlowstateServer.Signal`
// always sends a [v1.SignalDelivery], and a test that signalled the old shape
// would decode into a zero-value delivery — an empty payload and no sender —
// which would silently stop testing the payload path at all rather than fail
// loudly.
func testSignalDelivery(subject string, payload map[string]*v1.Value) *v1.SignalDelivery {
	return &v1.SignalDelivery{
		Payload: &v1.Node_Outputs{NamedValues: payload},
		Sender: &v1.SignalSender{
			Identity:   &v1.WorkloadIdentity{Subject: subject, Namespace: "team-a"},
			AcceptedAt: timestamppb.Now(),
		},
	}
}

// senderSubject reads `${<id>.sender.identity.subject}` back out of a wait's
// outputs, the way a workflow expression would.
func senderSubject(t *testing.T, outputs *v1.Node_Outputs) string {
	t.Helper()

	sender := outputs.GetNamedValues()[v1.SenderOutput].GetLiteral().GetMapValue()
	require.NotNil(t, sender, "the wait produced no sender mapping")

	for _, entry := range sender.GetEntries() {
		if entry.GetKey().GetStringValue() != "identity" {
			continue
		}
		for _, field := range entry.GetValue().GetMapValue().GetEntries() {
			if field.GetKey().GetStringValue() == "subject" {
				return field.GetValue().GetStringValue()
			}
		}
	}

	t.Fatalf("the sender mapping has no identity.subject")
	return ""
}

// senderLocal reads `${<id>.sender.local}` back out of a wait's outputs.
func senderLocal(t *testing.T, outputs *v1.Node_Outputs) bool {
	t.Helper()

	sender := outputs.GetNamedValues()[v1.SenderOutput].GetLiteral().GetMapValue()
	require.NotNil(t, sender, "the wait produced no sender mapping")

	for _, entry := range sender.GetEntries() {
		if entry.GetKey().GetStringValue() == "local" {
			return entry.GetValue().GetBoolValue()
		}
	}

	t.Fatalf("the sender mapping has no \"local\" field")
	return false
}

// TestWaitForSignalAcceptsTheLegacyWireShape is the #199 P1 fix, proven to
// bite: a signal sent as a bare Node_Outputs — exactly what every server sent
// before #194, and exactly what a signal already recorded in an execution's
// history from before that field existed still looks like — must not be
// dropped as a corrupted signal by a worker now running code that expects
// [v1.SignalDelivery].
//
// Before engine/signal_compat.go existed, this test hung: Temporal's default
// converter rejects the "namedValues" field against SignalDelivery's schema
// (no field by that name), channelImpl.Receive treats that as a corrupted
// signal, drops it, and keeps waiting — so the workflow never saw the
// approval at all and the wait ran out its timeout doing nothing.
func TestWaitForSignalAcceptsTheLegacyWireShape(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)

	// Sent as the bare shape every pre-#194 server used — never wrapped in a
	// SignalDelivery, which is exactly what a server mid-rollout still running
	// the old binary (or an already-recorded history entry) produces.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("deploy-approved", &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{
				"approved": v1.NewLiteral(true),
			},
		})
	}, time.Minute)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "gated-legacy-shape",
		Steps: []*v1.Node{
			logStep("request", "requesting approval"),
			signalStep("approval", "deploy-approved", 2*time.Minute),
			logStep("deploy", "deploying"),
		},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	approval := outputs.GetStepValues()["approval"]
	require.NotNil(t, approval, "the wait produced no outputs at all")

	// The payload is intact — the whole point of falling back rather than
	// dropping the signal.
	require.True(t, payloadField(t, approval, "approved").GetBoolValue(),
		"the legacy signal's payload was lost")
	require.False(t, approval.GetNamedValues()[v1.TimedOutOutput].GetLiteral().GetBoolValue(),
		"the wait timed out, meaning the legacy-shape signal was never delivered at all")

	// And it reads as unattested — never as an attested-but-anonymous sender,
	// which is what an empty-but-present SignalSender would look like.
	require.Empty(t, senderSubject(t, approval))
	require.True(t, senderLocal(t, approval),
		"a legacy-shape signal was not marked unattested")

	require.NotNil(t, outputs.GetStepValues()["deploy"],
		"the gated step did not run, so the legacy signal did not actually unblock the wait")
}

// TestWaitForSignal checks the approval gate: a run blocks until something
// outside it says to proceed, what the sender sent becomes the step's outputs
// under `payload`, and who the *server* attested becomes the step's outputs
// under `sender` — the fix for #194: a self-asserted `by` inside the payload is
// evidence, never identity, and must not be confused with the attested sender.
func TestWaitForSignal(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)

	// Sent after the run has started and reached the gate. In workflow time this
	// is a person approving a deploy — self-asserting a *different* name in the
	// payload's own `by` field, which is exactly the confusion #194 is about: the
	// attested sender below must not agree with it.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("deploy-approved", testSignalDelivery("real-approver@example.com", map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
			"by":       v1.NewLiteral("someone-else@example.com"),
		}))
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
	require.Equal(t, "someone-else@example.com", payloadField(t, approval, "by").GetStringValue())

	// And not at the top level, which is the property being protected.
	require.NotContains(t, approval.GetNamedValues(), "approved",
		"a sender's key reached the step's own output namespace")
	require.False(t, approval.GetNamedValues()[v1.TimedOutOutput].GetLiteral().GetBoolValue())

	// The attested identity is the engine's own, unrelated to whatever the
	// payload's `by` field claimed.
	require.Equal(t, "real-approver@example.com", senderSubject(t, approval),
		"the attested sender disagreed with what the engine was actually told")

	// The shared half of the #194 fix: an attested delivery must report itself
	// as attested, with the shape [conformance.AssertSignalSenderShape] checks. The
	// local half of this same assertion lives in wait_local_test.go.
	conformance.AssertSignalSenderShape(t, approval, false)

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

// TestWaitForSignalCancelsItsTimeoutTimer is the #770 regression: a bounded
// wait_for_signal a signal answers must not leave its timeout timer running.
//
// The existing tests for this outcome (TestWaitForSignal,
// TestWaitForSignalTimeout) stay green whether or not the timer is ever
// cancelled — both only assert the *outputs* the wait produced, and an
// abandoned timer changes neither. Per CLAUDE.md's "test that A cannot reach
// B, not that A can reach A", this asks the negative question instead: does
// anything outlive the wait that created it.
//
// The workflow deliberately does not end at the answered gate. A run that
// completes immediately after would let the SDK's own end-of-execution
// cleanup cancel every outstanding future, including a leaked one, and the
// bug (and this test) would both disappear along with it — which is exactly
// what the issue says makes a one-shot approval gate the case that is *not*
// where this costs anything. So a second, never-answered gate follows,
// keeping the run open past the first gate's own one-hour bound: the shape
// the issue names as the one that pays, `wait_for_signal:` inside something
// that keeps running (`examples/entity-order`'s loop is the real-world
// instance; this is its minimal shape).
func TestWaitForSignalCancelsItsTimeoutTimer(t *testing.T) {
	t.Parallel()

	env := newWaitEnv(t)

	type scheduledTimer struct {
		id       string
		duration time.Duration
	}
	var (
		scheduled []scheduledTimer
		canceled  = map[string]bool{}
		fired     = map[string]bool{}
	)

	env.SetOnTimerScheduledListener(func(timerID string, duration time.Duration) {
		scheduled = append(scheduled, scheduledTimer{id: timerID, duration: duration})
	})
	env.SetOnTimerCanceledListener(func(timerID string) { canceled[timerID] = true })
	env.SetOnTimerFiredListener(func(timerID string) { fired[timerID] = true })

	// Answered a minute in, far inside its own hour-long bound, so the signal
	// — not the timer — is what resolves this gate.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("deploy-approved", testSignalDelivery("approver@example.com", map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
		}))
	}, time.Minute)

	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: &v1.Workflow{
		Name: "answered-gate-stays-open",
		Steps: []*v1.Node{
			signalStep("approval", "deploy-approved", time.Hour),
			// Nothing ever signals this one, so its own two-hour bound is what
			// carries the test environment's virtual clock past the first
			// gate's one-hour mark — the window an abandoned timer would fire
			// in, and the reason this step is here at all.
			signalStep("hold", "never-arrives", 2*time.Hour),
		},
	}})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.Len(t, scheduled, 2, "expected one durable timer per bounded gate")
	approvalTimer, holdTimer := scheduled[0], scheduled[1]

	assert.True(t, canceled[approvalTimer.id],
		"the answered gate's timeout timer was never cancelled — see #770")
	assert.False(t, fired[approvalTimer.id],
		"the answered gate's abandoned timer fired into a run that had already moved past it — see #770")

	// The path #770 says was already right, for contrast: a gate nothing
	// answers keeps timing out on its own timer.
	assert.True(t, fired[holdTimer.id],
		"a lapsed gate's own timer stopped firing — that path must not change")
	assert.False(t, canceled[holdTimer.id],
		"a lapsed gate's timer was cancelled instead of left to fire")
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
		env.SignalWorkflow("deploy-approved", testSignalDelivery("early-approver@example.com", map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
		}))
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
			// Gated on both halves of what the approval carried, which is the
			// user-visible requirement: not merely that the gate opened, but that
			// what the approver sent — and who the engine attested sent it — are
			// still readable by a later step several suspends away. Referencing
			// `sender` here is also what makes it survive compaction: an output
			// field nothing downstream names is legitimately prunable at
			// Continue-As-New (see compactOutputsForRemainingSteps), exactly as
			// `payload.approved` would be if this condition did not name it.
			gatedOn(logStep("deploy", "deploying"),
				`approval.payload.approved && approval.sender.identity.subject != ""`),
		},
	}

	// A budget of one step forces a suspend after the first, which is before the
	// gate.
	first := newWaitEnv(t)
	first.RegisterDelayedCallback(func() {
		first.SignalWorkflow("deploy-approved", testSignalDelivery("carried-approver@example.com", map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
		}))
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
	require.Equal(t, "carried-approver@example.com",
		carried.GetPendingSignals()[0].GetSender().GetIdentity().GetSubject(),
		"the carried signal lost its attested sender — a suspend must not be a way to launder identity")

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
	require.Equal(t, "carried-approver@example.com", senderSubject(t, approval),
		"the attested sender did not survive being carried across Continue-As-New")

	require.NotNil(t, outputs.GetStepValues()["deploy"],
		"the resumed run never got past the gate it had already been approved through")
}

// TestPendingSignalWithoutSenderResumesAsUnattested is the old-writer,
// new-reader half of the #199 fix, pinned the way invariant 10 asks for.
//
// A RunState carrying a PendingSignal with no Sender is not a hypothetical: it
// is byte-for-byte what an interpreter running before #194 wrote, and what a
// signal already recorded in an execution's history from before then still
// looks like. This constructs that RunState directly — Sender left nil, the
// same shape protojson.Marshal produces for an unset message field with no
// extra work — and hands it to *this* build's interpreter the way the
// auto-upgrade seam at Continue-As-New would, without ever going through a
// live signal delivery. The resumed run must still find its payload, must
// still complete, and must report the pending signal as unattested rather
// than erroring on a field that never existed when it was written.
func TestPendingSignalWithoutSenderResumesAsUnattested(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		Name: "resumed-with-a-pre-attestation-pending-signal",
		Steps: []*v1.Node{
			signalStep("approval", "deploy-approved", 0),
			gatedOn(logStep("deploy", "deploying"), "approval.payload.approved"),
		},
	}

	state := &v1.RunState{
		Workflow: spec,
		PendingSignals: []*v1.PendingSignal{
			{
				Name: "deploy-approved",
				Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
					"approved": v1.NewLiteral(true),
				}},
				// No Sender: exactly what an old writer's RunState looks like.
			},
		},
	}

	env := newWaitEnv(t)
	env.ExecuteWorkflow(engine.Run, state)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"a PendingSignal missing the field this build added failed the run instead of "+
			"reading it as absent")

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&outputs))

	approval := outputs.GetStepValues()["approval"]
	require.NotNil(t, approval, "the pending signal was never consumed")
	require.True(t, payloadField(t, approval, "approved").GetBoolValue(),
		"the pending signal's payload was lost")

	require.Empty(t, senderSubject(t, approval))
	require.True(t, senderLocal(t, approval),
		"a pending signal carried from before sender attestation existed was not read as unattested")

	require.NotNil(t, outputs.GetStepValues()["deploy"],
		"the gated step did not run after the pending signal was consumed")
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

// TestACarriedSignalReachesAWaitInsideALoop is the join of two features that were
// each tested alone.
//
// The carry has two halves and only one of them was covered. Draining is tested by
// TestSignalNames, which enumerates the signal names nested anywhere in a spec so a
// loop body's channel is drained before a suspend — and says so, naming exactly this
// case as the one that would get lost. Consuming is the other half, and it happens in
// [executor.takePendingSignal], which reads `e.signals`.
//
// `signals` is documented as "shared by pointer with every nested executor", and a
// nested executor is built in three places — runIteration, runIterationsConcurrently
// and runParallel — none of which pass it. So a wait in a loop body finds a nil carry,
// blocks on a channel the previous run already drained, and waits for a signal the run
// is holding. That is the exact failure the carry exists to prevent, one level down,
// and both existing tests stay green through it because both waits are top-level.
func TestACarriedSignalReachesAWaitInsideALoop(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		Name: "approval-inside-a-loop",
		Steps: []*v1.Node{
			logStep("one", "1"),
			{
				Id: "each",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items:    v1.NewLiteralList("only"),
					Iterator: "item",
					Body:     []*v1.Node{signalStep("approval", "deploy-approved", 0)},
				}},
			},
		},
	}

	// One step of budget, so the run suspends after `one` and before the loop.
	first := newWaitEnv(t)
	first.RegisterDelayedCallback(func() {
		first.SignalWorkflow("deploy-approved", testSignalDelivery("loop-approver@example.com", map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
		}))
	}, 0)

	first.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec, StepsBudget: 1})
	require.True(t, first.IsWorkflowCompleted())

	err := first.GetWorkflowError()
	require.Error(t, err, "the run did not suspend, so this test proves nothing")

	var continueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continueAsNew)

	var carried v1.RunState
	require.NoError(t,
		converter.GetDefaultDataConverter().FromPayloads(continueAsNew.Input, &carried))

	require.Len(t, carried.GetPendingSignals(), 1,
		"the approval was not carried across the suspend, so the consuming half is untested")

	// Nothing signals the resumed run. It is holding the approval already, and the
	// only question is whether a wait one level down can see it.
	//
	// Reaching this line at all is most of the assertion: before the carry was
	// threaded into nested executors, the resumed run blocked on a channel the
	// previous run had already drained, and the test environment fired the wait's
	// ten-year timer instead of completing.
	outputs, _ := resumeToCompletion(t, &carried)

	// Read through the loop rather than at the top level. A body step's outputs
	// belong to the iteration that produced them — `each` holds a result per item —
	// so `approval` is not a top-level name and asserting there would pass for a run
	// that never entered the loop.
	loop := outputs.GetStepValues()["each"]
	require.NotNil(t, loop, "the loop never produced results, so the body did not finish")

	results := loop.GetNamedValues()["results"].GetLiteral().GetListValue().GetValues()
	require.Len(t, results, 1,
		"the loop did not run its one iteration to completion")

	var reached bool
	for _, entry := range results[0].GetMapValue().GetEntries() {
		if entry.GetKey().GetStringValue() == "approval" {
			reached = true
		}
	}
	require.True(t, reached,
		"a wait inside a loop body never saw the approval the run was carrying")
}

// TestACarriedSignalReachesAWaitInsideAParallelBranch is the other nesting the same
// fix covers, and it is worth its own test because it reaches the carry by a
// different path.
//
// A loop body runs on the workflow's own coroutine; a parallel branch runs on one
// started by workflow.Go, with its own executor and its own context. Sharing the
// carry by pointer is what makes a signal consumed in a branch consumed for the run
// — a copy per branch would let one approval satisfy two gates, which is the failure
// on the other side of this one and is why the field is a pointer rather than a
// slice.
func TestACarriedSignalReachesAWaitInsideAParallelBranch(t *testing.T) {
	t.Parallel()

	spec := &v1.Workflow{
		Name: "approval-inside-a-branch",
		Steps: []*v1.Node{
			logStep("one", "1"),
			{
				Id: "branches",
				Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
					Branches: []*v1.Parallel_Branch{
						{Steps: []*v1.Node{signalStep("approval", "deploy-approved", 0)}},
						{Steps: []*v1.Node{logStep("other", "other")}},
					},
				}},
			},
		},
	}

	first := newWaitEnv(t)
	first.RegisterDelayedCallback(func() {
		first.SignalWorkflow("deploy-approved", testSignalDelivery("branch-approver@example.com", map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
		}))
	}, 0)

	first.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec, StepsBudget: 1})
	require.True(t, first.IsWorkflowCompleted())

	err := first.GetWorkflowError()
	require.Error(t, err, "the run did not suspend, so this test proves nothing")

	var continueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continueAsNew)

	var carried v1.RunState
	require.NoError(t,
		converter.GetDefaultDataConverter().FromPayloads(continueAsNew.Input, &carried))
	require.Len(t, carried.GetPendingSignals(), 1,
		"the approval was not carried across the suspend")

	outputs, _ := resumeToCompletion(t, &carried)

	// A branch writes its steps into the run's outputs, unlike a loop body, so the
	// gate is a top-level name here.
	require.NotNil(t, outputs.GetStepValues()["approval"],
		"a wait inside a parallel branch never saw the approval the run was carrying")
}
