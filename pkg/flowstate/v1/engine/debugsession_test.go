package engine_test

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// The typed durable session (#2126), driven through the asks the RPCs send and
// read through the queries they read.

func typedSpec(name string) *v1.Workflow {
	child := &v1.Workflow{
		Name:    "child",
		Profile: v1.CurrentProfile,
		Steps:   []*v1.Node{logStep("greet", "hi"), logStep("wave", "bye")},
	}

	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			sleepStep("settle", settleFor),
			{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: child}}},
			logStep("first", "one"),
			logStep("second", "two"),
		},
		Debug: debugSpec(name).GetDebug(),
	}
}

func typedAsk(subject string, ask *v1.DebugAsk) *v1.SignalDelivery {
	payload, err := v1.NewTypedDebugAsk(ask)
	if err != nil {
		panic(err)
	}

	return &v1.SignalDelivery{
		Payload: payload,
		Sender: &v1.SignalSender{
			Identity: &v1.WorkloadIdentity{
				Issuer:    "https://issuer.example.com",
				Subject:   subject,
				Namespace: "team-a",
				Claims:    map[string]string{"role": "sre"},
			},
			AcceptedAt: timestamppb.Now(),
		},
	}
}

func querySnapshot(t *testing.T, env *testsuite.TestWorkflowEnvironment, request string) *v1.DebugSnapshot {
	t.Helper()

	encoded, err := env.QueryWorkflow(v1.DebugQuery, request)
	require.NoError(t, err)
	var snapshot v1.DebugSnapshot
	require.NoError(t, encoded.Get(&snapshot))

	return &snapshot
}

// timeline runs a spec with typed asks and snapshot reads scheduled on the
// workflow clock, returning the snapshots in the order they were read.
type timeline struct {
	t     *testing.T
	env   *testsuite.TestWorkflowEnvironment
	reads map[string]*v1.DebugSnapshot
}

func newTimeline(t *testing.T) *timeline {
	return &timeline{t: t, env: newWaitEnv(t), reads: map[string]*v1.DebugSnapshot{}}
}

func (tl *timeline) ask(at time.Duration, subject string, ask *v1.DebugAsk) {
	tl.env.RegisterDelayedCallback(func() {
		tl.env.SignalWorkflow(v1.DebugSignal, typedAsk(subject, ask))
	}, at)
}

func (tl *timeline) read(at time.Duration, name, request string) {
	tl.env.RegisterDelayedCallback(func() {
		tl.reads[name] = querySnapshot(tl.t, tl.env, request)
	}, at)
}

func TestATypedSessionStepsIntoAndOutOfACall(t *testing.T) {
	t.Parallel()

	tl := newTimeline(t)
	const sre = "sre-1@example.com"

	tl.ask(30*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	tl.read(31*time.Second, "pending", "attach")
	tl.read(61*time.Second, "held", "attach")
	tl.ask(70*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "in", Revision: 2,
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN})
	tl.read(71*time.Second, "in", "in")
	tl.ask(72*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "in", Revision: 2,
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN})
	tl.ask(73*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "stale", Revision: 2,
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN})
	tl.ask(74*time.Second, "sre-2@example.com", &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "foreign",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})
	tl.read(75*time.Second, "after-retries", "stale")
	tl.read(75*time.Second, "foreign", "foreign")
	tl.env.RegisterDelayedCallback(func() {
		encoded, err := tl.env.QueryWorkflow(v1.DebugInspectQuery, &v1.DebugInspectRequest{SessionId: "s1", Revision: 4})
		require.NoError(t, err)
		var roots v1.DebugInspectResponse
		require.NoError(t, encoded.Get(&roots))
		var groups []string
		for _, child := range roots.GetChildren() {
			groups = append(groups, child.GetName())
		}
		assert.Contains(t, groups, "run", "the held callee's own scope is listed")

		encoded, err = tl.env.QueryWorkflow(v1.DebugInspectQuery, &v1.DebugInspectRequest{SessionId: "s1", Revision: 4, Expression: "[1, 2, 3]", Children: true})
		require.NoError(t, err)
		var list v1.DebugInspectResponse
		require.NoError(t, encoded.Get(&list))
		assert.Equal(t, "list", list.GetValue().GetType())
		assert.Len(t, list.GetChildren(), 3)

		encoded, err = tl.env.QueryWorkflow(v1.DebugInspectQuery, &v1.DebugInspectRequest{SessionId: "s1", Revision: 3, Expression: "1"})
		require.Error(t, err, "an inspection of a revision the session left must be refused")
		_ = encoded

		encoded, err = tl.env.QueryWorkflow(v1.DebugInspectQuery, &v1.DebugInspectRequest{SessionId: "other", Revision: 4, Expression: "1"})
		require.Error(t, err, "an inspection naming another session must be refused")
		_ = encoded
	}, 76*time.Second)
	tl.ask(80*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "out", Revision: 4,
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT})
	tl.read(81*time.Second, "out", "out")
	tl.ask(90*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbBreakpoints, Session: "s1", Request: "bp",
		Breakpoints: &v1.DebugSetBreakpointsRequest{Breakpoints: []*v1.DebugBreakpoint{
			{Id: "b", Step: "second", Condition: "steps.first != null"},
			{Id: "nowhere", Step: "missing"},
		}}})
	tl.ask(95*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "go", Revision: 6,
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})
	tl.read(96*time.Second, "breakpoint", "go")
	tl.ask(100*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "bye",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH})
	tl.read(101*time.Second, "detached", "bye")

	spec := typedSpec("typed")
	spec.Steps = append(spec.Steps, sleepStep("linger", time.Hour))
	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())

	pending := tl.reads["pending"]
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_RUNNING, pending.GetState(),
		"attached mid-sleep: the ask waits for the next boundary, and nothing claims it was applied")
	assert.Nil(t, pending.GetReceipt(), "delivered is not applied")
	assert.EqualValues(t, v1.DebugProtocol, pending.GetProtocol())

	held := tl.reads["held"]
	require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, held.GetState())
	assert.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_PAUSE, held.GetReason())
	assert.Equal(t, "nested", held.GetOccurrence().GetAddress())
	assert.Equal(t, "s1", held.GetSession().GetSessionId())
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, held.GetReceipt().GetStatus())
	assert.EqualValues(t, 2, held.GetRevision())

	in := tl.reads["in"]
	require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, in.GetState())
	assert.Equal(t, "nested(child)/greet", in.GetOccurrence().GetAddress(), "step-in reaches the callee")
	assert.Equal(t, "child", in.GetOccurrence().GetSite().GetWorkflow())
	require.Len(t, in.GetFrames(), 2)
	assert.EqualValues(t, 4, in.GetRevision())

	retried := tl.reads["after-retries"]
	assert.EqualValues(t, 4, retried.GetRevision(), "a retried command did not advance twice")
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_STALE, retried.GetReceipt().GetStatus())
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_CONFLICT, tl.reads["foreign"].GetReceipt().GetStatus(),
		"a caller who is not the holder cannot move the run")

	out := tl.reads["out"]
	assert.Equal(t, "first", out.GetOccurrence().GetAddress(), "step-out leaves the call")

	stop := tl.reads["breakpoint"]
	assert.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, stop.GetReason())
	assert.Equal(t, "second", stop.GetOccurrence().GetAddress())
	assert.Equal(t, []string{"b"}, stop.GetBreakpointIds())
	require.Len(t, stop.GetBreakpoints(), 2)
	assert.True(t, stop.GetBreakpoints()[0].GetVerified())
	assert.EqualValues(t, 1, stop.GetBreakpoints()[0].GetHits())
	assert.False(t, stop.GetBreakpoints()[1].GetVerified())

	detached := tl.reads["detached"]
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_DETACHED, detached.GetState())
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, detached.GetReceipt().GetStatus())

	var observed []string
	for _, observation := range detached.GetObservations() {
		observed = append(observed, observation.GetText())
	}
	assert.Contains(t, observed, "greet finished")
}

func TestATypedSessionExpiresAndTheRunResumes(t *testing.T) {
	t.Parallel()

	tl := newTimeline(t)
	tl.ask(30*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 45 * time.Second})
	tl.read(time.Hour, "after", "")
	start := tl.env.Now()

	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: typedSpec("expiring")})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())
	assert.Less(t, tl.env.Now().Sub(start), 3*time.Minute, "an abandoned session must not hold the run past its lease")
}

func TestAnExpiredSessionIsReportedExpired(t *testing.T) {
	t.Parallel()

	tl := newTimeline(t)
	tl.ask(30*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 20 * time.Second})
	tl.read(62*time.Second, "held", "")
	tl.read(95*time.Second, "expired", "")
	tl.ask(96*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "late",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})
	// Asks are read at boundaries, so the receipt exists once the run
	// reaches the next one.
	tl.read(time.Hour+5*time.Minute, "late", "late")

	spec := typedSpec("expired")
	spec.Steps = append(spec.Steps, sleepStep("linger", time.Hour), logStep("tail", "t"), sleepStep("linger-more", time.Hour))
	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.NoError(t, tl.env.GetWorkflowError())

	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, tl.reads["held"].GetState())
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED, tl.reads["expired"].GetState())
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, tl.reads["late"].GetReceipt().GetStatus(),
		"a command for a session that expired is told so, not applied to a new one")
}

func TestATypedSessionSurvivesContinueAsNew(t *testing.T) {
	t.Parallel()

	spec := typedSpec("carried")
	first := newTimeline(t)
	first.ask(30*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	first.ask(70*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbBreakpoints, Session: "s1", Request: "bp",
		Breakpoints: &v1.DebugSetBreakpointsRequest{Breakpoints: []*v1.DebugBreakpoint{{Id: "b", Step: "second"}}}})
	first.ask(71*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "go",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})

	first.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec, StepsBudget: 2})
	require.True(t, first.env.IsWorkflowCompleted())

	var continueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, first.env.GetWorkflowError(), &continueAsNew)
	var carried v1.RunState
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continueAsNew.Input, &carried))
	require.NotEmpty(t, carried.GetDebug(), "the session was dropped at the seam")

	var carry v1.DebugCarry
	require.NoError(t, proto.Unmarshal(carried.GetDebug(), &carry))
	assert.Equal(t, "s1", carry.GetSessionId())
	require.Len(t, carry.GetBreakpoints(), 1)

	// Enough budget that the next segment reaches the breakpoint in itself.
	carried.StepsBudget = 100

	second := newTimeline(t)
	second.read(time.Second, "resumed", "go")
	second.ask(2*time.Minute, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "done",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH})
	second.env.ExecuteWorkflow(engine.Run, &carried)
	require.True(t, second.env.IsWorkflowCompleted())

	resumed := second.reads["resumed"]
	assert.Equal(t, "s1", resumed.GetSession().GetSessionId(), "the same session, in the new segment")
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, resumed.GetReceipt().GetStatus(),
		"a receipt from the last segment still answers a retry")
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, resumed.GetState())
	assert.Equal(t, "second", resumed.GetOccurrence().GetAddress(), "the carried breakpoint held the new segment")
	assert.EqualValues(t, 1, resumed.GetOccurrence().GetContinuation())
}

func TestFailureStopsAreRefusedDurably(t *testing.T) {
	t.Parallel()

	tl := newTimeline(t)
	tl.ask(30*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach"})
	tl.ask(62*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbBreakpoints, Session: "s1", Request: "catch",
		Breakpoints: &v1.DebugSetBreakpointsRequest{FailureMode: v1.DebugFailureMode_DEBUG_FAILURE_MODE_ALL}})
	tl.read(63*time.Second, "catch", "catch")
	tl.ask(64*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "bye",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH})

	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: typedSpec("no-catch")})
	require.NoError(t, tl.env.GetWorkflowError())

	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_UNSUPPORTED, tl.reads["catch"].GetReceipt().GetStatus(),
		"an unsupported request is refused explicitly, never silently ignored")
	assert.False(t, tl.reads["catch"].GetCapabilities().GetFailureBreakpoints())
}

// TestATypedAskIsBoundedWhereTheRunReadsIt sends asks no RPC would accept,
// the way a raw signal could: a request id past the schema's bound is refused
// with nothing kept under it, and a refusal that quotes a long, bad verb keeps
// a bounded message. A run carries its receipts across Continue-As-New, so an
// unbounded one would be a way to fail the run from outside it.
func TestATypedAskIsBoundedWhereTheRunReadsIt(t *testing.T) {
	t.Parallel()

	oversized := strings.Repeat("r", v1.MaxDebugRequestIDBytes+1)
	tl := newTimeline(t)
	tl.ask(30*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach"})
	tl.ask(31*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: oversized,
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})
	tl.ask(32*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: strings.Repeat("v", 4*v1.MaxDebugReceiptMessageRunes), Session: "s1", Request: "garbled"})
	tl.read(63*time.Second, "oversized", oversized)
	tl.read(63*time.Second+time.Millisecond, "garbled", "garbled")
	tl.ask(64*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "bye",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH})

	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: typedSpec("bounded")})
	require.NoError(t, tl.env.GetWorkflowError())

	require.NotNil(t, tl.reads["oversized"], "the run ended before it was read")
	assert.Nil(t, tl.reads["oversized"].GetReceipt(), "a receipt was kept under a request id past the schema's bound")
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, tl.reads["oversized"].GetState(),
		"a resume refused for its size moved the run")
	garbled := tl.reads["garbled"].GetReceipt()
	require.NotNil(t, garbled)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, garbled.GetStatus())
	assert.LessOrEqual(t, utf8.RuneCountInString(garbled.GetMessage()), v1.MaxDebugReceiptMessageRunes)
}
