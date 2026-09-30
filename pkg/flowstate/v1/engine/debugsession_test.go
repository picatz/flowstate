package engine_test

import (
	"fmt"
	"slices"
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
	// Each state carries the breakpoint as it was set, armed or not, so a
	// client that did not set the set can resend it whole.
	assert.True(t, proto.Equal(&v1.DebugBreakpoint{Id: "b", Step: "second", Condition: "steps.first != null"},
		stop.GetBreakpoints()[0].GetDefinition()), "%v", stop.GetBreakpoints()[0].GetDefinition())
	assert.True(t, proto.Equal(&v1.DebugBreakpoint{Id: "nowhere", Step: "missing"},
		stop.GetBreakpoints()[1].GetDefinition()), "%v", stop.GetBreakpoints()[1].GetDefinition())

	detached := tl.reads["detached"]
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_DETACHED, detached.GetState())
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, detached.GetReceipt().GetStatus())

	var observed []string
	for _, observation := range detached.GetObservations() {
		observed = append(observed, observation.GetText())
	}
	assert.Contains(t, observed, "greet completed")
}

// TestADurableBreakpointInsideABodyIsNotArmed: a durable run holds only where
// it has one position, so a breakpoint on a step inside a `for_each:` running
// several iterations at once is reported unarmed, with why, rather than armed
// and silent.
func TestADurableBreakpointInsideABodyIsNotArmed(t *testing.T) {
	t.Parallel()

	tl := newTimeline(t)
	const sre = "sre-1@example.com"
	tl.ask(30*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	tl.ask(65*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbBreakpoints, Session: "s1", Request: "bp",
		Breakpoints: &v1.DebugSetBreakpointsRequest{Breakpoints: []*v1.DebugBreakpoint{
			{Id: "body", Step: "each/touch"},
			{Id: "bare", Step: "touch"},
			{Id: "top", Step: "second"},
		}}})
	tl.read(66*time.Second, "set", "bp")
	tl.ask(70*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "go",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})
	tl.read(71*time.Second, "stop", "go")
	tl.ask(80*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "bye",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH})

	spec := typedSpec("bodies")
	spec.Steps = slices.Insert(spec.Steps, 3, &v1.Node{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
		Items: v1.NewLiteralList(v1.NewLiteral("a"), v1.NewLiteral("b")), Iterator: "item", MaxParallel: 2,
		Body: []*v1.Node{logStep("touch", "touched")},
	}}})
	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())

	states := map[string]*v1.DebugBreakpointState{}
	for _, state := range tl.reads["set"].GetBreakpoints() {
		states[state.GetId()] = state
	}
	require.Len(t, states, 3)
	for _, id := range []string{"body", "bare"} {
		assert.False(t, states[id].GetVerified(), "%s is armed where the durable run never holds", id)
		assert.Contains(t, states[id].GetMessage(), "never holds")
		assert.Empty(t, states[id].GetSites())
	}
	assert.True(t, states["top"].GetVerified())

	stop := tl.reads["stop"]
	assert.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, stop.GetReason())
	assert.Equal(t, "second", stop.GetOccurrence().GetAddress())
}

// TestADurableUntilInsideABodyIsRefused: `until` a step the durable run never
// holds at (one in a concurrent `for_each:`) would release the run to its end. It is refused with the
// breakpoint's reasoning, and the run stays held where it was.
func TestADurableUntilInsideABodyIsRefused(t *testing.T) {
	t.Parallel()

	tl := newTimeline(t)
	const sre = "sre-1@example.com"
	tl.ask(30*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	tl.read(65*time.Second, "held", "attach")
	tl.ask(70*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "into-body",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: "each/touch"})
	tl.read(71*time.Second, "refused", "into-body")
	tl.ask(80*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "to-second",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: "second"})
	tl.read(81*time.Second, "arrived", "to-second")
	tl.ask(90*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "bye",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH})

	spec := typedSpec("until-bodies")
	spec.Steps = slices.Insert(spec.Steps, 3, &v1.Node{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
		Items: v1.NewLiteralList(v1.NewLiteral("a"), v1.NewLiteral("b")), Iterator: "item", MaxParallel: 2,
		Body: []*v1.Node{logStep("touch", "touched")},
	}}})
	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())

	held, refused := tl.reads["held"], tl.reads["refused"]
	require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, held.GetState())
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, refused.GetReceipt().GetStatus())
	assert.Contains(t, refused.GetReceipt().GetMessage(), "inside a parallel branch or a for_each running several iterations at once")
	assert.Contains(t, refused.GetReceipt().GetMessage(), "run until the enclosing step instead")
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, refused.GetState(), "a refused until moved the run")
	assert.Equal(t, held.GetRevision(), refused.GetRevision())
	assert.Equal(t, held.GetOccurrence().GetAddress(), refused.GetOccurrence().GetAddress())

	arrived := tl.reads["arrived"]
	assert.Equal(t, v1.DebugStopReason_DEBUG_STOP_REASON_UNTIL, arrived.GetReason())
	assert.Equal(t, "second", arrived.GetOccurrence().GetAddress())
}

// TestAHistoryBeforeTheUntilRefusalStillReleasesTheRun is the replay half of
// [engine.UntilRefusalChange]: a run recorded by the engine before the refusal
// applied such a resume and ran on, so a history without the marker must be
// answered the same way, or its replay parks where history has it running.
func TestAHistoryBeforeTheUntilRefusalStillReleasesTheRun(t *testing.T) {
	t.Parallel()

	tl := newTimeline(t)
	tl.env.OnGetVersion(engine.UntilRefusalChange, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	// And before bodies were held in at all, or `each/touch` is a stop the
	// run makes and the resume is no release.
	tl.env.OnGetVersion(engine.HoldInBodiesChange, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	const sre = "sre-1@example.com"
	tl.ask(30*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	tl.ask(70*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "into-body",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: "each/touch"})
	tl.read(71*time.Second, "applied", "into-body")

	spec := typedSpec("until-before")
	spec.Steps = slices.Insert(spec.Steps, 3, &v1.Node{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
		Items: v1.NewLiteralList(v1.NewLiteral("a"), v1.NewLiteral("b")), Iterator: "item",
		Body: []*v1.Node{logStep("touch", "touched")},
	}}})
	// Alive when it is read, so the read sees where the resume left it.
	spec.Steps = append(spec.Steps, sleepStep("linger", time.Hour))
	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())

	applied := tl.reads["applied"]
	require.NotNil(t, applied)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, applied.GetReceipt().GetStatus(),
		"a history recorded before the refusal applied this resume")
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_RUNNING, applied.GetState(), "the resume did not release the run")
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
	tl.ask(32*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: strings.Repeat("é", 4*v1.MaxDebugReceiptMessageBytes), Session: "s1", Request: "garbled"})
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
	assert.LessOrEqual(t, len(garbled.GetMessage()), v1.MaxDebugReceiptMessageBytes)
	assert.True(t, utf8.ValidString(garbled.GetMessage()), "a capped message was cut inside a rune")
}

// TestATypedSessionIsNotReleasedByAMalformedOrLegacyResume: while a typed
// session holds the run, a legacy resume on the same channel, even from the
// holder, and a typed resume that names no action are both refused. Either
// would let the run go under a session that still believes it holds it.
func TestATypedSessionIsNotReleasedByAMalformedOrLegacyResume(t *testing.T) {
	t.Parallel()

	tl := newTimeline(t)
	tl.ask(30*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach"})
	tl.env.RegisterDelayedCallback(func() {
		tl.env.SignalWorkflow(v1.DebugSignal, debugAsk(v1.DebugVerbResume, "sre-1@example.com", 0))
	}, 62*time.Second)
	tl.ask(62*time.Second+time.Millisecond, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "no-action"})
	tl.read(63*time.Second, "after", "no-action")
	tl.ask(64*time.Second, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "bye",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH})

	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: typedSpec("legacy-resume")})
	require.NoError(t, tl.env.GetWorkflowError())

	after := tl.reads["after"]
	require.NotNil(t, after, "the run ended before it was read")
	assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, after.GetState(), "the session lost its hold")
	assert.Equal(t, "s1", after.GetSession().GetSessionId())
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, after.GetReceipt().GetStatus(),
		"a typed resume naming no action was applied")
}

// TestATruncatedDurableUntilRefusesAStepTheProgramNeverDeclares: past
// [v1.MaxDebugStaticSites] the sites cannot say a step is absent, but the
// program as written still can. An `until` naming a step no workflow declares,
// or one it declares only inside a parallel branch, where a durable run never holds,
// would release the held run to its end, so it is refused; one naming a step
// declared where the run holds is not.
func TestATruncatedDurableUntilRefusesAStepTheProgramNeverDeclares(t *testing.T) {
	t.Parallel()

	// A program at [v1.MaxDebugStaticSites] is at a bound: decoding it and
	// enumerating its sites is workflow-side work the worker's own budget
	// admits and the SDK's one-second default, under the race detector,
	// does not. See [atABound].
	tl := newTimeline(t)
	atABound(tl.env)
	const sre = "sre-1@example.com"
	tl.ask(30*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	tl.read(65*time.Second, "held", "attach")
	tl.ask(70*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "nowhere",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: "bogus"})
	tl.read(71*time.Second, "refused", "nowhere")
	tl.ask(72*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "in-arm",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: "s7"})
	tl.read(73*time.Second, "unholdable", "in-arm")
	tl.ask(80*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "to-second",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: "second"})
	tl.read(81*time.Second, "arrived", "to-second")
	tl.ask(90*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "bye",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH})

	// Enough sites to pass the cap, in a branch the run never takes.
	wide := &v1.Workflow{Name: "wide", Profile: v1.CurrentProfile}
	for i := range 512 {
		wide.Steps = append(wide.Steps, logStep(fmt.Sprintf("s%d", i), "x"))
	}
	var calls []*v1.Node
	for i := range v1.MaxDebugStaticSites/len(wide.Steps) + 1 {
		calls = append(calls, &v1.Node{Id: fmt.Sprintf("call%d", i), Kind: &v1.Node_Call{Call: &v1.Call{Workflow: wide}}})
	}
	spec := typedSpec("until-truncated")
	// Inside a `switch:` arm the run never takes, so the branch is walked as
	// written and never run (a `parallel:` this wide is refused when it starts).
	spec.Steps = append(spec.Steps, &v1.Node{Id: "never", Kind: &v1.Node_Switch{Switch: &v1.Switch{
		Value: v1.NewLiteral("live"),
		Cases: []*v1.Switch_Case{{Values: []*v1.Value{v1.NewLiteral("never")}, Steps: []*v1.Node{
			{Id: "fan", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{{Steps: calls}}}}},
		}}},
	}}})
	_, truncated := v1.DebugStaticSites(spec)
	require.True(t, truncated, "the program did not pass the cap, so this proves nothing")

	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())

	held, refused := tl.reads["held"], tl.reads["refused"]
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, refused.GetReceipt().GetStatus())
	assert.Contains(t, refused.GetReceipt().GetMessage(), `no step matches "bogus"`)
	assert.Equal(t, held.GetRevision(), refused.GetRevision(), "a refused until moved the run")
	unholdable := tl.reads["unholdable"]
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_REFUSED, unholdable.GetReceipt().GetStatus(),
		"an until the run can never stop at was applied")
	assert.Contains(t, unholdable.GetReceipt().GetMessage(), "inside a parallel branch or a for_each running several iterations at once")
	assert.Equal(t, held.GetRevision(), unholdable.GetRevision(), "a refused until moved the run")
	assert.Equal(t, "second", tl.reads["arrived"].GetOccurrence().GetAddress(), "a declared until was refused")
}
