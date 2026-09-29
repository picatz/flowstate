package engine_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// missedUntilNotices counts the notice observations saying a run completed
// past an `until`.
func missedUntilNotices(snapshot *v1.DebugSnapshot) []string {
	var notices []string
	for _, observation := range snapshot.GetObservations() {
		if observation.GetKind() == v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_NOTICE &&
			strings.HasPrefix(observation.GetText(), "the run completed without stopping at") {
			notices = append(notices, observation.GetText())
		}
	}

	return notices
}

// TestADurableRunCompletedPastItsUntilSaysSo is #2201: a durable run resumed
// with `until` toward a step it has already passed completes without holding
// there, and every read of the completed run carries the notice the local
// driver prints, in its words — however long after the resume the read comes,
// and in the snapshot the structured output formats serialize.
func TestADurableRunCompletedPastItsUntilSaysSo(t *testing.T) {
	t.Parallel()

	tl := newTimeline(t)
	const sre = "sre-1@example.com"
	tl.ask(30*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	tl.read(65*time.Second, "held", "attach")
	tl.ask(70*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "back",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: "settle"})

	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: typedSpec("until-passed")})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())
	require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, tl.reads["held"].GetState())

	after := querySnapshot(t, tl.env, "")
	assert.Equal(t, []string{flowdebug.MissedUntilNotice("settle", nil)}, missedUntilNotices(after),
		"a completed run said nothing of the `until` it never stopped at")
}

// TestADurableRunThatStoppedAtItsUntilOrFailedSaysNothing: the notice is for a
// run that completed past its `until`. One that stopped there, one never asked
// for an `until`, and one that failed before reaching it say nothing of it.
func TestADurableRunThatStoppedAtItsUntilOrFailedSaysNothing(t *testing.T) {
	t.Parallel()

	until := v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL
	for name, test := range map[string]struct {
		action v1.DebugResumeAction
		until  string
		fails  bool
		// outputsFail fails the run after every step completed, in its
		// declared outputs, with the `until` still armed.
		outputsFail bool
	}{
		"stopped at its until":  {action: until, until: "second"},
		"resumed without until": {action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE},
		"failed before it":      {action: until, until: "second", fails: true},
		// A resume carries `until` whatever its action. A step out at the top
		// level runs to the end, and a stray target on it is not an `until`.
		"stepped out naming a target": {action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_OUT, until: "settle"},
		"failed in its outputs":       {action: until, until: "settle", outputsFail: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			tl := newTimeline(t)
			const sre = "sre-1@example.com"
			tl.ask(30*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
			tl.ask(70*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "go",
				Action: test.action, Until: test.until})
			// Released by a `continue` once it stops at its `until`, so the run
			// completes with the session still attached: a detach would end the
			// session, and a notice that is absent because nobody is attached
			// proves nothing.
			tl.ask(80*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "on",
				Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})

			spec := typedSpec("until-" + strings.ReplaceAll(name, " ", "-"))
			if test.fails {
				// Before `second`, so the run fails while the `until` is armed.
				spec.Steps = append(spec.Steps[:3], append([]*v1.Node{
					{Id: "boom", Kind: &v1.Node_Value{Value: v1.NewExpr("1 / 0")}},
				}, spec.Steps[3:]...)...)
			}
			if test.outputsFail {
				spec.DeclaredOutputs = []*v1.OutputDeclaration{{Name: "bad", Value: v1.NewExpr("1 / 0")}}
			}
			tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
			require.True(t, tl.env.IsWorkflowCompleted())
			if test.fails || test.outputsFail {
				require.Error(t, tl.env.GetWorkflowError(), "the fixture's run did not fail")
			} else {
				require.NoError(t, tl.env.GetWorkflowError())
			}

			assert.Empty(t, missedUntilNotices(querySnapshot(t, tl.env, "")))
		})
	}
}

// TestTheDurableDriverSaysTheCorpussMissedUntil is the durable half of
// [conformance.MissedUntilCase]: the same program held at the same step and
// resumed toward the same target as the local half, and the same notice.
func TestTheDurableDriverSaysTheCorpussMissedUntil(t *testing.T) {
	t.Parallel()

	cases := conformance.MissedUntilCases()
	require.NotEmpty(t, cases, "the corpus is empty, so this asserts nothing")
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			spec := proto.CloneOf(test.Workflow)
			spec.Debug = debugSpec(spec.GetName()).GetDebug()

			tl := newTimeline(t)
			const sre = "sre-1@example.com"
			// Asked before the run starts, so it holds at the first boundary,
			// where the local session holds on entry.
			tl.ask(0, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
			tl.read(time.Second, "held", "attach")
			tl.ask(2*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "until",
				Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: test.Until})

			tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
			require.True(t, tl.env.IsWorkflowCompleted())
			require.NoError(t, tl.env.GetWorkflowError())

			held := tl.reads["held"]
			require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, held.GetState())
			require.Equal(t, test.HeldAt, held.GetOccurrence().GetAddress())

			assert.Equal(t, []string{flowdebug.MissedUntilNotice(test.Until, nil)}, missedUntilNotices(querySnapshot(t, tl.env, "")))
		})
	}
}

// TestADurableMissedUntilWithholdsTheAcceptingCalleesSensitiveInputs: an
// `until` applied while held inside a callee was written against that
// callee's scope, and a value only the callee declares sensitive is withheld
// from the notice the completed run records, although completion is judged
// in the root, which does not declare it (Codex, #2204).
func TestADurableMissedUntilWithholdsTheAcceptingCalleesSensitiveInputs(t *testing.T) {
	t.Parallel()

	spec := typedSpec("until-in-callee")
	call := spec.GetSteps()[1].GetCall()
	call.Workflow.DeclaredInputs = []*v1.InputDeclaration{{Name: "word", Type: v1.InputDeclaration_TYPE_STRING, Sensitive: true}}
	call.Arguments = map[string]*v1.Value{"word": v1.NewLiteral("greet")}

	tl := newTimeline(t)
	const sre = "sre-1@example.com"
	tl.ask(30*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	tl.ask(70*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "in", Revision: 2,
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN})
	tl.read(71*time.Second, "in", "in")
	tl.ask(80*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "back",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: "nested/greet"})

	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())
	require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, tl.reads["in"].GetState())
	require.Equal(t, "greet", tl.reads["in"].GetOccurrence().GetSite().GetPath()[len(tl.reads["in"].GetOccurrence().GetSite().GetPath())-1],
		"the run was not held inside the callee")

	assert.Equal(t, []string{"the run completed without stopping at `until nested/[redacted]`"},
		missedUntilNotices(querySnapshot(t, tl.env, "")), "the callee's sensitive input reached the notice")
}

// askedMidRun reports, once the run has finished, whether it was still
// running at at, when a test's ask is delivered.
func askedMidRun(tl *timeline, at time.Duration) *bool {
	running := new(bool)
	tl.env.RegisterDelayedCallback(func() { *running = !tl.env.IsWorkflowCompleted() }, at)

	return running
}

// TestTheDurableDriverSaysTheCorpussMissedPause is the durable half of
// [conformance.MissedPauseCase]: the same program, the same pause asked
// while its last step sleeps, and the same notice as the local half. The
// durable driver applies an ask at a step boundary, so this one is applied
// when the run completes, and its receipt says the run ended first.
func TestTheDurableDriverSaysTheCorpussMissedPause(t *testing.T) {
	t.Parallel()

	cases := conformance.MissedPauseCases()
	require.NotEmpty(t, cases, "the corpus is empty, so this asserts nothing")
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			spec := proto.CloneOf(test.Workflow)
			spec.Debug = debugSpec(spec.GetName()).GetDebug()

			tl := newTimeline(t)
			const sre = "sre-1@example.com"
			// Partway through the sleep, so the pause is asked while the
			// last step is under way.
			tl.ask(test.Sleep/2, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "pause", Lease: 5 * time.Minute})
			asked := askedMidRun(tl, test.Sleep/2)

			tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
			require.True(t, tl.env.IsWorkflowCompleted())
			require.NoError(t, tl.env.GetWorkflowError())
			require.True(t, *asked, "the pause was not asked while the run was under way, so this proves nothing")

			final := querySnapshot(t, tl.env, "pause")
			assert.NotEqual(t, v1.DebugRunState_DEBUG_RUN_STATE_PAUSE_REQUESTED, final.GetState(),
				"the completed run still says it holds at its next boundary")
			assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, final.GetReceipt().GetStatus(),
				"the pause asked while the last step slept was answered by nothing")
			assert.Equal(t, flowdebug.MissedPauseNotice, final.GetReceipt().GetMessage())
			var notices []string
			for _, observation := range final.GetObservations() {
				if observation.GetKind() == v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_NOTICE {
					notices = append(notices, observation.GetText())
				}
			}
			assert.Equal(t, []string{flowdebug.MissedPauseNotice}, notices)
		})
	}
}

// TestAHistoryBeforeTheLateAskChangeLeavesItsAsksUnread is the replay half
// of [engine.LateAskChange]: a run recorded by the engine before it completed
// with a pause asked during its last step unread, so a history without the
// marker must leave it unread, answering and recording nothing for it.
func TestAHistoryBeforeTheLateAskChangeLeavesItsAsksUnread(t *testing.T) {
	t.Parallel()

	cases := conformance.MissedPauseCases()
	require.NotEmpty(t, cases)
	test := cases[0]
	spec := proto.CloneOf(test.Workflow)
	spec.Debug = debugSpec(spec.GetName()).GetDebug()

	tl := newTimeline(t)
	tl.env.OnGetVersion(engine.LateAskChange, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	tl.ask(test.Sleep/2, "sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "pause", Lease: 5 * time.Minute})
	asked := askedMidRun(tl, test.Sleep/2)
	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())
	require.True(t, *asked, "the pause was not asked while the run was under way, so this proves nothing")

	final := querySnapshot(t, tl.env, "pause")
	assert.Nil(t, final.GetReceipt(), "a history recorded before the change never read this ask")
	assert.Empty(t, final.GetObservations(), "a history recorded before the change said nothing of it")
}

// TestEveryPauseAskedDuringTheLastStepIsAnswered: two pause asks, each its
// own request, arrive while the last step sleeps. The run completes without
// holding, and each is answered ENDED, not only the last (Copilot, #2220).
func TestEveryPauseAskedDuringTheLastStepIsAnswered(t *testing.T) {
	t.Parallel()

	cases := conformance.MissedPauseCases()
	require.NotEmpty(t, cases)
	test := cases[0]
	spec := proto.CloneOf(test.Workflow)
	spec.Debug = debugSpec(spec.GetName()).GetDebug()

	tl := newTimeline(t)
	const sre = "sre-1@example.com"
	tl.ask(test.Sleep/2, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "pause-1", Lease: 5 * time.Minute})
	tl.ask(test.Sleep/2, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "pause-2", Lease: 5 * time.Minute})
	asked := askedMidRun(tl, test.Sleep/2)
	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())
	require.True(t, *asked, "the pauses were not asked while the run was under way, so this proves nothing")

	for _, request := range []string{"pause-1", "pause-2"} {
		receipt := querySnapshot(t, tl.env, request).GetReceipt()
		assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, receipt.GetStatus(), "%s was answered by nothing", request)
	}
}

// TestEveryPauseAskedBeforeAHoldIsReceipted: two pause asks, each its own
// request, arrive while a step sleeps, and the run holds at the next
// boundary. Each is receipted APPLIED. A history recorded before
// [engine.PauseReceiptsChange] receipted only the last, and a replay of one
// must too, since a receipt decides whether a retry is applied again
// (#2220).
func TestEveryPauseAskedBeforeAHoldIsReceipted(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		before bool
		first  v1.DebugCommandStatus
	}{
		{name: "receipts every pause", first: v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED},
		{name: "a history before the change receipts only the last", before: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			tl := newTimeline(t)
			if test.before {
				tl.env.OnGetVersion(engine.PauseReceiptsChange, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
			}
			const sre = "sre-1@example.com"
			tl.ask(settleFor/2, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "pause-1", Lease: 5 * time.Minute})
			tl.ask(settleFor/2, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "pause-2", Lease: 5 * time.Minute})
			tl.read(settleFor+time.Second, "first", "pause-1")
			tl.read(settleFor+time.Second, "second", "pause-2")
			tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: debugSpec("receipts")})
			require.True(t, tl.env.IsWorkflowCompleted())
			require.NoError(t, tl.env.GetWorkflowError())

			require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, tl.reads["second"].GetState(), "the run did not hold after the pauses, so this proves nothing")
			assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, tl.reads["second"].GetReceipt().GetStatus())
			assert.Equal(t, test.first, tl.reads["first"].GetReceipt().GetStatus())
		})
	}
}

// TestAPauseCarriedAcrossContinueAsNewIsAnsweredAtCompletion: a pause an
// earlier segment carried across Continue-As-New, in a segment that completes
// without reaching a step boundary, is still answered as over and said
// (Codex, #2220).
func TestAPauseCarriedAcrossContinueAsNewIsAnsweredAtCompletion(t *testing.T) {
	t.Parallel()

	// A segment resumed past its last step reaches no boundary.
	spec := debugSpec("carried-pause")
	ask := typedAsk("sre-1@example.com", &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "carried", Lease: 5 * time.Minute})

	env := newWaitEnv(t)
	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow:       spec,
		NextStep:       int32(len(spec.GetSteps())),
		PendingSignals: []*v1.PendingSignal{{Name: v1.DebugSignal, Payload: ask.GetPayload(), Sender: ask.GetSender()}},
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	final := querySnapshot(t, env, "carried")
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, final.GetReceipt().GetStatus(),
		"a carried pause was answered by nothing")
	assert.Contains(t, final.GetReceipt().GetMessage(), flowdebug.MissedPauseNotice)
}

// TestAPauseBehindABacklogAtCompletionIsAnswered: more asks than one batch
// reads arrive while the last step sleeps, a pause last. The completing run
// reads them all, in paced batches, so the pause behind them is answered too
// (Codex, #2220).
func TestAPauseBehindABacklogAtCompletionIsAnswered(t *testing.T) {
	t.Parallel()

	cases := conformance.MissedPauseCases()
	require.NotEmpty(t, cases)
	test := cases[0]
	spec := proto.CloneOf(test.Workflow)
	spec.Debug = debugSpec(spec.GetName()).GetDebug()

	tl := newTimeline(t)
	const sre = "sre-1@example.com"
	for i := range v1.MaxDebugAsksPerBoundary {
		tl.ask(test.Sleep/2, sre, &v1.DebugAsk{Verb: v1.DebugVerbRenew, Session: "s1", Request: fmt.Sprintf("renew-%d", i)})
	}
	tl.ask(test.Sleep/2, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "pause", Lease: 5 * time.Minute})
	asked := askedMidRun(tl, test.Sleep/2)
	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())
	require.True(t, *asked, "the asks did not arrive while the run was under way, so this proves nothing")

	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, querySnapshot(t, tl.env, "pause").GetReceipt().GetStatus(),
		"a pause behind a full batch was answered by nothing")
}

// TestABreakpointSetAskedDuringTheLastStepIsNotApplied: beside the pause the
// completing run answers, a breakpoint set asked while the last step sleeps
// is read past, not installed and receipted at a run that has no boundary
// left to hold at: an editor reports it as one the run ended before applying
// (Codex, #2220).
func TestABreakpointSetAskedDuringTheLastStepIsNotApplied(t *testing.T) {
	t.Parallel()

	cases := conformance.MissedPauseCases()
	require.NotEmpty(t, cases)
	test := cases[0]
	spec := proto.CloneOf(test.Workflow)
	spec.Debug = debugSpec(spec.GetName()).GetDebug()

	tl := newTimeline(t)
	const sre = "sre-1@example.com"
	tl.ask(test.Sleep/2, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "pause", Lease: 5 * time.Minute})
	tl.ask(test.Sleep/2, sre, &v1.DebugAsk{Verb: v1.DebugVerbBreakpoints, Session: "s1", Request: "bp",
		Breakpoints: &v1.DebugSetBreakpointsRequest{Breakpoints: []*v1.DebugBreakpoint{{Id: "b1", Step: "first"}}}})
	asked := askedMidRun(tl, test.Sleep/2)
	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
	require.True(t, tl.env.IsWorkflowCompleted())
	require.NoError(t, tl.env.GetWorkflowError())
	require.True(t, *asked, "the asks did not arrive while the run was under way, so this proves nothing")

	require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_ENDED, querySnapshot(t, tl.env, "pause").GetReceipt().GetStatus(),
		"the pause beside it was not answered, so this proves nothing")
	final := querySnapshot(t, tl.env, "bp")
	assert.Nil(t, final.GetReceipt(), "a breakpoint set was receipted at a run with no boundary left")
	assert.Empty(t, final.GetBreakpoints(), "a breakpoint set was installed at a run with no boundary left")
}
