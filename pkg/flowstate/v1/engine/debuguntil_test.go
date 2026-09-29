package engine_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.Equal(t, []string{flowdebug.MissedUntilNotice("settle")}, missedUntilNotices(after),
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

			assert.Equal(t, []string{flowdebug.MissedUntilNotice(test.Until)}, missedUntilNotices(querySnapshot(t, tl.env, "")))
		})
	}
}
