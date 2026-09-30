package engine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTheDurableDriverWithholdsTheCorpussHeldSensitive is the durable half of
// [conformance.HeldSensitiveCase]: the same program, held at the same place
// inside the callee as the local half, and the same value withheld from the
// same inspection.
func TestTheDurableDriverWithholdsTheCorpussHeldSensitive(t *testing.T) {
	t.Parallel()

	cases := conformance.HeldSensitiveCases()
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
			tl.ask(2*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "until",
				Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: test.Until})
			var held *v1.DebugSnapshot
			var inspected v1.DebugInspectResponse
			tl.env.RegisterDelayedCallback(func() {
				held = querySnapshot(t, tl.env, "")
				encoded, err := tl.env.QueryWorkflow(v1.DebugInspectQuery, &v1.DebugInspectRequest{
					SessionId: "s1", Revision: held.GetRevision(), Expression: test.Expression,
				})
				require.NoError(t, err)
				require.NoError(t, encoded.Get(&inspected))
			}, 3*time.Second)
			tl.ask(4*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "on",
				Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})

			tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec})
			require.True(t, tl.env.IsWorkflowCompleted())
			require.NoError(t, tl.env.GetWorkflowError())

			require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, held.GetState())
			require.Equal(t, test.HeldAt, held.GetOccurrence().GetAddress())
			require.Empty(t, inspected.GetError(), "the inspection failed, so this proves nothing")
			encoded, err := protojson.Marshal(&inspected)
			require.NoError(t, err)
			assert.NotContains(t, string(encoded), test.Secret)
			assert.Contains(t, string(encoded), "[redacted]", "nothing was withheld, so the value may never have been reached")
		})
	}
}

// TestWhatACallHandedBackIsWithheldAcrossContinueAsNew: a segment holds what
// its calls handed back in memory, and the seam carries only that there was
// something, so the next segment withholds everything a session is shown
// rather than less than the segment before it did (#2213).
func TestWhatACallHandedBackIsWithheldAcrossContinueAsNew(t *testing.T) {
	t.Parallel()

	var test conformance.HeldSensitiveCase
	for _, candidate := range conformance.HeldSensitiveCases() {
		if candidate.Workflow.GetName() == "returned-sensitive" {
			test = candidate
		}
	}
	require.NotNil(t, test.Workflow, "the corpus lost its returned-output case")
	spec := proto.CloneOf(test.Workflow)
	spec.Debug = debugSpec(spec.GetName()).GetDebug()

	// No session in the first segment: what a call hands back is kept for
	// one that attaches later.
	first := newTimeline(t)
	first.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec, StepsBudget: 2})
	require.True(t, first.env.IsWorkflowCompleted())
	var continueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, first.env.GetWorkflowError(), &continueAsNew)
	var carried v1.RunState
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continueAsNew.Input, &carried))
	require.Contains(t, carried.GetOutputs().GetStepValues(), "nested", "the seam fell before the call returned, so this proves nothing")
	require.NotContains(t, carried.GetOutputs().GetStepValues(), "later", "the seam fell after the hold, so this proves nothing")

	var carry v1.DebugCarry
	require.NoError(t, proto.Unmarshal(carried.GetDebug(), &carry))
	assert.True(t, carry.GetReturnedWithheld(), "the seam forgot that a call handed back something withheld")
	assert.NotContains(t, string(carried.GetDebug()), test.Secret, "the carry held the value itself")

	carried.StepsBudget = 100
	second := newTimeline(t)
	const sre = "sre-1@example.com"
	second.ask(0, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	second.ask(2*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "until",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: test.Until})
	var held *v1.DebugSnapshot
	var inspected v1.DebugInspectResponse
	second.env.RegisterDelayedCallback(func() {
		held = querySnapshot(t, second.env, "")
		encoded, err := second.env.QueryWorkflow(v1.DebugInspectQuery, &v1.DebugInspectRequest{
			SessionId: "s1", Revision: held.GetRevision(), Expression: test.Expression,
		})
		require.NoError(t, err)
		require.NoError(t, encoded.Get(&inspected))
	}, 3*time.Second)
	second.ask(4*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "on",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})
	second.env.ExecuteWorkflow(engine.Run, &carried)
	require.True(t, second.env.IsWorkflowCompleted())
	require.NoError(t, second.env.GetWorkflowError())

	require.Equal(t, test.HeldAt, held.GetOccurrence().GetAddress())
	encoded, err := protojson.Marshal(&inspected)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), test.Secret, "the next segment showed what a call handed back before the seam")
	assert.Equal(t, "[redacted]", inspected.GetValue().GetRendered(), "the next segment did not withhold the answer whole")

	// And what the segment reported of the steps it ran: with what withheld
	// the values not carried across the seam, an account of a step that
	// finished is withheld whole, its step and output names included, and none
	// of it shows what a call handed back.
	var finished int
	for _, observation := range held.GetObservations() {
		assert.NotContains(t, observation.GetText(), test.Secret, "%s's account showed what a call handed back before the seam", observation.GetStepId())
		if observation.GetKind() == v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FINISHED {
			assert.Equal(t, "[redacted]", observation.GetText(), "an account of a step that finished was not withheld whole")
			finished++
		}
	}
	assert.NotZero(t, finished, "no step finished in the segment, so this proves nothing about its accounts")
}

// returnedSensitiveCase is the corpus's returned-output case, declaring
// `debug:`.
func returnedSensitiveCase(t *testing.T) (conformance.HeldSensitiveCase, *v1.Workflow) {
	t.Helper()

	for _, candidate := range conformance.HeldSensitiveCases() {
		if candidate.Workflow.GetName() == "returned-sensitive" {
			spec := proto.CloneOf(candidate.Workflow)
			spec.Debug = debugSpec(spec.GetName()).GetDebug()

			return candidate, spec
		}
	}
	require.FailNow(t, "the corpus lost its returned-output case")

	return conformance.HeldSensitiveCase{}, nil
}

// continuedAsNew is the state a segment that ended at a seam carried on, and
// the debug carry inside it.
func continuedAsNew(t *testing.T, tl *timeline) (*v1.RunState, *v1.DebugCarry) {
	t.Helper()

	require.True(t, tl.env.IsWorkflowCompleted())
	var continueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, tl.env.GetWorkflowError(), &continueAsNew, "the segment did not continue as new, so this proves nothing")
	var carried v1.RunState
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continueAsNew.Input, &carried))
	var carry v1.DebugCarry
	require.NoError(t, proto.Unmarshal(carried.GetDebug(), &carry))

	return &carried, &carry
}

// TestWhatACallHandedBackOutlivesTheSessionThatSawIt: a session attaches after
// a call handed back something withheld and detaches before the seam. The
// session's end replaces the carry, and the seam must still say a call
// handed something back (#2213).
func TestWhatACallHandedBackOutlivesTheSessionThatSawIt(t *testing.T) {
	t.Parallel()

	test, spec := returnedSensitiveCase(t)
	tl := newTimeline(t)
	const sre = "sre-1@example.com"
	tl.ask(0, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	tl.ask(2*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "until",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: "copied"})
	var held *v1.DebugSnapshot
	tl.env.RegisterDelayedCallback(func() { held = querySnapshot(t, tl.env, "") }, 3*time.Second)
	tl.ask(4*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "detach",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_DETACH})
	tl.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec, StepsBudget: 4})

	require.Equal(t, "copied", held.GetOccurrence().GetAddress(), "the session did not hold after the call returned, so this proves nothing")
	carried, carry := continuedAsNew(t, tl)
	require.NotContains(t, carried.GetOutputs().GetStepValues(), "later", "the seam fell after the run's last step, so this proves nothing")
	require.NotEqual(t, v1.DebugRunState_DEBUG_RUN_STATE_UNSPECIFIED, carry.GetEnded(), "the session did not end before the seam, so this proves nothing")
	assert.True(t, carry.GetReturnedWithheld(), "the session's end forgot that a call handed back something withheld")
	assert.NotContains(t, string(carried.GetDebug()), test.Secret, "the carry held the value itself")
}

// TestAnAttachAfterTheSeamKeepsWhatACallHandedBack: a segment that inherited
// the flag and was attached to replaces its carry on attach, and the next
// seam must still carry the flag on (#2213).
func TestAnAttachAfterTheSeamKeepsWhatACallHandedBack(t *testing.T) {
	t.Parallel()

	_, spec := returnedSensitiveCase(t)
	first := newTimeline(t)
	first.env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: spec, StepsBudget: 3})
	carried, carry := continuedAsNew(t, first)
	require.True(t, carry.GetReturnedWithheld(), "the first seam did not carry the flag, so this proves nothing")

	carried.StepsBudget = 1
	second := newTimeline(t)
	const sre = "sre-1@example.com"
	second.ask(0, sre, &v1.DebugAsk{Verb: v1.DebugVerbPause, Session: "s1", Request: "attach", Lease: 5 * time.Minute})
	second.ask(2*time.Second, sre, &v1.DebugAsk{Verb: v1.DebugVerbResume, Session: "s1", Request: "on",
		Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE})
	second.env.ExecuteWorkflow(engine.Run, carried)

	_, carry = continuedAsNew(t, second)
	require.NotEmpty(t, carry.GetSessionId(), "no session attached in the second segment, so this proves nothing")
	assert.True(t, carry.GetReturnedWithheld(), "an attach after the seam forgot that a call handed back something withheld")
}
