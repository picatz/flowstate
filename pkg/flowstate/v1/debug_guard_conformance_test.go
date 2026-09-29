package flowstatev1_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTheLocalDriverExplainsTheCorpussGuards is the local half of
// [conformance.GuardCase]: a session observing the run quotes each skip's
// condition and reports a condition that could not be evaluated as its step
// failing. The engine package runs the same programs on the durable driver.
func TestTheLocalDriverExplainsTheCorpussGuards(t *testing.T) {
	t.Parallel()

	cases := conformance.GuardCases()
	require.NotEmpty(t, cases, "the corpus is empty, so this asserts nothing")
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			session, err := flowdebug.New(flowdebug.Options{Controlled: true, Workflow: test.Workflow})
			require.NoError(t, err)
			t.Cleanup(func() { _ = session.Close() })
			go func() {
				ctx := v1.NewContextWithRunObserver(v1.NewContextWithDebugger(t.Context(), session), session)
				_, runErr := v1.RunWithInputs(ctx, test.Workflow, nil)
				session.Finished(runErr)
			}()

			want := v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED
			if test.Failed != "" {
				want = v1.DebugRunState_DEBUG_RUN_STATE_FAILED
			}
			next := func(after uint64, want v1.DebugRunState) *v1.DebugSnapshot {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				for {
					snapshot, err := session.WaitSnapshot(ctx, after)
					require.NoError(t, err)
					if snapshot.GetState() == want {
						return snapshot
					}
					after = snapshot.GetRevision()
				}
			}

			at := next(0, v1.DebugRunState_DEBUG_RUN_STATE_HELD)
			receipt, err := session.Resume(t.Context(), &v1.DebugResumeRequest{
				RequestId: "on", ExpectedRevision: at.GetRevision(), Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE,
			})
			require.NoError(t, err)
			require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus(), receipt.GetMessage())

			assertGuardAccount(t, test, next(receipt.GetRevision(), want).GetObservations())
			if test.Failed != "" {
				states := map[string]flowdebug.StepState{}
				for _, step := range session.Steps(0, 100).Steps {
					states[step.ID] = step.State
				}
				assert.Equal(t, flowdebug.StepFailed, states[test.Failed],
					"the step list shows a step whose condition failed as never reached")
			}
		})
	}
}

// assertGuardAccount holds one driver's observations to test's account.
func assertGuardAccount(t *testing.T, test conformance.GuardCase, observations []*v1.DebugObservation) {
	t.Helper()

	var skipped []string
	failed := map[string]string{}
	for _, observation := range observations {
		switch observation.GetKind() {
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_SKIPPED:
			skipped = append(skipped, observation.GetText())
		case v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED:
			failed[observation.GetStepId()] = observation.GetText()
		}
	}
	assert.Equal(t, test.Skipped, skipped)
	if test.Secret != "" {
		for _, observation := range observations {
			assert.NotContains(t, observation.GetText(), test.Secret, "%s's account showed the secret", observation.GetStepId())
		}
	}
	if test.Failed == "" {
		assert.Empty(t, failed)
		return
	}
	require.Contains(t, failed, test.Failed, "the condition that could not be evaluated was not reported")
	assert.Contains(t, failed[test.Failed], test.Quoted)
}
