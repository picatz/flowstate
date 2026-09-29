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

// TestTheLocalDriverWithholdsTheCorpussFailedSensitive is the local half of
// [conformance.FailedSensitiveCase]: a session observing the run, given no
// redactor of its own, reports every failed step without the secret, and so
// does the failed run's final message. The engine package runs the same
// programs on the durable driver.
func TestTheLocalDriverWithholdsTheCorpussFailedSensitive(t *testing.T) {
	t.Parallel()

	cases := conformance.FailedSensitiveCases()
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
			final := next(receipt.GetRevision(), v1.DebugRunState_DEBUG_RUN_STATE_FAILED)

			failed := map[string]string{}
			for _, observation := range final.GetObservations() {
				if observation.GetKind() == v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_FAILED {
					failed[observation.GetStepId()] = observation.GetText()
				}
			}
			for _, step := range test.Failed {
				require.Contains(t, failed, step, "no failure was reported for %s", step)
				assert.Contains(t, failed[step], test.Quoted, "%s's report does not quote its error, so this proves nothing", step)
				assert.NotContains(t, failed[step], test.Secret, "%s's report showed the secret", step)
			}
			assert.Contains(t, final.GetMessage(), test.Quoted, "the final message does not quote the error, so this proves nothing")
			assert.NotContains(t, final.GetMessage(), test.Secret, "the final message showed the secret")
		})
	}
}
