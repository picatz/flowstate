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

// TestTheLocalDriverSaysTheCorpussMissedUntil is the local half of
// [conformance.MissedUntilCase]. The engine package runs the same programs
// and targets on the durable driver, and both must record the one notice.
func TestTheLocalDriverSaysTheCorpussMissedUntil(t *testing.T) {
	t.Parallel()

	cases := conformance.MissedUntilCases()
	require.NotEmpty(t, cases, "the corpus is empty, so this asserts nothing")
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			session, err := flowdebug.New(flowdebug.Options{Controlled: true, Workflow: test.Workflow})
			require.NoError(t, err)
			t.Cleanup(func() { _ = session.Close() })
			go func() {
				_, runErr := v1.RunWithInputs(v1.NewContextWithDebugger(t.Context(), session), test.Workflow, nil)
				session.Finished(runErr)
			}()

			// Waits past a revision for a hold or the end of the run.
			settled := func(after uint64) *v1.DebugSnapshot {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				for {
					snapshot, err := session.WaitSnapshot(ctx, after)
					require.NoError(t, err)
					switch snapshot.GetState() {
					case v1.DebugRunState_DEBUG_RUN_STATE_HELD, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED,
						v1.DebugRunState_DEBUG_RUN_STATE_FAILED:
						return snapshot
					}
					after = snapshot.GetRevision()
				}
			}

			at := settled(0)
			require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, at.GetState())
			require.Equal(t, test.HeldAt, at.GetOccurrence().GetAddress())

			receipt, err := session.Resume(t.Context(), &v1.DebugResumeRequest{
				RequestId: "until", ExpectedRevision: at.GetRevision(),
				Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: test.Until,
			})
			require.NoError(t, err)
			require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus(), receipt.GetMessage())

			end := settled(receipt.GetRevision())
			require.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, end.GetState())

			var notices []string
			for _, observation := range end.GetObservations() {
				if observation.GetKind() == v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_NOTICE {
					notices = append(notices, observation.GetText())
				}
			}
			assert.Equal(t, []string{flowdebug.MissedUntilNotice(test.Until, nil)}, notices)
		})
	}
}
