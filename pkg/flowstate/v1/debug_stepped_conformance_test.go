package flowstatev1_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTheLocalDriverSteppedThroughTheCorpusStopsWhereItSays is the local half
// of [conformance.SteppedCase]: a session that steps in at every stop is held
// at exactly the addresses the corpus lists, from the first boundary to the
// end of the run. The engine package steps the same programs on the durable
// driver.
func TestTheLocalDriverSteppedThroughTheCorpusStopsWhereItSays(t *testing.T) {
	t.Parallel()

	cases := conformance.SteppedCases()
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

			var stops []string
			snapshot := awaitDebugState(t, session, 0, v1.DebugRunState_DEBUG_RUN_STATE_HELD)
			for step := range len(test.Stops) + 2 {
				stops = append(stops, snapshot.GetOccurrence().GetAddress())
				receipt, err := session.Resume(t.Context(), &v1.DebugResumeRequest{
					RequestId: fmt.Sprintf("step-%d", step), ExpectedRevision: snapshot.GetRevision(), Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN,
				})
				require.NoError(t, err)
				require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus(), receipt.GetMessage())

				snapshot = awaitNextStop(t, session, receipt.GetRevision())
				if snapshot.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD {
					break
				}
			}

			assert.Equal(t, test.Stops, stops)
			assert.Equal(t, v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, snapshot.GetState(), "the run did not end after the last stop")
		})
	}
}

// awaitNextStop waits past revision after for the run to be held again or to
// end, whichever comes first.
func awaitNextStop(t *testing.T, session *flowdebug.Session, after uint64) *v1.DebugSnapshot {
	t.Helper()

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
