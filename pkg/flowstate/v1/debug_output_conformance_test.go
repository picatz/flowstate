package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTheLocalDriverReportsTheCorpussOutputs is the local half of
// [conformance.OutputCase]: a session observing the run reports each finished
// step with the outputs it produced, withheld as a hold there would. The
// engine package runs the same programs on the durable driver.
func TestTheLocalDriverReportsTheCorpussOutputs(t *testing.T) {
	t.Parallel()

	cases := conformance.OutputCases()
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

			at := awaitDebugState(t, session, 0, v1.DebugRunState_DEBUG_RUN_STATE_HELD)
			receipt, err := session.Resume(t.Context(), &v1.DebugResumeRequest{
				RequestId: "on", ExpectedRevision: at.GetRevision(), Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE,
			})
			require.NoError(t, err)
			require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus(), receipt.GetMessage())

			done := awaitDebugState(t, session, receipt.GetRevision(), v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED)
			assert.Empty(t, test.Problems(done.GetObservations()))
		})
	}
}
