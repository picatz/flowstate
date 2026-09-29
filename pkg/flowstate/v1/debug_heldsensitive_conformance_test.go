package flowstatev1_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestTheLocalDriverWithholdsTheCorpussHeldSensitive is the local half of
// [conformance.HeldSensitiveCase]. The engine package holds the same programs
// at the same place on the durable driver, and both must withhold the value.
func TestTheLocalDriverWithholdsTheCorpussHeldSensitive(t *testing.T) {
	t.Parallel()

	cases := conformance.HeldSensitiveCases()
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

			held := func(after uint64) *v1.DebugSnapshot {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				for {
					snapshot, err := session.WaitSnapshot(ctx, after)
					require.NoError(t, err)
					if snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD {
						return snapshot
					}
					after = snapshot.GetRevision()
				}
			}

			at := held(0)
			receipt, err := session.Resume(t.Context(), &v1.DebugResumeRequest{
				RequestId: "until", ExpectedRevision: at.GetRevision(),
				Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_RUN_UNTIL, Until: test.Until,
			})
			require.NoError(t, err)
			require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus(), receipt.GetMessage())
			at = held(receipt.GetRevision())
			require.Equal(t, test.HeldAt, at.GetOccurrence().GetAddress())

			inspected, err := session.Inspect(t.Context(), &v1.DebugInspectRequest{Revision: at.GetRevision(), Expression: test.Expression})
			require.NoError(t, err)
			require.Empty(t, inspected.GetError(), "the inspection failed, so this proves nothing")
			encoded, err := protojson.Marshal(inspected)
			require.NoError(t, err)
			assert.NotContains(t, string(encoded), test.Secret)
			assert.Contains(t, string(encoded), "[redacted]", "nothing was withheld, so the value may never have been reached")
		})
	}
}
