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

// sleepGate is a [v1.Clock] whose waits end only when a test releases them,
// so a `sleep:` step is under way for exactly as long as the test needs.
type sleepGate struct {
	waiting chan time.Duration
	release chan time.Time
}

func (g *sleepGate) Now() time.Time { return time.Unix(0, 0) }

func (g *sleepGate) After(d time.Duration) <-chan time.Time {
	g.waiting <- d

	return g.release
}

// TestTheLocalDriverSaysTheCorpussMissedPause is the local half of
// [conformance.MissedPauseCase]. The engine package asks the same pause of
// the same programs on the durable driver, and both must record the one
// notice.
func TestTheLocalDriverSaysTheCorpussMissedPause(t *testing.T) {
	t.Parallel()

	cases := conformance.MissedPauseCases()
	require.NotEmpty(t, cases, "the corpus is empty, so this asserts nothing")
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			session, err := flowdebug.New(flowdebug.Options{Controlled: true, Workflow: test.Workflow})
			require.NoError(t, err)
			t.Cleanup(func() { _ = session.Close() })
			gate := &sleepGate{waiting: make(chan time.Duration, 1), release: make(chan time.Time)}
			go func() {
				ctx := v1.NewContextWithClock(v1.NewContextWithDebugger(t.Context(), session), gate)
				_, runErr := v1.RunWithInputs(ctx, test.Workflow, nil)
				session.Finished(runErr)
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			at, err := session.WaitSnapshot(ctx, 0)
			require.NoError(t, err)
			for at.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD {
				at, err = session.WaitSnapshot(ctx, at.GetRevision())
				require.NoError(t, err)
			}
			receipt, err := session.Resume(ctx, &v1.DebugResumeRequest{
				RequestId: "on", ExpectedRevision: at.GetRevision(), Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_CONTINUE,
			})
			require.NoError(t, err)
			require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_APPLIED, receipt.GetStatus(), receipt.GetMessage())

			select {
			case slept := <-gate.waiting:
				require.Equal(t, test.Sleep, slept, "the run waited on something other than %s", test.Sleeping)
			case <-ctx.Done():
				t.Fatalf("the run never began %s", test.Sleeping)
			}
			paused, err := session.Pause(ctx, "pause")
			require.NoError(t, err)
			require.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_PENDING, paused.GetStatus(), paused.GetMessage())
			gate.release <- time.Unix(0, 0).Add(test.Sleep)

			end, err := session.WaitSnapshot(ctx, paused.GetRevision())
			require.NoError(t, err)
			for end.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED {
				require.NotEqual(t, v1.DebugRunState_DEBUG_RUN_STATE_HELD, end.GetState(), "the run held after its last step")
				end, err = session.WaitSnapshot(ctx, end.GetRevision())
				require.NoError(t, err)
			}

			var notices []string
			for _, observation := range end.GetObservations() {
				if observation.GetKind() == v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_NOTICE {
					notices = append(notices, observation.GetText())
				}
			}
			assert.Equal(t, []string{flowdebug.MissedPauseNotice}, notices)
		})
	}
}
