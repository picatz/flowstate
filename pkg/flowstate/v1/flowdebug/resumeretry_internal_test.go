package flowdebug

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestARetryAfterTheCallerGaveUpNeverAdvancesTwice is the case a PENDING
// receipt exists for: the command reached the run, the caller stopped waiting
// before the answer, and the caller retries under the same request id. The
// retry carries no expected revision, so only the remembered request id stands
// between it and a second step.
func TestARetryAfterTheCallerGaveUpNeverAdvancesTwice(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.3
name: retry
steps:
  - id: one
    log:
      message: one
  - id: two
    log:
      message: two
  - id: three
    log:
      message: three
outputs: {}
`), 0o600))
	workflow, _, err := flowfile.ParseFile(path)
	require.NoError(t, err)

	session, err := New(Options{Controlled: true, Out: &strings.Builder{}, Workflow: workflow})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		_ = session.Close()
		cancel()
	})
	go func() {
		runCtx := v1.NewContextWithDebugger(ctx, session)
		runCtx = v1.NewContextWithRunObserver(runCtx, session)
		_, err := v1.RunWithInputs(runCtx, workflow, nil)
		session.Finished(err)
	}()

	held := func(after uint64) *v1.DebugSnapshot {
		t.Helper()
		wait, stop := context.WithTimeout(t.Context(), 10*time.Second)
		defer stop()
		for {
			snapshot, err := session.WaitSnapshot(wait, after)
			require.NoError(t, err)
			if snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD {
				return snapshot
			}
			after = snapshot.GetRevision()
		}
	}
	at := held(0)
	require.Equal(t, "one", at.GetOccurrence().GetAddress())

	// Deliver the step as Resume does, then walk away without reading the
	// acknowledgement: the caller's context ended between the two.
	release, err := session.takeControl(t.Context(), "step")
	require.NoError(t, err)
	_, err = session.deliverAcknowledged(t.Context(), "step", "gave-up", make(chan acknowledgement, 1))
	release()
	require.NoError(t, err)

	moved := held(at.GetRevision())
	require.Equal(t, "two", moved.GetOccurrence().GetAddress())

	retry, err := session.Resume(t.Context(), &v1.DebugResumeRequest{
		RequestId: "gave-up", Action: v1.DebugResumeAction_DEBUG_RESUME_ACTION_STEP_IN,
	})
	require.NoError(t, err)
	assert.Equal(t, v1.DebugCommandStatus_DEBUG_COMMAND_STATUS_DUPLICATE, retry.GetStatus(), retry.GetMessage())

	still, err := session.Snapshot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "two", still.GetOccurrence().GetAddress(), "a retried command advanced the run a second time")

}
