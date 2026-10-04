package flowdap_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdap"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// threeSteps is a program with nothing nondeterministic in it: every run of
// it stops where, and shows what, every other run does.
func threeSteps() *v1.Workflow {
	step := func(id string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Value{Value: v1.NewLiteral(1)}}
	}

	return &v1.Workflow{Name: "wf", Steps: []*v1.Node{step("a"), step("b"), step("c")}}
}

// rewindable is a [flowdebug.Reversible] over threeSteps, started the way a
// host would: a fresh controlled session and a run in its own goroutine, which
// is cancelled before its session is released.
func rewindable(t *testing.T) *flowdebug.Reversible {
	t.Helper()

	target, err := flowdebug.NewReversible(t.Context(), func(context.Context) (*flowdebug.Run, error) {
		session, err := flowdebug.New(flowdebug.Options{Controlled: true})
		if err != nil {
			return nil, err
		}
		runCtx, cancel := context.WithCancel(context.Background())
		// One start for every launch, replays included: `run.started_at` is part of
		// what a stop shows.
		runCtx = v1.NewContextWithRunStart(runCtx, time.Unix(1_700_000_000, 0).UTC())
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, err := v1.Run(v1.NewContextWithDebugger(runCtx, session), threeSteps())
			session.Finished(err)
		}()

		return &flowdebug.Run{Session: session, Stop: func() {
			cancel()
			_ = session.Close()
			<-done
		}}, nil
	})
	require.NoError(t, err)
	t.Cleanup(target.Stop)

	return target
}

// frameName is the name of the innermost frame the editor is shown.
func frameName(t *testing.T, c *client, seq int) string {
	t.Helper()

	c.send(seq, "stackTrace", map[string]any{"threadId": 1})
	frames := c.await("response", "stackTrace")["body"].(map[string]any)["stackFrames"].([]any)
	require.NotEmpty(t, frames)

	return frames[0].(map[string]any)["name"].(string)
}

func TestStepBackAndReverseContinueFollowTheStopsTheEditorWasShown(t *testing.T) {
	t.Parallel()

	target := rewindable(t)
	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })
	server := flowdap.NewServer(target, c)
	go func() { _ = server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	initialized := c.await("response", "initialize")
	assert.Equal(t, true, initialized["body"].(map[string]any)["supportsStepBack"])
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")
	assert.Equal(t, "wf.a (value)", frameName(t, c, 4))

	c.send(5, "stepIn", map[string]any{"threadId": 1})
	c.await("response", "stepIn")
	c.await("event", "stopped")
	c.send(6, "stepIn", map[string]any{"threadId": 1})
	c.await("response", "stepIn")
	c.await("event", "stopped")
	assert.Equal(t, "wf.c (value)", frameName(t, c, 7))

	// One stop back: the response comes first, then the stop it caused.
	c.send(8, "stepBack", map[string]any{"threadId": 1})
	back := c.next()
	require.Equal(t, "response", back["type"], "%v", back)
	require.Equal(t, "stepBack", back["command"])
	require.Equal(t, true, back["success"], "%v", back)
	stopped := c.await("event", "stopped")
	assert.Equal(t, "step", stopped["body"].(map[string]any)["reason"])
	assert.Equal(t, "wf.b (value)", frameName(t, c, 9))

	// Forward again reaches the same place as the first time.
	c.send(10, "stepIn", map[string]any{"threadId": 1})
	c.await("response", "stepIn")
	c.await("event", "stopped")
	assert.Equal(t, "wf.c (value)", frameName(t, c, 11))

	// With no breakpoint to stop at, reverseContinue goes to the first stop.
	c.send(12, "reverseContinue", map[string]any{"threadId": 1})
	reversed := c.await("response", "reverseContinue")
	require.Equal(t, true, reversed["success"], "%v", reversed)
	c.await("event", "stopped")
	assert.Equal(t, "wf.a (value)", frameName(t, c, 13))

	// There is nowhere before the first stop, and the refusal says so.
	c.send(14, "stepBack", map[string]any{"threadId": 1})
	refused := c.await("response", "stepBack")
	assert.Equal(t, false, refused["success"])
	assert.Contains(t, refused["message"], "first stop")
}

func TestASessionThatCannotStepBackSaysSoAndOffersNothing(t *testing.T) {
	t.Parallel()

	c, _, _ := walked(t, "a", "b")
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	initialized := c.await("response", "initialize")
	assert.Equal(t, false, initialized["body"].(map[string]any)["supportsStepBack"],
		"the capability is offered only where a target can honour it")
	c.send(2, "launch", map[string]any{})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")

	for i, command := range []string{"stepBack", "reverseContinue"} {
		c.send(4+i, command, map[string]any{"threadId": 1})
		refused := c.await("response", command)
		assert.Equal(t, false, refused["success"], command)
		assert.Contains(t, refused["message"], `"reverse": true`, command)
	}
}

func TestAFinishedRunHasNoStopToGoBackTo(t *testing.T) {
	t.Parallel()

	target := rewindable(t)
	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })
	server := flowdap.NewServer(target, c)
	go func() { _ = server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")

	c.send(4, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	// Until the run has ended, by the target's own account of it.
	ended, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for after := uint64(0); ; {
		snapshot, err := target.WaitSnapshot(ended, after)
		require.NoError(t, err)
		if snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED {
			break
		}
		after = snapshot.GetRevision()
	}

	c.send(5, "stepBack", map[string]any{"threadId": 1})
	refused := c.await("response", "stepBack")
	assert.Equal(t, false, refused["success"])
	assert.Contains(t, refused["message"], "has ended")
}
