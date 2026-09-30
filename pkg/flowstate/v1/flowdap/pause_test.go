package flowdap_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdap"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// gatedClock is a [v1.Clock] whose waits end only when the test says so, so a
// `sleep:` step is under way for exactly as long as a test needs it to be.
type gatedClock struct {
	waiting chan struct{}
	release chan time.Time
}

func newGatedClock() *gatedClock {
	return &gatedClock{waiting: make(chan struct{}, 1), release: make(chan time.Time)}
}

func (c *gatedClock) Now() time.Time { return time.Unix(0, 0) }

func (c *gatedClock) After(time.Duration) <-chan time.Time {
	c.waiting <- struct{}{}

	return c.release
}

// sleepingFlowfile sleeps, then has one more step to hold at.
const sleepingFlowfile = `edition: v2026.3
name: sleeping
steps:
  - id: nap
    sleep: 4s
  - id: after
    log:
      message: awake
`

// TestPauseDuringASleepStopsAtTheNextStep: a pause asked while a `sleep:` is
// under way succeeds, and the run stops, with reason pause, at the step
// boundary the sleep ends at, rather than running on to its end (#1297). The
// pause is answered there, just ahead of its stop.
func TestPauseDuringASleepStopsAtTheNextStep(t *testing.T) {
	t.Parallel()

	clock := newGatedClock()
	c, program, _ := launchedWith(t, "sleeping.yaml", sleepingFlowfile, func(ctx context.Context) context.Context {
		return v1.NewContextWithClock(ctx, clock)
	})
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program, "stopOnEntry": false})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")

	select {
	case <-clock.waiting:
	case <-time.After(20 * time.Second):
		t.Fatal("the run never began its sleep")
	}
	c.send(4, "pause", map[string]any{"threadId": 1})
	// Requests are handled in order, and a pause is answered only once the
	// run holds or ends, so this answer is what says the pause was taken
	// before the sleep ends.
	c.send(5, "threads", nil)
	c.await("response", "threads")
	release(t, clock)

	// Awaited in this order, so a stop sent ahead of the answer is skipped
	// and never arrives.
	assert.Equal(t, true, c.await("response", "pause")["success"])
	stop := c.await("event", "stopped")
	assert.Equal(t, "pause", body(stop)["reason"])
	c.send(5, "stackTrace", map[string]any{"threadId": 1})
	frames := body(c.await("response", "stackTrace"))["stackFrames"].([]any)
	require.NotEmpty(t, frames)
	assert.EqualValues(t, 6, frames[0].(map[string]any)["line"], "the run did not hold at the step after the sleep")

	c.send(6, "disconnect", map[string]any{"terminateDebuggee": true})
	c.await("response", "disconnect")
}

// release ends the sleep clock is holding.
func release(t *testing.T, clock *gatedClock) {
	t.Helper()

	select {
	case clock.release <- time.Unix(4, 0):
	case <-time.After(20 * time.Second):
		t.Fatal("the sleep stopped waiting before it was released")
	}
}

// TestPauseDuringTheLastStepIsRefusedWhenTheRunEnds: a pause asked while the
// run's last step sleeps has no boundary to hold at. DAP allows a pause's
// success only with a stop to follow, so the adapter answers it when the run
// ends, unsuccessfully and saying why, ahead of `terminated` (#1297).
func TestPauseDuringTheLastStepIsRefusedWhenTheRunEnds(t *testing.T) {
	t.Parallel()

	clock := newGatedClock()
	c, program, _ := launchedWith(t, "last.yaml", `edition: v2026.3
name: last
steps:
  - id: nap
    sleep: 4s
`, func(ctx context.Context) context.Context {
		return v1.NewContextWithClock(ctx, clock)
	})
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program, "stopOnEntry": false})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")

	select {
	case <-clock.waiting:
	case <-time.After(20 * time.Second):
		t.Fatal("the run never began its sleep")
	}
	c.send(4, "pause", map[string]any{"threadId": 1})
	// Requests are handled in order, and a pause is answered only once the
	// run holds or ends, so this answer is what says the pause was taken
	// before the sleep ends.
	c.send(5, "threads", nil)
	c.await("response", "threads")
	release(t, clock)

	var printed strings.Builder
	var answer map[string]any
	for answer == nil {
		message := c.next()
		switch {
		case message["type"] == "response" && message["command"] == "pause":
			answer = message
		case message["event"] == "output":
			printed.WriteString(body(message)["output"].(string))
		case message["event"] == "stopped", message["event"] == "terminated":
			require.Failf(t, "the pause was not answered first", "%v", message)
		}
	}
	assert.Equal(t, false, answer["success"], "a pause no stop followed was answered as a success")
	assert.Equal(t, flowdebug.MissedPauseNotice, answer["message"], "a completed run's refusal is not worded as both drivers word it")
	c.await("event", "terminated")
	assert.Contains(t, printed.String(), flowdebug.MissedPauseNotice, "the run's own notice was not written")
}

// TestPausesPastTheBoundAreRefusedAtOnce: a client repeating pause while a
// step is under way keeps at most maxPendingPauses waiting on the next stop;
// the next is refused at once, and every one waiting is still answered by
// the stop (Codex, #2220).
func TestPausesPastTheBoundAreRefusedAtOnce(t *testing.T) {
	t.Parallel()

	clock := newGatedClock()
	c, program, _ := launchedWith(t, "sleeping.yaml", sleepingFlowfile, func(ctx context.Context) context.Context {
		return v1.NewContextWithClock(ctx, clock)
	})
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program, "stopOnEntry": false})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	select {
	case <-clock.waiting:
	case <-time.After(20 * time.Second):
		t.Fatal("the run never began its sleep")
	}

	const waiting = 64
	for i := range waiting + 1 {
		c.send(10+i, "pause", map[string]any{"threadId": 1})
	}
	refused := c.await("response", "pause")
	assert.EqualValues(t, 10+waiting, refused["request_seq"], "a pause within the bound was answered before the stop")
	assert.Equal(t, false, refused["success"])
	assert.Contains(t, refused["message"], "already waiting")

	release(t, clock)
	answered := 0
	for answered < waiting {
		response := c.await("response", "pause")
		require.Equal(t, true, response["success"], "a waiting pause was not answered by the stop: %v", response)
		answered++
	}
	assert.Equal(t, "pause", body(c.await("event", "stopped"))["reason"])
}

// TestAPausePastTheBoundNeverReachesTheRun: the bound is checked before a
// pause is forwarded, so a client repeating pause against an attached durable
// run sends it no more than the bound (Codex, #2220).
func TestAPausePastTheBoundNeverReachesTheRun(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })
	remote := &fakeRemote{snapshot: &v1.DebugSnapshot{
		Revision:     1,
		State:        v1.DebugRunState_DEBUG_RUN_STATE_RUNNING,
		Capabilities: v1.DurableDebugCapabilities(),
	}}
	server := flowdap.NewServer(nil, c, flowdap.WithAttach(func(context.Context, flowdap.AttachArguments) (*flowdap.Attachment, error) {
		return &flowdap.Attachment{Target: remote}, nil
	}))
	go func() { _ = server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "attach", map[string]any{"workflowId": "wf-1"})
	c.await("response", "attach")

	const waiting = 64
	for i := range waiting + 1 {
		c.send(10+i, "pause", map[string]any{"threadId": 1})
	}
	refused := c.await("response", "pause")
	assert.EqualValues(t, 10+waiting, refused["request_seq"])
	assert.Equal(t, false, refused["success"])
	assert.Equal(t, int32(waiting), remote.paused.Load(), "a pause past the bound still reached the run")
}
