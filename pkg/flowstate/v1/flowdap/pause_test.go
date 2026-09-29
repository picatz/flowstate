package flowdap_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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
// boundary the sleep ends at, rather than running on to its end (#1297).
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
	assert.Equal(t, true, c.await("response", "pause")["success"])
	select {
	case clock.release <- time.Unix(4, 0):
	case <-time.After(20 * time.Second):
		t.Fatal("the sleep stopped waiting before it was released")
	}

	stop := c.await("event", "stopped")
	assert.Equal(t, "pause", body(stop)["reason"])
	c.send(5, "stackTrace", map[string]any{"threadId": 1})
	frames := body(c.await("response", "stackTrace"))["stackFrames"].([]any)
	require.NotEmpty(t, frames)
	assert.EqualValues(t, 6, frames[0].(map[string]any)["line"], "the run did not hold at the step after the sleep")

	c.send(6, "disconnect", map[string]any{"terminateDebuggee": true})
	c.await("response", "disconnect")
}
