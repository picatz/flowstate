package flowdap_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdap"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

func TestUnsupportedFunctionBreakpointsAreNotArmed(t *testing.T) {
	t.Parallel()

	for _, field := range []string{"condition", "hitCondition", "logMessage"} {
		for _, value := range []string{"sensitive-submitted-expression", ""} {
			t.Run(field+"/"+value, func(t *testing.T) {
				t.Parallel()
				c, session, finished := walked(t, "entry", "target", "ship")
				c.send(1, "initialize", nil)
				capabilities := c.await("response", "initialize")["body"].(map[string]any)
				for _, capability := range []string{"supportsConditionalBreakpoints", "supportsHitConditionalBreakpoints", "supportsLogPoints"} {
					assert.NotEqual(t, true, capabilities[capability])
				}
				c.await("event", "initialized")
				c.send(2, "configurationDone", nil)
				c.await("response", "configurationDone")
				c.await("event", "stopped")

				// Editing an existing marker must remove its unconditional form.
				c.send(3, "setFunctionBreakpoints", map[string]any{
					"breakpoints": []map[string]any{{"name": "target"}},
				})
				c.await("response", "setFunctionBreakpoints")
				c.send(4, "setFunctionBreakpoints", map[string]any{
					"breakpoints": []map[string]any{{"name": "target", field: value}, {"name": "ship"}},
				})
				answer := c.await("response", "setFunctionBreakpoints")
				require.Equal(t, true, answer["success"])
				points := answer["body"].(map[string]any)["breakpoints"].([]any)
				require.Len(t, points, 2)
				refused := points[0].(map[string]any)
				assert.Equal(t, false, refused["verified"])
				assert.Contains(t, refused["message"], "not supported")
				assert.NotContains(t, refused["message"], "sensitive-submitted-expression")
				assert.Equal(t, true, points[1].(map[string]any)["verified"])

				c.send(5, "continue", map[string]any{"threadId": 1})
				c.await("response", "continue")
				c.await("event", "stopped")
				at, paused := session.Paused()
				require.True(t, paused)
				require.Equal(t, "ship", at.Step, "the rejected entry was armed or its old unconditional form survived")
				require.NoError(t, session.Control(t.Context(), "continue"))
				require.NoError(t, <-finished)
			})
		}
	}
}

func TestMalformedBreakpointReplacementPreservesInstalledSet(t *testing.T) {
	t.Parallel()

	for name, arguments := range map[string]any{
		"missing arguments": nil,
		"missing list":      map[string]any{},
		"null list":         map[string]any{"breakpoints": nil},
		"wrong list type":   map[string]any{"breakpoints": "target"},
		"wrong name type":   map[string]any{"breakpoints": []map[string]any{{"name": 42}}},
		"wrong condition type": map[string]any{
			"breakpoints": []map[string]any{{"name": "target", "condition": 42}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c, session, finished := walked(t, "entry", "target", "ship")
			c.send(1, "configurationDone", nil)
			c.await("response", "configurationDone")
			c.await("event", "stopped")
			require.NoError(t, session.SetBreakpoints([]string{"target", "ship"}))

			c.send(2, "setFunctionBreakpoints", arguments)
			answer := c.await("response", "setFunctionBreakpoints")
			assert.Equal(t, false, answer["success"])
			assert.Equal(t, "invalid function breakpoint arguments", answer["message"])
			c.send(3, "continue", map[string]any{"threadId": 1})
			c.await("response", "continue")
			c.await("event", "stopped")
			at, paused := session.Paused()
			require.True(t, paused)
			assert.Equal(t, "target", at.Step)

			// An explicitly empty array, unlike an invalid request, clears it.
			c.send(4, "setFunctionBreakpoints", map[string]any{"breakpoints": []any{}})
			cleared := c.await("response", "setFunctionBreakpoints")
			assert.Equal(t, true, cleared["success"])
			assert.Empty(t, cleared["body"].(map[string]any)["breakpoints"])
			require.NoError(t, session.Control(t.Context(), "continue"))
			select {
			case err := <-finished:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("an empty replacement did not remove the ship breakpoint")
			}
		})
	}
}

func TestUnsupportedEntriesStillCountAgainstTheBreakpointLimit(t *testing.T) {
	t.Parallel()

	c, session, finished := walked(t, "entry", "target", "ship")
	c.send(1, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")
	require.NoError(t, session.SetBreakpoints([]string{"target"}))
	points := make([]map[string]any, flowdebug.MaxBreakpoints+1)
	for i := range points {
		points[i] = map[string]any{"name": fmt.Sprintf("step%d", i), "condition": "false"}
	}
	c.send(2, "setFunctionBreakpoints", map[string]any{"breakpoints": points})
	answer := c.await("response", "setFunctionBreakpoints")
	refused := answer["body"].(map[string]any)["breakpoints"].([]any)
	require.Len(t, refused, len(points))
	for _, point := range refused {
		assert.Equal(t, false, point.(map[string]any)["verified"])
		assert.Contains(t, point.(map[string]any)["message"], "at most")
	}
	c.send(3, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	c.await("event", "stopped")
	at, paused := session.Paused()
	require.True(t, paused)
	assert.Equal(t, "target", at.Step, "the oversized replacement changed the installed set")
	require.NoError(t, session.Control(t.Context(), "continue"))
	require.NoError(t, <-finished)
}

func TestPauseWhileRunningReportsUnsupportedWithoutStoppingTheRun(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })
	server := flowdap.NewServer(session, c)
	go func() { _ = server.Serve(t.Context()) }()

	running, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	finished := make(chan error, 1)
	go func() {
		<-server.Launched()
		scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{})
		err := session.BeforeStep(t.Context(), &v1.Node{Id: "work"}, scope)
		if err == nil {
			close(running)
			<-release // Deterministically stand in for work between boundaries.
			err = session.BeforeStep(t.Context(), &v1.Node{Id: "after"}, scope)
		}
		finished <- err
	}()
	c.send(1, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")
	c.send(2, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	select {
	case <-running:
	case <-time.After(10 * time.Second):
		t.Fatal("the run never left its entry stop")
	}
	c.send(3, "pause", map[string]any{"threadId": 1})
	answer := c.next()
	require.Equal(t, "response", answer["type"], "pause emitted a fictitious stop")
	require.Equal(t, "pause", answer["command"])
	assert.Equal(t, false, answer["success"])
	assert.Contains(t, answer["message"], "pause is not supported")
	_, paused := session.Paused()
	assert.False(t, paused)
	unblock()
	select {
	case err := <-finished:
		require.NoError(t, err, "a refused pause must neither abort nor hold the run")
	case <-time.After(10 * time.Second):
		t.Fatal("pause changed the run's continue mode")
	}
}
