package flowdap_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdap"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// recordedRun is three points of a run that failed at the last: what a server's
// DebugHistory answers, as the adapter's attach reads it.
func recordedRun(_ context.Context, event int64, inspections ...*v1.DebugHistoryInspection) (*v1.DebugHistoryResponse, error) {
	points := []int64{4, 10, 16}
	if event == 0 {
		event = points[len(points)-1]
	}
	frame := map[int64]string{4: "wf.a", 10: "wf.b", 16: "wf.c"}[event]
	state := v1.DebugRunState_DEBUG_RUN_STATE_HELD
	outcome := v1.DebugRunState_DEBUG_RUN_STATE_UNSPECIFIED
	if event == 16 {
		outcome = v1.DebugRunState_DEBUG_RUN_STATE_FAILED
	}

	answer := &v1.DebugHistoryResponse{
		EventId: event, Boundaries: points, Outcome: outcome, Fidelity: v1.DebugFidelity_DEBUG_FIDELITY_RECONSTRUCTED,
		Snapshot: &v1.DebugSnapshot{Revision: uint64(event), State: state, Frames: []*v1.DebugFrame{{Id: 1, Label: frame, Scoped: true}}},
	}
	for _, asked := range inspections {
		// A value that says which point it is the value at.
		answer.Inspected = append(answer.Inspected, &v1.DebugHistoryInspected{
			Result: &v1.DebugInspectResponse{Value: &v1.DebugValue{Type: "string", Rendered: fmt.Sprintf("%s@%d", asked.GetExpression(), event)}},
		})
	}

	return answer, nil
}

func TestAnAttachToARecordedRunStepsBackAndForwardWithoutExecutingAnything(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	t.Cleanup(func() { _ = c.Close() })
	server := flowdap.NewServer(nil, c,
		flowdap.WithAttach(func(ctx context.Context, args flowdap.AttachArguments) (*flowdap.Attachment, error) {
			if !args.History {
				return nil, errors.New("this test attaches to a history")
			}
			historical, err := flowdebug.OpenHistorical(ctx, recordedRun)
			if err != nil {
				return nil, err
			}

			return &flowdap.Attachment{Target: historical}, nil
		}))
	go func() { _ = server.Serve(t.Context()) }()

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "attach", map[string]any{"workflowId": "wf-1", "runId": "r-1", "history": true})
	c.await("response", "attach")

	// A recorded run is offered stepping back, and nothing that needs it to run.
	caps := body(c.await("event", "capabilities"))["capabilities"].(map[string]any)
	assert.Equal(t, true, caps["supportsStepBack"])
	assert.Equal(t, false, caps["supportsLogPoints"])
	assert.Equal(t, false, caps["supportsTerminateRequest"])
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")

	// The run failed, and the editor is still in a session: the last point is a
	// stop like any other, not the end of the run.
	stopped := c.await("event", "stopped")
	assert.Equal(t, "step", body(stopped)["reason"])
	assert.Equal(t, "wf.c", frameName(t, c, 4))

	c.send(5, "stepBack", map[string]any{"threadId": 1})
	back := c.next()
	require.Equal(t, "response", back["type"], "%v", back)
	require.Equal(t, true, back["success"], "%v", back)
	c.await("event", "stopped")
	assert.Equal(t, "wf.b", frameName(t, c, 6))

	c.send(7, "reverseContinue", map[string]any{"threadId": 1})
	require.Equal(t, true, c.await("response", "reverseContinue")["success"])
	c.await("event", "stopped")
	assert.Equal(t, "wf.a", frameName(t, c, 8), "with no breakpoints on a recorded run, the start")

	c.send(9, "stepBack", map[string]any{"threadId": 1})
	refused := c.await("response", "stepBack")
	assert.Equal(t, false, refused["success"])
	assert.Contains(t, refused["message"], "first point")

	c.send(10, "next", map[string]any{"threadId": 1})
	require.Equal(t, true, c.await("response", "next")["success"])
	c.await("event", "stopped")
	assert.Equal(t, "wf.b", frameName(t, c, 11))

	// A watch expression is read at the point the editor is looking at, and
	// follows it back and forward.
	c.send(12, "evaluate", map[string]any{"expression": "total", "frameId": 1, "context": "watch"})
	assert.Equal(t, "total@10", body(c.await("response", "evaluate"))["result"])
	c.send(13, "stepBack", map[string]any{"threadId": 1})
	c.await("response", "stepBack")
	c.await("event", "stopped")
	c.send(14, "evaluate", map[string]any{"expression": "total", "frameId": 1, "context": "watch"})
	assert.Equal(t, "total@4", body(c.await("response", "evaluate"))["result"])
}
