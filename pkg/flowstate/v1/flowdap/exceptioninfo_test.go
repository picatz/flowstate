package flowdap_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExceptionInfoDescribesTheStopItWasRecordedAt(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")
	c.send(3, "setExceptionBreakpoints", map[string]any{"filters": []string{"all"}})
	c.await("response", "setExceptionBreakpoints")
	c.send(4, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")
	c.send(5, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	failed := c.await("event", "stopped")
	require.Equal(t, "exception", body(failed)["reason"])

	c.send(6, "exceptionInfo", map[string]any{"threadId": 1})
	assert.Equal(t, "always", body(c.await("response", "exceptionInfo"))["breakMode"])

	// Changing the filter while stopped does not rewrite the stop.
	c.send(7, "setExceptionBreakpoints", map[string]any{"filters": []string{"uncaught"}})
	c.await("response", "setExceptionBreakpoints")
	c.send(8, "exceptionInfo", map[string]any{"threadId": 1})
	assert.Equal(t, "always", body(c.await("response", "exceptionInfo"))["breakMode"])

	// threadId is required.
	c.send(9, "exceptionInfo", nil)
	assert.Equal(t, false, c.await("response", "exceptionInfo")["success"])

	// Ending the run at the failure leaves nothing to describe.
	c.send(10, "terminate", nil)
	c.await("response", "terminate")
	c.send(11, "exceptionInfo", map[string]any{"threadId": 1})
	assert.Equal(t, false, c.await("response", "exceptionInfo")["success"])
}
