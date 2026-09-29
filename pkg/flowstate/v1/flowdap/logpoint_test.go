package flowdap_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnEditorsLogpointWritesToItsOutputAndNeverStops: a line breakpoint an
// editor sets with a `logMessage` evaluates its `{expr}` holes at every
// arrival, in the dialect the console's `break if` uses, and writes the
// message to the editor's output instead of stopping (#1873).
func TestAnEditorsLogpointWritesToItsOutputAndNeverStops(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program, "stopOnEntry": false})
	c.await("response", "launch")

	// Line 13 is inside `touch`, in the loop body.
	c.send(3, "setBreakpoints", map[string]any{
		"source":      map[string]any{"path": program},
		"breakpoints": []map[string]any{{"line": 13, "logMessage": "saw {item} of {{4}}"}},
	})
	set := body(c.await("response", "setBreakpoints"))["breakpoints"].([]any)
	require.Len(t, set, 1)
	require.Equal(t, true, set[0].(map[string]any)["verified"], set[0])

	// No exception filter, so the program's failing step does not stop the
	// run either: nothing here may stop it (Codex, #2219).
	c.send(4, "setExceptionBreakpoints", map[string]any{"filters": []string{}})
	require.Equal(t, true, c.await("response", "setExceptionBreakpoints")["success"])

	c.send(5, "configurationDone", nil)
	var printed strings.Builder
	for {
		message := c.next()
		if message["type"] != "event" {
			continue
		}
		switch message["event"] {
		case "output":
			printed.WriteString(body(message)["output"].(string))
		case "stopped":
			require.Failf(t, "the run stopped", "a logpoint, or nothing at all, stopped the run: %v", body(message))
		}
		if message["event"] == "exited" {
			break
		}
	}

	out := printed.String()
	for _, want := range []string{"saw 1 of {4}", "saw 2 of {4}", "saw 3 of {4}", "saw 4 of {4}"} {
		assert.Equal(t, 1, strings.Count(out, want), "the logpoint did not write its message once at each arrival")
	}
}

// TestAnEditorsMalformedHitConditionIsUnverifiedWithAReason: a hit condition
// the session cannot parse comes back to the editor unverified and saying
// why, rather than dropped or armed as something else (#1873).
func TestAnEditorsMalformedHitConditionIsUnverifiedWithAReason(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")

	c.send(3, "setFunctionBreakpoints", map[string]any{"breakpoints": []map[string]any{
		{"name": "price", "hitCondition": "banana"},
		{"name": "start", "hitCondition": ">= 1"},
	}})
	answered := body(c.await("response", "setFunctionBreakpoints"))["breakpoints"].([]any)
	require.Len(t, answered, 2)
	malformed := answered[0].(map[string]any)
	assert.Equal(t, false, malformed["verified"], "a hit condition nothing can parse was armed")
	assert.Contains(t, malformed["message"], "hit condition", "the editor is not told why")
	assert.Equal(t, true, answered[1].(map[string]any)["verified"], "a well-formed hit condition beside it must still arm")

	c.send(4, "setBreakpoints", map[string]any{
		"source":      map[string]any{"path": program},
		"breakpoints": []map[string]any{{"line": 13, "hitCondition": "% 0"}},
	})
	line := body(c.await("response", "setBreakpoints"))["breakpoints"].([]any)[0].(map[string]any)
	assert.Equal(t, false, line["verified"], "a line breakpoint's malformed hit condition was armed")
	assert.Contains(t, line["message"], "hit condition")
}
