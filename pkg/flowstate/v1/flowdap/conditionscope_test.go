package flowdap_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnEditorsConditionNothingCanBindIsUnverified is #2194 in an editor: a
// condition reading a name no site the breakpoint fires at can bind answers
// `verified: false` with the reason, where it used to answer verified and
// never fire. The loop's own binding, set before the loop runs, is verified.
func TestAnEditorsConditionNothingCanBindIsUnverified(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)

	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")

	// Line 13 is inside `touch`, in the loop body that binds `item`; line 16
	// is `price`, after the loop.
	c.send(3, "setBreakpoints", map[string]any{
		"source": map[string]any{"path": program},
		"breakpoints": []map[string]any{
			{"line": 13, "condition": "itme == 3"},
			{"line": 16, "condition": "item == 3"},
			{"line": 13, "condition": "item == 3"},
		},
	})
	lines := body(c.await("response", "setBreakpoints"))["breakpoints"].([]any)
	require.Len(t, lines, 3)
	typo := lines[0].(map[string]any)
	assert.Equal(t, false, typo["verified"], "a condition naming a typo was verified")
	assert.Contains(t, typo["message"], "did you mean `item`?")
	after := lines[1].(map[string]any)
	assert.Equal(t, false, after["verified"], "a line after the loop verified a condition on the loop's binding")
	assert.Contains(t, after["message"], "`item` is bound only inside")
	assert.Equal(t, true, lines[2].(map[string]any)["verified"], lines[2])

	c.send(4, "setFunctionBreakpoints", map[string]any{"breakpoints": []map[string]any{
		{"name": "price", "condition": "item == 1"},
		{"name": "touch", "condition": "item == 1"},
	}})
	functions := body(c.await("response", "setFunctionBreakpoints"))["breakpoints"].([]any)
	require.Len(t, functions, 2)
	outside := functions[0].(map[string]any)
	assert.Equal(t, false, outside["verified"], "a loop binding named outside the loop was verified")
	assert.Contains(t, outside["message"], "`item` is bound only inside")
	assert.Equal(t, true, functions[1].(map[string]any)["verified"], functions[1])
}
