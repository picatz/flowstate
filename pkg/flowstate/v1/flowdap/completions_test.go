package flowdap_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stoppedAtEntry launches the rich program and returns once it is held at its
// first step.
func stoppedAtEntry(t *testing.T) (*client, string) {
	t.Helper()

	c, program, _ := launched(t)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")

	return c, program
}

func completionTargets(t *testing.T, c *client, id int, text string, column int) []map[string]any {
	t.Helper()

	c.send(id, "completions", map[string]any{"frameId": 1, "text": text, "column": column})
	response := c.await("response", "completions")
	require.Equal(t, true, response["success"], response)
	var out []map[string]any
	for _, item := range body(response)["targets"].([]any) {
		out = append(out, item.(map[string]any))
	}

	return out
}

// continueTo runs the held run on to a breakpoint on step.
func continueTo(t *testing.T, c *client, id int, step string) {
	t.Helper()

	c.send(id, "setFunctionBreakpoints", map[string]any{"breakpoints": []map[string]any{{"name": step}}})
	c.await("response", "setFunctionBreakpoints")
	c.send(id+1, "continue", map[string]any{"threadId": 1})
	c.await("response", "continue")
	c.await("event", "stopped")
}

func labels(targets []map[string]any) []string {
	var out []string
	for _, target := range targets {
		out = append(out, target["label"].(string))
	}

	return out
}

func TestCompletionsAreAdvertisedWithTheDot(t *testing.T) {
	t.Parallel()

	c, _, _ := launched(t)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	caps := body(c.await("response", "initialize"))
	assert.Equal(t, true, caps["supportsCompletionsRequest"])
	assert.Equal(t, []any{"."}, caps["completionTriggerCharacters"])
}

func TestCompletionsOfferTheHeldRunsScope(t *testing.T) {
	t.Parallel()

	c, _ := stoppedAtEntry(t)
	assert.ElementsMatch(t, []string{"run.", "steps.", "trigger."}, labels(completionTargets(t, c, 4, "", 1)))

	continueTo(t, c, 5, "price")
	targets := completionTargets(t, c, 7, "steps.", 7)
	assert.ElementsMatch(t, []string{"start", "each"}, labels(targets), "the steps that finished are the names under steps")
	for _, target := range targets {
		assert.Equal(t, "field", target["type"])
		assert.EqualValues(t, 6, target["start"], "the name replaces what follows `steps.`")
		assert.EqualValues(t, 0, target["length"])
	}

	targets = completionTargets(t, c, 8, "steps.e", 8)
	require.Len(t, targets, 1)
	assert.Equal(t, "each", targets[0]["label"])
	assert.EqualValues(t, 6, targets[0]["start"])
	assert.EqualValues(t, 1, targets[0]["length"], "the typed `e` is what the name replaces")

	// A root continues, so the editor is offered it as a module.
	targets = completionTargets(t, c, 9, "ste", 4)
	require.Len(t, targets, 1)
	assert.Equal(t, "steps.", targets[0]["label"])
	assert.Equal(t, "module", targets[0]["type"])
}

func TestCompletionsReadOnlyTheTextBeforeTheCursor(t *testing.T) {
	t.Parallel()

	c, _ := stoppedAtEntry(t)
	continueTo(t, c, 4, "price")

	targets := completionTargets(t, c, 6, "steps.s + other", 8)
	assert.Equal(t, []string{"start"}, labels(targets))

	// Positions are in the UTF-16 units an editor counts in: the é is one and an
	// emoji is two.
	targets = completionTargets(t, c, 7, "\"é😀\" + steps.s", 16)
	require.Len(t, targets, 1)
	assert.EqualValues(t, 14, targets[0]["start"])
	assert.EqualValues(t, 1, targets[0]["length"])

	assert.ElementsMatch(t, []string{"run.", "steps.", "trigger."}, labels(completionTargets(t, c, 8, "steps.s", 0)),
		"a cursor before everything has typed nothing, so it is offered the roots")
}

func TestCompletionsAreNamesAndNeverTheDataUnderThem(t *testing.T) {
	t.Parallel()

	c, _ := stoppedAtEntry(t)
	continueTo(t, c, 4, "boom")

	assert.Contains(t, labels(completionTargets(t, c, 6, "steps.", 7)), "price")
	assert.Equal(t, []string{"value"}, labels(completionTargets(t, c, 7, "steps.price.", 13)), "a step's outputs are named")
	assert.Empty(t, completionTargets(t, c, 8, "steps.price.value.", 19),
		"a key inside a produced value is the datum, which the run produced and no author wrote")
	assert.Empty(t, completionTargets(t, c, 9, "steps.price.value.t", 20))
}

func TestCompletionsAfterTheRunMovedAreNotTheOldStops(t *testing.T) {
	t.Parallel()

	c, _ := stoppedAtEntry(t)
	assert.Empty(t, completionTargets(t, c, 4, "steps.e", 8), "nothing has finished at the first step")

	continueTo(t, c, 5, "price")
	assert.Equal(t, []string{"each"}, labels(completionTargets(t, c, 7, "steps.e", 8)))
}

func TestCompletionsNeedAStoppedRunAndABoundedText(t *testing.T) {
	t.Parallel()

	c, _, _ := launched(t)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate"})
	c.await("response", "initialize")
	c.await("event", "initialized")
	assert.Empty(t, completionTargets(t, c, 2, "steps.", 7), "before a launch there is no scope to read")

	// The bound is on the text and not on what it would complete: the word at
	// the end is one that completes, so only the limit can empty the answer.
	held, _ := stoppedAtEntry(t)
	atLimit := strings.Repeat(" ", 4096-3) + "ste"
	require.Len(t, atLimit, 4096)
	assert.Equal(t, []string{"steps."}, labels(completionTargets(t, held, 4, atLimit, len(atLimit)+1)), "a text at the limit is answered")
	over := " " + atLimit
	assert.Empty(t, completionTargets(t, held, 5, over, len(over)+1), "a text over it is not")
}

func TestAZeroBasedClientCountsItsColumnsFromZero(t *testing.T) {
	t.Parallel()

	c, program, _ := launched(t)
	c.send(1, "initialize", map[string]any{"adapterID": "flowstate", "columnsStartAt1": false})
	c.await("response", "initialize")
	c.await("event", "initialized")
	c.send(2, "launch", map[string]any{"program": program})
	c.await("response", "launch")
	c.send(3, "configurationDone", nil)
	c.await("response", "configurationDone")
	c.await("event", "stopped")

	// Column 3 is the cursor after `ste` when the first column is 0.
	targets := completionTargets(t, c, 4, "ste + 1", 3)
	require.Len(t, targets, 1)
	assert.Equal(t, "steps.", targets[0]["label"])
	assert.EqualValues(t, 0, targets[0]["start"])
	assert.EqualValues(t, 3, targets[0]["length"])
}
