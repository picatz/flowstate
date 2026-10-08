package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestRunAndTriggerRootsCompleteTheirClosedFieldSets holds the roots half of
// #1434: the editor offers `run` and `trigger`, and after the dot exactly the
// fields the validator refuses an unknown one against.
func TestRunAndTriggerRootsCompleteTheirClosedFieldSets(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const head = "edition: v2026.4\nname: roots\nsteps:\n  - id: a\n    log:\n      message: ${"
	complete := func(name, typed string) []string {
		text, pos := splitCursor(t, head+typed+"|}\n")
		uri := "file:///" + name + ".yaml"
		c.open(uri, text)
		return labels(c.complete(uri, pos.Line, pos.Character).Items)
	}

	assert.Subset(t, complete("roots", ""), []string{"steps", "run", "trigger"})
	assert.ElementsMatch(t, []string{"identity", "local", "workflow_id", "run_id", "started_at"}, complete("run", "run."))
	assert.ElementsMatch(t, []string{"kind", "name", "principal", "delivery_id", "scheduled_at"}, complete("trigger", "trigger."))
	assert.Contains(t, complete("identity", "run.identity."), "subject")
}

// TestRunAndTriggerRootsHoverWithTheirFields pins the hover half: the same
// account the menu gives, ending in the closed field list.
func TestRunAndTriggerRootsHoverWithTheirFields(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const src = "edition: v2026.4\nname: roots\nsteps:\n  - id: a\n    log:\n      message: ${size(run.workflow_id) + size(trigger.name)}\n"
	c.open("file:///rootshover.yaml", src)

	run := positionOf(t, src, "run.workflow_id", 1)
	assert.Contains(t, hoverText(c.hover("file:///rootshover.yaml", run.Line, run.Character+1)), "Fields: `identity`, `local`, `workflow_id`, `run_id`, `started_at`.")
	trigger := positionOf(t, src, "trigger.name", 1)
	assert.Contains(t, hoverText(c.hover("file:///rootshover.yaml", trigger.Line, trigger.Character+1)), "`delivery_id`")
}
