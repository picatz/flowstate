package lsp

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
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
	assert.ElementsMatch(t, flowfile.RunIdentityFields(), complete("identity", "run.identity."))

	// A workflow var is evaluated before the run exists, and the validator refuses
	// both roots there.
	vars := "edition: v2026.4\nname: roots\nvars:\n  greeting: ${|}\nsteps:\n  - id: a\n    log:\n      message: hi\n"
	text, pos := splitCursor(t, vars)
	c.open("file:///varsroots.yaml", text)
	got := labels(c.complete("file:///varsroots.yaml", pos.Line, pos.Character).Items)
	assert.NotContains(t, got, "run")
	assert.NotContains(t, got, "trigger")
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
	assert.Contains(t, hoverText(c.hover("file:///rootshover.yaml", trigger.Line, trigger.Character+1)), "Fields: `kind`, `name`, `principal`, `delivery_id`, `scheduled_at`.")

	// A cursor on a field after the dot is not on the root.
	field := positionOf(t, src, "workflow_id", 1)
	assert.Empty(t, hoverText(c.hover("file:///rootshover.yaml", field.Line, field.Character+2)))
}
