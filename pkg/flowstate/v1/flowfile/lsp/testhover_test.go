package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHoverOnAStubTaskNameShowsTheTaskDoc: hovering a stub's `task:` value
// answers with the same registry-derived documentation ([taskDoc]) a
// workflow step's own task-name hover shows — reused, not rewritten.
func TestHoverOnAStubTaskNameShowsTheTaskDoc(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	line := "      - task: log"
	text := "tests:\n  - name: the case\n    stubs:\n" + line + "\n        returns: {}\n"
	c.open("file:///suite.test.yaml", text)

	// Column lands mid-word, on "log" — the value, not the "task" key.
	h := c.hover("file:///suite.test.yaml", 3, len(line)-1)
	require.NotNil(t, h)
	assert.Contains(t, hoverText(h), "task `log`")
}

// TestHoverOnAStubTaskKeyIsNil is the negative half of the position: the
// *key* "task" is not the value "log", and only the value has a task name to
// look up.
func TestHoverOnAStubTaskKeyIsNil(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	line := "      - task: log"
	text := "tests:\n  - name: the case\n    stubs:\n" + line + "\n        returns: {}\n"
	c.open("file:///suite.test.yaml", text)

	// Column 9 lands inside the word "task" itself, before the colon.
	h := c.hover("file:///suite.test.yaml", 3, 9)
	assert.Nil(t, h)
}

// TestHoverOnAnUnregisteredStubTaskIsNil: a stub naming a task this
// registry does not know has nothing to show — the same silence a workflow
// step's task-name hover gives an unregistered task, and the honest answer
// next to the diagnostic already reporting it as unstubbable.
func TestHoverOnAnUnregisteredStubTaskIsNil(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	line := "      - task: no-such-task"
	text := "tests:\n  - name: the case\n    stubs:\n" + line + "\n        returns: {}\n"
	c.open("file:///suite.test.yaml", text)

	h := c.hover("file:///suite.test.yaml", 3, len(line)-1)
	assert.Nil(t, h)
}

// TestHoverElsewhereInATestDocumentIsNil covers the rest of the surface
// #1110 item 8 deliberately leaves silent: an expect: key (no generated
// per-field prose exists to show, see testhover.go's own doc), a case's
// name: value, and a stub's step: value.
func TestHoverElsewhereInATestDocumentIsNil(t *testing.T) {
	t.Parallel()

	text := "tests:\n  - name: the case\n    stubs:\n      - step: only\n    expect:\n      ran: [only]\n"

	positions := []struct {
		name string
		line int
		char int
	}{
		{"a case's own name:", 1, 12},
		{"a stub's step: value", 3, 20},
		{"an expect.ran: key", 5, 8},
	}

	for _, p := range positions {
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()
			c := newClient(t)
			c.initialize()
			c.open("file:///suite.test.yaml", text)
			assert.Nil(t, c.hover("file:///suite.test.yaml", p.line, p.char))
		})
	}
}

// TestHoverOnAStubTaskNameInsideTestDefaults: a stub's task: name inside
// testdefaults.yaml gets the same hover a suite's own does — the shared
// [flowtest.Defaults] shape, reached through [document.isTestDocument]
// rather than a check naming docTestFile alone.
func TestHoverOnAStubTaskNameInsideTestDefaults(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	line := "      - task: log"
	text := "defaults:\n  stubs:\n" + line + "\n        returns: {}\n"
	c.open("file:///testdefaults.yaml", text)

	h := c.hover("file:///testdefaults.yaml", 2, len(line)-1)
	require.NotNil(t, h)
	assert.Contains(t, hoverText(h), "task `log`")
}
