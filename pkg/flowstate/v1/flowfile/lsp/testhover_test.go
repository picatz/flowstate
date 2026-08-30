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

// TestHoverOnAStubTaskKeyDescribesTheTestLanguageKey is the other half of the
// position: the key explains the stub field, while its value explains the
// registry task.
func TestHoverOnAStubTaskKeyDescribesTheTestLanguageKey(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	line := "      - task: log"
	text := "tests:\n  - name: the case\n    stubs:\n" + line + "\n        returns: {}\n"
	c.open("file:///suite.test.yaml", text)

	// Column 9 lands inside the word "task" itself, before the colon.
	h := c.hover("file:///suite.test.yaml", 3, 9)
	require.NotNil(t, h)
	assert.Contains(t, hoverText(h), "task name this replaces")
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

// TestHoverOnDataValuesInATestDocumentIsNil covers values that have no shared
// semantic source to explain. Test-language keys themselves are covered below.
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
		{"an expect.ran: value", 5, 13},
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

func TestHoverDescribesTestLanguageKeysAndQuotedKeys(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	text := "tests:\n  - name: x\n    \"exp\\u0065ct\":\n      ran: [only]\n"
	c.open("file:///keys.test.yaml", text)

	expect := c.hover("file:///keys.test.yaml", 2, 8)
	require.NotNil(t, expect)
	assert.Contains(t, hoverText(expect), "What the run must have produced")
	ran := c.hover("file:///keys.test.yaml", 3, 8)
	require.NotNil(t, ran)
	assert.Contains(t, hoverText(ran), "Names steps that must have executed")
	assert.Equal(t, `"exp\u0065ct"`, textInRange(text, *expect.Range))
}

func TestHoverDescribesTestDefaultsKeysWithoutOfferingSuiteOnlyKeys(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()
	c.open("file:///testdefaults.yaml", "vars:\n  region: us-east-1\n")

	h := c.hover("file:///testdefaults.yaml", 0, 1)
	require.NotNil(t, h)
	assert.Contains(t, hoverText(h), "every suite in this directory")
}

func TestTestLanguageAnswersDoNotEchoSecretOrFixtureExamples(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()
	const secret = "super-secret-test-value"
	const example = "https://fixture.example.invalid/private"
	text := "tests:\n  - name: x\n    inputs:\n      endpoint: " + example + "\n    secrets:\n      env:API_KEY: " + secret + "\n    expect:\n      \n"
	c.open("file:///private.test.yaml", text)

	h := c.hover("file:///private.test.yaml", 4, 5)
	require.NotNil(t, h)
	answer := hoverText(h)
	for _, item := range c.complete("file:///private.test.yaml", 7, 6).Items {
		answer += item.Label + item.Documentation
	}
	assert.NotContains(t, answer, secret)
	assert.NotContains(t, answer, example)
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

// TestHoverIgnoresAFixtureShapedLikeAStub is the hover half of the
// suffix-match finding (Codex, #1173): a fixture map named `stubs` inside a
// case's `inputs:` is the author's data, and its `task:` key is not a
// stub's — a hover there answered with the registry's doc for whatever task
// name the fixture happened to hold, presenting the DSL's reading of data
// that is not the DSL's.
func TestHoverIgnoresAFixtureShapedLikeAStub(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	text := "tests:\n" +
		"  - name: the case\n" +
		"    inputs:\n" +
		"      stubs:\n" +
		"        - task: log\n"
	c.open("file:///fixture.test.yaml", text)

	// Column 17 sits inside the value "log" — the position the real stub
	// hover answers at, on a line whose enclosing chain is the author's data.
	h := c.hover("file:///fixture.test.yaml", 4, 17)
	assert.Nil(t, h,
		"a task hover answered inside a fixture map, reading the author's data as a stub")
}
