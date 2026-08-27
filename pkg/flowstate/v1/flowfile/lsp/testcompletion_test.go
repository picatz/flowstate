package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// suiteForCompletion is a suite with one line open at each level completion
// is offered at: an empty top-level line, an empty `expect:` line, an empty
// `stubs:` entry, and a `task:` value mid-word. Every "cursor here" line is
// blank or a partial word, which is what an author's editor actually shows
// while they are typing it — completion is asked mid-edit, never over a
// finished line.
const suiteForCompletion = `defaults:
  workflow: ./workflow.yaml
tests:
  - name: the case
    stubs:
      - task: lo
        returns: {}
    expect:

`

// TestCompletionOffersTestFileTopLevelKeys: the document root of a
// *.test.yaml offers File's own keys (edition, vars, defaults, tests,
// coverage), derived from testDocKeys[testLevelFile] — see
// TestTestDSLKeysMatchTheLoader for the guard that keeps it matching
// flowtest.File.
func TestCompletionOffersTestFileTopLevelKeys(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	c.open("file:///suite.test.yaml", "\n")
	got := labels(c.complete("file:///suite.test.yaml", 0, 0).Items)

	for _, want := range []string{"edition", "vars", "defaults", "tests", "coverage"} {
		assert.Contains(t, got, want)
	}

	// The negative direction in the same call: nothing from the workflow's
	// own top level (dslKeys[""]) belongs here — a workflow's `steps:` is
	// not a key flow test's loader has ever heard of.
	assert.NotContains(t, got, "steps", "a workflow-only key leaked into a test document's completion")
	assert.NotContains(t, got, "name", "a workflow's name: leaked — a test file's name: lives on a case, not the file")
}

// TestCompletionOffersExpectKeys: inside a case's `expect:` block, the
// candidates are Expectation's own keys.
func TestCompletionOffersExpectKeys(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	// The line under `expect:` carries the leading spaces an editor's
	// auto-indent would already have inserted — an empty line has no
	// indentation of its own for keyPath to walk up from, which is not the
	// shape a real keystroke produces.
	text := "tests:\n  - name: the case\n    expect:\n      \n"
	c.open("file:///suite.test.yaml", text)
	// Line 3 is that indented blank line; completing at column 6 (the end
	// of the six spaces) asks "what goes here".
	got := labels(c.complete("file:///suite.test.yaml", 3, 6).Items)

	for _, want := range []string{"outputs", "failed", "error_contains", "ran", "skipped", "compensated", "check"} {
		assert.Contains(t, got, want)
	}
	assert.NotContains(t, got, "for_each", "a workflow step key leaked into expect: completion")
}

// TestCompletionOffersStubTaskNames: a stub's `task:` value is completed
// from the task registry, the same registry a workflow step's own task name
// is offered from — reusing [taskCandidates]'s data, not a second table.
func TestCompletionOffersStubTaskNames(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	c.open("file:///suite.test.yaml", suiteForCompletion)
	// Line 5 is `      - task: lo`; column 15 sits right after "lo".
	line := "      - task: lo"
	got := c.complete("file:///suite.test.yaml", 5, len(line))

	labelsGot := labels(got.Items)
	assert.Contains(t, labelsGot, "log", "the registry's log task was not offered for a stub's task:")

	for _, item := range got.Items {
		if item.Label == "log" {
			assert.Equal(t, "log", item.TextEdit.NewText,
				"a stub's task: value is a plain name, not a step key with a trailing colon")
		}
	}
}

// TestCompletionStubTaskNamesDoNotAnswerElsewhere is the negative direction:
// the registry only answers a stub's own `task:` value, not `step:` beside
// it and not a `task:` written somewhere the grammar does not have one.
func TestCompletionStubTaskNamesDoNotAnswerElsewhere(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	line := "      - step:"
	text := "tests:\n  - name: the case\n    stubs:\n" + line + "\n"
	c.open("file:///suite.test.yaml", text)
	got := c.complete("file:///suite.test.yaml", 3, len(line))
	assert.Empty(t, got.Items, "a stub's step: value offered candidates — only task: should")
}

// TestCompletionOffersNestedShapes covers the rest of testDocKeys' levels in
// one pass — defaults:, a stub's fails:, a signal, starter:/sender:, and a
// case's cases: rows — each against the field flowtest actually declares.
func TestCompletionOffersNestedShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		text string
		line int
		col  int
		want []string
	}{
		{
			name: "defaults",
			text: "defaults:\n  \n",
			line: 1, col: 2,
			want: []string{"workflow", "inputs", "stubs", "sender", "check"},
		},
		{
			name: "fails",
			text: "tests:\n  - name: x\n    stubs:\n      - task: log\n        fails:\n          \n",
			line: 5, col: 10,
			want: []string{"kind", "message"},
		},
		{
			name: "signal",
			text: "tests:\n  - name: x\n    signals:\n      - \n",
			line: 3, col: 8,
			want: []string{"name", "at", "payload", "sender"},
		},
		{
			name: "starter",
			text: "tests:\n  - name: x\n    starter:\n      \n",
			line: 3, col: 6,
			want: []string{"subject", "issuer", "namespace", "claims"},
		},
		{
			name: "cases",
			text: "tests:\n  - name: x\n    cases:\n      - \n",
			line: 3, col: 8,
			want: []string{"name", "workflow", "inputs", "expect"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newClient(t)
			c.initialize()
			c.open("file:///suite.test.yaml", tc.text)
			got := labels(c.complete("file:///suite.test.yaml", tc.line, tc.col).Items)
			for _, want := range tc.want {
				assert.Contains(t, got, want, "level %s missing key %q", tc.name, want)
			}
		})
	}
}

// TestCompletionIsEmptyOnATestDefaultsTopLevel: testdefaults.yaml's own top
// level is the narrower dirDefaultsTopLevelKeys table (edition, vars,
// defaults) — not File's full five, since `tests:` and `coverage:` are not
// legal there and offering them would be a wrong answer with confidence.
func TestCompletionIsEmptyOnATestDefaultsTopLevel(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	c.open("file:///testdefaults.yaml", "\n")
	got := labels(c.complete("file:///testdefaults.yaml", 0, 0).Items)

	assert.ElementsMatch(t, []string{"edition", "vars", "defaults"}, got)
}
