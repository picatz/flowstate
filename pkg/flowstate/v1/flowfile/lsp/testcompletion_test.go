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

// TestCompletionStopsAtTheAuthorsOwnData is the negative direction the
// suffix match failed (Codex, #1173): a fixture map whose key happens to
// spell a stanza name is the author's data, and offering the stanza's keys
// inside it is a wrong answer with full confidence — worse than silence, per
// this package's own doc.
func TestCompletionStopsAtTheAuthorsOwnData(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	// `expect:` here is a key of the workflow's input fixture, two levels
	// inside `inputs:` — not the case's expect stanza.
	text := "tests:\n" +
		"  - name: the case\n" +
		"    inputs:\n" +
		"      expect:\n" +
		"        \n"
	c.open("file:///suite.test.yaml", text)
	got := labels(c.complete("file:///suite.test.yaml", 4, 8).Items)

	assert.Empty(t, got,
		"a fixture map named expect was completed as the DSL's expect: stanza")

	// And the same collision for a value position: a fixture map named
	// `stubs` does not make its `task:` key a stub's task.
	text = "tests:\n" +
		"  - name: the case\n" +
		"    inputs:\n" +
		"      stubs:\n" +
		"        - task: lo\n"
	c.open("file:///fixture.test.yaml", text)
	got = labels(c.complete("file:///fixture.test.yaml", 4, 18).Items)

	assert.Empty(t, got,
		"a fixture map named stubs had its task: value completed from the registry")
}

func TestCompletionWalksQuotedTestGrammarKeysSemantically(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	text := "\"t\\u0065sts\":\n" +
		"  - \"na\\u006de\": the case\n" +
		"    \"exp\\u0065ct\": # structural comment\n" +
		"      \n"
	c.open("file:///quoted.test.yaml", text)
	got := labels(c.complete("file:///quoted.test.yaml", 3, 6).Items)
	assert.Contains(t, got, "outputs")

	text = "\"tests\":\n" +
		"  - \"name\": the case\n" +
		"    \"stubs\":\n" +
		"      - \"ta\\u0073k\": lo\n"
	c.open("file:///quoted-stub.test.yaml", text)
	got = labels(c.complete("file:///quoted-stub.test.yaml", 3, len(`      - "ta\u0073k": lo`)).Items)
	assert.Contains(t, got, "log",
		"keyAndPosition did not decode the escaped quoted key at its value position")

	text = "\"tests\":\n" +
		"  - \"name\": the case\n" +
		"    \"inputs\": # fixture data begins here\n" +
		"      \"expect: not grammar # still a key\":\n" +
		"        \n"
	c.open("file:///quoted-fixture.test.yaml", text)
	got = labels(c.complete("file:///quoted-fixture.test.yaml", 4, 8).Items)
	assert.Empty(t, got,
		"a quoted fixture-data key containing colon/comment text reopened the test grammar")
}

func TestCompletionSurvivesAMalformedPartialTestEdit(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()
	text := "tests:\n  - name: x\n    expect:\n      err\n    stubs: [\n"
	c.open("file:///partial.test.yaml", text)
	got := c.complete("file:///partial.test.yaml", 3, len("      err"))
	assert.Contains(t, labels(got.Items), "error_contains")
	for _, item := range got.Items {
		assert.NotContains(t, item.Documentation, "secret-value")
	}
}

// TestTheTransitionMapNamesOnlyRealKeys holds [testLevelChildren] to the
// derived key tables in both the direction it states and the one it omits.
// Every transition names a key its parent level really has — so the map
// cannot open a level under a key the loader would refuse — and the keys
// deliberately left out, the ones that hold the author's own data, are
// pinned absent by name so completing the map later has to argue with the
// reason rather than just add the line.
func TestTheTransitionMapNamesOnlyRealKeys(t *testing.T) {
	t.Parallel()

	for parent, children := range testLevelChildren {
		keys := map[string]bool{}
		for _, k := range testDocKeys[parent] {
			keys[k.name] = true
		}
		for child := range children {
			assert.True(t, keys[child],
				"testLevelChildren[%q] names %q, which testDocKeys says that level does not have",
				parent, child)
		}
	}

	for _, dataKey := range []struct {
		level testDocLevel
		key   string
		why   string
	}{
		{testLevelFile, "vars", "a vars: value is the author's literal, not a grammar level"},
		{testLevelCase, "inputs", "a case's inputs hold whatever the workflow declares"},
		{testLevelCase, "secrets", "secret plaintexts, keyed by reference text"},
		{testLevelStub, "returns", "a stub's returns are the task's own output shape"},
		{testLevelStub, "response", "a raw response body is the peer's shape, not the DSL's"},
		{testLevelSignal, "payload", "a signal carries whatever the workflow reads"},
		{testLevelIdentity, "claims", "claims are the policy's vocabulary, not this grammar's"},
		{testLevelExpect, "outputs", "expected outputs mirror the workflow's declarations"},
		{testLevelExpect, "inputs", "expected inputs mirror a delivery's bindings"},
	} {
		_, opens := testLevelChildren[dataKey.level][dataKey.key]
		assert.False(t, opens,
			"testLevelChildren[%q][%q] opens a level, but %s — below it is the author's data "+
				"and nothing there is the DSL's to complete",
			dataKey.level, dataKey.key, dataKey.why)
	}
}
