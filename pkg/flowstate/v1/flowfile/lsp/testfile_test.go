package lsp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A `*.test.yaml` in the editor: checked by the loader
// `flow test` runs, never by the workflow grammar. The negative direction is
// the one that was live: before the document kind existed, a test file
// attached to this server drew a workflow's diagnostics — `tests:` an unknown
// key, no `steps:` — false squiggles on a correct file.

const validSuite = `
defaults:
  workflow: ./workflow.yaml
  stubs:
    - task: log
      returns: {}
tests:
  - name: the case
    expect:
      ran: [hello]
`

// TestAValidTestFileDrawsNoDiagnostics is the false-diagnosis regression: a
// correct suite must be silent, which it cannot be if the workflow grammar is
// consulted at all.
func TestAValidTestFileDrawsNoDiagnostics(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	params := c.open("file:///suite.test.yaml", validSuite)
	assert.Empty(t, params.Diagnostics,
		"a correct test file drew diagnostics — the workflow grammar is leaking into the test language")
}

// TestATestFilesUnknownKeyIsPositioned: the loader's strict decode carries
// goccy's own [line:col], and the diagnostic lands on it — the same precision
// a workflow's YAML mistakes get.
func TestATestFilesUnknownKeyIsPositioned(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	text := strings.Replace(validSuite, "expect:", "expct:", 1)
	params := c.open("file:///typo.test.yaml", text)
	require.Len(t, params.Diagnostics, 1)
	d := params.Diagnostics[0]
	assert.Equal(t, codeTestFile, d.Code)
	assert.Contains(t, d.Message, `unknown field "expct"`)

	lines := strings.Split(text, "\n")
	require.Less(t, d.Range.Start.Line, len(lines))
	assert.Contains(t, lines[d.Range.Start.Line], "expct:",
		"the diagnostic is not on the line holding the mistake")
}

// TestASemanticRefusalUsesTheLoadersPosition: the position-carrying loader
// points at the value it refused; the LSP no longer searches diagnostic prose
// for a case name and guesses its name: line.
func TestASemanticRefusalUsesTheLoadersPosition(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	text := `
defaults:
  workflow: ./workflow.yaml
tests:
  - name: fine
    expect:
      ran: [hello]
  - name: replays wrongly
    trigger:
      webhook: stripe
      payload: ./delivery.json
      signature: sometimes
    expect:
      refused: true
`
	params := c.open("file:///semantic.test.yaml", text)
	require.Len(t, params.Diagnostics, 1)
	d := params.Diagnostics[0]
	assert.Equal(t, codeTestFile, d.Code)
	assert.Contains(t, d.Message, `"sometimes"`)

	lines := strings.Split(text, "\n")
	require.Less(t, d.Range.Start.Line, len(lines))
	assert.Contains(t, lines[d.Range.Start.Line], "signature: sometimes",
		"the diagnostic did not use the loader-owned value position")
}

// TestTheWorkflowFeaturesStayQuietOnATestFile: none of these six answer a
// test document from the *workflow* grammar (#1110 item 8's whole point —
// a step's `for_each:`, a task's inputs, a Marshal-shaped format edit would
// all be wrong answers with confidence in a document that has no `steps:`
// at all). It no longer means every feature answers nothing: completion,
// hover and the outline now have real, narrower answers of their own for
// the test language, asserted here by their *absence of a workflow leak*
// rather than by blanket emptiness, and exercised on their own positive
// terms in testcompletion_test.go, testhover_test.go and
// testsymbols_test.go. Format and code actions still answer nothing at all
// — see their own docTest branches (format.go, codeaction.go) for why.
func TestTheWorkflowFeaturesStayQuietOnATestFile(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	c.open("file:///quiet.test.yaml", validSuite)

	// The key gets the test language's own answer, never the workflow's.
	h := c.hover("file:///quiet.test.yaml", 4, 8)
	require.NotNil(t, h)
	assert.Contains(t, hoverText(h), "task name this replaces")

	// Completion at the same position offers the test language's own
	// stub-level keys (task, step, where, ...), never the workflow's. The
	// leak this asserts against is a workflow-only candidate, not the
	// presence of any candidate at all.
	got := labels(c.complete("file:///quiet.test.yaml", 4, 8).Items)
	for _, workflowOnly := range []string{"for_each", "loop", "parallel", "sleep", "wait_until", "steps"} {
		assert.NotContains(t, got, workflowOnly,
			"a workflow-only key %q leaked into a test file's completion", workflowOnly)
	}

	// The outline names the suite's one case — the test language's own
	// answer, never a workflow's steps.
	symbols := c.symbols("file:///quiet.test.yaml")
	require.Len(t, symbols, 1)
	assert.Equal(t, "the case", symbols[0].Name)

	assert.Empty(t, c.format("file:///quiet.test.yaml"),
		"formatting answered a test document — no flowtest analogue of flowfile.Marshal exists")
	assert.Empty(t, c.codeAction("file:///quiet.test.yaml", wholeOf(validSuite), nil, nil),
		"code actions answered a test document — nothing here computes one yet")
}

// TestTheDirectoryDefaultsFileIsNeverAWorkflow: a standalone
// `testdefaults.yaml` gets strict loader feedback but not fold-dependent
// semantic diagnostics — and above all, not the workflow grammar's opinion of
// a `defaults:` block. Its completion and hover are covered separately.
func TestTheDirectoryDefaultsFileIsNeverAWorkflow(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	params := c.open("file:///testdefaults.yaml", `
vars:
  region: us-east-1
defaults:
  stubs:
    - task: log
      returns: {}
`)
	assert.Empty(t, params.Diagnostics,
		"a correct testdefaults.yaml drew diagnostics — the workflow grammar is leaking")

	broken := c.open("file:///dir2/testdefaults.yaml", "vars: [unclosed\n")
	require.Len(t, broken.Diagnostics, 1)
	assert.Equal(t, codeYAMLSyntax, broken.Diagnostics[0].Code,
		"a syntax error is still reported, as itself")

	unknown := c.open("file:///dir3/testdefaults.yaml", "tests: []\n")
	require.Len(t, unknown.Diagnostics, 1)
	assert.Equal(t, codeTestFile, unknown.Diagnostics[0].Code)
	assert.Contains(t, unknown.Diagnostics[0].Message, `unknown field "tests"`)
}

// TestABrokenDefaultsFileIsPublishedOnItsOwnURI: the loader owns both source
// position and provenance, and the protocol carries the latter on the
// publishDiagnostics notification rather than mapping it onto the suite.
func TestABrokenDefaultsFileIsPublishedOnItsOwnURI(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "testdefaults.yaml"),
		[]byte("defaults:\n  stubs: [\n"), 0o600))

	suiteURI := "file://" + dir + "/suite.test.yaml"
	c.open(suiteURI, validSuite)
	var defaults lsp.PublishDiagnosticsParams
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, published := range c.published {
			if strings.HasSuffix(string(published.URI), "/testdefaults.yaml") && len(published.Diagnostics) > 0 {
				defaults = published
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
	require.Len(t, defaults.Diagnostics, 1)
	d := defaults.Diagnostics[0]
	assert.Equal(t, codeTestFile, d.Code)
	assert.Contains(t, string(defaults.URI), "testdefaults.yaml")
	assert.Equal(t, 1, d.Range.Start.Line)
	assert.NotEqual(t, documentStart, d.Range)
}

func TestAnOpenDefaultsSyntaxDiagnosticWinsOverAnIncludingSuiteDuplicate(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()
	dir := t.TempDir()
	defaultsURI := lsp.DocumentURI("file://" + filepath.Join(dir, "testdefaults.yaml"))
	suiteURI := "file://" + filepath.Join(dir, "suite.test.yaml")
	c.open(string(defaultsURI), "defaults:\n  stubs: [\n")
	c.open(suiteURI, validSuite)

	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := len(c.published) - 1; i >= 0; i-- {
			p := c.published[i]
			if p.URI != defaultsURI || len(p.Diagnostics) == 0 {
				continue
			}
			return p.Diagnostics[0].Code == codeYAMLSyntax
		}
		return false
	}, time.Second, time.Millisecond,
		"the directly open document must deterministically own the duplicate diagnostic code")
}

func TestAnUnsavedDefaultsBufferOwnsItsSemanticDiagnosticAndClearsIt(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()
	dir := t.TempDir()
	defaultsURI := "file://" + dir + "/testdefaults.yaml"
	suiteURI := "file://" + dir + "/suite.test.yaml"
	bad := "defaults:\n  stubs:\n    - returns: {}\n"
	c.open(defaultsURI, bad)
	c.open(suiteURI, validSuite)

	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := len(c.published) - 1; i >= 0; i-- {
			p := c.published[i]
			if p.URI == lsp.DocumentURI(defaultsURI) && len(p.Diagnostics) > 0 {
				return p.Diagnostics[0].Range.Start.Line == 2 &&
					strings.Contains(p.Diagnostics[0].Message, "names neither a task nor a step")
			}
		}
		return false
	}, time.Second, time.Millisecond)

	good := "defaults:\n  stubs:\n    - task: log\n      returns: {}\n"
	c.change(defaultsURI, good, 2)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := len(c.published) - 1; i >= 0; i-- {
			p := c.published[i]
			if p.URI == lsp.DocumentURI(defaultsURI) {
				return len(p.Diagnostics) == 0
			}
		}
		return false
	}, time.Second, time.Millisecond)
}

func TestIncludedDefaultsRetainTheEditorsLocalhostURI(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()
	dir := t.TempDir()
	defaultsURI := lsp.DocumentURI("file://localhost" + filepath.Join(dir, "testdefaults.yaml"))
	suiteURI := "file://localhost" + filepath.Join(dir, "suite.test.yaml")
	c.open(string(defaultsURI), "defaults:\n  stubs:\n    - returns: {}\n")
	c.open(suiteURI, validSuite)

	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := len(c.published) - 1; i >= 0; i-- {
			p := c.published[i]
			if p.URI == defaultsURI && len(p.Diagnostics) > 0 {
				return strings.Contains(p.Diagnostics[0].Message, "names neither a task nor a step")
			}
		}
		return false
	}, time.Second, time.Millisecond)
}

func TestSiblingDocumentURIRetainsFileSpelling(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		uri  lsp.DocumentURI
		want lsp.DocumentURI
	}{
		{"file://localhost/tmp/suite.test.yaml", "file://localhost/tmp/testdefaults.yaml"},
		{"file:///tmp/a%20b/suite.test.yaml", "file:///tmp/a%20b/testdefaults.yaml"},
		{"file://C:/work/suite.test.yaml", "file://C:/work/testdefaults.yaml"},
	} {
		assert.Equal(t, tc.want, siblingDocumentURI(tc.uri, "testdefaults.yaml"))
	}
}

func TestClosingDefaultsReturnsDependentSuitesToTheSavedFile(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()
	dir := t.TempDir()
	defaultsPath := filepath.Join(dir, "testdefaults.yaml")
	defaultsURI := lsp.DocumentURI("file://" + defaultsPath)
	suiteURI := "file://" + filepath.Join(dir, "suite.test.yaml")
	require.NoError(t, os.WriteFile(defaultsPath,
		[]byte("defaults:\n  stubs:\n    - task: log\n      returns: {}\n"), 0o600))

	c.open(string(defaultsURI), "defaults:\n  stubs:\n    - returns: {}\n")
	c.open(suiteURI, validSuite)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := len(c.published) - 1; i >= 0; i-- {
			if c.published[i].URI == defaultsURI {
				return len(c.published[i].Diagnostics) > 0
			}
		}
		return false
	}, time.Second, time.Millisecond)

	wait := c.expectPublish()
	require.NoError(t, c.conn.Notify(t.Context(), "textDocument/didClose", lsp.DidCloseTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: defaultsURI},
	}))
	c.await(wait)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := len(c.published) - 1; i >= 0; i-- {
			if c.published[i].URI == defaultsURI {
				return len(c.published[i].Diagnostics) == 0
			}
		}
		return false
	}, time.Second, time.Millisecond)
}

func TestLiveDefaultsRevalidationHasAnExplicitDependentBound(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()
	dir := t.TempDir()
	defaultsURI := lsp.DocumentURI("file://" + filepath.Join(dir, "testdefaults.yaml"))
	c.open(string(defaultsURI), "defaults: {}\n")

	var firstSuite lsp.DocumentURI
	for i := range maxTestDefaultsDependents {
		uri := "file://" + filepath.Join(dir, fmt.Sprintf("suite-%d.test.yaml", i))
		if i == 0 {
			firstSuite = lsp.DocumentURI(uri)
		}
		c.open(uri, validSuite)
		require.Eventually(t, func() bool {
			c.server.testDiagnosticsMu.Lock()
			defer c.server.testDiagnosticsMu.Unlock()
			return c.server.testDefaultsBySuite[lsp.DocumentURI(uri)] == defaultsURI
		}, time.Second, time.Millisecond)
	}
	overflow := "file://" + filepath.Join(dir, "overflow.test.yaml")
	c.open(overflow, validSuite)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := len(c.published) - 1; i >= 0; i-- {
			if c.published[i].URI == lsp.DocumentURI(overflow) {
				return len(c.published[i].Diagnostics) == 1 &&
					c.published[i].Diagnostics[0].Code == codeTestDefaultsDependents
			}
		}
		return false
	}, time.Second, time.Millisecond)

	require.NoError(t, c.conn.Notify(t.Context(), "textDocument/didClose", lsp.DidCloseTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: firstSuite},
	}))
	require.Eventually(t, func() bool {
		c.server.testDiagnosticsMu.Lock()
		defer c.server.testDiagnosticsMu.Unlock()
		return c.server.testDefaultsBySuite[lsp.DocumentURI(overflow)] == defaultsURI
	}, time.Second, time.Millisecond, "the bounded overflow candidate was not promoted")
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := len(c.published) - 1; i >= 0; i-- {
			if c.published[i].URI == lsp.DocumentURI(overflow) {
				return len(c.published[i].Diagnostics) == 0
			}
		}
		return false
	}, time.Second, time.Millisecond, "the promoted suite retained its limit warning")
}

func TestOverflowSuiteDoesNotPublishSavedErrorsOnAnOpenDefaultsURI(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()
	dir := t.TempDir()
	defaultsPath := filepath.Join(dir, "testdefaults.yaml")
	defaultsURI := lsp.DocumentURI("file://" + defaultsPath)
	require.NoError(t, os.WriteFile(defaultsPath, []byte("tests: []\n"), 0o600))
	assert.Empty(t, c.open(string(defaultsURI), "defaults: {}\n").Diagnostics)

	for i := range maxTestDefaultsDependents {
		uri := "file://" + filepath.Join(dir, fmt.Sprintf("suite-%d.test.yaml", i))
		c.open(uri, validSuite)
		require.Eventually(t, func() bool {
			c.server.testDiagnosticsMu.Lock()
			defer c.server.testDiagnosticsMu.Unlock()
			return c.server.testDefaultsBySuite[lsp.DocumentURI(uri)] == defaultsURI
		}, time.Second, time.Millisecond)
	}
	overflow := "file://" + filepath.Join(dir, "overflow.test.yaml")
	c.open(overflow, validSuite)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := len(c.published) - 1; i >= 0; i-- {
			if c.published[i].URI == lsp.DocumentURI(overflow) {
				return len(c.published[i].Diagnostics) == 1 &&
					c.published[i].Diagnostics[0].Code == codeTestDefaultsDependents
			}
		}
		return false
	}, time.Second, time.Millisecond)

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.published) - 1; i >= 0; i-- {
		if c.published[i].URI == defaultsURI {
			assert.Empty(t, c.published[i].Diagnostics,
				"the overflow suite mapped a saved-file error onto the newer open buffer")
			return
		}
	}
	t.Fatal("the open defaults document never published diagnostics")
}

func TestOpeningDefaultsRetractsAnOverflowSuitesSavedErrors(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()
	dir := t.TempDir()
	defaultsPath := filepath.Join(dir, "testdefaults.yaml")
	defaultsURI := lsp.DocumentURI("file://" + defaultsPath)
	require.NoError(t, os.WriteFile(defaultsPath, []byte("tests: []\n"), 0o600))

	for i := range maxTestDefaultsDependents {
		uri := "file://" + filepath.Join(dir, fmt.Sprintf("suite-%d.test.yaml", i))
		c.open(uri, validSuite)
		require.Eventually(t, func() bool {
			c.server.testDiagnosticsMu.Lock()
			defer c.server.testDiagnosticsMu.Unlock()
			return c.server.testDefaultsBySuite[lsp.DocumentURI(uri)] == defaultsURI
		}, time.Second, time.Millisecond)
	}
	overflow := "file://" + filepath.Join(dir, "overflow.test.yaml")
	c.open(overflow, validSuite)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := len(c.published) - 1; i >= 0; i-- {
			if c.published[i].URI == lsp.DocumentURI(overflow) {
				return len(c.published[i].Diagnostics) == 1 &&
					c.published[i].Diagnostics[0].Code == codeTestDefaultsDependents
			}
		}
		return false
	}, time.Second, time.Millisecond)
	overflow2 := "file://" + filepath.Join(dir, "overflow-2.test.yaml")
	c.open(overflow2, validSuite)
	require.Eventually(t, func() bool {
		c.server.testDiagnosticsMu.Lock()
		defer c.server.testDiagnosticsMu.Unlock()
		return len(c.server.testDiagnosticsBySource[lsp.DocumentURI(overflow2)][lsp.DocumentURI(overflow2)]) == 1
	}, time.Second, time.Millisecond)
	c.server.testDiagnosticsMu.Lock()
	assert.LessOrEqual(t, len(c.server.testSourcesByTarget[defaultsURI]), maxTestDefaultsDependents+1,
		"overflow contributors grew target aggregation beyond the tracked set and one candidate")
	c.server.testDiagnosticsMu.Unlock()

	c.open(string(defaultsURI), "defaults: {}\n")
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := len(c.published) - 1; i >= 0; i-- {
			if c.published[i].URI == defaultsURI {
				return len(c.published[i].Diagnostics) == 0
			}
		}
		return false
	}, time.Second, time.Millisecond, "opening defaults retained an overflow suite's saved-file error")
	c.server.testDiagnosticsMu.Lock()
	assert.Contains(t, c.server.testSourcesByTarget[defaultsURI], lsp.DocumentURI(overflow),
		"the target index did not retain the hidden saved-file contribution")
	c.server.testDiagnosticsMu.Unlock()

	wait := c.expectPublish()
	require.NoError(t, c.conn.Notify(t.Context(), "textDocument/didClose", lsp.DidCloseTextDocumentParams{
		TextDocument: lsp.TextDocumentIdentifier{URI: defaultsURI},
	}))
	c.await(wait)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := len(c.published) - 1; i >= 0; i-- {
			if c.published[i].URI == defaultsURI {
				return len(c.published[i].Diagnostics) > 0
			}
		}
		return false
	}, time.Second, time.Millisecond, "closing defaults did not restore the overflow suite's saved-file error")
}

func TestAStaleDefaultsAnalysisCannotReplaceCurrentDiagnostics(t *testing.T) {
	t.Parallel()
	s := &FlowfileServer{Logger: discardLogger()}
	uri := lsp.DocumentURI("file:///stale/testdefaults.yaml")
	stale := s.docs.open(uri, 1, "defaults:\n  stubs: [\n", nil)
	current := s.docs.change(uri, 2, []lsp.TextDocumentContentChangeEvent{{Text: "defaults: {}\n"}}, nil)
	require.NotNil(t, current)
	s.testDiagnosticsBySource = map[lsp.DocumentURI]map[lsp.DocumentURI][]lsp.Diagnostic{
		uri: {uri: []lsp.Diagnostic{{Code: codeYAMLSyntax, Message: "current"}}},
	}

	s.publishTestDiagnostics(t.Context(), nil, uri, []diagnosticPublication{{
		uri: uri, diagnostics: []lsp.Diagnostic{{Code: codeTestFile, Message: "stale"}},
	}}, stale)

	require.Len(t, s.testDiagnosticsBySource[uri][uri], 1)
	assert.Equal(t, "current", s.testDiagnosticsBySource[uri][uri][0].Message)
}

func TestSavedDefaultsAnalysisCannotReplaceASuiteAfterDefaultsOpen(t *testing.T) {
	t.Parallel()
	s := &FlowfileServer{Logger: discardLogger()}
	suiteURI := lsp.DocumentURI("file:///stale/suite.test.yaml")
	defaultsURI := lsp.DocumentURI("file:///stale/testdefaults.yaml")
	suite := s.docs.open(suiteURI, 1, validSuite, nil)
	s.docs.open(defaultsURI, 1, "defaults: {}\n", nil)
	s.testDefaultsBySuite = map[lsp.DocumentURI]lsp.DocumentURI{suiteURI: defaultsURI}
	s.testDiagnosticsBySource = map[lsp.DocumentURI]map[lsp.DocumentURI][]lsp.Diagnostic{
		suiteURI: {suiteURI: []lsp.Diagnostic{{Code: codeTestFile, Message: "current live result"}}},
	}

	s.publishTestDiagnostics(t.Context(), nil, suiteURI, []diagnosticPublication{{
		uri: suiteURI, diagnostics: []lsp.Diagnostic{{Code: codeTestFile, Message: "stale saved result"}},
	}}, suite)

	require.Len(t, s.testDiagnosticsBySource[suiteURI][suiteURI], 1)
	assert.Equal(t, "current live result", s.testDiagnosticsBySource[suiteURI][suiteURI][0].Message)
}

func TestCloseCleanupDoesNotDeleteAReopenedTestDocument(t *testing.T) {
	t.Parallel()
	s := &FlowfileServer{Logger: discardLogger()}
	uri := lsp.DocumentURI("file:///reopened/suite.test.yaml")
	reopened := s.docs.open(uri, 2, validSuite, nil)
	s.testDiagnosticsBySource = map[lsp.DocumentURI]map[lsp.DocumentURI][]lsp.Diagnostic{
		uri: {uri: []lsp.Diagnostic{{Code: codeTestFile, Message: "reopened"}}},
	}

	promoted := s.clearTestDiagnostics(t.Context(), nil, uri)

	assert.Empty(t, promoted)
	current, ok := s.docs.get(uri)
	assert.True(t, ok)
	assert.Same(t, reopened, current)
	require.Len(t, s.testDiagnosticsBySource[uri][uri], 1)
	assert.Equal(t, "reopened", s.testDiagnosticsBySource[uri][uri][0].Message)
}

func TestAClosedSuiteCannotConsumeADefaultsDependencySlot(t *testing.T) {
	t.Parallel()
	s := &FlowfileServer{Logger: discardLogger()}
	uri := lsp.DocumentURI("file:///closed/suite.test.yaml")
	suite := s.docs.open(uri, 1, validSuite, nil)
	s.docs.close(uri)

	tracked, current := s.rememberTestDefaults(suite, "file:///closed/testdefaults.yaml")
	assert.False(t, tracked)
	assert.False(t, current)
	assert.Empty(t, s.testDefaultsBySuite)
	assert.Empty(t, s.testSuitesByDefaults)
}

func TestQuotedKeyUsesTheLoadersPosition(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()
	text := "defaults:\n  workflow: ./workflow.yaml\ntests:\n  - name: x\n    stubs:\n      - \"task\":\n        returns: {}\n"
	params := c.open("file:///quoted-unknown.test.yaml", text)
	require.NotEmpty(t, params.Diagnostics)
	d := params.Diagnostics[0]
	assert.Equal(t, 5, d.Range.Start.Line)
	assert.Contains(t, d.Message, "names neither a task nor a step")
	assert.NotContains(t, d.Message, "API_KEY")
}

func TestTestDocumentRequestsHonorCancellationAndBounds(t *testing.T) {
	t.Parallel()
	tooLarge := newDocument("file:///bounded.test.yaml", 1, strings.Repeat("x", maxDocumentBytes+1), nil)
	assert.Empty(t, completeAt(tooLarge, lsp.Position{}).Items)
	assert.Nil(t, hoverAt(tooLarge, lsp.Position{}))
	diagnostics := diagnose(tooLarge)
	require.Len(t, diagnostics, 1)
	assert.Equal(t, codeTooLarge, diagnostics[0].Code)

	var store documentStore
	uri := lsp.DocumentURI("file:///cancelled.test.yaml")
	store.beginBuild(uri)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	doc, ok := store.await(ctx, nil, uri)
	assert.False(t, ok)
	assert.Nil(t, doc)
	store.endBuild(uri)
}

func TestAProblemRangeWithoutSourceUsesAConservativeCharacter(t *testing.T) {
	t.Parallel()
	rng := testProblemRange("", 7, 200)
	assert.Equal(t, 6, rng.Start.Line)
	assert.Zero(t, rng.Start.Character)
}

// TestASuitesOwnErrorIsNotMistakenForTheDefaultsFile (Codex, #1109) is the
// negative direction of the test above, and it is the one that was wrong.
//
// Provenance was decided by looking for "testdefaults.yaml" in the error's
// prose, so an ordinary refusal from *this* suite that happened to quote that
// string — a case named after the file — was filed as a sibling's problem:
// anchored at the document start, shown whole, and bypassing the positioning
// every other loader error gets. The error is asked where it came from now.
func TestASuitesOwnErrorIsNotMistakenForTheDefaultsFile(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	// No sibling defaults file exists here at all, so anything blamed on one
	// is a misfiling by construction.
	dir := t.TempDir()

	params := c.open("file://"+dir+"/suite.test.yaml", `edition: v2026.3
tests:
  - name: testdefaults.yaml
    expect:
      failed: false
`)
	require.Len(t, params.Diagnostics, 1)

	d := params.Diagnostics[0]
	assert.Equal(t, codeTestFile, d.Code)
	assert.NotEqual(t, documentStart, d.Range,
		"a refusal about this suite's own case was filed as a sibling defaults file's, "+
			"so it lost the position every other loader error gets")
}

// TestADirectoryNamedAfterTheDefaultsFileIsStillOrdinary is the same
// misfiling reached through the path rather than the content: the loader
// prefixes its errors with the suite's path, so every suite under a directory
// whose name contains `testdefaults.yaml` matched the prose test.
func TestADirectoryNamedAfterTheDefaultsFileIsStillOrdinary(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	c.initialize()

	dir := filepath.Join(t.TempDir(), "testdefaults.yaml.d")
	require.NoError(t, os.MkdirAll(dir, 0o750))

	params := c.open("file://"+dir+"/suite.test.yaml", `edition: v2026.3
tests:
  - name: it runs
    expect:
      failed: false
`)
	require.Len(t, params.Diagnostics, 1)

	d := params.Diagnostics[0]
	assert.Equal(t, codeTestFile, d.Code)
	assert.NotEqual(t, documentStart, d.Range,
		"the directory's name decided where this suite's own diagnostic was anchored")
}
