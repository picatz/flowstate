package lsp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const definitionCallee = "edition: v2026.4\nname: target\nsteps:\n  - id: noop\n    log:\n      message: hi\n"

// testDefinitionAt opens text as the file name inside dir and asks for the
// definition at the first occurrence of needle, offset bytes in.
func testDefinitionAt(t *testing.T, dir, name, text, needle string, offset int) []lsp.Location {
	t.Helper()
	doc := newDocument(fileURI(filepath.Join(dir, name)), 1, text, nil)
	return definitionAt(doc, positionOf(t, text, needle, offset))
}

func realPath(t *testing.T, p string) string {
	t.Helper()
	real, err := filepath.EvalSymlinks(p)
	require.NoError(t, err)
	return real
}

// TestDefinitionOfATestCaseWorkflowOpensTheFlowfileAtItsName covers a case's
// `workflow:` and the suite-level `defaults:` spelling, plain and quoted.
func TestDefinitionOfATestCaseWorkflowOpensTheFlowfileAtItsName(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wf.yaml"), []byte(definitionCallee), 0o600))

	for name, text := range map[string]string{
		"case":     "tests:\n  - name: a\n    workflow: wf.yaml\n",
		"defaults": "defaults:\n  workflow: wf.yaml\ntests:\n  - name: a\n",
		"quoted":   "tests:\n  - name: a\n    workflow: \"wf.yaml\"\n",
	} {
		locs := testDefinitionAt(t, dir, "s.test.yaml", text, "wf.yaml", 2)
		require.Len(t, locs, 1, name)
		assert.Equal(t, fileURI(realPath(t, filepath.Join(dir, "wf.yaml"))), locs[0].URI, name)
		assert.Equal(t, 1, locs[0].Range.Start.Line, "%s: lands on name:", name)
	}
}

// TestDefinitionInATestDefaultsFile: a testdefaults.yaml's `defaults:` names a
// workflow too.
func TestDefinitionInATestDefaultsFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wf.yaml"), []byte(definitionCallee), 0o600))

	locs := testDefinitionAt(t, dir, "testdefaults.yaml", "defaults:\n  workflow: wf.yaml\n", "wf.yaml", 1)
	require.Len(t, locs, 1)
	assert.Equal(t, fileURI(realPath(t, filepath.Join(dir, "wf.yaml"))), locs[0].URI)
	assert.Equal(t, 1, locs[0].Range.Start.Line)
}

// TestDefinitionOfATestWorkflowFollowsFlowTestPathRules: `flow test` accepts an
// absolute or parent-relative workflow and follows symlinks, so a suite that
// runs gets a jump too — unlike `call:`, whose containment is the compiler's.
func TestDefinitionOfATestWorkflowFollowsFlowTestPathRules(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	require.NoError(t, os.Mkdir(sub, 0o700))
	wf := filepath.Join(dir, "wf.yaml")
	require.NoError(t, os.WriteFile(wf, []byte(definitionCallee), 0o600))

	atValue := func(value string) []lsp.Location {
		text := "tests:\n  - name: a\n    workflow: " + value + "\n"
		return testDefinitionAt(t, sub, "s.test.yaml", text, value, 1)
	}
	for name, value := range map[string]string{"parent-relative": "../wf.yaml", "absolute": wf} {
		locs := atValue(value)
		require.Len(t, locs, 1, name)
		assert.Equal(t, fileURI(wf), locs[0].URI, name)
	}

	if err := os.Symlink(wf, filepath.Join(sub, "link.yaml")); err == nil {
		assert.Len(t, atValue("link.yaml"), 1, "a symlink is followed")
	}
}

// TestDefinitionOfATestWorkflowIsNilWhereThereIsNothingToOpen: a missing file, a
// directory, an expression, the key rather than the value, a trailing comment,
// an unclosed quote, author data, and an untitled buffer all navigate nowhere.
func TestDefinitionOfATestWorkflowIsNilWhereThereIsNothingToOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "adir"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wf.yaml"), []byte(definitionCallee), 0o600))

	at := func(value, needle string, offset int) []lsp.Location {
		text := "tests:\n  - name: a\n    workflow: " + value + "\n"
		return testDefinitionAt(t, dir, "s.test.yaml", text, needle, offset)
	}

	assert.Empty(t, at("missing.yaml", "missing.yaml", 1), "missing file")
	assert.Empty(t, at("adir", "adir", 1), "not a regular file")
	assert.Empty(t, at("${inputs.x}", "${inputs.x}", 1), "expression")
	assert.Empty(t, at("wf.yaml", "workflow", 2), "cursor on the key")
	assert.NotEmpty(t, at("wf.yaml # note", "wf.yaml", 2), "control: a trailing comment leaves the value resolving")
	assert.Empty(t, at("wf.yaml # note", "note", 1), "cursor on the comment")
	assert.Empty(t, at(`"wf.yaml`, "wf.yaml", 1), "unclosed quote")

	data := "tests:\n  - name: a\n    inputs:\n      workflow: wf.yaml\n"
	assert.Empty(t, testDefinitionAt(t, dir, "s.test.yaml", data, "wf.yaml", 2), "author data")

	text := "tests:\n  - name: a\n    workflow: wf.yaml\n"
	doc := newDocument("untitled:Untitled-1", 1, text, nil)
	assert.Empty(t, definitionAt(doc, positionOf(t, text, "wf.yaml", 2)), "untitled buffer")
}
