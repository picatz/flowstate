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

// TestDefinitionOfATestWorkflowIsNilWhereCallDefinitionIsNil: a missing file,
// an escaping or absolute path, a directory, an expression, the key rather
// than the value, author data, and an untitled buffer all navigate nowhere.
func TestDefinitionOfATestWorkflowIsNilWhereCallDefinitionIsNil(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	require.NoError(t, os.Mkdir(sub, 0o700))
	require.NoError(t, os.Mkdir(filepath.Join(sub, "adir"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wf.yaml"), []byte(definitionCallee), 0o600))

	atValue := func(value string) []lsp.Location {
		text := "tests:\n  - name: a\n    workflow: " + value + "\n"
		return testDefinitionAt(t, sub, "s.test.yaml", text, value, 1)
	}

	assert.Empty(t, atValue("missing.yaml"), "missing file")
	assert.Empty(t, atValue("../wf.yaml"), "escapes the test file's directory")
	assert.Empty(t, atValue(filepath.Join(dir, "wf.yaml")), "absolute path")
	assert.Empty(t, atValue("adir"), "not a regular file")
	assert.Empty(t, atValue("${inputs.x}"), "expression")

	require.NoError(t, os.WriteFile(filepath.Join(sub, "wf.yaml"), []byte(definitionCallee), 0o600))
	text := "tests:\n  - name: a\n    workflow: wf.yaml\n"
	assert.Empty(t, testDefinitionAt(t, sub, "s.test.yaml", text, "workflow", 2), "cursor on the key")
	assert.NotEmpty(t, testDefinitionAt(t, sub, "s.test.yaml", text, "wf.yaml", 2), "control: the value resolves")

	data := "tests:\n  - name: a\n    inputs:\n      workflow: wf.yaml\n"
	assert.Empty(t, testDefinitionAt(t, sub, "s.test.yaml", data, "wf.yaml", 2), "author data")

	doc := newDocument("untitled:Untitled-1", 1, text, nil)
	assert.Empty(t, definitionAt(doc, positionOf(t, text, "wf.yaml", 2)), "untitled buffer")
}

// TestDefinitionOfATestWorkflowRefusesASymlinkOut: a path that stays inside the
// directory as written but resolves outside it is refused, as for `call:`.
func TestDefinitionOfATestWorkflowRefusesASymlinkOut(t *testing.T) {
	t.Parallel()
	outside, dir := t.TempDir(), t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "wf.yaml"), []byte(definitionCallee), 0o600))
	if err := os.Symlink(filepath.Join(outside, "wf.yaml"), filepath.Join(dir, "link.yaml")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	text := "tests:\n  - name: a\n    workflow: link.yaml\n"
	assert.Empty(t, testDefinitionAt(t, dir, "s.test.yaml", text, "link.yaml", 2))
}
