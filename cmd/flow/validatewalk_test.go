package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestValidateAcceptsADirectory is #394's central claim: `flow validate demo`
// walks demo exactly as `flow fix demo` and `flow test demo` already do,
// instead of refusing with a bare directory error. Both a workflow and a
// Flowfile test live in the directory, and both must be found and checked.
func TestValidateAcceptsADirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(cleanWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"tests:\n  - name: it runs\n    workflow: ./workflow.yaml\n    expect:\n      ran: [s]\n"), 0o600))

	out, err := validateOutput(t, dir)
	require.NoError(t, err, "output: %s", out)

	require.Contains(t, out, "workflow.yaml")
	require.Contains(t, out, "workflow.test.yaml")
	require.Contains(t, out, "ok")
	require.NotContains(t, out, "is a directory")
}

// TestValidateOnADirectoryReportsATestFilesOwnSchemaProblem is the negative
// direction: a Flowfile test with a structural mistake is still found by the
// directory walk and reported as a diagnostic under its own schema, not
// silently accepted because it does not compile as a workflow.
func TestValidateOnADirectoryReportsATestFilesOwnSchemaProblem(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(cleanWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"tests:\n  - name: broken\n    workflow: ./workflow.yaml\n    expect:\n      ran: \"s\"\n"), 0o600))

	out, err := validateOutput(t, dir)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, out, "workflow.test.yaml")
}

// TestValidateOnAMissingFileMatchesRunLocalsWording is #394's second, smaller
// claim: a missing file is reported in the same shape `flow run local`
// already uses for the same failure, the path once as the position, then
// Go's own `open <path>: ...`, rather than the `error reading X: open X:
// ...` wrap that repeated the "reading" framing a second time for no fact the
// first mention had not already given.
func TestValidateOnAMissingFileMatchesRunLocalsWording(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	missing := filepath.Join(dir, "nope.yaml")

	root := newRootCommand()
	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"validate", missing})

	err := root.Execute()
	require.Error(t, err)
	require.NotContains(t, err.Error(), "error reading")
	require.Equal(t, missing+": open "+missing+": no such file or directory", err.Error())
}

// TestValidateJSONOnADirectoryReportsBothFiles is [TestValidateAcceptsADirectory]
// through the machine surface, which walks a separate code path
// ([validateMachine]) that has to agree with the text one.
func TestValidateJSONOnADirectoryReportsBothFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(cleanWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"tests:\n  - name: it runs\n    workflow: ./workflow.yaml\n    expect:\n      ran: [s]\n"), 0o600))

	out, err := validateOutput(t, dir, "-o", "json")
	require.NoError(t, err, "output: %s", out)

	var report struct {
		Files []struct {
			File        string `json:"file"`
			Diagnostics []any  `json:"diagnostics"`
		} `json:"files"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &report))
	require.Len(t, report.Files, 2)
}
