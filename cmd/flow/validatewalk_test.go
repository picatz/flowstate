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

	err := runFlow(t, "validate", missing).Err
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

// validateStdin runs `flow validate -` with body on stdin and returns stdout.
func validateStdin(t *testing.T, body string, extra ...string) (string, error) {
	t.Helper()

	res := runFlowStdin(t, body, append([]string{"validate", "-"}, extra...)...)

	return res.Stdout, res.Err
}

// TestValidateDashReadsAWorkflowFromStdin is #397's central claim: `flow
// validate -` reads the document from stdin instead of trying to open a file
// literally named "-", which is what it did before.
func TestValidateDashReadsAWorkflowFromStdin(t *testing.T) {
	t.Parallel()

	out, err := validateStdin(t, cleanWorkflow)
	require.NoError(t, err, "output: %s", out)
	require.Contains(t, out, "-: ")
	require.Contains(t, out, "ok")
}

// TestValidateDashReadsATestFileFromStdin proves stdin is checked under the
// right schema, not forced through the workflow validator regardless of what
// it actually holds: a Flowfile test piped in still validates as a test.
func TestValidateDashReadsATestFileFromStdin(t *testing.T) {
	t.Parallel()

	out, err := validateStdin(t, "tests:\n  - name: it runs\n    workflow: ./workflow.yaml\n    expect:\n      ran: [s]\n")
	require.NoError(t, err, "output: %s", out)
	require.Contains(t, out, "ok")
}

// TestValidateDashReportsAMalformedDocumentFromStdin is the negative
// direction: a broken document piped in is still reported as a diagnostic,
// not silently accepted just because it arrived over a pipe instead of a
// file.
func TestValidateDashReportsAMalformedDocumentFromStdin(t *testing.T) {
	t.Parallel()

	out, err := validateStdin(t, brokenWorkflow)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, out, "-:")
}

// TestValidateDashCannotBeCombinedWithAnotherPath is #397's stated boundary:
// stdin is exactly one document, so mixing "-" with a real path in one
// invocation is refused with a message naming why, rather than silently
// picking one of the two or reading neither.
func TestValidateDashCannotBeCombinedWithAnotherPath(t *testing.T) {
	t.Parallel()

	path := writeWorkflow(t, "workflow.yaml", cleanWorkflow)

	out, err := validateStdin(t, cleanWorkflow, path)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, err.Error(), "cannot be combined")
}

// TestValidateDashRefusesStdinPastTheByteLimit is the bound CLAUDE.md
// requires for anything reading untrusted input: stdin is refused rather
// than read without limit, matching the same document bound a file on disk
// gets.
func TestValidateDashRefusesStdinPastTheByteLimit(t *testing.T) {
	t.Parallel()

	oversized := strings.Repeat("a", maxValidateDocumentBytes+1)

	out, err := validateStdin(t, oversized)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, err.Error(), "exceeds")
}

// TestValidateOnAnEmptyDirectoryIsRefused is the sibling refusal
// [collectFlowfiles] and [collectTestFiles] already give: a directory with no
// Flowfile and no Flowfile test in it must not exit 0 having silently
// checked nothing, which reads as CI having validated a path it never
// actually found anything in.
func TestValidateOnAnEmptyDirectoryIsRefused(t *testing.T) {
	t.Parallel()

	out, err := validateOutput(t, t.TempDir())
	require.Error(t, err, "output: %s", out)
	require.Contains(t, err.Error(), "no Flowfiles")
}

// TestValidateDirectoryWalkDoesNotReadAnOversizedFileWhole is #394's bound
// finding: a directory can hold a file of any size a caller (or an attacker)
// chose, and classifying it must not read the whole thing into memory first
// only to have flowfile's own size check answer false; the read itself has
// to be bounded, the same way stdin already is. The oversized file is
// skipped outright (never reported, ok or not) while a genuine, ordinary
// Flowfile beside it in the same directory is still found and validated,
// which is what proves the walk kept going rather than aborting.
func TestValidateDirectoryWalkDoesNotReadAnOversizedFileWhole(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(cleanWorkflow), 0o600))

	oversized := strings.Repeat("x", maxValidateDocumentBytes+1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "huge.yaml"), []byte("name: huge\n"+oversized), 0o600))

	out, err := validateOutput(t, dir)
	require.NoError(t, err, "output: %s", out)
	require.Contains(t, out, "workflow.yaml")
	require.NotContains(t, out, "huge.yaml")
}

// TestValidateReportsMisspelledSignalNameInTestFile is #1443's validate-surface
// claim: `flow validate` compiles the referenced workflow and checks each
// scripted signal's name against its declared gates, so a typo is caught before
// `flow test` rather than passing green with a signal delivered into the void.
func TestValidateReportsMisspelledSignalNameInTestFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(`
edition: v2026.3
name: gated
steps:
  - id: gate
    wait_for_signal:
      name: approve
      timeout: 10s
outputs: {}
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(`
tests:
  - name: misspelled signal
    workflow: ./workflow.yaml
    signals:
      - name: aprove
        at: 1s
        payload: {}
    expect:
      outputs: {}
`), 0o600))

	out, err := validateOutput(t, dir)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, out, `"aprove"`)
	require.Contains(t, out, "approve")
}
