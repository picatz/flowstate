package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `flow fix --check` and `flow fmt --check` printed prose and nothing else, so
// CI wanting structured "what would change / what was refused" had to scrape
// stderr. These tests pin what `--output json`/`--output jsonl` mean there —
// exactly what they mean on `flow validate`: protojson over the schema message
// the command already builds for a person, never a shape invented for the
// occasion.

// TestFixJSONReportsAChangeWithItsPosition is the point of the format: a
// consumer reads a line and message by field instead of parsing
// `path:line: message`.
func TestFixJSONReportsAChangeWithItsPosition(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", oldStyleGreeter)

	out, _, err := runFixCommand(t, "--check", "-o", "json", path)
	require.Error(t, err, "--check found work to do and still exited zero")

	var report struct {
		Files []struct {
			File    string `json:"file"`
			Changed bool   `json:"changed"`
			Changes []struct {
				Line    uint32 `json:"line"`
				Message string `json:"message"`
			} `json:"changes"`
			Refusals []any `json:"refusals"`
		} `json:"files"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &report), "the answer is not one JSON document:\n%s", out)

	require.Len(t, report.Files, 1)
	got := report.Files[0]
	assert.Equal(t, path, got.File)
	assert.True(t, got.Changed)
	assert.Empty(t, got.Refusals)

	require.NotEmpty(t, got.Changes)
	var found bool
	for _, change := range got.Changes {
		if change.Line == uint32(greeterFirstTaskLine) {
			found = true
			assert.Contains(t, change.Message, "http")
		}
	}
	assert.True(t, found, "no change was reported at line %d:\n%+v", greeterFirstTaskLine, got.Changes)
}

// TestFixJSONReportsARefusalWithItsPosition is the other half: a shape the
// rewriter would not touch still produces a report, with the refusal in it
// rather than only a nonzero exit status.
func TestFixJSONReportsARefusalWithItsPosition(t *testing.T) {
	const inFlowStyle = `edition: v2026.3
name: flow-style
steps:
  - id: greet
    task: {name: log, inputs: {message: hi}}
`
	const taskLine = 5

	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", inFlowStyle)

	out, _, err := runFixCommand(t, "--check", "-o", "json", path)
	require.Error(t, err)

	var report struct {
		Files []struct {
			Changed  bool `json:"changed"`
			Refusals []struct {
				Line    uint32 `json:"line"`
				Column  uint32 `json:"column"`
				Message string `json:"message"`
			} `json:"refusals"`
		} `json:"files"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &report), "the answer is not JSON:\n%s", out)

	require.Len(t, report.Files, 1)
	got := report.Files[0]
	assert.False(t, got.Changed)
	require.Len(t, got.Refusals, 1)
	assert.Equal(t, uint32(taskLine), got.Refusals[0].Line)
	assert.Contains(t, strings.ToLower(got.Refusals[0].Message), "flow style")
}

// TestFixJSONIsOneDocumentAndJSONLIsOneLinePerFile pins the same distinction
// `flow validate` draws: `json` is one document per invocation, `jsonl` is one
// per line.
func TestFixJSONIsOneDocumentAndJSONLIsOneLinePerFile(t *testing.T) {
	dir := t.TempDir()
	first := writeFixture(t, dir, "first.yaml", oldStyleGreeter)
	second := writeFixture(t, dir, "second.yaml", oldStyleSingle)

	asJSON, _, err := runFixCommand(t, "--check", "-o", "json", first, second)
	require.Error(t, err)

	var one any
	require.NoError(t, json.Unmarshal([]byte(asJSON), &one),
		"`-o json` over two files is not a single document:\n%s", asJSON)

	asJSONL, _, err := runFixCommand(t, "--check", "-o", "jsonl", first, second)
	require.Error(t, err)

	lines := strings.Split(strings.TrimSpace(asJSONL), "\n")
	require.Len(t, lines, 2, "`-o jsonl` did not write one line per file:\n%s", asJSONL)
	for i, line := range lines {
		var report map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &report), "line %d is not a document: %s", i+1, line)
		assert.Contains(t, report, "file", "a jsonl line does not name its file: %s", line)
	}
}

// TestFixJSONOnACurrentFileReportsCleanRatherThanOmitting is the same distinction
// `flow validate` draws between "checked and clean" and "not checked": a file
// with nothing to do still produces a report, with changed:false rather than
// being left out.
func TestFixJSONOnACurrentFileReportsCleanRatherThanOmitting(t *testing.T) {
	dir := t.TempDir()
	before := copyExamplesInto(t, dir)

	require.NotEmpty(t, before)
	var path string
	for p := range before {
		path = p
		break
	}

	out, err := func() (string, error) {
		o, _, e := runFixCommand(t, "--check", "-o", "jsonl", path)
		return o, e
	}()
	require.NoError(t, err, "--check reported work on a file that is already current:\n%s", out)

	var report struct {
		File     string `json:"file"`
		Changed  bool   `json:"changed"`
		Changes  []any  `json:"changes"`
		Refusals []any  `json:"refusals"`
	}
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(out)), &report))
	assert.Equal(t, path, report.File)
	assert.False(t, report.Changed)
	assert.Empty(t, report.Changes)
	assert.Empty(t, report.Refusals)
}

// TestFixTextOutputIsUnchanged is the regression guard: adding a machine format
// must not quietly change what a person sees when they never asked for one.
func TestFixTextOutputIsUnchanged(t *testing.T) {
	dir := t.TempDir()
	before := copyExamplesInto(t, dir)
	require.NotEmpty(t, before)
	var path string
	for p := range before {
		path = p
		break
	}

	out, _, err := runFixCommand(t, "--check", path)
	require.NoError(t, err)
	assert.Equal(t, path+": already current\n", out)
}

// TestFixStdoutAndMachineOutputAreRefused pins the flag combination that cannot
// be honoured: --stdout and a machine format both want stdout for something
// different.
func TestFixStdoutAndMachineOutputAreRefused(t *testing.T) {
	dir := t.TempDir()
	path := writeFixture(t, dir, "workflow.yaml", oldStyleGreeter)

	out, _, err := runFixCommand(t, "--stdout", "-o", "json", path)
	if err == nil {
		t.Error("--stdout and -o json were accepted together")
	}
	if strings.Contains(out, "message:") {
		t.Errorf("a refused invocation wrote a document anyway:\n%s", out)
	}
}

// TestFmtJSONReportsAChangeAndARefusal is `flow fmt`'s equivalent of the fix
// tests above: changed is projected from the same bytes.Equal comparison the
// text form already makes, and a file that does not parse produces a refusal
// rather than only a nonzero status.
func TestFmtJSONReportsAChangeAndARefusal(t *testing.T) {
	dir := t.TempDir()
	changedPath := writeFixture(t, dir, "changed.yaml", cleanWorkflow+"\n")
	badPath := writeFixture(t, dir, "bad.yaml", "edition: v2026.3\nname: x\n  steps: [\n")

	out, _, err := runFmtCommand(t, "--check", "-o", "jsonl", changedPath, badPath)
	require.Error(t, err)

	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2)

	var reports []struct {
		File     string `json:"file"`
		Changed  bool   `json:"changed"`
		Refusals []struct {
			Message string `json:"message"`
		} `json:"refusals"`
	}
	for _, line := range lines {
		var r struct {
			File     string `json:"file"`
			Changed  bool   `json:"changed"`
			Refusals []struct {
				Message string `json:"message"`
			} `json:"refusals"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &r), "line is not a document: %s", line)
		reports = append(reports, r)
	}

	byFile := map[string]int{}
	for i, r := range reports {
		byFile[r.File] = i
	}

	changed := reports[byFile[changedPath]]
	assert.True(t, changed.Changed)
	assert.Empty(t, changed.Refusals)

	bad := reports[byFile[badPath]]
	assert.False(t, bad.Changed)
	require.NotEmpty(t, bad.Refusals)
	assert.NotEmpty(t, bad.Refusals[0].Message)
}

// cleanWorkflow is declared in validate_output_test.go and reused here so the
// two commands' machine output is checked against the same fixture.
