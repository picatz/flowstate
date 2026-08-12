package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// `flow validate` printed diagnostics and nothing else, so anything driving it had to
// parse them back out of prose.
//
// The decision this settles is what `--output json` *means* here. On `get` and `list`
// it is documented as carrying the server's own schema, so a field is addressable by
// name; a `validate` answering with a shape the CLI made up would give one flag two
// meanings in one binary. So a diagnostic is a schema message — `v1.Diagnostic`, read
// by this command, by the language server, and by whatever asks a future `Validate`
// RPC.

// validateOutput runs `flow validate` with the given arguments and returns stdout.
//
// Through the real command rather than the function under it, because the flag plumbing
// is part of what is being tested: a `--output` that parses and is then ignored would
// pass every test written one level down.
func validateOutput(t *testing.T, args ...string) (string, error) {
	t.Helper()

	root := newRootCommand()
	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"validate"}, args...))

	err := root.Execute()

	return out.String(), err
}

// writeWorkflow puts a Flowfile in a temporary directory and returns its path.
func writeWorkflow(t *testing.T, name, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	return path
}

const cleanWorkflow = `edition: v2026.3
name: fine
steps:
  - id: s
    log:
      message: hello
`

// A method the schema's own pattern refuses, so there is a diagnostic with a position,
// a step and a field — every part of the message worth carrying.
const brokenWorkflow = `edition: v2026.3
name: broken
steps:
  - id: web
    http:
      method: FETCH
      url: https://example.com
`

// TestValidateJSONCarriesTheSchemasFields is the point of the format: a consumer reads
// a field by name instead of parsing a sentence.
func TestValidateJSONCarriesTheSchemasFields(t *testing.T) {
	path := writeWorkflow(t, "broken.yaml", brokenWorkflow)

	out, err := validateOutput(t, path, "-o", "json")
	require.Error(t, err, "a file with a problem reported success")

	var report struct {
		Files []struct {
			File        string `json:"file"`
			Diagnostics []struct {
				Line    uint32 `json:"line"`
				Column  uint32 `json:"column"`
				Message string `json:"message"`
				Step    string `json:"step"`
				Field   string `json:"field"`
			} `json:"diagnostics"`
		} `json:"files"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &report), "the answer is not one JSON document:\n%s", out)

	require.Len(t, report.Files, 1)
	assert.Equal(t, path, report.Files[0].File, "the report does not name the file it is about")

	require.Len(t, report.Files[0].Diagnostics, 1)
	got := report.Files[0].Diagnostics[0]

	// The position, addressable rather than embedded in prose. An editor or an agent
	// jumping to line 6 column 15 is the whole reason this format exists.
	assert.Equal(t, uint32(6), got.Line)
	assert.Equal(t, uint32(15), got.Column)
	assert.Equal(t, "web", got.Step)
	assert.Equal(t, "method", got.Field)
	assert.Contains(t, got.Message, "does not match regex pattern")
}

// TestValidateJSONIsOneDocumentAndJSONLIsOneLinePerFile pins the distinction the two
// formats carry everywhere else in this CLI.
//
// `json` is one document per invocation and `jsonl` is one per line. Checking three
// files must not turn `json` into three documents, which is what makes the
// invocation-level wrapper necessary rather than decorative.
func TestValidateJSONIsOneDocumentAndJSONLIsOneLinePerFile(t *testing.T) {
	first := writeWorkflow(t, "one.yaml", cleanWorkflow)
	second := writeWorkflow(t, "two.yaml", brokenWorkflow)

	asJSON, err := validateOutput(t, first, second, "-o", "json")
	require.Error(t, err)

	var one any
	require.NoError(t, json.Unmarshal([]byte(asJSON), &one),
		"`-o json` over two files is not a single document:\n%s", asJSON)

	asJSONL, err := validateOutput(t, first, second, "-o", "jsonl")
	require.Error(t, err)

	lines := strings.Split(strings.TrimSpace(asJSONL), "\n")
	require.Len(t, lines, 2, "`-o jsonl` did not write one line per file:\n%s", asJSONL)
	for i, line := range lines {
		var report map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &report), "line %d is not a document: %s", i+1, line)
		assert.Contains(t, report, "file", "a jsonl line does not name its file: %s", line)
	}
}

// TestValidateReportsACleanFileRatherThanOmittingIt is the distinction a consumer
// cannot recover on its own.
//
// "Checked and clean" and "not checked" are different facts. A reader that only ever
// saw failures could not tell a valid file from one the command never reached — an
// argument typo, a glob that matched nothing — and would report success for both.
func TestValidateReportsACleanFileRatherThanOmittingIt(t *testing.T) {
	path := writeWorkflow(t, "fine.yaml", cleanWorkflow)

	out, err := validateOutput(t, path, "-o", "jsonl")
	require.NoError(t, err, "a valid file was reported as a failure")

	var report struct {
		File        string `json:"file"`
		Diagnostics []any  `json:"diagnostics"`
	}
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(out)), &report))

	assert.Equal(t, path, report.File)
	assert.Empty(t, report.Diagnostics, "a clean file reported diagnostics")
}

// TestValidateExitStatusIsTheSameInEveryFormat keeps `flow validate x -o json && ...`
// behaving the way a shell reader expects.
//
// The format decides how the answer is written, never what the answer is. A machine
// format that exited zero on a broken file would be the worst possible combination:
// silent in the shell and only wrong to whoever reads the JSON.
func TestValidateExitStatusIsTheSameInEveryFormat(t *testing.T) {
	clean := writeWorkflow(t, "fine.yaml", cleanWorkflow)
	broken := writeWorkflow(t, "broken.yaml", brokenWorkflow)

	for _, format := range []string{"text", "json", "jsonl"} {
		t.Run(format, func(t *testing.T) {
			_, err := validateOutput(t, clean, "-o", format)
			assert.NoError(t, err, "a valid file failed in %s", format)

			_, err = validateOutput(t, broken, "-o", format)
			assert.Error(t, err, "a broken file succeeded in %s", format)
		})
	}
}

// TestValidateJSONReportsAFileThatDoesNotParse keeps a document that is not YAML from
// disappearing.
//
// It is still a fact about the *workflow* rather than about the invocation, so it
// belongs in the report — with no position, which is honest, rather than pinned to line
// 1, which would send a reader somewhere arbitrary.
func TestValidateJSONReportsAFileThatDoesNotParse(t *testing.T) {
	path := writeWorkflow(t, "notyaml.yaml", "edition: v2026.3\nname: t\nsteps:\n  - id: a\n   bad indent\n")

	out, err := validateOutput(t, path, "-o", "json")
	require.Error(t, err, "a file that does not parse was reported as valid")

	var report struct {
		Files []struct {
			Diagnostics []struct {
				Message string `json:"message"`
			} `json:"diagnostics"`
		} `json:"files"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &report), "the answer is not JSON:\n%s", out)

	require.Len(t, report.Files, 1)
	require.NotEmpty(t, report.Files[0].Diagnostics, "a file that does not parse produced no diagnostics")
	assert.NotEmpty(t, report.Files[0].Diagnostics[0].Message)
}

// TestValidateTextOutputIsUnchanged is the regression guard for everyone who is not a
// machine.
//
// The default has to stay exactly what it was: adding a format must not quietly change
// what somebody sees when they do not ask for one.
func TestValidateTextOutputIsUnchanged(t *testing.T) {
	clean := writeWorkflow(t, "fine.yaml", cleanWorkflow)

	out, err := validateOutput(t, clean)
	require.NoError(t, err)
	assert.Equal(t, clean+": ok\n", out)
}
