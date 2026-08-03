package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/google/go-cmp/cmp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow compile` promises one thing: that what it writes is the specification the
// engine would run. A test that only checks something was printed proves the command
// has a stdout, not that it has an answer — so the tests here read the document back
// into a [v1.Workflow] and compare it to what the compiler produces directly.

// compileOutput runs `flow compile` with the given arguments and returns stdout and
// stderr separately.
//
// Separately because the split is half of what is under test: the specification is the
// answer and goes to stdout, a diagnostic is the account of why there is none and goes
// to stderr. Through the real command, so the flag plumbing is exercised too.
func compileOutput(t *testing.T, args ...string) (string, string, error) {
	t.Helper()

	root := newRootCommand()
	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"compile"}, args...))

	err := root.Execute()

	return out.String(), errOut.String(), err
}

// TestCompileWritesTheSpecificationTheCompilerProduces is the join, and the reason
// this command is worth having at all.
//
// Not "it printed JSON" and not "the JSON has a name field": the document on stdout,
// parsed back into the schema message, must equal what [flowfile.Unmarshal] produces
// for the same bytes. Anything less would pass for a command that printed a
// specification with a step quietly missing from it.
func TestCompileWritesTheSpecificationTheCompilerProduces(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "hello-world", "workflow.yaml")

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	want, err := flowfile.Unmarshal(data)
	require.NoError(t, err)

	out, errOut, err := compileOutput(t, path)
	require.NoError(t, err, "a valid example was refused; stderr said:\n%s", errOut)

	var got v1.Workflow
	require.NoError(t, protojson.Unmarshal([]byte(out), &got),
		"the answer is not a workflow document:\n%s", out)

	if diff := cmp.Diff(want, &got, protocmp.Transform()); diff != "" {
		t.Fatalf("the printed specification is not the one the compiler produces (-want +got):\n%s", diff)
	}
}

// TestCompileRefusesAFileWithProblems pins all four halves of a refusal, because
// three of them passing is still a broken pipeline.
//
// The diagnostics carry their position and go to stderr; stdout stays empty, since a
// consumer reading it must never receive prose where a document was promised; and the
// exit status is non-zero, so `flow compile x.yaml > spec.json && ./submit.sh` stops.
func TestCompileRefusesAFileWithProblems(t *testing.T) {
	path := writeWorkflow(t, "broken.yaml", brokenWorkflow)

	out, errOut, err := compileOutput(t, path)
	require.Error(t, err, "a file with a problem compiled anyway")

	assert.Empty(t, out, "a refused file still wrote something to stdout:\n%s", out)

	// The same `file:line:column: message` shape `flow validate` writes, because an
	// editor and a `make` wrapper parse one form and this is it.
	assert.Contains(t, errOut, path+":6:15: ",
		"the diagnostic does not carry the position, so nothing can jump to it:\n%s", errOut)
	assert.Contains(t, errOut, `step "web" input "method"`,
		"the diagnostic does not name the step and field:\n%s", errOut)
}

// TestCompileRefusesAFileThatDoesNotParse keeps the refusal covering the other way a
// file fails.
//
// A document that is not YAML at all never reaches the validator, so it arrives as a
// parse failure rather than as a list of checks — and it has to be refused the same
// way, rather than falling through to an empty specification.
func TestCompileRefusesAFileThatDoesNotParse(t *testing.T) {
	path := writeWorkflow(t, "notyaml.yaml", "edition: v2026.2\nname: t\nsteps:\n  - id: a\n   bad indent\n")

	out, errOut, err := compileOutput(t, path)
	require.Error(t, err, "a file that does not parse compiled anyway")

	assert.Empty(t, out, "a file that does not parse still wrote a specification:\n%s", out)
	assert.NotEmpty(t, strings.TrimSpace(errOut), "a file that does not parse produced no diagnostic")
}

// TestCompileTextAndJSONAreTheSameDocument is the format decision, written down where
// it can stop being true.
//
// A compiled specification is a protobuf message and protojson is the only faithful
// way to write one down, so `text` is deliberately not a second rendering — inventing
// one would be inventing a shape nothing could read back. `jsonl` is the same document
// on one line.
func TestCompileTextAndJSONAreTheSameDocument(t *testing.T) {
	path := writeWorkflow(t, "fine.yaml", cleanWorkflow)

	asText, _, err := compileOutput(t, path)
	require.NoError(t, err)

	asJSON, _, err := compileOutput(t, path, "-o", "json")
	require.NoError(t, err)

	assert.Equal(t, asJSON, asText, "`-o text` and `-o json` wrote different documents")

	asJSONL, _, err := compileOutput(t, path, "-o", "jsonl")
	require.NoError(t, err)

	assert.Equal(t, 1, strings.Count(strings.TrimSpace(asJSONL), "\n")+1,
		"`-o jsonl` did not write one line:\n%s", asJSONL)

	// One line and the same value: a reader that switches formats gets a different
	// spelling of the answer, never a different answer.
	var compact, indented v1.Workflow
	require.NoError(t, protojson.Unmarshal([]byte(asJSONL), &compact))
	require.NoError(t, protojson.Unmarshal([]byte(asJSON), &indented))

	if diff := cmp.Diff(&indented, &compact, protocmp.Transform()); diff != "" {
		t.Fatalf("`-o jsonl` and `-o json` are different specifications (-json +jsonl):\n%s", diff)
	}
}

// TestCompileJSONCarriesTheSchemasFieldNames is what makes the answer addressable.
//
// protojson over the schema rather than an encoder of this command's own, so a
// consumer reads `.steps[].task.name` — a documented field — instead of whatever shape
// happened to be convenient the day it was written.
func TestCompileJSONCarriesTheSchemasFieldNames(t *testing.T) {
	path := writeWorkflow(t, "fine.yaml", cleanWorkflow)

	out, _, err := compileOutput(t, path, "-o", "json")
	require.NoError(t, err)

	var document struct {
		Name  string `json:"name"`
		Steps []struct {
			ID   string `json:"id"`
			Task struct {
				Name string `json:"name"`
			} `json:"task"`
		} `json:"steps"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &document),
		"the answer is not one JSON document:\n%s", out)

	assert.Equal(t, "fine", document.Name)
	require.Len(t, document.Steps, 1)
	assert.Equal(t, "s", document.Steps[0].ID)
	assert.Equal(t, "log", document.Steps[0].Task.Name)
}

// TestCompileRefusesAnOutputFormatItDoesNotHave keeps the flag honest here for the
// same reason it is refused everywhere else: a caller who wrote `--output yaml` wants
// YAML, and quietly handing them JSON is a worse answer than saying no.
func TestCompileRefusesAnOutputFormatItDoesNotHave(t *testing.T) {
	path := writeWorkflow(t, "fine.yaml", cleanWorkflow)

	out, _, err := compileOutput(t, path, "-o", "yaml")
	require.Error(t, err, "an unknown format was accepted")
	assert.Empty(t, out, "an unknown format still wrote a document:\n%s", out)
}

// TestCompileReportsAFileItCannotRead separates a fact about the invocation from a
// fact about a workflow.
//
// A path that does not exist is fixed in the shell, not in the file, so it is an error
// rather than a diagnostic — reporting it as one would file "you typed the wrong path"
// beside "this step references a step that does not exist".
func TestCompileReportsAFileItCannotRead(t *testing.T) {
	out, _, err := compileOutput(t, filepath.Join(t.TempDir(), "nothing-here.yaml"))
	require.Error(t, err)

	assert.Contains(t, err.Error(), "error reading workflow file")
	assert.Empty(t, out)
}

// TestCompileCompilesEveryExample is the breadth the single-file tests cannot give.
//
// The examples are the files CI already keeps honest and the ones an author copies
// from, so a shape any of them uses — a loop, a wait, a secret reference, a federated
// credential — is a shape this command has to be able to write down. Each answer is
// read back into the schema message, so an example that compiled to something
// unprintable fails here rather than at whoever pipes it somewhere.
func TestCompileCompilesEveryExample(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err, "finding the examples")
	require.NotEmpty(t, paths, "no examples were found, so this test proves nothing")

	for _, path := range paths {
		t.Run(filepath.Base(filepath.Dir(path)), func(t *testing.T) {
			data, err := os.ReadFile(path)
			require.NoError(t, err)

			want, err := flowfile.Unmarshal(data)
			require.NoError(t, err)

			out, errOut, err := compileOutput(t, path)
			require.NoError(t, err, "the example was refused; stderr said:\n%s", errOut)
			assert.Empty(t, errOut, "a clean example wrote to stderr:\n%s", errOut)

			var got v1.Workflow
			require.NoError(t, protojson.Unmarshal([]byte(out), &got),
				"the answer is not a workflow document:\n%s", out)

			if diff := cmp.Diff(want, &got, protocmp.Transform()); diff != "" {
				t.Fatalf("%s compiled to something else (-want +got):\n%s", path, diff)
			}
		})
	}
}
