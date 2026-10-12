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

	res := runFlow(t, append([]string{"compile"}, args...)...)

	return res.Stdout, res.Stderr, res.Err
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

	// From the file, as `flow compile` compiles it: a program compiled from a
	// file records the digest of its bytes.
	want, positions, _, err := flowfile.ParseAndValidateFileAt(path)
	require.NoError(t, err)
	require.NotEmpty(t, want.GetSourceDigest())
	flowfile.AttachSources(want, positions, path)
	require.NotNil(t, want.GetSteps()[0].GetSource(), "compile must carry where each step is written")

	out, errOut, err := compileOutput(t, path)
	require.NoError(t, err, "a valid example was refused; stderr said:\n%s", errOut)

	var got v1.Workflow
	require.NoError(t, protojson.Unmarshal([]byte(out), &got),
		"the answer is not a workflow document:\n%s", out)

	if diff := cmp.Diff(want, &got, protocmp.Transform()); diff != "" {
		t.Fatalf("the printed specification is not the one the compiler produces (-want +got):\n%s", diff)
	}
}

// TestCompileAcceptsPluginTasksFromEitherDescriptorSource pins both authoring
// postures: a developer may launch the plugin they have, while CI and reviewers
// may consume the checked-in catalog without executing it.
func TestCompileAcceptsPluginTasksFromEitherDescriptorSource(t *testing.T) {
	bin := buildFlowBinary(t)
	catalog := pluginCatalogFor(t, bin)

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"catalog", []string{"--" + pluginCatalogFlag, catalog}},
		{"plugin directory", []string{"--plugin-dir", buildExamplePluginDir(t)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"compile"}, tc.args...)
			args = append(args, exampleGreetWorkflow)
			res := runFlowBinary(t, bin, args...)

			require.NoError(t, res.Err, "a declared plugin task did not compile:\n%s", res.Output())
			var workflow v1.Workflow
			require.NoError(t, protojson.Unmarshal([]byte(res.Stdout), &workflow),
				"the compiled specification is not a workflow document:\n%s", res.Stdout)
			require.NotEmpty(t, workflow.GetSteps())
			assert.Equal(t, "example.greet", workflow.GetSteps()[0].GetTask().GetName(),
				"the compiled specification does not carry the plugin task")
			assert.NotContains(t, res.Output(), "no plugin task",
				"the descriptor source did not reach compilation:\n%s", res.Output())
		})
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
	path := writeWorkflow(t, "notyaml.yaml", "edition: v2026.4\nname: t\nsteps:\n  - id: a\n   bad indent\n")

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
	path := filepath.Join(t.TempDir(), "nothing-here.yaml")

	out, _, err := compileOutput(t, path)
	require.Error(t, err)

	// The path itself, not a generic noun. `flow compile` takes one file and the
	// commands beside it take several, so a reader who mistyped one of four needs
	// to be told which — and "Flowfile" is the word every other surface uses for
	// it, per docs/CLI.md's one-vocabulary rule.
	assert.Contains(t, err.Error(), "error reading "+path)
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
			want, positions, _, err := flowfile.ParseAndValidateFileAt(path)
			require.NoError(t, err)
			flowfile.AttachSources(want, positions, path)

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

// TestCompileSchemaWritesTheContractAsJSONSchema pins that `--schema` answers with the
// projection of the declarations and not the specification, through the real command.
func TestCompileSchemaWritesTheContractAsJSONSchema(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "record-types", "workflow.yaml")

	out, errOut, err := compileOutput(t, path, "--schema", "inputs")
	require.NoError(t, err, "stderr said:\n%s", errOut)

	var schema map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &schema), "the answer is not a JSON document:\n%s", out)
	assert.Equal(t, "https://json-schema.org/draft/2020-12/schema", schema["$schema"])
	assert.Contains(t, schema["properties"], "order")
	assert.Contains(t, schema["$defs"], "Order")
	assert.NotContains(t, schema, "steps", "this is the contract, not the specification")

	outputs, _, err := compileOutput(t, path, "--schema", "outputs", "-o", "jsonl")
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(strings.TrimSpace(outputs), "\n")+1, "jsonl is one line")

	_, _, err = compileOutput(t, path, "--schema", "steps")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"inputs" or "outputs"`)
}

// compileSourceMap compiles path with --source-map and returns the specification read
// back from stdout and the map read back from the file written beside it.
func compileSourceMap(t *testing.T, path, mapPath string, extra ...string) (*v1.Workflow, *v1.DebugSourceMap, string) {
	t.Helper()

	out, errOut, err := compileOutput(t, append([]string{path, "--source-map", mapPath}, extra...)...)
	require.NoError(t, err, "stderr said:\n%s", errOut)

	var workflow v1.Workflow
	require.NoError(t, protojson.Unmarshal([]byte(out), &workflow), "stdout is not a workflow:\n%s", out)

	raw, err := os.ReadFile(mapPath)
	require.NoError(t, err)
	var sourceMap v1.DebugSourceMap
	require.NoError(t, protojson.Unmarshal(raw, &sourceMap), "the file is not a source map:\n%s", raw)

	return &workflow, &sourceMap, string(raw)
}

// TestCompileSourceMapDescribesThePrintedSpecification is the join for --source-map:
// the digest in the file must be the digest of the specification on stdout, and the
// entries must put a step on the line it is written on.
func TestCompileSourceMapDescribesThePrintedSpecification(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("..", "..", "examples", "hello-world", "workflow.yaml"))
	require.NoError(t, err)
	mapPath := filepath.Join(t.TempDir(), "map.json")

	workflow, sourceMap, raw := compileSourceMap(t, path, mapPath)

	assert.Equal(t, flowfile.IRDigest(workflow), sourceMap.GetIrDigest(),
		"the map is bound to a different program than the one printed")
	assert.True(t, strings.HasSuffix(raw, "}\n") && !strings.HasSuffix(raw, "\n\n"), "want one trailing newline")

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, sourceMap.GetDocuments(), 1)
	assert.Equal(t, path, sourceMap.GetDocuments()[0].GetUri())
	assert.Equal(t, v1.ContentDigest(data), sourceMap.GetDocuments()[0].GetDigest())

	// The same map the library makes for these bytes.
	want, positions, _, err := flowfile.ParseAndValidateFileAt(path)
	require.NoError(t, err)
	if diff := cmp.Diff(flowfile.SourceMap(path, data, want, positions), sourceMap, protocmp.Transform()); diff != "" {
		t.Fatalf("the map is not the library's for this file (-want +got):\n%s", diff)
	}

	// And the step "hello" is where the file says it is.
	var line uint32
	for _, entry := range sourceMap.GetEntries() {
		if strings.Join(entry.GetSite().GetPath(), "/") == "hello" {
			line = entry.GetLocation().GetRange().GetStartLine()
		}
	}
	var wantLine uint32
	for i, text := range strings.Split(string(data), "\n") {
		if strings.Contains(text, "id: hello") {
			wantLine = uint32(i + 1)
		}
	}
	require.NotZero(t, wantLine)
	assert.Equal(t, wantLine, line, "step hello is not mapped to the line it is written on: %s", raw)
}

func TestCompileSourceMapIsRefusedWithSchema(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "hello-world", "workflow.yaml")
	mapPath := filepath.Join(t.TempDir(), "map.json")

	out, _, err := compileOutput(t, path, "--schema", "inputs", "--source-map", mapPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--source-map")
	assert.Empty(t, out)
	assert.NoFileExists(t, mapPath)
}

func TestCompileSourceMapIsNotWrittenForARefusedFile(t *testing.T) {
	path := writeWorkflow(t, "broken.yaml", brokenWorkflow)
	mapPath := filepath.Join(t.TempDir(), "map.json")

	out, _, err := compileOutput(t, path, "--source-map", mapPath)
	require.Error(t, err)
	assert.Empty(t, out)
	assert.NoFileExists(t, mapPath, "a refused compile left a source map behind")

	entries, err := os.ReadDir(filepath.Dir(mapPath))
	require.NoError(t, err)
	assert.Empty(t, entries, "a refused compile left a file beside the requested path")
}

// A called Flowfile is a second document, bound to its exact bytes.
func TestCompileSourceMapNamesCalleeDocumentsByDigest(t *testing.T) {
	dir, err := filepath.Abs(filepath.Join("..", "..", "examples", "call-a-workflow"))
	require.NoError(t, err)
	caller := filepath.Join(dir, "workflow.yaml")
	callee := filepath.Join(dir, "workflows", "provision-tenant.yaml")

	_, sourceMap, raw := compileSourceMap(t, caller, filepath.Join(t.TempDir(), "map.json"))

	require.Len(t, sourceMap.GetDocuments(), 2, raw)
	for i, file := range []string{caller, callee} {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		assert.Equal(t, file, sourceMap.GetDocuments()[i].GetUri())
		assert.Equal(t, v1.ContentDigest(data), sourceMap.GetDocuments()[i].GetDigest())
	}
	var calleeEntries int
	for _, entry := range sourceMap.GetEntries() {
		if entry.GetLocation().GetDocument() == 1 {
			calleeEntries++
		}
	}
	assert.NotZero(t, calleeEntries, "no step of the callee is mapped")
}

func TestCompileSourceMapRefusesAnUnwritablePath(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "hello-world", "workflow.yaml")
	dir := t.TempDir()

	for name, target := range map[string]string{
		"empty":     "",
		"directory": dir,
		"no parent": filepath.Join(dir, "missing", "map.json"),
	} {
		t.Run(name, func(t *testing.T) {
			out, _, err := compileOutput(t, path, "--source-map", target)
			require.Error(t, err)
			assert.Empty(t, out, "stdout was written before the path was refused")
		})
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}
