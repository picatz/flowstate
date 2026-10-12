package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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

// copyExample copies an example directory into a temporary one, so a test whose
// failure would overwrite a Flowfile overwrites a copy.
func copyExample(t *testing.T, name string) string {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.CopyFS(dir, os.DirFS(filepath.Join("..", "..", "examples", name))))

	return dir
}

// A map written over a Flowfile it describes destroys the source; each spelling
// of that file is refused, and the bytes and the directory are left as they were.
func TestCompileSourceMapRefusesToReplaceAFlowfile(t *testing.T) {
	for _, tc := range []struct {
		name string
		// target names the source map path, given the caller and callee paths.
		target func(t *testing.T, caller, callee string) string
	}{
		{"same path", func(t *testing.T, caller, callee string) string { return caller }},
		{"symlink alias", func(t *testing.T, caller, callee string) string {
			link := filepath.Join(filepath.Dir(caller), "alias.yaml")
			require.NoError(t, os.Symlink(caller, link))
			return link
		}},
		{"hard link", func(t *testing.T, caller, callee string) string {
			link := filepath.Join(filepath.Dir(caller), "hard.yaml")
			require.NoError(t, os.Link(caller, link))
			return link
		}},
		{"callee", func(t *testing.T, caller, callee string) string { return callee }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := copyExample(t, "call-a-workflow")
			caller := filepath.Join(dir, "workflow.yaml")
			callee := filepath.Join(dir, "workflows", "provision-tenant.yaml")
			target := tc.target(t, caller, callee)

			before := map[string][]byte{}
			for _, file := range []string{caller, callee} {
				data, err := os.ReadFile(file)
				require.NoError(t, err)
				before[file] = data
			}
			names := dirNames(t, dir)
			calleeNames := dirNames(t, filepath.Dir(callee))

			out, _, err := compileOutput(t, caller, "--source-map", target)
			require.Error(t, err, "a map was written over a Flowfile")
			if target != callee {
				assert.Empty(t, out, "the input was refused only after compiling it")
			}
			assert.Contains(t, err.Error(), "--source-map")

			for file, want := range before {
				got, err := os.ReadFile(file)
				require.NoError(t, err)
				assert.Equal(t, string(want), string(got), "%s was changed", file)
			}
			assert.Equal(t, names, dirNames(t, dir), "a file was created or left behind")
			assert.Equal(t, calleeNames, dirNames(t, filepath.Dir(callee)))
		})
	}
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}

	return names
}

// A map replaces a file already at PATH without widening who can read it, and a
// refused compile leaves that file as it was.
func TestCompileSourceMapKeepsTheModeOfAFileItReplaces(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "hello-world", "workflow.yaml")
	mapPath := filepath.Join(t.TempDir(), "map.json")
	require.NoError(t, os.WriteFile(mapPath, []byte("old"), 0o600))

	_, _, raw := compileSourceMap(t, path, mapPath)

	info, err := os.Stat(mapPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "the replaced file's mode was widened")
	data, err := os.ReadFile(mapPath)
	require.NoError(t, err)
	assert.Equal(t, raw, string(data))

	// A new file takes the neighbours' mode.
	fresh := filepath.Join(t.TempDir(), "fresh.json")
	compileSourceMap(t, path, fresh)
	info, err = os.Stat(fresh)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o644), info.Mode().Perm())

	// A refused compile does not touch what is there.
	broken := writeWorkflow(t, "broken.yaml", brokenWorkflow)
	_, _, err = compileOutput(t, broken, "--source-map", mapPath)
	require.Error(t, err)
	data, err = os.ReadFile(mapPath)
	require.NoError(t, err)
	assert.Equal(t, raw, string(data), "a refused compile replaced the previous map")
}

func TestCompileSourceMapAcceptsALongFileName(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "hello-world", "workflow.yaml")
	mapPath := filepath.Join(t.TempDir(), strings.Repeat("m", 250)+".json")

	compileSourceMap(t, path, mapPath)
	assert.FileExists(t, mapPath)
}

// Without --source-map the refusals of a path that is not a readable Flowfile
// are the ones they were before the flag existed.
func TestCompileRefusesWhatIsNotAReadableFlowfileInTheWordsItAlwaysUsed(t *testing.T) {
	dir := t.TempDir()

	directory := filepath.Join(dir, "dir.yaml")
	require.NoError(t, os.Mkdir(directory, 0o755))
	fifo := filepath.Join(dir, "fifo.yaml")
	require.NoError(t, syscall.Mkfifo(fifo, 0o600))
	big := filepath.Join(dir, "big.yaml")
	require.NoError(t, os.WriteFile(big, make([]byte, 1<<20+1), 0o600))

	for _, tc := range []struct{ path, want string }{
		{directory, directory + ": " + directory + " is a directory; name the Flowfile inside it"},
		{fifo, fifo + ": " + fifo + " is not a regular file (named pipe); name a regular file instead"},
		{big, big + ": " + big + " is larger than the 1048576 byte limit a Flowfile is compiled up to; nothing was read"},
	} {
		out, errOut, err := compileOutput(t, tc.path)
		require.ErrorIs(t, err, errCompileRefused)
		assert.Empty(t, out)
		assert.Contains(t, errOut, tc.want)
	}
}

func TestSourceMapReachesBounds(t *testing.T) {
	documents := func(n int) *v1.DebugSourceMap {
		m := &v1.DebugSourceMap{}
		for range n {
			m.Documents = append(m.Documents, &v1.DebugSourceDocument{})
		}
		return m
	}
	entries := func(n int) *v1.DebugSourceMap {
		m := &v1.DebugSourceMap{}
		for range n {
			m.Entries = append(m.Entries, &v1.DebugSourceEntry{})
		}
		return m
	}

	assert.True(t, sourceMapReachesBounds(documents(flowfile.MaxSourceMapDocuments)))
	assert.True(t, sourceMapReachesBounds(entries(flowfile.MaxSourceMapEntries)))
	assert.False(t, sourceMapReachesBounds(documents(flowfile.MaxSourceMapDocuments-1)))
	assert.False(t, sourceMapReachesBounds(entries(flowfile.MaxSourceMapEntries-1)))
	assert.False(t, sourceMapReachesBounds(&v1.DebugSourceMap{}))
}

// A relative input is mapped by absolute path, callee included, and the map
// belongs to the printed workflow in every output form.
func TestCompileSourceMapFromARelativePathInEveryOutputForm(t *testing.T) {
	t.Chdir(filepath.Join("..", "..", "examples"))
	root, err := os.Getwd()
	require.NoError(t, err)

	for _, format := range []string{"json", "jsonl", "text"} {
		t.Run(format, func(t *testing.T) {
			mapPath := filepath.Join(t.TempDir(), "map.json")

			workflow, sourceMap, raw := compileSourceMap(t, filepath.Join("call-a-workflow", "workflow.yaml"), mapPath, "-o", format)

			assert.Equal(t, flowfile.IRDigest(workflow), sourceMap.GetIrDigest())
			require.Len(t, sourceMap.GetDocuments(), 2, raw)
			assert.Equal(t, filepath.Join(root, "call-a-workflow", "workflow.yaml"), sourceMap.GetDocuments()[0].GetUri())
			assert.Equal(t, filepath.Join(root, "call-a-workflow", "workflows", "provision-tenant.yaml"), sourceMap.GetDocuments()[1].GetUri())
		})
	}
}
