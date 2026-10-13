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
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const specSource = `edition: v2026.4
name: two-steps
steps:
  - id: first
    log:
      message: one
  - id: second
    log:
      message: two
`

// brokenSpec compiles specSource beside its source map and then edits the
// compiled program the way a producer that is not this compiler might: the
// second step names a task nothing registers. The map is reissued for the
// edited program, as that producer would, so it still locates every step.
func brokenSpec(t *testing.T) (specPath, mapPath, source string) {
	t.Helper()

	dir := t.TempDir()
	source = filepath.Join(dir, "flow.yaml")
	require.NoError(t, os.WriteFile(source, []byte(specSource), 0o600))
	mapPath = filepath.Join(dir, "flow.map.json")
	workflow, sourceMap, _ := compileSourceMap(t, source, mapPath)

	var edited bool
	for _, node := range workflow.GetSteps() {
		if node.GetId() == "second" && node.GetTask() != nil {
			node.GetTask().Name = "nope.task"
			edited = true
		}
	}
	require.True(t, edited, "the fixture has no task step called second")
	sourceMap.IrDigest = flowfile.IRDigest(workflow)

	specPath = filepath.Join(dir, "flow.json")
	writeProto(t, specPath, workflow)
	writeProto(t, mapPath, sourceMap)

	return specPath, mapPath, source
}

func writeProto(t *testing.T, path string, message proto.Message) {
	t.Helper()

	data, err := protojson.Marshal(message)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
}

func TestValidateSpecWithoutAMapNamesTheStepByAddress(t *testing.T) {
	spec, _, _ := brokenSpec(t)

	res := runFlow(t, "validate", "--spec", spec)

	require.Error(t, res.Err)
	assert.Contains(t, res.Output(), "second", "the problem names its step")
	assert.NotRegexp(t, `\.yaml:\d+`, res.Output(), "no map, so no invented line")
}

func TestValidateSpecWithItsMapReportsTheFlowfileLine(t *testing.T) {
	spec, mapPath, source := brokenSpec(t)

	res := runFlow(t, "validate", "--spec", spec, "--source-map", mapPath)

	require.Error(t, res.Err)
	assert.Contains(t, res.Output(), source+":7:5:", "the problem is at the step's line:\n%s", res.Output())

	// The same finding as data: under the document it is in, at that line.
	res = runFlow(t, "validate", "--spec", spec, "--source-map", mapPath, "-o", "json")
	require.Error(t, res.Err)
	var report struct {
		Files []struct {
			File        string `json:"file"`
			Diagnostics []struct {
				Line uint32   `json:"line"`
				Path []string `json:"path"`
			} `json:"diagnostics"`
		} `json:"files"`
	}
	require.NoError(t, json.Unmarshal([]byte(res.Stdout), &report), res.Stdout)
	var found bool
	for _, file := range report.Files {
		if file.File != source {
			continue
		}
		for _, d := range file.Diagnostics {
			found = found || (d.Line == 7 && strings.Join(d.Path, "/") == "second")
		}
	}
	assert.True(t, found, "no diagnostic on line 7 of the Flowfile:\n%s", res.Stdout)
}

func TestValidateSpecRefusesAMapOfAnotherProgram(t *testing.T) {
	spec, _, _ := brokenSpec(t)

	// The map the compiler wrote for the program before it was edited.
	dir := t.TempDir()
	other := filepath.Join(dir, "flow.yaml")
	require.NoError(t, os.WriteFile(other, []byte(specSource), 0o600))
	_, _, _ = compileSourceMap(t, other, filepath.Join(dir, "old.map.json"))

	res := runFlow(t, "validate", "--spec", spec, "--source-map", filepath.Join(dir, "old.map.json"))

	require.Error(t, res.Err)
	assert.Contains(t, res.Output(), "different program")
	assert.NotContains(t, res.Output(), "nope.task", "nothing is checked against a map that does not hold")
}

func TestValidateSpecWithholdsLinesOfASourceThatChanged(t *testing.T) {
	spec, mapPath, source := brokenSpec(t)
	require.NoError(t, os.WriteFile(source, []byte("# a comment that moves every line\n"+specSource), 0o600))

	res := runFlow(t, "validate", "--spec", spec, "--source-map", mapPath)

	require.Error(t, res.Err)
	assert.NotContains(t, res.Output(), source, "the file is not the bytes the map was made from:\n%s", res.Output())
	assert.Contains(t, res.Output(), "second", "the address is still there")
}

// A source the map names that is not on this machine cannot be compared, and is
// trusted as the map says.
func TestValidateSpecTrustsAMapForASourceItCannotSee(t *testing.T) {
	spec, mapPath, source := brokenSpec(t)
	require.NoError(t, os.Remove(source))

	res := runFlow(t, "validate", "--spec", spec, "--source-map", mapPath)

	require.Error(t, res.Err)
	assert.Contains(t, res.Output(), source+":7:5:")
}

func TestValidateSpecFlagsMeanWhatTheySay(t *testing.T) {
	spec, mapPath, _ := brokenSpec(t)

	res := runFlow(t, "validate", "--source-map", mapPath, spec)
	require.Error(t, res.Err)
	assert.Contains(t, res.Output(), "--spec")

	res = runFlow(t, "validate", "--spec", spec, spec, "--source-map", mapPath)
	require.Error(t, res.Err)
	assert.Contains(t, res.Output(), "one specification")

	res = runFlow(t, "validate", "--spec", mapPath)
	require.Error(t, res.Err)
	assert.Regexp(t, `is not a\s+compiled\s+specification`, res.Output())

	clean := filepath.Join(t.TempDir(), "clean.yaml")
	require.NoError(t, os.WriteFile(clean, []byte(specSource), 0o600))
	good := filepath.Join(filepath.Dir(clean), "good.json")
	workflow, _, _ := compileSourceMap(t, clean, filepath.Join(filepath.Dir(clean), "good.map.json"))
	writeProto(t, good, workflow)
	res = runFlow(t, "validate", "--spec", good)
	require.NoError(t, res.Err, res.Output())
	assert.Contains(t, res.Output(), "ok")

}
