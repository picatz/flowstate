package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// handBuiltNested is a workflow with no source: a for_each `pages` whose body is
// one task step per id, each naming a task that does not exist.
func handBuiltNested(bodyIDs ...string) *v1.Workflow {
	body := make([]*v1.Node, 0, len(bodyIDs))
	for _, id := range bodyIDs {
		body = append(body, &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: "nosuchtask"}}})
	}

	return &v1.Workflow{
		Name: "hand-built",
		Steps: []*v1.Node{{
			Id:   "pages",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{Body: body}},
		}},
	}
}

// A specification with no source has no positions, and still names its step.
func TestDiagnosticPathComesFromTheWorkflowNotTheSource(t *testing.T) {
	ds := flowfile.Validate(handBuiltNested("fetch"))

	var found *flowfile.Diagnostic
	for i := range ds {
		if ds[i].Step == "fetch" {
			found = &ds[i]
		}
	}
	require.NotNil(t, found, "want a diagnostic on step fetch, got %v", ds)
	assert.Equal(t, []string{"pages", "fetch"}, found.Path)
	assert.Zero(t, found.Line)
	assert.Equal(t, []string{"pages", "fetch"}, found.Proto().GetPath())
}

func TestDiagnosticPathEmptyForAnAmbiguousHandBuiltID(t *testing.T) {
	wf := handBuiltNested("dup")
	wf.Steps = append(wf.Steps, &v1.Node{Id: "other", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
		Body: []*v1.Node{{Id: "dup", Kind: &v1.Node_Task{Task: &v1.Task{Name: "nosuchtask"}}}},
	}}})

	var seen int
	for _, d := range flowfile.Validate(wf) {
		if d.Step == "dup" {
			seen++
			assert.Empty(t, d.Path)
		}
	}
	require.Positive(t, seen)
}

// The path a diagnostic carries is the key the source map files the same step under.
func TestDiagnosticPathEqualsTheSourceMapSite(t *testing.T) {
	wf, positions, err := flowfile.Parse([]byte(nestedDefect))
	require.NoError(t, err)
	sourceMap := flowfile.SourceMap("f.yaml", []byte(nestedDefect), wf, positions)

	var mapped []string
	for _, entry := range sourceMap.GetEntries() {
		if path := entry.GetSite().GetPath(); len(path) > 0 && path[len(path)-1] == "fetch" {
			mapped = path
		}
	}
	require.NotEmpty(t, mapped)

	for _, d := range pathDiagnostics(t, nestedDefect) {
		if d.Step == "fetch" {
			assert.Equal(t, mapped, d.Path)

			return
		}
	}
	t.Fatal("no diagnostic on step fetch")
}

// Schema diagnostics are appended after the validator runs and name steps too.
func TestSchemaDiagnosticCarriesThePathOfItsStep(t *testing.T) {
	var b strings.Builder
	b.WriteString("edition: v2026.4\nname: body\nsteps:\n  - id: again\n    loop:\n      until: ${true}\n      max_iterations: 1\n      steps:\n")
	for i := range 101 {
		fmt.Fprintf(&b, "        - id: b%d\n          log:\n            message: hi\n", i)
	}

	for _, d := range pathDiagnostics(t, b.String()) {
		if d.Step == "b100" {
			assert.Equal(t, []string{"again", "b100"}, d.Path)

			return
		}
	}
	t.Fatal("no schema diagnostic on step b100")
}
