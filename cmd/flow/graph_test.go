package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `flow graph` promises the graph the files declare, in a fixed order, from files
// alone. The tests read the JSON back into the schema message rather than check
// that something printed.

func TestGraphShowsACallAndItsCalleesTasks(t *testing.T) {
	dir := filepath.Join("..", "..", "examples", "call-a-workflow")

	res := runFlow(t, "graph", dir)

	require.NoError(t, res.Err, res.Stderr)
	assert.Equal(t, `call-a-workflow
  calls provision-tenant
  uses  log
provision-tenant
  uses  log x2
`, res.Stdout, "the test suite beside the workflow is not a workflow, and the callee read from its own file is the callee inlined")
}

func TestGraphJSONIsTheSchemaMessage(t *testing.T) {
	res := runFlow(t, "graph", filepath.Join("..", "..", "examples", "approval-gate"), "-o", "json")
	require.NoError(t, res.Err, res.Stderr)

	var g v1.Graph
	require.NoError(t, protojson.Unmarshal([]byte(res.Stdout), &g))
	require.NoError(t, v1.Validate(&g))

	var waits []string
	for _, e := range g.GetEdges() {
		if e.GetKind() == v1.GraphEdgeKind_GRAPH_EDGE_KIND_WAITS {
			waits = append(waits, e.GetTo())
		}
	}
	assert.Equal(t, []string{"signal:deploy-approved"}, waits)

	again := runFlow(t, "graph", filepath.Join("..", "..", "examples", "approval-gate"), "-o", "json")
	assert.Equal(t, res.Stdout, again.Stdout, "the same files are the same bytes")
}

func TestGraphLeavesOutAFileThatDoesNotCompileAndSaysSo(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.flow.yaml")
	bad := filepath.Join(dir, "bad.flow.yaml")
	require.NoError(t, os.WriteFile(good, []byte("edition: v2026.4\nname: good\nsteps:\n  - id: a\n    log:\n      message: hi\n"), 0o600))
	require.NoError(t, os.WriteFile(bad, []byte("edition: v2026.4\nname: bad\nsteps: [\n"), 0o600))

	res := runFlow(t, "graph", good, bad)

	require.NoError(t, res.Err, "the rest of the graph is true, so the command still succeeds")
	assert.Contains(t, res.Stdout, "good\n  uses  log\n")
	assert.Contains(t, res.Stdout, "partial:")
	assert.Contains(t, res.Stdout, "bad.flow.yaml does not compile and was left out")
}

func TestGraphRefusesAPathThatIsNotThere(t *testing.T) {
	res := runFlow(t, "graph", filepath.Join(t.TempDir(), "missing"))

	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "error reading")
}
