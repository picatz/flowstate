package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
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

func TestGraphRefusesMoreFilesThanItReads(t *testing.T) {
	dir := t.TempDir()
	body := []byte("edition: v2026.4\nname: w\nsteps:\n  - id: a\n    log:\n      message: hi\n")
	for i := range maxGraphFiles + 1 {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("w%04d.flow.yaml", i)), body, 0o600))
	}

	res := runFlow(t, "graph", dir)

	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "name a narrower directory")
	assert.Empty(t, res.Stdout)
}

func liveRun(name string, status v1.RunResponse_Status) *v1.RunSummary {
	return &v1.RunSummary{WorkflowId: name + "-1", Name: name, Status: status}
}

func TestGraphLiveLaysRunsOverTheFiles(t *testing.T) {
	fake := &fakeWorkflowService{listResponses: []*v1.ListResponse{{Runs: []*v1.RunSummary{
		liveRun("call-a-workflow", v1.RunResponse_STATUS_RUNNING),
		liveRun("call-a-workflow", v1.RunResponse_STATUS_FAILED),
		liveRun("billing", v1.RunResponse_STATUS_COMPLETED),
	}}}}
	serveFake(t, fake)

	res := runFlow(t, "graph", "--live", "--filter", `status != "CANCELED"`,
		filepath.Join("..", "..", "examples", "call-a-workflow"))

	require.NoError(t, res.Err, res.Stderr)
	assert.Equal(t, `billing
  runs  1 COMPLETED
call-a-workflow
  runs  1 FAILED, 1 RUNNING
  calls provision-tenant
  uses  log
provision-tenant
  uses  log x2
`, res.Stdout, "a workflow the server runs and no file declares is shown, and the files' edges are untouched")
	assert.Equal(t, `status != "CANCELED"`, fake.lastListFilter, "the filter did not reach the server")
}

func TestGraphLiveWithNoFilesShowsOnlyWhatRuns(t *testing.T) {
	fake := &fakeWorkflowService{listResponses: []*v1.ListResponse{{Runs: []*v1.RunSummary{
		liveRun("billing", v1.RunResponse_STATUS_RUNNING),
	}}}}
	serveFake(t, fake)

	res := runFlow(t, "graph", "--live", "-o", "json")
	require.NoError(t, res.Err, res.Stderr)

	var g v1.Graph
	require.NoError(t, protojson.Unmarshal([]byte(res.Stdout), &g))
	require.NoError(t, v1.Validate(&g))
	require.Len(t, g.GetOverlays(), 1)
	assert.Equal(t, "workflow:billing", g.GetOverlays()[0].GetEntries()[0].GetNode())
}

func TestGraphLiveSaysWhenItStoppedReadingRuns(t *testing.T) {
	fake := &fakeWorkflowService{}
	for i := range maxGraphRunPages {
		fake.listResponses = append(fake.listResponses, &v1.ListResponse{
			Runs:          []*v1.RunSummary{liveRun("billing", v1.RunResponse_STATUS_RUNNING)},
			NextPageToken: fmt.Sprintf("page-%d", i+1),
		})
	}
	serveFake(t, fake)

	res := runFlow(t, "graph", "--live", "-o", "json")
	require.NoError(t, res.Err, res.Stderr)

	var g v1.Graph
	require.NoError(t, protojson.Unmarshal([]byte(res.Stdout), &g))
	assert.True(t, g.GetPartial())
	assert.Contains(t, g.GetNotes()[0], "the first 10 runs were counted; more exist")
	assert.Equal(t, maxGraphRunPages, fake.listCalls, "the read must stop at its bound")
}

func TestGraphLiveRefusalsAreMadeBeforeAnyRequest(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)

	for name, args := range map[string][]string{
		"no paths and no --live":     {"graph"},
		"--filter without --live":    {"graph", "--filter", `status == "FAILED"`, "."},
		"a filter that cannot parse": {"graph", "--live", "--filter", "stauts =="},
	} {
		res := runFlow(t, args...)
		require.Error(t, res.Err, name)
	}
	assert.Zero(t, fake.listCalls, "a refusal that needed the server was not made up front")
}

func TestGraphLiveReportsAServerThatRefuses(t *testing.T) {
	serveFake(t, &fakeWorkflowService{listErr: connect.NewError(connect.CodePermissionDenied, errors.New("no"))})

	res := runFlow(t, "graph", "--live")

	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "refused while listing runs")
}

func TestGraphLiveSaysWhenTheFilterCouldNotBeEvaluatedForSomeRuns(t *testing.T) {
	serveFake(t, &fakeWorkflowService{listResponses: []*v1.ListResponse{
		{Runs: []*v1.RunSummary{liveRun("billing", v1.RunResponse_STATUS_RUNNING)}, ExcludedByError: 2, NextPageToken: "p2"},
		{ExcludedByError: 3},
	}})

	res := runFlow(t, "graph", "--live", "--filter", `labels["team"] == "x"`, "-o", "json")
	require.NoError(t, res.Err, res.Stderr)

	var g v1.Graph
	require.NoError(t, protojson.Unmarshal([]byte(res.Stdout), &g))
	assert.True(t, g.GetPartial(), "an undercount must not read as complete")
	assert.Contains(t, g.GetNotes()[0], "5 runs were left out because --filter could not be evaluated")
}

func TestGraphWorkflowZoomsToOneWorkflowsSteps(t *testing.T) {
	dir := filepath.Join("..", "..", "examples", "approval-gate")

	res := runFlow(t, "graph", dir, "--workflow", "approval-gate")
	require.NoError(t, res.Err, res.Stderr)
	assert.Equal(t, `approval-gate
  steps
    request  task "log"
    settle  wait
    approval  wait_for_signal "deploy-approved"
    decision  switch
      deploy  task "log"  @ decision?0/deploy
      rejected  task "log"  @ decision?1/rejected
      undecided  task "log"  @ decision?2/undecided
`, res.Stdout)

	asJSON := runFlow(t, "graph", dir, "--workflow", "approval-gate", "-o", "json")
	require.NoError(t, asJSON.Err, asJSON.Stderr)
	var g v1.Graph
	require.NoError(t, protojson.Unmarshal([]byte(asJSON.Stdout), &g))
	require.NoError(t, v1.Validate(&g))
	var addresses []string
	for _, n := range g.GetNodes() {
		if n.GetKind() == v1.GraphNodeKind_GRAPH_NODE_KIND_STEP {
			addresses = append(addresses, n.GetAddress())
		}
	}
	assert.Contains(t, addresses, "decision?1/rejected", "a step carries the address the debugger writes")
}

func TestGraphWorkflowNamesWhatTheFilesDeclareWhenTheNameIsWrong(t *testing.T) {
	res := runFlow(t, "graph", filepath.Join("..", "..", "examples", "approval-gate"), "--workflow", "nope")

	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), `no workflow named "nope"; the files declare: approval-gate`)
}

func TestGraphWorkflowIsRefusedWithLiveBeforeAnyRequest(t *testing.T) {
	res := runFlow(t, "graph", "--live", "--workflow", "x")

	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "--workflow shows the steps a file declares")
}

func TestGraphWorkflowRefusesANameTwoFilesDeclareDifferently(t *testing.T) {
	dir := t.TempDir()
	for name, message := range map[string]string{"a.flow.yaml": "hi", "b.flow.yaml": "there"} {
		body := "edition: v2026.4\nname: dup\nsteps:\n  - id: a\n    log:\n      message: " + message + "\n"
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
	}

	res := runFlow(t, "graph", dir, "--workflow", "dup")

	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), `2 files declare a workflow named "dup" with different definitions`)
}

func TestGraphWorkflowSaysWhichFilesDidNotCompileWhenTheNameIsNotFound(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "good.flow.yaml"), []byte("edition: v2026.4\nname: good\nsteps:\n  - id: a\n    log:\n      message: hi\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.flow.yaml"), []byte("name: wanted\nsteps: [\n"), 0o600))

	res := runFlow(t, "graph", dir, "--workflow", "wanted")

	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "the files declare: good")
	assert.Contains(t, res.Err.Error(), "does not compile and was left out", "a broken file is not mistaken for a misspelling")
}
