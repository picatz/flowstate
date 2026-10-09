package exploretui

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// runner is a Runs whose answer a test changes.
type runner struct {
	mu    sync.Mutex
	runs  []*v1.RunSummary
	more  bool
	err   error
	asked []string
}

func (r *runner) read(_ context.Context, workflow string) ([]*v1.RunSummary, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.asked = append(r.asked, workflow)

	return r.runs, r.more, r.err
}

func runSummary(id, run string, status v1.RunResponse_Status) *v1.RunSummary {
	return &v1.RunSummary{
		WorkflowId: id, RunId: run, Status: status, Name: "checkout",
		StartTime: timestamppb.New(time.Date(2026, 10, 8, 14, 3, 59, 0, time.UTC)),
	}
}

func withRuns(r *runner) func(*Config) { return func(c *Config) { c.Runs = r.read } }

// checkout is the third root; its first child is the "runs" row.
func openRuns(t *testing.T, m Model) Model {
	t.Helper()

	return press(m, "j", "j", "enter", "j", "enter")
}

func TestARunsRowListsAWorkflowsRecentRunsAndDescribesEach(t *testing.T) {
	r := &runner{runs: []*v1.RunSummary{
		runSummary("orders-2", "r2", v1.RunResponse_STATUS_FAILED),
		runSummary("orders-1", "r1", v1.RunResponse_STATUS_COMPLETED),
	}}
	r.runs[0].Starter = "https://idp.example#kent"
	r.runs[0].Labels = map[string]string{"team": "payments", "env": "prod"}
	m, _ := started(t, fleet(), withRuns(r))

	m = openRuns(t, m)
	assert.Equal(t, []string{"checkout"}, r.asked, "the server is asked about the workflow by its declared name")
	assert.Equal(t, []string{
		"audit|1 COMPLETED", "charge|", "checkout|1 FAILED, 2 RUNNING",
		"runs|recent", "orders-2|FAILED  started 2026-10-08 14:03Z", "orders-1|COMPLETED  started 2026-10-08 14:03Z",
		"charge|calls", "approved|waits for", "http|uses x2",
	}, rowsOf(m))

	m = press(m, "j")
	out := view(m)
	for _, want := range []string{"run", "orders-2", "r2", "FAILED", "2026-10-08 14:03Z", "https://idp.example#kent", "env=prod, team=payments", "flow get orders-2"} {
		assert.Contains(t, out, want)
	}
}

func TestWithoutARunReaderThereAreNoRunsRows(t *testing.T) {
	m, _ := started(t, fleet())
	m = press(m, "j", "j", "enter")

	for _, row := range rowsOf(m) {
		assert.False(t, strings.HasPrefix(row, "runs|"), row)
	}
}

func TestARunsRowSaysWhenThereAreNoRunsAndWhenThereAreMore(t *testing.T) {
	r := &runner{}
	m, _ := started(t, fleet(), withRuns(r))
	m = openRuns(t, m)
	assert.Contains(t, rowsOf(m), "no runs|none match")

	r = &runner{runs: []*v1.RunSummary{runSummary("a", "1", v1.RunResponse_STATUS_RUNNING)}, more: true}
	m, _ = started(t, fleet(), withRuns(r))
	m = openRuns(t, m)
	assert.Contains(t, rowsOf(m), "more runs exist|flow list --filter 'name == …' reads them all")
}

func TestAFailedRunsReadIsSaidAndCanBeAskedAgain(t *testing.T) {
	r := &runner{err: errors.New("the server refused")}
	m, _ := started(t, fleet(), withRuns(r))

	m = openRuns(t, m)
	assert.Contains(t, view(m), "cannot read the runs: the server refused")
	assert.False(t, m.Screen().Tree.Open(m.Screen().Tree.Selected()), "the row is closed again, so opening it asks again")

	r.mu.Lock()
	r.err, r.runs = nil, []*v1.RunSummary{runSummary("a", "1", v1.RunResponse_STATUS_RUNNING)}
	r.mu.Unlock()
	m = press(m, "enter")
	assert.Contains(t, view(m), "RUNNING")
	assert.Len(t, r.asked, 2)
}

func TestARefreshReadsTheOpenRunsAgain(t *testing.T) {
	r := &runner{runs: []*v1.RunSummary{runSummary("a", "1", v1.RunResponse_STATUS_RUNNING)}}
	m, l := started(t, fleet(), withRuns(r))
	m = openRuns(t, m)
	require.Len(t, r.asked, 1)

	r.mu.Lock()
	r.runs = []*v1.RunSummary{runSummary("a", "1", v1.RunResponse_STATUS_COMPLETED), runSummary("b", "2", v1.RunResponse_STATUS_RUNNING)}
	r.mu.Unlock()
	l.set(fleet(), nil)
	m = press(m, "r")

	assert.Len(t, r.asked, 2, "a refresh of a screen with runs open reads them again")
	assert.Contains(t, rowsOf(m), "a|COMPLETED  started 2026-10-08 14:03Z")
	assert.Contains(t, rowsOf(m), "b|RUNNING  started 2026-10-08 14:03Z")
}

func TestRunRowsAreBoundedAndUnique(t *testing.T) {
	var runs []*v1.RunSummary
	for range MaxRunsShown + 10 {
		runs = append(runs, runSummary("same", "run", v1.RunResponse_STATUS_RUNNING))
	}
	rows, byRow := RunRows("p", runs, false)
	assert.Len(t, rows, 1, "two rows with one id would not both be reachable")
	assert.Len(t, byRow, 1)

	for i := range runs {
		runs[i] = runSummary("w", string(rune('a'+i%26))+string(rune('a'+i/26)), v1.RunResponse_STATUS_RUNNING)
	}
	rows, _ = RunRows("p", runs, false)
	assert.Len(t, rows, MaxRunsShown)
}

func TestARunWithControlCharactersIsNeverWrittenRaw(t *testing.T) {
	bad := runSummary("w\x1b[2J", "r\x07", v1.RunResponse_STATUS_RUNNING)
	bad.Starter = "evil\x1b]0;x\x07"
	bad.Labels = map[string]string{"k\x1b": "v\x1b[31m"}
	m, _ := started(t, fleet(), withRuns(&runner{runs: []*v1.RunSummary{bad}}))

	m = press(openRuns(t, m), "j")
	out := view(m)
	for _, raw := range []string{"\x1b[2J", "\x07", "\x1b]0;", "\x1b[31m"} {
		assert.NotContains(t, out, raw)
	}
}

func TestOnlyAWorkflowHasARunsRow(t *testing.T) {
	x := NewIndex(fleet()).WithRunRows()
	roots := x.Roots()
	children, _, err := x.Loader()(pane.Request{Parent: roots[2].ID})
	require.NoError(t, err)
	assert.Equal(t, "runs", children[0].Label)
	assert.Equal(t, 4, roots[2].Total, "three edges and the runs row")

	// A task or signal reached by an edge is not a workflow.
	for _, c := range children[1:] {
		if c.Label == "http" || c.Label == "approved" {
			assert.Zero(t, c.Total)
		}
	}
	_, _, err = x.Loader()(pane.Request{Parent: children[0].ID})
	require.Error(t, err, "the children of a runs row are not the index's to give")
}
