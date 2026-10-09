package exploretui

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What is under a "runs" row is read from the server when the row is opened, so
// it is not in the graph and not in the [Index]: a "runs" row stands for the
// workflow it is under, and a run row for one run of it. Both are named in the
// same namespace as graph nodes, which cannot collide with them (`workflow:`,
// `task:` and `signal:`).
const (
	runsPrefix     = "runs:"
	runPrefix      = "run:"
	noRunsPrefix   = "none:"
	moreRunsPrefix = "more:"
)

// MaxRunsShown bounds the runs one "runs" row holds. More are said to exist, and
// `flow list` reads them all.
const MaxRunsShown = 50

// maxLabelsShown bounds the labels a run's details list.
const maxLabelsShown = 8

// isRunsRow reports whether a node id stands for a workflow's runs.
func isRunsRow(node string) bool { return strings.HasPrefix(node, runsPrefix) }

// RunsOf is the workflow a "runs" row stands for: the name to ask the server
// about.
func (x *Index) RunsOf(rowID string) (string, bool) {
	id, ok := nodeOf(rowID)
	if !ok || !isRunsRow(id) {
		return "", false
	}

	return x.label(strings.TrimPrefix(id, runsPrefix)), true
}

// RunRows are the rows for a page of runs under the "runs" row parent, and the
// summary each stands for, by row id. more says the server has runs this page
// leaves out, which a row at the end says too.
func RunRows(parent string, runs []*v1.RunSummary, more bool) ([]pane.Node, map[string]*v1.RunSummary) {
	runs = runs[:min(len(runs), MaxRunsShown)]
	rows := make([]pane.Node, 0, len(runs)+1)
	byRow := make(map[string]*v1.RunSummary, len(runs))
	for _, r := range runs {
		id := treeID(parent, runPrefix+r.GetWorkflowId()+"@"+r.GetRunId())
		if _, dup := byRow[id]; dup {
			continue
		}
		byRow[id] = r
		rows = append(rows, pane.Node{ID: id, Label: r.GetWorkflowId(), Value: runValue(r)})
	}
	switch {
	case len(rows) == 0:
		rows = append(rows, pane.Node{ID: treeID(parent, noRunsPrefix), Label: "no runs", Value: "none match"})
	case more:
		rows = append(rows, pane.Node{ID: treeID(parent, moreRunsPrefix), Label: "more runs exist", Value: "flow list --filter 'name == …' reads them all"})
	}

	return rows, byRow
}

// runValue is a run's row: its status and when it began.
func runValue(r *v1.RunSummary) string {
	parts := []string{v1.StatusName(r.GetStatus())}
	if r.GetStartTime().IsValid() {
		parts = append(parts, "started "+stamp(r.GetStartTime().AsTime()))
	}

	return strings.Join(parts, "  ")
}

// stamp writes a time the one way the screen does, in UTC and to the minute.
func stamp(t time.Time) string { return t.UTC().Format("2006-01-02 15:04Z") }

// RunDetails describes one run.
func RunDetails(r *v1.RunSummary) pane.Inspector {
	fields := []pane.Field{
		{Key: "kind", Value: "run"},
		{Key: "workflow_id", Value: r.GetWorkflowId()},
		{Key: "run_id", Value: r.GetRunId()},
		{Key: "status", Value: v1.StatusName(r.GetStatus())},
	}
	if r.GetName() != "" {
		fields = append([]pane.Field{fields[0], {Key: "workflow", Value: r.GetName()}}, fields[1:]...)
	}
	if r.GetStartTime().IsValid() {
		fields = append(fields, pane.Field{Key: "started", Value: stamp(r.GetStartTime().AsTime())})
	}
	if r.GetCloseTime().IsValid() {
		fields = append(fields, pane.Field{Key: "closed", Value: stamp(r.GetCloseTime().AsTime())})
	}
	if r.GetStarter() != "" {
		fields = append(fields, pane.Field{Key: "started by", Value: r.GetStarter()})
	}
	if labels := r.GetLabels(); len(labels) > 0 {
		keys := slices.Sorted(maps.Keys(labels))
		var shown []string
		for _, k := range keys[:min(len(keys), maxLabelsShown)] {
			shown = append(shown, k+"="+labels[k])
		}
		value := strings.Join(shown, ", ")
		if extra := len(keys) - maxLabelsShown; extra > 0 {
			value += fmt.Sprintf(", and %d more", extra)
		}
		fields = append(fields, pane.Field{Key: "labels", Value: value})
	}

	return pane.Inspector{
		Fields: fields,
		Note:   "flow get " + r.GetWorkflowId() + " --run-id " + r.GetRunId(),
	}
}
