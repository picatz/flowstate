package tests

import (
	"fmt"
	"sort"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

type TraceOperation struct {
	Name    string
	StepID  string
	Attempt int64
}

// TraceWorkflow is the smallest execution containing a task attempt and a wait.
// Both drivers run this exact message when asserting their domain trace shape.
func TraceWorkflow() *v1.Workflow {
	return &v1.Workflow{Name: "trace-agreement", Steps: []*v1.Node{
		{Id: "say", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
			"message": v1.NewLiteral("trace agreement"),
		}}}},
		{Id: "pause", Kind: &v1.Node_Wait{Wait: &v1.Wait{
			Kind: &v1.Wait_Duration{Duration: durationpb.New(0)},
		}}},
	}}
}

// TraceCompensationWorkflow succeeds once, registers an undo, then fails while
// resolving the next task so both drivers unwind the same entry.
func TraceCompensationWorkflow() *v1.Workflow {
	logTask := func(message string) *v1.Task {
		return &v1.Task{Name: "log", Inputs: map[string]*v1.Value{"message": v1.NewLiteral(message)}}
	}
	return &v1.Workflow{Name: "trace-compensation", Steps: []*v1.Node{
		{Id: "prepare", Kind: &v1.Node_Task{Task: logTask("prepared")},
			Undo: &v1.Compensation{Task: logTask("undone")}},
		{Id: "fail", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}},
	}}
}

func AssertCompensationTrace(t testing.TB, operations []TraceOperation) {
	t.Helper()
	var found []string
	for _, operation := range operations {
		if operation.Name == "flowstate.attempt" {
			found = append(found, operation.Name+":"+operation.StepID+":"+fmt.Sprint(operation.Attempt))
		} else if operation.Name == "flowstate.compensation" {
			found = append(found, operation.Name+":"+operation.StepID+":"+fmt.Sprint(operation.Attempt))
		}
	}
	sort.Strings(found)
	require.Equal(t, []string{
		"flowstate.attempt:fail:1",
		"flowstate.attempt:prepare:1",
		"flowstate.attempt:prepare:1",
		"flowstate.compensation:prepare:0",
	}, found)
}

// AssertTraceOperations compares the stable domain surface, deliberately
// ignoring Temporal runtime spans that exist only in the durable driver.
func AssertTraceOperations(t testing.TB, operations []TraceOperation) {
	t.Helper()
	var flowstate []string
	for _, operation := range operations {
		switch operation.Name {
		case "flowstate.attempt":
			flowstate = append(flowstate, operation.Name+":"+operation.StepID+":"+fmt.Sprint(operation.Attempt))
		case "flowstate.step", "flowstate.wait", "flowstate.compensation":
			flowstate = append(flowstate,
				operation.Name+":"+operation.StepID+":"+fmt.Sprint(operation.Attempt))
		}
	}
	sort.Strings(flowstate)
	require.Equal(t, []string{
		"flowstate.attempt:say:1",
		"flowstate.step:pause:0",
		"flowstate.step:say:0",
		"flowstate.wait:pause:0",
	}, flowstate)
}
