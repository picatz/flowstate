package tests

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// RunAddressWorkflow returns a workflow that reports the run's own address —
// `run.workflow_id` and `run.run_id` — through its declared outputs, and reads
// it in two more positions where a scope is rebuilt rather than used as it
// stands.
//
// One workflow for both drivers, for the reason every shared [Case] is: a value
// with one meaning has to be checked once, from one definition, or the two
// drivers can silently disagree about it the way CLAUDE.md's retry-attempts
// story describes.
//
// The two extra positions are not decoration. `run.workflow_id` in a step's
// ordinary input proves the address survives the scope an executor hands a task,
// and `run.run_id` inside a `for_each` body proves it survives
// [v1.Scope.WithLocal] and [v1.Scope.WithOutputs] — the copy helpers where
// `ambient_vars` was added once and silently dropped, so that a loop body's task
// stopped finding its iterator five retries deep. An unresolvable reference
// fails the run on both drivers, so a dropped field here is a failure rather
// than a quietly empty output.
func RunAddressWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name: "run-address-shape",
		Steps: []*v1.Node{
			{
				Id: "report",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "log",
					Inputs: map[string]*v1.Value{
						"message": v1.NewExpr(`"reporting at " + run.workflow_id`),
					},
				}},
			},
			{
				Id: "each",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items:    v1.NewExpr(`["one"]`),
					Iterator: "which",
					Body: []*v1.Node{
						{
							Id: "note",
							Kind: &v1.Node_Task{Task: &v1.Task{
								Name: "log",
								Inputs: map[string]*v1.Value{
									"message": v1.NewExpr(`which + " of run " + run.run_id`),
								},
							}},
						},
					},
				}},
			},
		},
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "workflow_id", Value: v1.NewExpr("run.workflow_id")},
			{Name: "run_id", Value: v1.NewExpr("run.run_id")},
		},
	}
}

// AssertRunAddressShape checks that a run's declared outputs report the address
// the driver was asked to report, and that neither half is ever blank.
//
// The blank check is the part worth writing down. An empty `run.workflow_id`
// does not read as "this driver has no addressing"; it reads as a field that
// failed to populate, and it sends an author looking for the bug in their own
// file. So every driver owes an answer that is *true*: the durable driver's is
// Temporal's workflow id and the first run id of the continued-execution chain,
// and the local driver's is [v1.LocalRunAddress] — a sentinel that says outright
// this run is not reachable by any name, which is the same honesty `run.local`
// already gives for the identity beside it.
func AssertRunAddressShape(t testing.TB, outputs *v1.Workflow_StepOutputs, wantWorkflowID, wantRunID string) {
	t.Helper()

	run := outputs.GetRunOutputs()
	if run == nil {
		t.Fatalf("the run produced no declared outputs")
	}

	values := run.GetValues()

	for field, want := range map[string]string{
		"workflow_id": wantWorkflowID,
		"run_id":      wantRunID,
	} {
		value, ok := values[field]
		if !ok {
			t.Fatalf("the run's outputs have no %q field", field)
		}

		got := value.GetLiteral().GetStringValue()
		if got == "" {
			t.Fatalf("run.%s is empty; a driver that cannot address a run owes a sentinel "+
				"that says so, never a blank an author will read as a field that failed "+
				"to populate", field)
		}
		if got != want {
			t.Fatalf("run.%s = %q, want %q", field, got, want)
		}
	}
}
