package flowtest

import (
	"errors"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestSkipClaimsOverParallelBranchesAreRefusedOnFailedRuns pins the fail-closed
// answer to an unknowable claim: when a run fails, a parallel branch step's
// outputs are not merged to the top level, so its absence cannot be told apart
// from the block not finishing. A `skipped:` naming such a step, or an
// `others: skipped` closing over one, must be refused with a diagnostic that
// says why, never accepted on the strength of absence alone. Internal because
// it exercises assertExpectation directly: the refusal is a property of the
// judging step, not of any one driver's run path.
func TestSkipClaimsOverParallelBranchesAreRefusedOnFailedRuns(t *testing.T) {
	t.Parallel()

	expectFailed := true
	spec := &v1.Workflow{
		Name: "p",
		Steps: []*v1.Node{
			{Id: "before", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}},
			{Id: "block", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
				{Steps: []*v1.Node{{Id: "inside", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}}},
			}}}},
		},
	}
	outputs := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"before": {},
	}}
	runErr := errors.New("the block did not finish")

	failures := assertExpectation(&Expectation{Failed: &expectFailed, Skipped: []string{"inside"}}, spec, outputs, runErr)
	if len(failures) != 1 || !strings.Contains(failures[0].GetMessage(), "cannot be told apart") {
		t.Fatalf("skipped: naming a parallel branch step on a failed run was not refused: %+v", failures)
	}

	failures = assertExpectation(&Expectation{Failed: &expectFailed, Ran: []string{"before"}, Others: OthersSkipped}, spec, outputs, runErr)
	if len(failures) != 1 || !strings.Contains(failures[0].GetMessage(), "cannot verify it was skipped") {
		t.Fatalf("others: skipped closing over a parallel branch step on a failed run was not refused: %+v", failures)
	}
}
