package server

import (
	"strconv"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
)

func TestValidateSpecificationRefusesOversizedParallelAtomicBlock(t *testing.T) {
	branches := make([]*v1.Parallel_Branch, 51)
	for branch := range branches {
		steps := make([]*v1.Node, 100)
		for step := range steps {
			steps[step] = &v1.Node{
				Id:   "b" + strconv.Itoa(branch) + "-task-" + strconv.Itoa(step),
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
			}
		}
		branches[branch] = &v1.Parallel_Branch{Steps: steps}
	}
	wf := &v1.Workflow{Name: "oversized-parallel", Steps: []*v1.Node{{
		Id:   "block",
		Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: branches}},
	}}}

	err := (&FlowstateServer{}).validateSpecification(wf)
	require.ErrorContains(t, err,
		`step "block": `+v1.AtomicBlockBodyActivitiesError(v1.MaxAtomicBlockActivities).Error())
}
