package flowtest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestStubbedCallPathsReadsTheRewriteNotTheTaskName: a plugin task may be named
// `call.<x>`, so only the node that was a `call:` and became a task is stubbed.
func TestStubbedCallPathsReadsTheRewriteNotTheTaskName(t *testing.T) {
	t.Parallel()

	callee := &v1.Workflow{Name: "provision"}
	original := &v1.Workflow{Steps: []*v1.Node{
		{Id: "real", Kind: &v1.Node_Task{Task: &v1.Task{Name: callStubTaskPrefix + "provision"}}},
		{Id: "provision", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}},
		{Id: "fan", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
			{Steps: []*v1.Node{{Id: "inner", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}}}},
		}}}},
	}}
	rewritten := proto.Clone(original).(*v1.Workflow)
	rewritten.Steps[1].Kind = &v1.Node_Task{Task: &v1.Task{Name: callStubTaskPrefix + "provision"}}

	require.Equal(t, map[string]bool{childPath("", 1): true}, stubbedCallPaths(original, rewritten),
		"the real call.* task and the call nobody stubbed are not stubbed calls")
	require.Nil(t, stubbedCallPaths(original, original), "no rewrite, nothing stubbed")

	rewritten.Steps[2].GetParallel().Branches[0].Steps[0].Kind = &v1.Node_Task{Task: &v1.Task{Name: callStubTaskPrefix + "provision"}}
	require.Equal(t, map[string]bool{
		childPath("", 1): true,
		childPath("", 2) + "/" + pathBranch + "0" + "/0": true,
	}, stubbedCallPaths(original, rewritten), "a call inside a parallel branch is found under its branch path")
}
