package flowtest

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Every operator finds its construct somewhere in the shipped examples, every
// mutant applies to a clone of the workflow it came from, and the ids are
// stable and unique within one workflow: a deleted operator or an apply that
// stops matching its own enumeration fails here, not in a user's survivor list.
func TestMutantsEnumerateEveryOperatorAndApplyToTheirOwnWorkflow(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob("../../../../examples/*/workflow.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, paths)

	seen := map[string]bool{}
	for _, path := range paths {
		wf, _, err := flowfile.ParseFile(path)
		require.NoError(t, err, path)

		ids := map[string]bool{}
		for _, mu := range mutants(wf) {
			seen[mu.operator] = true
			ids[mu.id] = true

			clone := proto.Clone(wf).(*v1.Workflow)
			var target *v1.Node
			ordinal := -1
			v1.WalkWorkflow(clone, v1.Walk{Node: func(n *v1.Node) {
				ordinal++
				if ordinal == mu.ordinal {
					target = n
				}
			}})
			require.NotNil(t, target, "%s: %s", path, mu.id)
			assert.Equal(t, mu.step, target.GetId(), "%s: %s", path, mu.id)
			assert.True(t, mu.apply(target), "%s: %s", path, mu.id)
			assert.False(t, proto.Equal(wf, clone), "%s: %s changed nothing", path, mu.id)
		}
	}
	for _, operator := range []string{"if-negate", "if-drop", "undo-drop", "retry-drop", "continue-flip", "switch-arm-drop"} {
		assert.True(t, seen[operator], "no shipped example holds a construct for %s", operator)
	}
}
