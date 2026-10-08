package flowtest

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// shippedExamples parses the shipped examples once per process: the parse is
// the test's whole cost, and `-count=N` under `-race` (the ordering leg) would
// otherwise pay it N times for the same files.
var shippedExamples = sync.OnceValues(func() (map[string]*v1.Workflow, error) {
	paths, err := filepath.Glob("../../../../examples/*/workflow.yaml")
	if err != nil {
		return nil, err
	}
	out := make(map[string]*v1.Workflow, len(paths))
	for _, path := range paths {
		wf, _, err := flowfile.ParseFile(path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		out[path] = wf
	}

	return out, nil
})

// Every operator finds its construct somewhere in the shipped examples, every
// mutant applies to a clone of the workflow it came from, and the ids are
// stable and unique within one workflow: a deleted operator or an apply that
// stops matching its own enumeration fails here, not in a user's survivor list.
func TestMutantsEnumerateEveryOperatorAndApplyToTheirOwnWorkflow(t *testing.T) {
	t.Parallel()
	firstIteration(t)

	examples, err := shippedExamples()
	require.NoError(t, err)
	require.NotEmpty(t, examples)

	seen := map[string]bool{}
	for path, wf := range examples {

		ids := map[string]bool{}
		for _, mu := range mutants(wf) {
			seen[mu.operator] = true
			assert.False(t, ids[mu.id], "%s: %s repeats", path, mu.id)
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
	for _, operator := range []string{"if-negate", "if-drop", "undo-drop", "retry-drop", "continue-flip", "switch-arm-drop", "switch-default-drop"} {
		assert.True(t, seen[operator], "no shipped example holds a construct for %s", operator)
	}
}

// Two loop bodies may each declare a step with one id; a mutant id names
// exactly one of them, and its position is withheld rather than guessed.
func TestMutantIDsAreUniqueWhenAStepIDRepeats(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(`
edition: v2026.4
name: twins
steps:
  - id: first
    for_each:
      items: ${["a"]}
      steps:
        - id: inner
          if: ${item == "a"}
          log:
            message: one
  - id: second
    for_each:
      items: ${["b"]}
      steps:
        - id: inner
          if: ${item == "b"}
          log:
            message: two
outputs: {}
`))
	require.NoError(t, err)

	var ids []string
	for _, mu := range mutants(wf) {
		if mu.step == "inner" {
			ids = append(ids, mu.id)
			assert.True(t, mu.ambiguous, mu.id)
		}
	}
	assert.Equal(t, []string{
		"if-negate@inner.if", "if-drop@inner.if",
		"if-negate@inner#2.if", "if-drop@inner#2.if",
	}, ids)
}

// firstIteration is the internal-package twin of the external tests' helper of
// the same purpose: a deterministic mutation test runs once per process.
func firstIteration(t *testing.T) {
	t.Helper()
	if _, again := internalMutateRan.LoadOrStore(t.Name(), true); again {
		t.Skip("deterministic; the first iteration in this process proved it")
	}
}

var internalMutateRan sync.Map
