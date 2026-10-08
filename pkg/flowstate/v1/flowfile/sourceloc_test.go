package flowfile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const sourceLocFlowfile = `edition: v2026.4
name: t
steps:
  - id: only
    log:
      message: hi
  - id: first
    for_each:
      items: ${[1]}
      steps:
        - id: body
          log:
            message: a
  - id: second
    for_each:
      items: ${[1]}
      steps:
        - id: body
          log:
            message: b
`

func stepNamed(t *testing.T, nodes []*v1.Node, id string) *v1.Node {
	t.Helper()

	var found *v1.Node
	v1.WalkNodes(nodes, v1.Walk{Node: func(node *v1.Node) {
		if node.GetId() == id && found == nil {
			found = node
		}
	}})
	require.NotNil(t, found, id)

	return found
}

func TestAttachSourcesLocatesOnlyStepsWithAUniqueId(t *testing.T) {
	t.Parallel()

	wf, positions, err := ParseAt([]byte(sourceLocFlowfile), "t.yaml")
	require.NoError(t, err)
	AttachSources(wf, positions, "flows/t.yaml")

	only := stepNamed(t, wf.GetSteps(), "only").GetSource()
	require.NotNil(t, only)
	assert.Equal(t, "flows/t.yaml", only.GetFile())
	assert.EqualValues(t, 4, only.GetLine())
	assert.EqualValues(t, 5, only.GetColumn())
	assert.EqualValues(t, 7, stepNamed(t, wf.GetSteps(), "first").GetSource().GetLine())

	assert.Nil(t, stepNamed(t, wf.GetSteps(), "body").GetSource(),
		"an id two bodies declare is located nowhere rather than at the first")

	AttachSources(nil, positions, "x")
	AttachSources(wf, nil, "x")
}

func TestAttachSourcesKeepsAnAbsolutePathOutOfTheSpecification(t *testing.T) {
	t.Parallel()

	wf, positions, err := ParseAt([]byte(sourceLocFlowfile), "/home/someone/t.yaml")
	require.NoError(t, err)
	AttachSources(wf, positions, "/home/someone/t.yaml")

	assert.Equal(t, "t.yaml", stepNamed(t, wf.GetSteps(), "only").GetSource().GetFile())
}

// TestSourceLocationsAreNotPartOfTheProgram pins that carrying locations changes
// neither digest: a source map bound to the program a run executes must still
// verify against it, and a re-indented file is still the same program.
func TestSourceLocationsAreNotPartOfTheProgram(t *testing.T) {
	t.Parallel()

	bare, positions, err := ParseAt([]byte(sourceLocFlowfile), "t.yaml")
	require.NoError(t, err)
	located, _, err := ParseAt([]byte(sourceLocFlowfile), "t.yaml")
	require.NoError(t, err)
	AttachSources(located, positions, "t.yaml")
	require.NotNil(t, stepNamed(t, located.GetSteps(), "only").GetSource())

	assert.Equal(t, v1.CanonicalDigest(bare), v1.CanonicalDigest(located))
	assert.Equal(t, v1.WorkflowIRDigest(bare), v1.WorkflowIRDigest(located))
	assert.NotEmpty(t, v1.WorkflowIRDigest(located))
}
