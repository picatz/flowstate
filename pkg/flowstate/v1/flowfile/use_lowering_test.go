package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestAModuleLowersToWhatBothDriversRun compiles the file the shared conformance
// case describes and asserts the compiler emits exactly that shape, so the case is
// held to what a Flowfile produces and not to what was typed into the corpus.
func TestAModuleLowersToWhatBothDriversRun(t *testing.T) {
	t.Parallel()

	module := `edition: ` + flowfile.CurrentEdition + `
name: ids
functions:
  isCode:
    params:
      s: string
    returns: bool
    body: ${s.matches("^[a-z]{3}-[0-9]{2}$")}
types:
  Code:
    type: string
    must: isCode(this)
  Tag:
    fields:
      code:
        type: Code
        required: true
`
	src := workflowUsing(useIds, `inputs:
  id:
    type: ids.Code
    required: true
  tag:
    type: ids.Tag
`)
	dir := tree(t, map[string]string{"w.yaml": src, "lib/ids.yaml": module})
	wf, err := compileAt(t, dir, "w.yaml")
	require.NoError(t, err)

	id := wf.GetDeclaredInputs()[0]
	assert.Equal(t, conformance.FunctionMustExpansion, id.GetMust())
	assert.Equal(t, "ids.Code", id.GetTypeSource())
	assert.Equal(t, "ids.Tag", wf.GetDeclaredInputs()[1].GetValueType().GetMessage())
	assert.Equal(t, []string{"ids.Code", "ids.Tag"}, declaredNames(wf.GetDeclaredTypes()))
	assert.Equal(t, conformance.FunctionMustExpansion, wf.GetDeclaredTypes()[0].GetMust())
	assert.Equal(t, conformance.FunctionMustExpansion, wf.GetDeclaredTypes()[1].GetFields()[0].GetMust())
}
