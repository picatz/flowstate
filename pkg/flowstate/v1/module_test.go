package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func moduleSpec() *v1.Workflow {
	return &v1.Workflow{
		Name:           "ids",
		Profile:        v1.CurrentProfile,
		DeclaredErrors: []*v1.ErrorDeclaration{{Name: "NotFound"}},
	}
}

// TestAModuleIsDerivedFromTheSpec states what makes a module and what does not.
// There is no marker: the property is read off a workflow, so a hand-built spec
// cannot claim it, and anything that runs or takes input disqualifies it.
func TestAModuleIsDerivedFromTheSpec(t *testing.T) {
	t.Parallel()

	assert.True(t, v1.IsModule(moduleSpec()))

	for name, mutate := range map[string]func(*v1.Workflow){
		"a step":         func(w *v1.Workflow) { w.Steps = []*v1.Node{{Id: "a"}} },
		"an input":       func(w *v1.Workflow) { w.DeclaredInputs = []*v1.InputDeclaration{{Name: "a"}} },
		"an output":      func(w *v1.Workflow) { w.DeclaredOutputs = []*v1.OutputDeclaration{{Name: "a"}} },
		"vars":           func(w *v1.Workflow) { w.Vars = map[string]*v1.Value{"a": {}} },
		"labels":         func(w *v1.Workflow) { w.Labels = map[string]string{"a": "b"} },
		"triggers":       func(w *v1.Workflow) { w.Triggers = &v1.Triggers{} },
		"no declaration": func(w *v1.Workflow) { w.DeclaredErrors = nil },
	} {
		w := moduleSpec()
		mutate(w)
		assert.False(t, v1.IsModule(w), name)
	}
	assert.False(t, v1.IsModule(nil))
}

// TestEveryEntryRefusesASpecWithNoSteps is the fail-closed half, on a spec built
// by hand rather than compiled from a file: the schema and the local driver both
// turn a module away, and say it is a module.
func TestEveryEntryRefusesASpecWithNoSteps(t *testing.T) {
	t.Parallel()

	spec := moduleSpec()

	require.Error(t, v1.Validate(spec), "the schema requires a step")

	_, err := v1.RunWithInputs(t.Context(), spec, nil)
	require.ErrorIs(t, err, v1.ErrModule)
	assert.Contains(t, err.Error(), `workflow "ids" is a module (no steps)`)

	_, err = v1.RunWithInputs(t.Context(), &v1.Workflow{Name: "empty"}, nil)
	require.Error(t, err)
	assert.NotErrorIs(t, err, v1.ErrModule, "an empty workflow is not called a module")
}
