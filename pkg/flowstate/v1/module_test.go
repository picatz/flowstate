package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
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

// TestNoStepsCasesLocally runs the shared no-steps cases through the local
// driver; engine/module_test.go runs the same cases durably. The schema refuses
// the same specs independently.
func TestNoStepsCasesLocally(t *testing.T) {
	t.Parallel()

	require.Error(t, v1.Validate(moduleSpec()), "the schema requires a step")

	conformance.AssertNoStepsCases(t, func(w *v1.Workflow) error {
		_, err := v1.RunWithInputs(t.Context(), w, nil)
		return err
	})
}
