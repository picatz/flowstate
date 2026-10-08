package flowstatev1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func httpDef(t *testing.T) TaskDef {
	t.Helper()
	def, ok := DefaultRegistry().Lookup("http")
	require.True(t, ok)

	return def
}

// TestAResultTheDeclaredSchemaRefusesIsNotAnOutput is #2507: a status code no
// HTTP status can be, from a peer that answered, fails the step with a permanent
// kind rather than becoming an output.
func TestAResultTheDeclaredSchemaRefusesIsNotAnOutput(t *testing.T) {
	t.Parallel()

	out := &Node_Outputs{NamedValues: map[string]*Value{"status_code": NewValue(999)}}
	err := checkDeclaredOutputs(&Task{Name: "http"}, httpDef(t), out)

	require.Error(t, err)
	require.Contains(t, err.Error(), "status_code")
	require.Equal(t, ErrorKindUpstreamUnknown, ClassifyError(err))
}

// TestAResultTheSchemaAcceptsPasses is the negative direction: an in-bounds
// status, a name the schema does not declare, and a non-literal value are all
// left alone.
func TestAResultTheSchemaAcceptsPasses(t *testing.T) {
	t.Parallel()

	out := &Node_Outputs{NamedValues: map[string]*Value{
		"status_code": NewValue(204),
		"shaped_name": NewValue("anything"),
	}}
	require.NoError(t, checkDeclaredOutputs(&Task{Name: "http"}, httpDef(t), out))
	require.NoError(t, checkDeclaredOutputs(&Task{Name: "http"}, httpDef(t), nil))
}

// TestAStepThatShapesItsOutputsIsNotHeldToTheDeclaredNames keeps the boundary:
// the shaped result replaces the declared names, so the declared schema is not
// the measure.
func TestAStepThatShapesItsOutputsIsNotHeldToTheDeclaredNames(t *testing.T) {
	t.Parallel()

	out := &Node_Outputs{NamedValues: map[string]*Value{"status_code": NewValue(999)}}
	shaped := &Task{Name: "http", Inputs: map[string]*Value{ShapingInput: NewValue("shape")}}

	require.NoError(t, checkDeclaredOutputs(shaped, httpDef(t), out))
	require.Error(t, checkDeclaredOutputs(&Task{Name: "http"}, httpDef(t), out), "the same result without shaping is refused")
}
