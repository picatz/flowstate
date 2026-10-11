package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The *From helpers answer from the registry they are given and from nothing
// else, and a nil registry is the build's own. A name only the given registry
// holds is the case that tells them apart from the global-reading originals.
func TestRegistryFromHelpersReadOnlyTheGivenRegistry(t *testing.T) {
	t.Parallel()

	const name = "acme.registry_from_probe"
	own := v1.NewRegistry()
	require.NoError(t, own.Register(v1.TaskDef{
		Name:               name,
		Fn:                 func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) { return nil, nil },
		ShapesOutputs:      true,
		DeferredInputs:     []string{"outputs"},
		ExpressionInputs:   []string{"expect"},
		NestedSecretInputs: []string{"headers"},
	}))
	inputs := map[string]*v1.Value{"outputs": v1.NewLiteral("x"), "message": v1.NewLiteral("y")}

	def, ok := v1.LookupTaskFrom(own, name)
	require.True(t, ok)
	assert.Equal(t, name, def.Name)
	assert.Contains(t, v1.TaskNamesFrom(own), name)
	assert.True(t, v1.TaskShapesOutputsFrom(own, name))
	assert.True(t, v1.MustBeExpressionFrom(own, name, "expect"))
	assert.True(t, v1.AcceptsNestedSecretFrom(own, name, "headers"))
	assert.Equal(t, []string{"headers"}, v1.NestedSecretInputsFrom(own, name))
	resolve, deferred := v1.ResolvableInputsFrom(own, name, inputs)
	assert.Contains(t, deferred, "outputs")
	assert.Contains(t, resolve, "message")

	// The build's own registry has never heard of it, so the same questions are
	// the closed answers, whether asked through nil or through the originals.
	_, ok = v1.LookupTaskFrom(nil, name)
	assert.False(t, ok)
	assert.NotContains(t, v1.TaskNamesFrom(nil), name)
	assert.False(t, v1.TaskShapesOutputsFrom(nil, name))
	assert.False(t, v1.TaskShapesOutputs(name))
	assert.False(t, v1.MustBeExpressionFrom(nil, name, "expect"))
	assert.False(t, v1.AcceptsNestedSecretFrom(nil, name, "headers"))
	assert.Empty(t, v1.NestedSecretInputsFrom(nil, name))
	resolve, deferred = v1.ResolvableInputsFrom(nil, name, inputs)
	assert.Empty(t, deferred)
	assert.Len(t, resolve, 2)

	// And asking one registry never registers into another.
	_, ok = v1.DefaultRegistry().Lookup(name)
	assert.False(t, ok)

	// nil is the build's: a built-in answers the same through both spellings.
	builtin, ok := v1.LookupTaskFrom(nil, "log")
	require.True(t, ok)
	want, _ := v1.LookupTask("log")
	assert.Equal(t, want.Name, builtin.Name)
}
