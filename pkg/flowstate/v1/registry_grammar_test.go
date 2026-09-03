package flowstatev1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The grammar and uniqueness contract issue #1431 asks for.
//
// Five illegal names are refused, a duplicate is refused, and a plugin-namespaced
// name still registers — the three directions described in the issue.

var grammarNoop = func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
	return &v1.Node_Outputs{}, nil
}

func TestRegisterRefusesIllegalNames(t *testing.T) {
	t.Parallel()

	registry := v1.NewRegistry()

	for _, bad := range []struct {
		name   string
		reason string
	}{
		{"UPPER", "uppercase"},
		{"has-hyphen", "hyphen in bare name"},
		{"has space", "space"},
		{"123start", "digit-leading bare name"},
	} {
		t.Run(bad.reason, func(t *testing.T) {
			err := registry.Register(v1.TaskDef{Name: bad.name, Fn: grammarNoop})
			require.Error(t, err, "name %q should be refused", bad.name)
			assert.Contains(t, err.Error(), "grammar",
				"the error should mention the grammar")
		})
	}

	t.Run("empty", func(t *testing.T) {
		err := registry.Register(v1.TaskDef{Name: "", Fn: grammarNoop})
		require.Error(t, err, "an empty name should be refused")
		assert.Contains(t, err.Error(), "no name")
	})
}

func TestRegisterRefusesDuplicateName(t *testing.T) {
	t.Parallel()

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{Name: "unique_task", Fn: grammarNoop}))

	err := registry.Register(v1.TaskDef{Name: "unique_task", Fn: grammarNoop})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
	assert.Contains(t, err.Error(), "Replace")
}

func TestReplaceOverwritesExistingDefinition(t *testing.T) {
	t.Parallel()

	registry := v1.NewRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{
		Name: "overwrite_me", Summary: "original", Fn: grammarNoop,
	}))

	require.NoError(t, registry.Replace(v1.TaskDef{
		Name: "overwrite_me", Summary: "replaced", Fn: grammarNoop,
	}))

	def, ok := registry.Lookup("overwrite_me")
	require.True(t, ok)
	assert.Equal(t, "replaced", def.Summary)
}

func TestPluginNamespacedTaskRegisters(t *testing.T) {
	t.Parallel()

	registry := v1.NewRegistry()
	err := registry.Register(v1.TaskDef{Name: "acme.provision", Fn: grammarNoop})
	require.NoError(t, err, "a plugin-namespaced name must be accepted")

	_, ok := registry.Lookup("acme.provision")
	require.True(t, ok)
}

func TestPluginNameWithHyphenInPrefixRegisters(t *testing.T) {
	t.Parallel()

	registry := v1.NewRegistry()
	err := registry.Register(v1.TaskDef{Name: "my-plugin.fetch", Fn: grammarNoop})
	require.NoError(t, err, "a plugin prefix may contain hyphens")
}

func TestRegisterRefusesNilFunction(t *testing.T) {
	t.Parallel()

	registry := v1.NewRegistry()
	err := registry.Register(v1.TaskDef{Name: "no_fn"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no function")
}

func TestRegisterRefusesReservedStepKeys(t *testing.T) {
	t.Parallel()

	registry := v1.NewRegistry()
	for _, reserved := range v1.ReservedStepKeys() {
		err := registry.Register(v1.TaskDef{Name: reserved, Fn: grammarNoop})
		assert.Error(t, err, "reserved step key %q should be refused as a task name", reserved)
	}
}
