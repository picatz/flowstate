package lsp

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The completion table is built, and its menu asked for, while parallel tests
// register tasks into the process default for their own duration. A task that is
// registered when the table is built and gone when the menu is read made
// TestCompletion fail on an expectation that named it. The helpers it now uses
// answer for the built-ins alone, whatever else the default holds at the moment.
//
// Not parallel: it registers into the default, and runs while the parallel tests
// are paused.
func TestCompletionExpectationsIgnoreTasksOtherTestsRegister(t *testing.T) {
	const foreign = "test_completion_isolation_probe"
	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name: foreign,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return nil, nil
		},
	}))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(foreign) })

	require.Contains(t, v1.TaskNames(), foreign, "the default really holds the foreign task")
	assert.NotContains(t, builtinTaskNames(), foreign)
	assert.Contains(t, builtinTaskNames(), "http")
	assert.Equal(t, []string{"http", "log"}, withoutForeignTasks([]string{"http", foreign, "log"}))
	assert.Equal(t, []string{"not_a_task"}, withoutForeignTasks([]string{"not_a_task"}), "a label that is no task stays")
}

// The helpers are only worth anything if the menu really carries the foreign
// task while it is registered, so the strip in TestCompletion is load-bearing.
func TestCompletionMenuStripIsLoadBearing(t *testing.T) {
	const foreign = "test_completion_isolation_menu_probe"
	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name: foreign,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return nil, nil
		},
	}))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(foreign) })

	src, pos := splitCursor(t, "name: c\nsteps:\n  - |\nedition: v2026.4\n")
	c := newClient(t)
	c.initialize()
	const uri = "file:///completion-isolation-menu.yaml"
	c.open(uri, src)

	raw := labels(c.complete(uri, pos.Line, pos.Character).Items)
	require.Contains(t, raw, foreign, "the raw menu names the task another test registered")
	stripped := withoutForeignTasks(slices.Clone(raw))
	assert.NotContains(t, stripped, foreign)
	// The menu also offers the step keys, so the built-in tasks are a subset.
	for _, name := range builtinTaskNames() {
		assert.Contains(t, stripped, name)
	}
}

// A built-in missing from the default registry is missing from builtinTaskNames
// too, so the menu is also held to the tasks every flowfile leans on.
func TestCompletionOffersTheCoreBuiltinTasks(t *testing.T) {
	t.Parallel()

	src, pos := splitCursor(t, "name: c\nsteps:\n  - |\nedition: v2026.4\n")
	c := newClient(t)
	c.initialize()
	const uri = "file:///completion-core-builtins.yaml"
	c.open(uri, src)

	got := labels(c.complete(uri, pos.Line, pos.Character).Items)
	for _, name := range []string{"exec", "http", "log"} {
		assert.Contains(t, got, name)
	}
}
