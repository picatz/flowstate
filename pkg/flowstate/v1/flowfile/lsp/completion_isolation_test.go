package lsp

import (
	"context"
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
