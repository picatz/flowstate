package flowtest_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

const registeredTasksWorkflow = `
edition: v2026.4
name: plugged
steps:
  - id: use
    registeredtasks.greet:
      name: ada
`

// runRequiringRegistered runs one suite with [flowtest.RunOptions.RequireRegisteredTasks]
// set, over a registry that holds registeredtasks.greet for the test's length.
func runRequiringRegistered(t *testing.T, stub string) *flowtest.RunResult {
	t.Helper()

	// Unique to this file, so the global registry other parallel tests read
	// never sees a task they could mistake for their own.
	registry := v1.DefaultRegistry()
	require.NoError(t, registry.Register(v1.TaskDef{
		Name: "registeredtasks.greet",
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return &v1.Node_Outputs{}, nil
		},
	}))
	t.Cleanup(func() { registry.Unregister("registeredtasks.greet") })

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), registeredTasksWorkflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: greets
    workflow: ./workflow.yaml
    stubs: [{task: `+stub+`, returns: {}}]
    expect:
      ran: [use]
`)
	run := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{RequireRegisteredTasks: true})

	return &run
}

// A misspelled task name is stubbable only because swapRegistry registers a
// placeholder for any stubbed name; with the registry told what exists, that
// must be a failure, with the nearest real name offered (#1294).
func TestAStubNamingAnUnregisteredTaskIsRefusedWhenRegisteredTasksAreRequired(t *testing.T) {
	run := runRequiringRegistered(t, "registeredtasks.gret")

	require.Len(t, run.Report.GetCases(), 1)
	c := run.Report.GetCases()[0]
	assert.False(t, c.GetPassed())
	assert.Contains(t, c.GetError(), `stub names task "registeredtasks.gret"`)
	assert.Contains(t, c.GetError(), `did you mean "registeredtasks.greet"?`)
}

// The other direction: a stub for a task the registry holds is unaffected.
func TestAStubNamingARegisteredTaskStillPassesWhenRegisteredTasksAreRequired(t *testing.T) {
	run := runRequiringRegistered(t, "registeredtasks.greet")

	require.Len(t, run.Report.GetCases(), 1)
	c := run.Report.GetCases()[0]
	assert.True(t, c.GetPassed(), "%s %v", c.GetError(), c.GetFailures())
}
