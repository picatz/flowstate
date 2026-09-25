package engine_test

import (
	"sync"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/proto"
)

// TestCapabilityCompositionPrototype is the durable half of the executable
// composition slice. The local half lives in capabilitybindings_test.go; both
// consume the same conformance fixture and therefore the same normalized graph.
func TestCapabilityCompositionPrototype(t *testing.T) {
	var (
		mu     sync.Mutex
		ledger []string
	)
	wf, defs, catalog, expected, err := conformance.CompositionPrototype(func(event string) {
		mu.Lock()
		defer mu.Unlock()
		ledger = append(ledger, event)
	})
	require.NoError(t, err)
	for _, def := range defs {
		require.NoError(t, v1.DefaultRegistry().Register(def))
		def := def
		t.Cleanup(func() { v1.DefaultRegistry().Unregister(def.Name) })
	}
	require.NoError(t, v1.ResolveCapabilityBindings(wf, map[string]string{
		"billing": "billing-environment-stable", "support": "support-environment-stable",
	}, catalog))
	require.NoError(t, v1.ResolvePlugins(wf, catalog))
	require.NoError(t, v1.ResolveTaskCapabilities(wf, v1.DefaultRegistry()))

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	engine.Register(env, engine.TaskRuntimeConfig{}.WithPluginCatalog(catalog))
	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var output v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&output))
	require.True(t, proto.Equal(expected, &output))
	mu.Lock()
	defer mu.Unlock()
	require.ElementsMatch(t, []string{
		"billing-environment:provision", "billing-environment:release",
		"support-environment:provision", "support-environment:release",
	}, ledger)
}
