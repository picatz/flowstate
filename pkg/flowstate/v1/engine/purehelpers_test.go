package engine_test

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/proto"
)

// TestPureHelperPrototype is the durable half of the shared normalization case.
// The engine receives only ordinary CEL: no helper registry or runtime evaluator
// is available here, so success proves the imported declaration was compiled out.
func TestPureHelperPrototype(t *testing.T) {
	wf, helpers, expected := conformance.PureHelperPrototype()
	require.NoError(t, v1.ExpandPureHelpers(wf, helpers))
	require.NoError(t, v1.ResolveTaskCapabilities(wf, v1.DefaultRegistry()))

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	engine.Register(env)
	env.ExecuteWorkflow(engine.Run, &v1.RunState{Workflow: wf})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var output v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&output))
	require.True(t, proto.Equal(expected, &output))
}
