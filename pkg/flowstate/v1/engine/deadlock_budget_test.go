package engine_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// sdkDefaultDeadlockBudget is the SDK's own default, written here because it is
// unexported there and this file is the one place that needs to say what is
// being raised above.
const sdkDefaultDeadlockBudget = time.Second

// stallsPastTheDefaultBudget occupies its workflow goroutine, without yielding,
// for longer than the SDK's default deadlock budget allows and less than the
// raised one does.
//
// The sleep is a real one on purpose: workflow.Sleep parks the coroutine, which
// is precisely what the deadlock detector is watching for the absence of. This
// stands in for the at-the-bound work the boundary tests do, without needing a
// bound-sized fixture or a machine under load to make the point.
func stallsPastTheDefaultBudget(workflow.Context) error {
	time.Sleep(sdkDefaultDeadlockBudget + 500*time.Millisecond)

	return nil
}

// TestABoundaryEnvironmentRaisesTheDeadlockBudget proves the option reaches the
// worker the test environment runs, rather than trusting that it does (#431).
//
// The seam has no getter: [testsuite.TestWorkflowEnvironment] takes worker
// options and never gives them back, so the only honest check is behavioral. A
// workflow goroutine that does not yield for a second and a half is failed by
// the SDK's default budget and tolerated by the raised one, so this passing
// means [atABound] was applied and passing means nothing else.
//
// If [atABound] ever stops reaching the environment, this fails with
// TMPRL1101 naming the detector, which is a far better diagnosis than the
// boundary tests going intermittently red on a loaded machine again.
func TestABoundaryEnvironmentRaisesTheDeadlockBudget(t *testing.T) {
	require.Greater(t, tests.BoundaryDeadlockDetectionTimeout, sdkDefaultDeadlockBudget,
		"the boundary budget has to be above the default it exists to raise")

	suite := &testsuite.WorkflowTestSuite{}
	env := atABound(suite.NewTestWorkflowEnvironment())
	env.RegisterWorkflow(stallsPastTheDefaultBudget)

	env.ExecuteWorkflow(stallsPastTheDefaultBudget)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"the raised deadlock budget did not reach the test environment's worker")
}
